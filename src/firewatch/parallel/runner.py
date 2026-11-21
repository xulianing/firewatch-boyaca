"""
firewatch.parallel.runner — Orquestador de L2 (secuencial y paralelo con multiprocessing).

Base: firewatch_l2_secuencial_v0.1 (5 nov 2026). La limpieza aplica la política congelada de CLAUDE.md §4
(ver firewatch.parallel.cleaning).

    Fase 0 (serial):   cleaning.fase0 → validación de archivos (V-C1…V-C6), corte SP/NRT (V-C5),
                       máscara de fuentes estáticas type 2 (V-C13) → CleaningContext
    Plan:              bloques de ~bloque_filas filas, cortados SIEMPRE en cambio de (satélite, fecha),
                       ordenados de mayor a menor (LPT)
    Fase 1:            por bloque, en la MISMA tarea: clean_chunk → spatial_join
                       p = 1: bucle secuencial (línea base) · p > 1: Pool spawn con initializer
                       (contexto + polígonos una vez por worker) e imap_unordered en orden LPT
    Reducción:         Σ CleaningMetrics → V-C7…V-C12, V-C14 + V-SJ1…V-SJ3
    Escritura:         solo si no hay fallos bloqueantes → silver/detecciones (Hive por anio) y, como
                       auditoría, silver/_reports/{mascara_fuentes_estaticas.parquet, cleaning_metrics.json}

Uso:
    python -m firewatch.parallel.runner                       # corrida completa (paralelo.procesos_produccion)
    python -m firewatch.parallel.runner --procesos 1          # línea base secuencial
    python -m firewatch.parallel.runner --solo-fase0          # valida, construye la máscara y muestra el plan
    python -m firewatch.parallel.runner --bloque-filas 20000  # otro tamaño de bloque (la salida no cambia)

API para run_pipeline y benchmarks: ejecutar_silver(n_procesos). Con cualquier n_procesos la salida es
idéntica (mismo content_sha256): solo cambia el ejecutor del map de la Fase 1.

Códigos de salida: 0 ok · 1 validación bloqueante fallida (no se publica nada) · 2 configuración/lectura.
Requiere pyarrow ≥ 10 (escritura particionada) y GeoPandas (cruce espacial).
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np
import pandas as pd

from firewatch.ingest.dane import CheckLog
from firewatch.parallel import cleaning as cl
from firewatch.utils.io import replace_atomic, write_atomic

log = logging.getLogger(__name__)

# Contrato silver.detecciones (Arquitectura §9.1), en orden
SILVER_COLUMNS = ("id_det", "lat", "lon", "fecha", "hora_utc", "anio", "mes", "daynight", "confianza", "frp",
                  "bright_ti4", "cod_dpto", "cod_mpio", "satelite", "proceso", "fuente_estatica")


class RunnerValidationError(Exception):
    """Falló al menos un check bloqueante: no se publicó ningún artefacto."""


# =============================================================================
# Plan de bloques
# =============================================================================

def plan_chunks(norm: pd.DataFrame, target_rows: int) -> list[np.ndarray]:
    """
    Agrupa filas por (satélite, fecha) y acumula grupos consecutivos hasta ~target_rows.
    Invariante: ningún (satélite, fecha) queda repartido entre dos bloques (deduplicación F6 exacta).
    Devuelve posiciones de fila por bloque, ordenadas de mayor a menor tamaño (LPT).
    """
    groups = norm.groupby(["satelite", "fecha"], sort=True, dropna=False).indices
    chunks: list[np.ndarray] = []
    cur: list[np.ndarray] = []
    n = 0
    for key in sorted(groups, key=lambda k: (str(k[0]), pd.Timestamp(k[1]))):
        idx = groups[key]
        cur.append(idx)
        n += len(idx)
        if n >= target_rows:
            chunks.append(np.sort(np.concatenate(cur)))
            cur, n = [], 0
    if cur:
        chunks.append(np.sort(np.concatenate(cur)))
    chunks.sort(key=len, reverse=True)
    return chunks


# =============================================================================
# Escritura
# =============================================================================

def write_detecciones(df: pd.DataFrame, root: Path) -> None:
    """Escribe silver/detecciones particionado por anio (Hive) con reemplazo atómico del directorio."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        ("id_det", pa.string()), ("lat", pa.float64()), ("lon", pa.float64()), ("fecha", pa.date32()),
        ("hora_utc", pa.int32()), ("anio", pa.int16()), ("mes", pa.int16()), ("daynight", pa.string()),
        ("confianza", pa.string()), ("frp", pa.float64()), ("bright_ti4", pa.float64()),
        ("cod_dpto", pa.string()), ("cod_mpio", pa.string()), ("satelite", pa.string()),
        ("proceso", pa.string()), ("fuente_estatica", pa.bool_()),
    ])
    d = df[list(SILVER_COLUMNS)].copy()
    d["fecha"] = d["fecha"].dt.date
    table = pa.Table.from_pandas(d, schema=schema, preserve_index=False)

    tmp, old = root.with_name(root.name + ".tmp"), root.with_name(root.name + ".old")
    for p in (tmp, old):
        if p.exists():
            shutil.rmtree(p)
    pq.write_to_dataset(table, root_path=str(tmp), partition_cols=["anio"], basename_template="part-{i}.parquet")
    if root.exists():
        root.rename(old)
    tmp.rename(root)
    if old.exists():
        shutil.rmtree(old)


def _write_parquet(df: pd.DataFrame, dest: Path) -> None:
    tmp = dest.with_suffix(".parquet.part")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(tmp, index=False)
    replace_atomic(tmp, dest)


def _write_report(path: Path, report: dict[str, Any]) -> None:
    write_atomic(path, json.dumps(report, ensure_ascii=False, indent=2, default=str).encode("utf-8"))

# =============================================================================
# Fase 1 en paralelo: estado por worker (multiprocessing, método spawn)
# =============================================================================
# Cada proceso hijo tiene su propia copia de estas variables. Las llena _iniciar_worker
# una sola vez al crear el Pool, para no serializar el contexto ni los polígonos en cada tarea.
_W_CTX: cl.CleaningContext | None = None
_W_POLYS: Any = None
_W_JOINER: Callable[[pd.DataFrame, Any], pd.DataFrame] | None = None
_W_T_ARRANQUE: float = 0.0          # segundos que tardó _iniciar_worker en este proceso (perfil)
_W_T_SPAWN: float = 0.0             # segundos desde que el padre creó el Pool hasta que este hijo arrancó


def _iniciar_worker(ctx: cl.CleaningContext, geo_path: Path,
                    polygons_loader: Callable[[Path], Any],
                    joiner: Callable[[pd.DataFrame, Any], pd.DataFrame], t_pool: float = 0.0) -> None:
    """Initializer del Pool: se ejecuta UNA vez en cada proceso hijo. Mide su propia duración (perfil).

    t_pool es el reloj de pared (time.time) del padre al crear el Pool: la diferencia con el momento en que
    arranca este hijo es la latencia de spawn (crear el intérprete y reimportar los módulos).
    """
    global _W_CTX, _W_POLYS, _W_JOINER, _W_T_ARRANQUE, _W_T_SPAWN
    _W_T_SPAWN = max(0.0, time.time() - t_pool) if t_pool else 0.0
    t0 = time.perf_counter()
    _W_CTX = ctx
    _W_POLYS = polygons_loader(geo_path)
    _W_JOINER = joiner
    _W_T_ARRANQUE = time.perf_counter() - t0


def _procesar_bloque(df: pd.DataFrame) -> tuple[pd.DataFrame, cl.CleaningMetrics, int, dict[str, Any]]:
    """Tarea del Pool: limpieza + cruce espacial de UN bloque (mismo código que la versión secuencial).

    Devuelve (bloque unido, métricas del bloque, filas limpias que entraron al cruce, medición de la tarea).
    La medición se toma DENTRO del proceso hijo: separa el cómputo real del costo de comunicación.
    """
    t0 = time.perf_counter()
    clean, m = cl.clean_chunk(df, _W_CTX)
    unido = _W_JOINER(clean, _W_POLYS)
    med = {"pid": os.getpid(), "t_tarea": time.perf_counter() - t0, "t_arranque": _W_T_ARRANQUE,
           "t_spawn": _W_T_SPAWN, "filas": len(df)}
    return unido, m, len(clean), med


def resumir_perfil(mediciones: list[dict[str, Any]], t_pared: float, n_procesos: int) -> dict[str, Any]:
    """
    Perfil de la Fase 1 a partir de las mediciones por tarea (tomadas dentro de cada proceso).
      suma_tareas_s     Σ tiempo de cómputo de todas las tareas. Si con p procesos supera mucho al de p = 1,
                        cada proceso calcula más lento (competencia por frecuencia, caché o memoria).
      spawn_max_s       Latencia de spawn más alta: del Pool() del padre al inicio del hijo (intérprete + imports).
      arranque_max_s    El inicializador más lento (carga de contexto y polígonos, después del spawn).
      desbalance        Carga del proceso más ocupado ÷ carga media (1 = reparto perfecto).
      ocupacion         Σ tareas ÷ (p × tiempo de pared): fracción del tiempo en que los procesos calcularon.
    """
    if not mediciones:
        return {}
    por_pid: dict[int, float] = {}
    arranques: dict[int, float] = {}
    spawns: dict[int, float] = {}
    for med in mediciones:
        por_pid[med["pid"]] = por_pid.get(med["pid"], 0.0) + med["t_tarea"]
        arranques[med["pid"]] = med["t_arranque"]
        spawns[med["pid"]] = med.get("t_spawn", 0.0)
    cargas = list(por_pid.values())
    suma = sum(cargas)
    tareas = [med["t_tarea"] for med in mediciones]
    return {
        "n_tareas": len(mediciones),
        "procesos_con_trabajo": len(por_pid),
        "suma_tareas_s": round(suma, 3),
        "tarea_min_s": round(min(tareas), 3),
        "tarea_max_s": round(max(tareas), 3),
        "spawn_max_s": round(max(spawns.values()), 3),
        "arranque_max_s": round(max(arranques.values()), 3),
        "desbalance": round(max(cargas) / (suma / len(cargas)), 3),
        "ocupacion": round(suma / (n_procesos * t_pared), 3) if t_pared > 0 else None,
    }



# =============================================================================
# Orquestación
# =============================================================================

def ejecutar_fase1(norm: pd.DataFrame, chunks: list[np.ndarray], ctx: cl.CleaningContext, geo_path: Path,
                   n_procesos: int, polygons_loader: Callable[[Path], Any],
                   joiner: Callable[[pd.DataFrame, Any], pd.DataFrame],
                   polys: Any = None) -> tuple[list[pd.DataFrame], cl.CleaningMetrics, int, dict[str, Any]]:
    """
    Fase 1 (limpieza + cruce espacial) sobre los bloques dados. ÚNICA implementación: la usan run() y el
    benchmark de escalado débil, así que el benchmark mide exactamente el código de producción.

    p = 1 → bucle secuencial (línea base). p > 1 → Pool spawn: initializer carga contexto y polígonos una
    vez por worker; imap_unordered reparte los bloques (en el orden recibido, LPT) a medida que se liberan.
    Devuelve (bloques unidos en orden de llegada, Σ métricas, filas limpias que entraron al cruce,
    perfil de la fase: ver resumir_perfil).
    """
    parts: list[pd.DataFrame] = []
    metrics = cl.CleaningMetrics()
    n_join_in = 0
    mediciones: list[dict[str, Any]] = []
    t_inicio = time.perf_counter()
    if n_procesos == 1:
        t_arr = 0.0
        if polys is None:
            t0 = time.perf_counter()
            polys = polygons_loader(geo_path)
            t_arr = time.perf_counter() - t0
        for k, idx in enumerate(chunks, start=1):
            t0 = time.perf_counter()
            clean, m = cl.clean_chunk(norm.iloc[idx], ctx)
            n_join_in += len(clean)
            parts.append(joiner(clean, polys))
            mediciones.append({"pid": os.getpid(), "t_tarea": time.perf_counter() - t0, "t_arranque": t_arr,
                               "filas": len(idx)})
            metrics = metrics + m
            if k % 10 == 0 or k == len(chunks):
                log.info("Bloques %d/%d · filas limpias acumuladas %d", k, len(chunks), n_join_in)
        return parts, metrics, n_join_in, resumir_perfil(mediciones, time.perf_counter() - t_inicio, 1)

    bloques = (norm.iloc[idx] for idx in chunks)
    ctx_mp = mp.get_context("spawn")          # explícito: igual en Windows y en Linux (tests, CI)
    with ctx_mp.Pool(processes=n_procesos, initializer=_iniciar_worker,
                     initargs=(ctx, geo_path, polygons_loader, joiner, time.time())) as pool:
        for k, (unido, m, n, med) in enumerate(pool.imap_unordered(_procesar_bloque, bloques, chunksize=1),
                                               start=1):
            parts.append(unido)
            metrics = metrics + m
            n_join_in += n
            mediciones.append(med)
            if k % 10 == 0 or k == len(chunks):
                log.info("Bloques %d/%d (p=%d) · filas limpias acumuladas %d", k, len(chunks), n_procesos, n_join_in)
    return parts, metrics, n_join_in, resumir_perfil(mediciones, time.perf_counter() - t_inicio, n_procesos)


def run(cfg: cl.FirmsConfig, *, solo_fase0: bool = False, bloque_filas: int | None = None,
        polygons_loader: Callable[[Path], Any] | None = None,
        joiner: Callable[[pd.DataFrame, Any], pd.DataFrame] | None = None,
        n_procesos: int = 1) -> dict[str, Any]:
    """
    Ejecuta L2 con n_procesos (1 = línea base secuencial; >1 = Pool spawn en la Fase 1).
    `polygons_loader` y `joiner` permiten inyectar dobles en tests (en paralelo deben ser funciones
    a nivel de módulo, porque se envían a los workers por nombre);
    por defecto son spatial_join.load_polygons y spatial_join.spatial_join.
    Lanza RunnerValidationError (tras escribir el informe) si falla un check bloqueante.
    """
    if n_procesos < 1:
        raise ValueError(f"n_procesos debe ser ≥ 1 (recibido {n_procesos})")

    lg = CheckLog()
    t: dict[str, float] = {}
    report: dict[str, Any] = {"etapa": "cleaning+spatial_join",
                              "modo": "secuencial" if n_procesos == 1 else "paralelo", "n_procesos": n_procesos,
                              "generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}

    t0 = time.perf_counter()
    norm, ctx, mask_table, info = cl.fase0(cfg, lg)
    t["fase0_s"] = time.perf_counter() - t0
    report.update(info)

    def fail() -> None:
        report.update({"tiempos": t, **lg.to_dict()})
        _write_report(cfg.out_report, report)
        raise RunnerValidationError("; ".join(f"{c.id}: {c.message}" for c in lg.failures))

    if ctx is None:
        fail()

    t0 = time.perf_counter()
    chunks = plan_chunks(norm, bloque_filas or cfg.bloque_filas)
    t["plan_s"] = time.perf_counter() - t0
    sizes = [len(c) for c in chunks]
    report["plan"] = {"bloque_filas": bloque_filas or cfg.bloque_filas, "n_bloques": len(chunks),
                      "filas_min": min(sizes, default=0), "filas_max": max(sizes, default=0),
                      "filas_media": float(np.mean(sizes)) if sizes else 0.0}
    log.info("Plan: %d bloques (filas: min %d · max %d)", len(chunks), report["plan"]["filas_min"],
             report["plan"]["filas_max"])

    if solo_fase0:
        report.update({"tiempos": t, **lg.to_dict(), "status": "fase0_ok", "salidas": None})
        _write_report(cfg.out_report.with_name("cleaning_fase0.json"), report)
        return report

    if polygons_loader is None or joiner is None:
        from firewatch.parallel import spatial_join as sj
        polygons_loader = polygons_loader or sj.load_polygons
        joiner = joiner or sj.spatial_join
    polys = None
    t0 = time.perf_counter()
    if n_procesos == 1:
        polys = polygons_loader(cfg.geo_path)   # en paralelo los carga cada worker (_iniciar_worker)
    t["carga_poligonos_s"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    parts, metrics, n_join_in, perfil = ejecutar_fase1(norm, chunks, ctx, cfg.geo_path, n_procesos,
                                                       polygons_loader, joiner, polys=polys)
    t["fase1_s"] = time.perf_counter() - t0
    report["perfil_fase1"] = perfil

    t0 = time.perf_counter()
    df = (pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=list(SILVER_COLUMNS) + ["_archivo"]))
    df = df.sort_values("id_det", kind="mergesort").reset_index(drop=True)
    content_sha = cl.validate_output(df, metrics, cfg, lg)
    from firewatch.parallel.spatial_join import validate_join
    validate_join(df, n_join_in, cfg.sin_municipio_max, lg)
    t["reduccion_validacion_s"] = time.perf_counter() - t0
    report["metricas"] = metrics.to_dict()
    report["content_sha256"] = content_sha
    report["resumen"] = {
        "n_salida": int(len(df)),
        "n_fuente_estatica": int(df["fuente_estatica"].sum()),
        "n_vegetacion": int((~df["fuente_estatica"]).sum()),
        "n_boyaca_vegetacion": int(((df["cod_dpto"] == "15") & ~df["fuente_estatica"]).sum()),
        "n_sin_municipio": int(df["cod_mpio"].isna().sum()),
    }
    if lg.failures:
        fail()

    t0 = time.perf_counter()
    write_detecciones(df, cfg.out_detecciones)
    _write_parquet(mask_table, cfg.out_mascara)
    t["escritura_s"] = time.perf_counter() - t0
    t["total_s"] = sum(t.values())
    serial = t["fase0_s"] + t["plan_s"] + t.get("carga_poligonos_s", 0) + t["reduccion_validacion_s"] + t["escritura_s"]
    t["fraccion_serial"] = serial / t["total_s"] if t["total_s"] else 0.0
    report.update({"tiempos": t, **lg.to_dict(),
                   "salidas": {"detecciones": str(cfg.out_detecciones), "mascara": str(cfg.out_mascara)}})
    _write_report(cfg.out_report, report)
    return report


def _load_settings(settings_path: Path) -> tuple[dict[str, Any], Path]:
    import yaml
    settings_path = Path(settings_path).resolve()
    return yaml.safe_load(settings_path.read_text(encoding="utf-8")), settings_path.parent.parent


def ejecutar_silver(n_procesos: int = 1, settings_path: str | Path = "config/settings.yaml",
                    **kwargs: Any) -> dict[str, Any]:
    """
    Punto de entrada de L2 para run_pipeline y los benchmarks: mismo código de producción, solo cambia
    n_procesos. Devuelve el informe (tiempos por fase, conteos, hash de contenido).
    """
    settings, base_dir = _load_settings(Path(settings_path))
    return run(cl.load_config(settings, base_dir=base_dir), n_procesos=n_procesos, **kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="firewatch.parallel.runner", description="L2 secuencial: limpieza + cruce")
    ap.add_argument("--settings", default="config/settings.yaml")
    ap.add_argument("--solo-fase0", action="store_true", help="valida archivos, construye la máscara y muestra el plan")
    ap.add_argument("--bloque-filas", type=int, default=None)
    ap.add_argument("--procesos", type=int, default=None,
                    help="procesos de la Fase 1 (por defecto: paralelo.procesos_produccion de settings.yaml)")
    args = ap.parse_args(argv)

    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)
    try:
        settings, base_dir = _load_settings(Path(args.settings))
        cfg = cl.load_config(settings, base_dir=base_dir)
        log_dir = cfg.silver_dir / "_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_dir / f"runner_{datetime.now():%Y%m%dT%H%M%S}.log", encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
        n = args.procesos or int(settings["paralelo"]["procesos_produccion"])
        report = run(cfg, solo_fase0=args.solo_fase0, bloque_filas=args.bloque_filas, n_procesos=n)
    except (OSError, cl.FirmsError) as exc:
        log.error("%s", exc)
        return 2
    except RunnerValidationError as exc:
        log.error("Validación bloqueante fallida; no se publicó nada. Ver %s\n%s", "cleaning_metrics.json", exc)
        return 1

    resumen = {k: report.get(k) for k in ("status", "plan", "resumen", "tiempos", "content_sha256", "salidas")}
    resumen["avisos"] = [f"{c['id']}: {c['message']}" for c in report["checks"] if c["status"] == "warn"]
    log.info("Resumen L2:\n%s", json.dumps(resumen, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())

