"""
firewatch.parallel.runner — Orquestador de L2 (versión SECUENCIAL).

Base: firewatch_l2_secuencial_v0.1 (5 nov 2026). La limpieza aplica la política congelada de CLAUDE.md §4
(ver firewatch.parallel.cleaning).

    Fase 0 (serial):   cleaning.fase0 → validación de archivos (V-C1…V-C6), corte SP/NRT (V-C5),
                       máscara de fuentes estáticas type 2 (V-C13) → CleaningContext
    Plan:              bloques de ~bloque_filas filas, cortados SIEMPRE en cambio de (satélite, fecha),
                       ordenados de mayor a menor (LPT)
    Fase 1:            por bloque, en la MISMA tarea: clean_chunk → spatial_join
                       (hoy un bucle; la versión con multiprocessing solo reemplazará el ejecutor del map)
    Reducción:         Σ CleaningMetrics → V-C7…V-C12, V-C14 + V-SJ1…V-SJ3
    Escritura:         solo si no hay fallos bloqueantes → silver/detecciones (Hive por anio) y, como
                       auditoría, silver/_reports/{mascara_fuentes_estaticas.parquet, cleaning_metrics.json}

Uso:
    python -m firewatch.parallel.runner                       # corrida completa
    python -m firewatch.parallel.runner --solo-fase0          # valida, construye la máscara y muestra el plan
    python -m firewatch.parallel.runner --bloque-filas 20000  # otro tamaño de bloque (la salida no cambia)

API para run_pipeline y benchmarks: ejecutar_silver(n_procesos). Hoy solo admite n_procesos = 1
(línea base); la versión con multiprocessing reemplazará únicamente el ejecutor del map de la Fase 1.

Códigos de salida: 0 ok · 1 validación bloqueante fallida (no se publica nada) · 2 configuración/lectura.
Requiere pyarrow ≥ 10 (escritura particionada) y GeoPandas (cruce espacial).
"""

from __future__ import annotations

import argparse
import json
import logging
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
# Orquestación
# =============================================================================

def run(cfg: cl.FirmsConfig, *, solo_fase0: bool = False, bloque_filas: int | None = None,
        polygons_loader: Callable[[Path], Any] | None = None,
        joiner: Callable[[pd.DataFrame, Any], pd.DataFrame] | None = None) -> dict[str, Any]:
    """
    Ejecuta L2 en modo secuencial. `polygons_loader` y `joiner` permiten inyectar dobles en tests;
    por defecto son spatial_join.load_polygons y spatial_join.spatial_join.
    Lanza RunnerValidationError (tras escribir el informe) si falla un check bloqueante.
    """
    lg = CheckLog()
    t: dict[str, float] = {}
    report: dict[str, Any] = {"etapa": "cleaning+spatial_join", "modo": "secuencial",
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
    t0 = time.perf_counter()
    polys = polygons_loader(cfg.geo_path)
    t["carga_poligonos_s"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    parts: list[pd.DataFrame] = []
    metrics = cl.CleaningMetrics()
    n_join_in = 0
    for k, idx in enumerate(chunks, start=1):
        clean, m = cl.clean_chunk(norm.iloc[idx], ctx)
        n_join_in += len(clean)
        parts.append(joiner(clean, polys))
        metrics = metrics + m
        if k % 10 == 0 or k == len(chunks):
            log.info("Bloques %d/%d · filas limpias acumuladas %d", k, len(chunks), n_join_in)
    t["fase1_s"] = time.perf_counter() - t0

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
    if n_procesos != 1:
        raise NotImplementedError("L2 paralelo (multiprocessing) aún no está integrado; usar n_procesos=1")
    settings, base_dir = _load_settings(Path(settings_path))
    return run(cl.load_config(settings, base_dir=base_dir), **kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="firewatch.parallel.runner", description="L2 secuencial: limpieza + cruce")
    ap.add_argument("--settings", default="config/settings.yaml")
    ap.add_argument("--solo-fase0", action="store_true", help="valida archivos, construye la máscara y muestra el plan")
    ap.add_argument("--bloque-filas", type=int, default=None)
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
        report = run(cfg, solo_fase0=args.solo_fase0, bloque_filas=args.bloque_filas)
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
