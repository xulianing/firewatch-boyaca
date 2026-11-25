"""Punto de entrada L2 + L3: pipeline reproducible de punta a punta.

Uso (desde la raíz del repo, con el venv activo):
    python scripts/run_pipeline.py --stage silver   # L2: runner.ejecutar_silver(p de producción)
    python scripts/run_pipeline.py --stage gold     # L3: session → build_daily → climatology
                                                    #     → irat → export_gold
    python scripts/run_pipeline.py --stage all      # silver y luego gold
    python scripts/run_pipeline.py --stage all --resumen-json docs/notas/run_pipeline_resumen.json

Diseño:
  - Orquesta, no reimplementa: llama a los puntos de entrada reales de cada módulo
    (runner.ejecutar_silver y <modulo>.ejecutar(spark, cfg) de L3), que ya validan y solo publican
    si pasan sus checks bloqueantes.
  - Falla rápido: si una etapa falla, no corre las siguientes (gold depende de silver y cada
    etapa de L3 depende de la anterior) y el proceso termina con código 1.
  - PySpark se importa solo dentro de la etapa gold: `--stage silver` funciona en una máquina sin Java.
  - Conciliación K1: detecciones de vegetación de Boyacá en silver (informe de L2) frente a K1 del gold
    (Σ fact_municipio_mes.n_det, informe de export_gold). Si no coinciden, código 1.
  - Al final imprime un resumen: conteos por capa, tiempos por etapa y conciliación.

Códigos de salida: 0 todo OK · 1 etapa fallida o conciliación distinta · 2 error de uso/configuración.
Dueño: B (orquestación) + C (etapa gold) · Tarea: B10
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ / "src") not in sys.path:          # permite correr el script sin `pip install -e .`
    sys.path.insert(0, str(RAIZ / "src"))

ETAPAS_GOLD = ("build_daily", "climatology", "irat", "export_gold")


class EtapaFallida(RuntimeError):
    """Una etapa terminó sin publicar (check bloqueante, error de datos o excepción)."""


@dataclass
class Resultado:
    etapa: str
    ok: bool
    tiempo_s: float
    conteos: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


# --------------------------------------------------------------------------- #
# Etapas (cada una devuelve un dict de conteos o lanza EtapaFallida)
# --------------------------------------------------------------------------- #

def etapa_silver(cfg: dict, settings_path: Path, n_procesos: int) -> dict[str, Any]:
    """L2: limpieza + cruce espacial con multiprocessing (código de producción)."""
    from firewatch.parallel.runner import ejecutar_silver
    rep = ejecutar_silver(n_procesos, settings_path=settings_path)
    if rep.get("status") != "completo":
        fallas = [f"{c['id']}: {c['message']}" for c in rep.get("checks", []) if c.get("status") == "fail"]
        raise EtapaFallida(f"silver no publicado (status={rep.get('status')}): {fallas[:3]}")
    res = rep.get("resumen", {})
    return {"n_raw": rep.get("metricas", {}).get("n_raw"), "n_silver": res.get("n_salida"),
            "n_vegetacion": res.get("n_vegetacion"), "k1_silver": res.get("n_boyaca_vegetacion"),
            "procesos": n_procesos, "content_sha256": rep.get("content_sha256")}


def etapas_gold(cfg: dict) -> list[tuple[str, Callable[[Any, dict], dict]]]:
    """Los cuatro pasos de L3, en orden, con su función `ejecutar` real."""
    from firewatch.spark import build_daily, climatology, export_gold, irat
    mods = {"build_daily": build_daily, "climatology": climatology, "irat": irat, "export_gold": export_gold}
    return [(n, mods[n].ejecutar) for n in ETAPAS_GOLD]


def conteos_gold(nombre: str, informe: dict) -> dict[str, Any]:
    """Extrae del informe de cada módulo lo que va al resumen."""
    if nombre == "export_gold":
        k = informe.get("kpis_control", {})
        return {"filas": informe.get("filas"), "k1_gold": k.get("K1_detecciones"),
                "k6_denominador": k.get("denominador_K6_nacional")}
    return dict(informe.get("conteos") or {})


# --------------------------------------------------------------------------- #
# Orquestación
# --------------------------------------------------------------------------- #

def _medir(etapa: str, fn: Callable[[], dict]) -> Resultado:
    t0 = time.perf_counter()
    try:
        conteos = fn()
        return Resultado(etapa, True, time.perf_counter() - t0, conteos)
    except SystemExit as e:                               # algunos módulos usan SystemExit(1)
        return Resultado(etapa, False, time.perf_counter() - t0, error=f"SystemExit({e.code})")
    except Exception as e:                                # noqa: BLE001 — se reporta y se sale con 1
        return Resultado(etapa, False, time.perf_counter() - t0, error=f"{type(e).__name__}: {e}")


def correr(stage: str, cfg: dict, settings_path: Path, n_procesos: int,
           silver_fn: Callable[..., dict] = etapa_silver,
           gold_fn: Callable[[dict], list] = etapas_gold,
           sesion_fn: Callable[[dict], Any] | None = None) -> list[Resultado]:
    """Corre las etapas pedidas; se detiene en la primera que falla."""
    resultados: list[Resultado] = []
    if stage in ("silver", "all"):
        resultados.append(_medir("silver", lambda: silver_fn(cfg, settings_path, n_procesos)))
        if not resultados[-1].ok:
            return resultados
    if stage in ("gold", "all"):
        if sesion_fn is None:
            from firewatch.spark.session import crear_sesion
            sesion_fn = crear_sesion
        t0 = time.perf_counter()
        try:
            spark = sesion_fn(cfg)
        except Exception as e:                            # noqa: BLE001 — Java/winutils ausente, etc.
            resultados.append(Resultado("session", False, time.perf_counter() - t0,
                                        error=f"{type(e).__name__}: {e}"))
            return resultados
        master = getattr(getattr(spark, "sparkContext", None), "master", None)
        resultados.append(Resultado("session", True, time.perf_counter() - t0, {"master": master}))
        try:
            for nombre, ejecutar in gold_fn(cfg):
                r = _medir(nombre, lambda ej=ejecutar, n=nombre: conteos_gold(n, ej(spark, cfg)))
                resultados.append(r)
                if not r.ok:
                    break
        finally:
            stop = getattr(spark, "stop", None)
            if callable(stop):
                stop()
    return resultados


def k1_silver(resultados: list[Resultado], cfg: dict) -> int | None:
    """K1 de silver: de esta corrida o, si no se corrió silver, del informe publicado de L2."""
    for r in resultados:
        if r.etapa == "silver" and r.ok:
            return r.conteos.get("k1_silver")
    ruta = RAIZ / cfg["rutas"]["silver"] / "_reports" / "cleaning_metrics.json"
    if ruta.exists():
        return json.loads(ruta.read_text(encoding="utf-8")).get("resumen", {}).get("n_boyaca_vegetacion")
    return None


def conciliar(resultados: list[Resultado], cfg: dict) -> dict[str, Any]:
    """K1 silver = K1 gold. Solo se evalúa si en esta corrida se publicó el gold."""
    k1_g = next((r.conteos.get("k1_gold") for r in resultados if r.etapa == "export_gold" and r.ok), None)
    k1_s = k1_silver(resultados, cfg)
    if k1_g is None:
        return {"evaluada": False, "k1_silver": k1_s, "k1_gold": None, "ok": True}
    return {"evaluada": True, "k1_silver": k1_s, "k1_gold": k1_g, "ok": k1_s is not None and k1_s == k1_g}


def imprimir_resumen(resultados: list[Resultado], conc: dict[str, Any]) -> None:
    print("\n" + "=" * 72 + "\nRESUMEN run_pipeline\n" + "=" * 72)
    print(f"{'Etapa':<14}{'Estado':<10}{'Tiempo (s)':>12}  Conteos")
    for r in resultados:
        estado = "OK" if r.ok else "FALLA"
        detalle = r.error if r.error else ", ".join(f"{k}={v}" for k, v in r.conteos.items()
                                                    if k != "content_sha256")
        print(f"{r.etapa:<14}{estado:<10}{r.tiempo_s:>12.1f}  {detalle}")
    total = sum(r.tiempo_s for r in resultados)
    print(f"{'TOTAL':<24}{total:>12.1f}")
    if conc["evaluada"]:
        marca = "OK" if conc["ok"] else "DISTINTA"
        print(f"\nConciliación K1 (vegetación Boyacá): silver={conc['k1_silver']} · gold={conc['k1_gold']} → {marca}")
    else:
        print(f"\nConciliación K1: no evaluada (no se publicó gold en esta corrida) · silver={conc['k1_silver']}")


def main(argv: list[str] | None = None, **inyectar: Any) -> int:
    ap = argparse.ArgumentParser(description="FireWatch · pipeline L2 + L3")
    ap.add_argument("--stage", choices=["silver", "gold", "all"], required=True)
    ap.add_argument("--config", default="config/settings.yaml")
    ap.add_argument("--procesos", type=int, default=None,
                    help="procesos de L2 (por defecto: paralelo.procesos_produccion)")
    ap.add_argument("--resumen-json", default=None, help="guarda el resumen en este archivo JSON")
    a = ap.parse_args(argv)

    settings_path = (RAIZ / a.config) if not Path(a.config).is_absolute() else Path(a.config)
    try:
        cfg = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
        n = a.procesos if a.procesos is not None else int(cfg["paralelo"]["procesos_produccion"])
    except (OSError, KeyError, TypeError, ValueError) as e:
        print(f"ERROR de configuración: {e}", file=sys.stderr)
        return 2
    if n < 1:
        print("ERROR: --procesos debe ser >= 1", file=sys.stderr)
        return 2

    resultados = correr(a.stage, cfg, settings_path, n, **inyectar)
    conc = conciliar(resultados, cfg)
    imprimir_resumen(resultados, conc)
    if a.resumen_json:
        Path(a.resumen_json).write_text(json.dumps(
            {"stage": a.stage, "etapas": [r.__dict__ for r in resultados], "conciliacion": conc},
            ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0 if all(r.ok for r in resultados) and conc["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
