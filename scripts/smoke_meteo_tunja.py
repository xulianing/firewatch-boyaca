"""
Prueba de humo de extremo a extremo de firewatch.ingest.meteo (v0.1) con Tunja (15001).

    centroides_boyaca.csv → load_config → load_locations → build_tasks
        → make_session → process_task (fetch → validate_payload → write_atomic)
        → data/raw/meteo/15001.json + informe de validación + verificación de reproducibilidad

Ejecutar desde la raíz del repositorio:
    python scripts/smoke_meteo_tunja.py                 # tramo base (2022-01-01 → 2026-10-25), peso ≈ 63
    python scripts/smoke_meteo_tunja.py --allow-partial # si el CSV aún no tiene los 123 municipios

Reproducibilidad: si data/raw/meteo/15001.json ya existe, se calcula su content_sha256 ANTES de
descargar y se compara con el de la nueva descarga. Correr el script dos veces debe dar
"REPRODUCIBLE: sí".

Códigos de salida: 0 OK · 1 INVALID/FAILED · 2 error de configuración o de la API (HTTP 400).
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import yaml

from firewatch.ingest.meteo import (
    MeteoError, TaskStatus, build_segments, build_tasks, check_segment_available,
    content_fingerprint, load_config, load_locations, make_session, process_task, target_path,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--settings", default=str(ROOT / "config" / "settings.yaml"))
    ap.add_argument("--cod", default="15001")
    ap.add_argument("--tramo", choices=["base", "cola"], default="base")
    ap.add_argument("--allow-partial", action="store_true",
                    help="no exigir los 123 municipios en el CSV de centroides")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")

    try:
        settings = yaml.safe_load(Path(args.settings).read_text(encoding="utf-8"))
        cfg = load_config(settings, base_dir=ROOT)
        segment = build_segments(cfg, cfg.cola_start)[args.tramo]
        check_segment_available(segment)
        locations = load_locations(cfg, require_complete=not args.allow_partial)
        tasks = build_tasks(locations, segment, only=[args.cod])
        if len(tasks) != 1:
            raise MeteoError(f"{args.cod} no está en el archivo de centroides")
        task = tasks[0]

        dest = target_path(task, cfg)
        previous = None
        if dest.exists():
            previous = content_fingerprint(json.loads(dest.read_bytes()))
            logging.info("Archivo previo encontrado: content_sha256=%s", previous[:16])

        with make_session(cfg) as session:
            result = process_task(session, task, cfg)        # sin limiter ni budget (v0.1)
    except MeteoError as exc:                               # incluye FatalRequestError
        logging.error("%s: %s", type(exc).__name__, exc)
        return 2

    rep = result.validation
    print("\n" + "=" * 72)
    print(f"Municipio      : {task.location.cod_mpio} {task.location.nombre} "
          f"({task.location.lat}, {task.location.lon})")
    print(f"Tramo          : {segment.name} {segment.start} → {segment.end} ({segment.n_hours} h esperadas)")
    print(f"Estado         : {result.status.value}   HTTP {result.http_status}   "
          f"{(result.elapsed_s or 0):.1f} s   peso≈{result.weight:.1f}")
    print(f"Archivo        : {result.path}")
    print(f"Bytes / sha256 : {result.bytes} / {result.sha256}")
    print(f"Huella params  : {result.fingerprint}")
    if rep:
        print("Checks         : " + "  ".join(f"{k}={'✔' if v else '✘'}" for k, v in rep.checks.items()))
        for key in ("n_hours", "time_range", "continuity", "nulls", "metadata", "out_of_range", "cell", "elevation"):
            if key in rep.details:
                print(f"  {key:<13}: {rep.details[key]}")
        for k, s in rep.details.get("stats", {}).items():
            print(f"  {k:<27} min={s['min']:<8} media={s['mean']:<8} max={s['max']}")
        for e in rep.errors:
            print(f"  ERROR {e}")
    if result.status == TaskStatus.OK:
        print(f"content_sha256 : {result.content_sha256}")
        if previous is None:
            print("REPRODUCIBLE   : (primera descarga; vuelve a ejecutar para comparar)")
        else:
            same = previous == result.content_sha256
            print(f"REPRODUCIBLE   : {'sí' if same else 'NO — el contenido cambió (ver nota ERA5T en el README)'}")
    if result.error:
        print(f"Error          : {result.error}")
    print("=" * 72)
    return 0 if result.status == TaskStatus.OK else 1


if __name__ == "__main__":
    raise SystemExit(main())
