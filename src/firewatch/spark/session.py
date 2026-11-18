"""Sesión Spark local de FireWatch Boyacá (L3).

El benchmark solo sobrescribe ``master``, ``particiones`` y ``aqe``; el resto sale
de ``config/settings.yaml`` (clave ``spark``).
"""
from __future__ import annotations

import logging
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

RUTA_CONFIG = Path("config/settings.yaml")


def cargar_config(ruta: Path | str = RUTA_CONFIG) -> dict:
    """Lee ``settings.yaml`` y devuelve el diccionario completo."""
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)


def crear_sesion(
    cfg: dict,
    master: str | None = None,
    particiones: int | None = None,
    aqe: bool = True,
) -> SparkSession:
    """Crea (o reutiliza) la SparkSession local.

    Args:
        cfg: configuración completa (``settings.yaml``).
        master: sobrescribe ``spark.master`` (p. ej. ``local[4]``).
        particiones: sobrescribe ``spark.shuffle_partitions``.
        aqe: Adaptive Query Execution. El benchmark de particiones lo apaga: con AQE
            activo Spark fusiona particiones y el experimento {4, 8, 16, 200} pierde sentido.
    """
    sp = cfg["spark"]
    master = master or sp["master_produccion"]
    particiones = particiones or sp["shuffle_partitions_produccion"]
    spark = (
        SparkSession.builder.appName(sp.get("app_name", "firewatch"))
        .master(master)
        .config("spark.driver.memory", str(sp["driver_memory"]))
        .config("spark.sql.shuffle.partitions", str(particiones))
        .config("spark.sql.adaptive.enabled", str(aqe).lower())
        # build_daily agrupa por `fecha` (precalculada en silver), nunca por ts_local.
        .config("spark.sql.session.timeZone", "America/Bogota")
        # pandas/pyarrow escriben datetime64[ns] como TIMESTAMP(NANOS), que Spark 3.5
        # no sabe leer como timestamp. Con esta opción lo lee como long (ns desde epoch).
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(
        "Spark %s · master=%s · particiones=%s · aqe=%s",
        spark.version, master, particiones, aqe,
    )
    return spark
