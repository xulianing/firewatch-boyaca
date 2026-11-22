"""L3 · Agregado municipio-mes, línea base climatológica, anomalías, LOYO y rezago.

Lee solo ``int.municipio_dia`` (de ``build_daily``) y ``silver.dim_municipio`` (área).
Si falla un check bloqueante no se publica nada (D-L8).

Salidas (Parquet, en ``rutas.intermedio``):

    municipio_mes   municipio × mes (7.134)
                    cod_mpio, fecha_mes, anio, mes, n_dias, n_det, frp_total, frp_medio,
                    det_100km2, precip_mes, tmax_media, rh_min_media, wind_max_media,
                    racha_seca_max, anom_precip, anom_tmax, anom_rh, dens_loyo,
                    det_3m_previos
    linea_base      municipio × mes calendario (1.476)
                    cod_mpio, mes, precip_lb, tmax_lb, rh_min_lb, dens_suma_lb, n_anios_lb

Decisiones aprobadas (21 nov):
    P-A  racha_seca_max se recorta al mes: rango [0, n_dias].
    P-B  det_3m_previos = media de det_100km2 de los ``spark.meses_previos_h`` meses
         previos; nula hasta tener esos meses completos (123 × 3 = 369 celdas).

Informe: ``rutas.intermedio/_reports/climatology_validation.json``.

Uso::

    python -m firewatch.spark.climatology
    python -m firewatch.spark.climatology --solo-validar
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from firewatch.spark.build_daily import (
    CheckError,
    Registro,
    _escribir_json,
    _escribir_tmp,
    _intercambiar,
    huella_contenido,
)
from firewatch.spark.session import cargar_config, crear_sesion

log = logging.getLogger(__name__)

COLS_MUNICIPIO_MES = [
    "cod_mpio", "fecha_mes", "anio", "mes", "n_dias", "n_det", "frp_total", "frp_medio",
    "det_100km2", "precip_mes", "tmax_media", "rh_min_media", "wind_max_media",
    "racha_seca_max", "anom_precip", "anom_tmax", "anom_rh", "dens_loyo", "det_3m_previos",
]
COLS_LINEA_BASE = [
    "cod_mpio", "mes", "precip_lb", "tmax_lb", "rh_min_lb", "dens_suma_lb", "n_anios_lb",
]
# Columnas que nunca pueden ser nulas en municipio_mes (frp_medio y det_3m_previos sí).
COLS_SIN_NULOS = [
    "n_dias", "n_det", "frp_total", "det_100km2", "precip_mes", "tmax_media",
    "rh_min_media", "wind_max_media", "racha_seca_max", "anom_precip", "anom_tmax",
    "anom_rh", "dens_loyo",
]
TOL_ANOM = 1e-6   # media de anomalías en la línea base (propiedad algebraica)
TOL_NEG = 1e-9    # dens_loyo ≥ 0 salvo error de redondeo


# --------------------------------------------------------------------------- #
# Parámetros (todo sale de settings.yaml, C26)
# --------------------------------------------------------------------------- #
def _anios_linea_base(cfg: dict) -> tuple[int, ...]:
    """Acepta lista [2022, ...], dict {inicio, fin} o texto "2022-2025".

    Se busca en ``ventana.anios_linea_base`` y, si no está, en ``irat.anios_linea_base``.
    """
    v = (cfg.get("ventana") or {}).get("anios_linea_base")
    if v is None:
        v = (cfg.get("irat") or {}).get("anios_linea_base")
    if v is None:
        raise KeyError("Falta ventana.anios_linea_base en settings.yaml (p. ej. [2022, 2023, 2024, 2025])")
    if isinstance(v, dict):
        a, b = int(v.get("inicio", v.get("desde"))), int(v.get("fin", v.get("hasta")))
        anios = range(a, b + 1)
    elif isinstance(v, str):
        a, b = (int(x) for x in v.replace("–", "-").split("-"))
        anios = range(a, b + 1)
    else:
        anios = [int(x) for x in v]
    anios = tuple(sorted(set(anios)))
    if len(anios) < 2:
        raise ValueError(f"LOYO necesita al menos 2 años de línea base; hay {anios}")
    return anios


@dataclass(frozen=True)
class ParamsClima:
    silver: Path
    inter: Path
    inicio: str
    fin: str
    anios_lb: tuple[int, ...]
    meses_previos: int
    decimales_huella: int

    CLAVES = (
        ("rutas", "silver"), ("rutas", "intermedio"),
        ("ventana", "fecha_inicio"), ("ventana", "fecha_fin"),
        ("spark", "meses_previos_h"),
    )

    @classmethod
    def desde_cfg(cls, cfg: dict) -> "ParamsClima":
        faltan = [".".join(k) for k in cls.CLAVES if k[1] not in (cfg.get(k[0]) or {})]
        if faltan:
            raise KeyError(f"Faltan claves en settings.yaml: {faltan}")
        sp = cfg["spark"]
        return cls(
            silver=Path(cfg["rutas"]["silver"]),
            inter=Path(cfg["rutas"]["intermedio"]),
            inicio=str(cfg["ventana"]["fecha_inicio"]),
            fin=str(cfg["ventana"]["fecha_fin"]),
            anios_lb=_anios_linea_base(cfg),
            meses_previos=int(sp["meses_previos_h"]),
            decimales_huella=int(sp.get("decimales_huella", 6)),
        )

    @property
    def n_dias(self) -> int:
        return (date.fromisoformat(self.fin) - date.fromisoformat(self.inicio)).days + 1

    @property
    def n_meses(self) -> int:
        a, b = date.fromisoformat(self.inicio), date.fromisoformat(self.fin)
        return (b.year - a.year) * 12 + (b.month - a.month) + 1


# --------------------------------------------------------------------------- #
# Lectura
# --------------------------------------------------------------------------- #
def leer_entradas(spark: SparkSession, p: ParamsClima) -> tuple[DataFrame, DataFrame]:
    """Devuelve (municipio_dia, dim_municipio[cod_mpio, area_km2])."""
    md = spark.read.parquet(str(p.inter / "municipio_dia"))
    dim = spark.read.parquet(str(p.silver / "dim_municipio")).select(
        F.col("cod_mpio").cast("string").alias("cod_mpio"),
        F.col("area_km2").cast("double").alias("area_km2"),
    )
    return md, dim


def leer_informe(ruta: Path) -> dict | None:
    if not ruta.exists():
        return None
    return json.loads(ruta.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# Transformaciones (funciones puras DataFrame → DataFrame)
# --------------------------------------------------------------------------- #
def racha_seca_mes(md: DataFrame) -> DataFrame:
    """Racha seca más larga de cada municipio-mes, recortada al mes (P-A).

    ``id_racha`` identifica una racha en toda la serie; al agrupar también por
    ``fecha_mes`` una racha que cruza el cambio de mes se parte en dos.
    """
    return (
        md.filter(F.col("dia_seco"))
        .withColumn("fecha_mes", F.trunc("fecha", "month"))
        .groupBy("cod_mpio", "fecha_mes", "id_racha")
        .agg(F.count(F.lit(1)).alias("largo"))
        .groupBy("cod_mpio", "fecha_mes")
        .agg(F.max("largo").cast("int").alias("racha_seca_max"))
    )


def agregar_mes(md: DataFrame, dim: DataFrame) -> DataFrame:
    """municipio_dia → municipio-mes con conteos, clima medio, densidad y racha."""
    mes = (
        md.withColumn("fecha_mes", F.trunc("fecha", "month"))
        .groupBy("cod_mpio", "fecha_mes")
        .agg(
            F.count(F.lit(1)).cast("int").alias("n_dias"),
            F.sum("n_det").cast("int").alias("n_det"),
            F.sum("frp_sum").alias("frp_total"),
            F.sum("precip").alias("precip_mes"),
            F.avg("tmax").alias("tmax_media"),
            F.avg("rh_min").alias("rh_min_media"),
            F.avg("wind_max").alias("wind_max_media"),
        )
    )
    return (
        mes.join(racha_seca_mes(md), ["cod_mpio", "fecha_mes"], "left")
        .fillna({"racha_seca_max": 0})
        .join(F.broadcast(dim), "cod_mpio", "inner")
        .withColumn("anio", F.year("fecha_mes").cast("int"))
        .withColumn("mes", F.month("fecha_mes").cast("int"))
        # Un promedio de cero detecciones no existe: nulo, no 0.
        .withColumn("frp_medio", F.when(F.col("n_det") > 0, F.col("frp_total") / F.col("n_det")))
        .withColumn("det_100km2", F.col("n_det") / F.col("area_km2") * F.lit(100.0))
        .drop("area_km2")
    )


def linea_base(mm: DataFrame, anios: tuple[int, ...]) -> DataFrame:
    """Media por municipio y mes calendario en los años de la línea base (sin desviación, C23)."""
    return (
        mm.filter(F.col("anio").isin(list(anios)))
        .groupBy("cod_mpio", "mes")
        .agg(
            F.avg("precip_mes").alias("precip_lb"),
            F.avg("tmax_media").alias("tmax_lb"),
            F.avg("rh_min_media").alias("rh_min_lb"),
            F.sum("det_100km2").alias("dens_suma_lb"),
            F.count(F.lit(1)).cast("int").alias("n_anios_lb"),
        )
        .select(*COLS_LINEA_BASE)
    )


def municipio_mes(mm: DataFrame, lb: DataFrame, p: ParamsClima) -> DataFrame:
    """Anomalías (diferencia simple), propensión LOYO y rezago de densidad.

    LOYO: para un año de la línea base se excluye ese mismo año de la media
    (``(suma − propio) / (n − 1)``); para un año fuera de la línea base (2026) no hay
    nada que excluir (``suma / n``).
    """
    en_lb = F.col("anio").isin(list(p.anios_lb))
    df = (
        mm.join(F.broadcast(lb), ["cod_mpio", "mes"], "left")
        .withColumn("anom_precip", F.col("precip_mes") - F.col("precip_lb"))
        .withColumn("anom_tmax", F.col("tmax_media") - F.col("tmax_lb"))
        .withColumn("anom_rh", F.col("rh_min_media") - F.col("rh_min_lb"))
        .withColumn(
            "dens_loyo",
            F.when(en_lb, (F.col("dens_suma_lb") - F.col("det_100km2")) / (F.col("n_anios_lb") - 1))
            .otherwise(F.col("dens_suma_lb") / F.col("n_anios_lb")),
        )
    )
    # rowsBetween es válido porque CL-3/CL-4 exigen la grilla mensual completa.
    m = p.meses_previos
    w = Window.partitionBy("cod_mpio").orderBy("fecha_mes")
    df = (
        df.withColumn("_k", F.row_number().over(w))
        .withColumn(
            "det_3m_previos",
            F.when(F.col("_k") > m, F.avg("det_100km2").over(w.rowsBetween(-m, -1))),
        )
    )
    return df.select(*COLS_MUNICIPIO_MES)


# --------------------------------------------------------------------------- #
# Validación
# --------------------------------------------------------------------------- #
def validar(md, dim, mmf, lb, informe_bd, p: ParamsClima) -> tuple[Registro, dict]:
    r = Registro()
    ini, fin = date.fromisoformat(p.inicio), date.fromisoformat(p.fin)

    # --- Entrada ---------------------------------------------------------------
    publicado = bool(informe_bd and informe_bd.get("publicado"))
    r.add("CL-0", "build_daily publicado (informe con publicado = true)", publicado,
          publicado, True)
    em = md.agg(F.count(F.lit(1)).alias("filas"), F.countDistinct("cod_mpio").alias("mpios"),
                F.sum("n_det").alias("det"), F.sum("frp_sum").alias("frp")).first()
    n_mpio = em["mpios"]
    esp_dia = n_mpio * p.n_dias
    filas_inf = (informe_bd or {}).get("conteos", {}).get("municipio_dia")
    r.add("CL-1", "filas municipio_dia = municipios × días = informe de build_daily",
          em["filas"] == esp_dia and (filas_inf is None or filas_inf == em["filas"]),
          {"filas": em["filas"], "informe": filas_inf}, esp_dia)
    sin_area = (md.select("cod_mpio").distinct()
                .join(dim.filter(F.col("area_km2") > 0), "cod_mpio", "left_anti").count())
    r.add("CL-2", "todo municipio tiene area_km2 > 0 en dim_municipio", sin_area == 0,
          sin_area, 0)

    # --- int.municipio_mes -------------------------------------------------------
    esp_mes = n_mpio * p.n_meses
    dias_esperados = F.datediff(
        F.least(F.last_day("fecha_mes"), F.lit(p.fin).cast("date")),
        F.greatest(F.col("fecha_mes"), F.lit(p.inicio).cast("date")),
    ) + 1
    corte_rezago = F.add_months(F.trunc(F.lit(p.inicio).cast("date"), "month"), p.meses_previos)
    ef = mmf.agg(
        F.count(F.lit(1)).alias("filas"),
        F.sum("n_det").alias("det"),
        F.sum("frp_total").alias("frp"),
        F.sum((F.col("n_dias") != dias_esperados).cast("int")).alias("n_dias_mal"),
        F.sum(((F.col("racha_seca_max") < 0) | (F.col("racha_seca_max") > F.col("n_dias")))
              .cast("int")).alias("racha_fuera"),
        F.sum(F.coalesce(F.col("dens_loyo") < -TOL_NEG, F.lit(True)).cast("int")).alias("loyo_mal"),
        F.sum(F.col("det_3m_previos").isNull().cast("int")).alias("rez_nulos"),
        F.sum((F.col("det_3m_previos").isNull() & (F.col("fecha_mes") >= corte_rezago))
              .cast("int")).alias("rez_nulos_tarde"),
        F.sum((F.col("frp_medio").isNull() != (F.col("n_det") == 0)).cast("int")).alias("frp_incoh"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"nul_{c}") for c in COLS_SIN_NULOS],
    ).first()
    n_pk = mmf.select("cod_mpio", "fecha_mes").distinct().count()
    r.add("CL-3", "filas municipio_mes = municipios × meses; PK única",
          ef["filas"] == esp_mes and n_pk == ef["filas"],
          {"filas": ef["filas"], "pk": n_pk}, esp_mes)
    r.add("CL-4", "n_dias = días del mes calendario dentro de la ventana", ef["n_dias_mal"] == 0,
          ef["n_dias_mal"], 0)
    frp_ok = abs((ef["frp"] or 0.0) - (em["frp"] or 0.0)) <= 1e-6 * max(1.0, abs(em["frp"] or 0.0))
    r.add("CL-5", "Σ n_det y Σ FRP se conservan (día → mes)",
          ef["det"] == em["det"] and frp_ok,
          {"n_det": ef["det"], "frp": ef["frp"]}, {"n_det": em["det"], "frp": em["frp"]})

    # --- int.linea_base ----------------------------------------------------------
    el = lb.agg(F.count(F.lit(1)).alias("filas"),
                F.sum((F.col("n_anios_lb") != len(p.anios_lb)).cast("int")).alias("anios_mal")).first()
    esp_lb = n_mpio * 12
    r.add("CL-6", f"linea_base = municipios × 12; n_anios_lb = {len(p.anios_lb)} en todas",
          el["filas"] == esp_lb and el["anios_mal"] == 0,
          {"filas": el["filas"], "anios_mal": el["anios_mal"]}, {"filas": esp_lb, "anios_mal": 0})
    medias = (
        mmf.filter(F.col("anio").isin(list(p.anios_lb)))
        .groupBy("cod_mpio", "mes")
        .agg(*[F.avg(c).alias(c) for c in ("anom_precip", "anom_tmax", "anom_rh")])
        .agg(*[F.max(F.abs(c)).alias(c) for c in ("anom_precip", "anom_tmax", "anom_rh")])
        .first()
    )
    max_anom = max(abs(medias[c] or 0.0) for c in ("anom_precip", "anom_tmax", "anom_rh"))
    r.add("CL-7", "media de cada anomalía en la línea base = 0 (join correcto)",
          max_anom <= TOL_ANOM, max_anom, f"≤ {TOL_ANOM}")

    # --- Rachas, LOYO, rezago, nulos -------------------------------------------
    secos = (md.withColumn("fecha_mes", F.trunc("fecha", "month"))
             .groupBy("cod_mpio", "fecha_mes")
             .agg(F.sum(F.col("dia_seco").cast("int")).alias("n_secos")))
    incoh = (mmf.join(secos, ["cod_mpio", "fecha_mes"], "left")
             .filter((F.col("racha_seca_max") > F.coalesce("n_secos", F.lit(0)))
                     | ((F.col("racha_seca_max") == 0) != (F.coalesce("n_secos", F.lit(0)) == 0)))
             .count())
    r.add("CL-8", "racha_seca_max ∈ [0, n_dias], ≤ días secos y 0 ⇔ sin días secos (P-A)",
          ef["racha_fuera"] == 0 and incoh == 0,
          {"fuera_de_rango": ef["racha_fuera"], "incoherentes": incoh}, 0)
    r.add("CL-9", "dens_loyo no nulo y ≥ 0", ef["loyo_mal"] == 0, ef["loyo_mal"], 0)
    esp_rez = n_mpio * p.meses_previos
    r.add("CL-10", f"det_3m_previos nulo solo en los primeros {p.meses_previos} meses (P-B)",
          ef["rez_nulos"] == esp_rez and ef["rez_nulos_tarde"] == 0,
          {"nulos": ef["rez_nulos"], "nulos_fuera_del_borde": ef["rez_nulos_tarde"]},
          {"nulos": esp_rez, "nulos_fuera_del_borde": 0})
    r.add("CL-11", "frp_medio nulo ⇔ n_det = 0", ef["frp_incoh"] == 0, ef["frp_incoh"], 0)
    nulos = {c: ef[f"nul_{c}"] for c in COLS_SIN_NULOS}
    r.add("CL-12", "0 nulos en las columnas obligatorias", sum(nulos.values()) == 0, nulos, 0)

    conteos = {
        "municipio_dia": em["filas"], "municipio_mes": ef["filas"], "linea_base": el["filas"],
        "municipios": n_mpio, "meses": p.n_meses, "detecciones": ef["det"],
        "det_3m_previos_nulos": ef["rez_nulos"],
        "celdas_con_deteccion": mmf.filter(F.col("n_det") > 0).count(),
    }
    return r, conteos


# --------------------------------------------------------------------------- #
# Orquestación
# --------------------------------------------------------------------------- #
def calcular(md: DataFrame, dim: DataFrame, p: ParamsClima) -> tuple[DataFrame, DataFrame]:
    mm = agregar_mes(md, dim).cache()
    lb = linea_base(mm, p.anios_lb).cache()
    return municipio_mes(mm, lb, p), lb


def ejecutar(spark: SparkSession, cfg: dict, publicar: bool = True) -> dict:
    p = ParamsClima.desde_cfg(cfg)
    t: dict[str, float] = {}
    t_total = time.perf_counter()
    reportes = p.inter / "_reports"
    informe_bd = leer_informe(reportes / "build_daily_validation.json")

    md, dim = leer_entradas(spark, p)
    md = md.cache()

    t0 = time.perf_counter()
    mmf, lb = calcular(md, dim, p)
    mmf = mmf.cache()
    mmf.count()
    t["calculo_s"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    reg, conteos = validar(md, dim, mmf, lb, informe_bd, p)
    t["validacion_s"] = time.perf_counter() - t0

    informe = {
        "modulo": "firewatch.spark.climatology",
        "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
        "spark": {
            "version": spark.version,
            "master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
        },
        "params": {k: str(v) for k, v in asdict(p).items()},
        "decisiones": {"P-A": "racha_seca_max recortada al mes",
                       "P-B": f"det_3m_previos nulo en los primeros {p.meses_previos} meses"},
        "entrada_build_daily": (informe_bd or {}).get("huellas"),
        "conteos": conteos,
        "esquemas": {"municipio_mes": mmf.schema.simpleString(),
                     "linea_base": lb.schema.simpleString()},
        "checks": [asdict(c) for c in reg.checks],
        "publicado": False,
    }
    reporte = reportes / "climatology_validation.json"

    if reg.fallas:
        informe["tiempos"] = {k: round(v, 3) for k, v in t.items()}
        _escribir_json(informe, reporte)
        reg.exigir()

    informe["huellas"] = {
        "municipio_mes": huella_contenido(mmf, p.decimales_huella),
        "linea_base": huella_contenido(lb, p.decimales_huella),
    }

    if publicar:
        t0 = time.perf_counter()
        tmps = {
            "municipio_mes": _escribir_tmp(mmf.orderBy("cod_mpio", "fecha_mes"),
                                           p.inter / "municipio_mes", n_archivos=1),
            "linea_base": _escribir_tmp(lb.orderBy("cod_mpio", "mes"),
                                        p.inter / "linea_base", n_archivos=1),
        }
        for nombre, tmp in tmps.items():
            _intercambiar(tmp, p.inter / nombre)
        t["escritura_s"] = time.perf_counter() - t0
        informe["publicado"] = True

    t["total_s"] = time.perf_counter() - t_total
    informe["tiempos"] = {k: round(v, 3) for k, v in t.items()}
    _escribir_json(informe, reporte)
    for df in (md, mmf, lb):
        df.unpersist()
    log.info("climatology terminado · conteos=%s · tiempos=%s", conteos, informe["tiempos"])
    return informe


def main() -> None:
    ap = argparse.ArgumentParser(description="FireWatch · climatology (L3)")
    ap.add_argument("--config", default="config/settings.yaml")
    ap.add_argument("--master", default=None)
    ap.add_argument("--particiones", type=int, default=None)
    ap.add_argument("--sin-aqe", action="store_true")
    ap.add_argument("--solo-validar", action="store_true", help="valida sin publicar")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = cargar_config(a.config)
    spark = crear_sesion(cfg, a.master, a.particiones, aqe=not a.sin_aqe)
    try:
        ejecutar(spark, cfg, publicar=not a.solo_validar)
    except CheckError as e:
        log.error("No se publicó: %s", e)
        raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
