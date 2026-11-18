"""L3 · Horario → diario, grilla municipio-día y agregado departamento-mes.

Spark solo lee silver (D-L6). Si falla un check bloqueante no se publica nada (D-L8).

Salidas (Parquet, en ``rutas.intermedio``):

    clima_diario    municipio × día     cod_mpio, fecha, tmax, tmean, precip, rh_min,
                                        wind_max, et0, n_horas
    municipio_dia   municipio × día     cod_mpio, fecha, tmax, tmean, precip, rh_min,
                                        wind_max, et0, n_det, frp_sum, precip_30d,
                                        dia_seco, id_racha
    agg_depto_mes   departamento × mes  cod_dpto, fecha_mes, n_det, frp_total

Informe: ``rutas.intermedio/_reports/build_daily_validation.json`` (checks, conteos,
esquemas, huellas de contenido y tiempos). Las cifras del IEEE salen de ahí.

Uso::

    python -m firewatch.spark.build_daily
    python -m firewatch.spark.build_daily --master local[4] --particiones 16 --sin-aqe
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from firewatch.spark.session import cargar_config, crear_sesion

log = logging.getLogger(__name__)

# Columnas publicadas (contrato §6.6 + n_horas de auditoría, aprobada el 18 nov).
COLS_CLIMA_DIARIO = [
    "cod_mpio", "fecha", "tmax", "tmean", "precip", "rh_min", "wind_max", "et0", "n_horas",
]
COLS_MUNICIPIO_DIA = [
    "cod_mpio", "fecha", "tmax", "tmean", "precip", "rh_min", "wind_max", "et0",
    "n_det", "frp_sum", "precip_30d", "dia_seco", "id_racha",
]
COLS_AGG_DEPTO_MES = ["cod_dpto", "fecha_mes", "n_det", "frp_total"]
VARS_HORARIAS = ["temp", "rh", "precip", "wind", "et0"]
VARS_DIARIAS = ["tmax", "tmean", "precip", "rh_min", "wind_max", "et0"]


# --------------------------------------------------------------------------- #
# Parámetros (todo sale de settings.yaml, C26)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Params:
    silver: Path
    inter: Path
    inicio: str
    fin: str
    dpto_boyaca: str
    ventana_precip: int
    umbral_dia_seco: float
    viento_max_kmh: float
    decimales_huella: int

    # ÚNICO lugar con nombres de claves de settings.yaml.
    CLAVES = (
        ("rutas", "silver"), ("rutas", "intermedio"),
        ("ventana", "fecha_inicio"), ("ventana", "fecha_fin"),
        ("territorio", "cod_dpto_foco"),
        ("spark", "ventana_precip_dias"), ("spark", "umbral_dia_seco_mm"),
    )

    @classmethod
    def desde_cfg(cls, cfg: dict) -> "Params":
        faltan = [".".join(k) for k in cls.CLAVES if k[1] not in (cfg.get(k[0]) or {})]
        if faltan:
            raise KeyError(f"Faltan claves en settings.yaml: {faltan}")
        sp = cfg["spark"]
        return cls(
            silver=Path(cfg["rutas"]["silver"]),
            inter=Path(cfg["rutas"]["intermedio"]),
           inicio=str(cfg["ventana"]["fecha_inicio"]),
           fin=str(cfg["ventana"]["fecha_fin"]),
            dpto_boyaca=str(cfg["territorio"]["cod_dpto_foco"]),
            ventana_precip=int(sp["ventana_precip_dias"]), 
           umbral_dia_seco=float(sp["umbral_dia_seco_mm"]),
            # 40 m/s (cota V-M5) × 3,6 = 144 km/h
            viento_max_kmh=float(sp.get("viento_max_kmh", 144.0)),
            decimales_huella=int(cfg.get("spark", {}).get("decimales_huella", 6)),
        )

    @property
    def n_dias(self) -> int:
        return (date.fromisoformat(self.fin) - date.fromisoformat(self.inicio)).days + 1

    @property
    def n_meses(self) -> int:
        a, b = date.fromisoformat(self.inicio), date.fromisoformat(self.fin)
        return (b.year - a.year) * 12 + (b.month - a.month) + 1


# --------------------------------------------------------------------------- #
# Registro de checks
# --------------------------------------------------------------------------- #
class CheckError(RuntimeError):
    """Al menos un check bloqueante falló: no se publica nada."""


@dataclass
class Check:
    id: str
    descripcion: str
    nivel: str  # "bloqueante" | "advertencia"
    ok: bool
    valor: Any
    esperado: Any


@dataclass
class Registro:
    checks: list[Check] = field(default_factory=list)

    def add(self, id_: str, desc: str, ok: bool, valor: Any, esperado: Any,
            nivel: str = "bloqueante") -> None:
        c = Check(id_, desc, nivel, bool(ok), _json_safe(valor), _json_safe(esperado))
        self.checks.append(c)
        log.info("%s %-7s %s · %s (esperado %s)",
                 "OK   " if c.ok else ("FALLA" if nivel == "bloqueante" else "AVISO"),
                 id_, desc, c.valor, c.esperado)

    @property
    def fallas(self) -> list[Check]:
        return [c for c in self.checks if c.nivel == "bloqueante" and not c.ok]

    def exigir(self) -> None:
        if self.fallas:
            raise CheckError("; ".join(f"{c.id}: {c.descripcion} = {c.valor}" for c in self.fallas))


def _json_safe(v: Any) -> Any:
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _json_safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
    return v


# --------------------------------------------------------------------------- #
# Lectura de silver
# --------------------------------------------------------------------------- #
def leer_horario(spark: SparkSession, p: Params) -> DataFrame:
    """Lee ``silver.clima_horario`` y normaliza ``ts_local`` a ``dia_ts`` (fecha de ts_local).

    ``ts_local`` puede llegar como long (ns, por ``nanosAsLong``), TIMESTAMP_NTZ o
    TIMESTAMP. Solo se usa para validar la PK y la coherencia con ``fecha``; la
    agregación usa ``fecha``.
    """
    raw = spark.read.parquet(str(p.silver / "clima_horario"))
    tipo = raw.schema["ts_local"].dataType
    ts = F.col("ts_local")
    if isinstance(tipo, T.LongType):  # nanosegundos desde epoch, hora local ingenua
        dia_ts = F.expr("date_from_unix_date(CAST(floor(ts_local / 86400000000000) AS INT))")
    elif isinstance(tipo, T.TimestampNTZType):
        dia_ts = F.to_date(ts)
    elif isinstance(tipo, T.TimestampType):
        # Valor ingenuo leído como instante UTC: los micros crudos son la hora local.
        dia_ts = F.expr("date_from_unix_date(CAST(floor(unix_micros(ts_local) / 86400000000) AS INT))")
    else:
        raise TypeError(f"Tipo inesperado de ts_local: {tipo}")
    log.info("ts_local leído como %s", tipo.simpleString())
    return raw.select(
        F.col("cod_mpio").cast("string").alias("cod_mpio"),
        ts.alias("ts_local"),
        F.col("fecha").cast("date").alias("fecha"),
        dia_ts.alias("dia_ts"),
        *[F.col(c).cast("double").alias(c) for c in VARS_HORARIAS],
    )


def leer_entradas(spark: SparkSession, p: Params):
    """Devuelve (horario, detecciones, dim_municipio, dim_departamento)."""
    s = lambda nombre: str(p.silver / nombre)  # noqa: E731
    h = leer_horario(spark, p)
    det = spark.read.parquet(s("detecciones")).select(
        F.col("cod_dpto").cast("string").alias("cod_dpto"),
        F.col("cod_mpio").cast("string").alias("cod_mpio"),
        F.col("fecha").cast("date").alias("fecha"),
        F.col("frp").cast("double").alias("frp"),
        F.col("fuente_estatica").cast("boolean").alias("fuente_estatica"),
    )
    mun = spark.read.parquet(s("dim_municipio")).select(F.col("cod_mpio").cast("string"))
    dpto = spark.read.parquet(s("dim_departamento")).select(F.col("cod_dpto").cast("string"))
    return h, det, mun, dpto


def meses_ventana(spark: SparkSession, p: Params) -> DataFrame:
    """Un renglón por mes de la ventana."""
    return (
        spark.range(1)
        .select(F.explode(F.sequence(
            F.trunc(F.lit(p.inicio).cast("date"), "month"),
            F.trunc(F.lit(p.fin).cast("date"), "month"),
            F.expr("interval 1 month"),
        )).alias("fecha_mes"))
    )


def _en_ventana(col: str, p: Params):
    return F.col(col).between(F.lit(p.inicio).cast("date"), F.lit(p.fin).cast("date"))


# --------------------------------------------------------------------------- #
# Transformaciones (funciones puras DataFrame → DataFrame)
# --------------------------------------------------------------------------- #
def clima_diario(h: DataFrame) -> DataFrame:
    """Horario → diario. Etapa principal del benchmark de Spark (shuffle por cod_mpio, fecha)."""
    return h.groupBy("cod_mpio", "fecha").agg(
        F.max("temp").alias("tmax"),
        F.avg("temp").alias("tmean"),
        F.sum("precip").alias("precip"),
        F.min("rh").alias("rh_min"),
        F.max("wind").alias("wind_max"),  # ya en km/h (silver)
        F.sum("et0").alias("et0"),
        F.count(F.lit(1)).cast("int").alias("n_horas"),
    ).select(*COLS_CLIMA_DIARIO)


def detecciones_boyaca(det: DataFrame, p: Params) -> DataFrame:
    """Vegetación en Boyacá dentro de la ventana: sin fuentes estáticas y con municipio."""
    return det.filter(
        (F.col("cod_dpto") == p.dpto_boyaca)
        & (~F.col("fuente_estatica"))
        & F.col("cod_mpio").isNotNull()
        & _en_ventana("fecha", p)
    ).select("cod_mpio", "fecha", "frp")


def municipio_dia(clima: DataFrame, det_boy: DataFrame, p: Params) -> DataFrame:
    """Grilla completa municipio-día con ceros y ventanas (precip_30d, rachas secas).

    La base es el clima (todos los municipio-día). Las ventanas usan ``rowsBetween``,
    válido solo porque ``validar`` exige la serie diaria completa por municipio.
    """
    conteos = det_boy.groupBy("cod_mpio", "fecha").agg(
        F.count(F.lit(1)).cast("int").alias("n_det"), F.sum("frp").alias("frp_sum")
    )
    base = (
        clima.join(F.broadcast(conteos), ["cod_mpio", "fecha"], "left")
        .fillna({"n_det": 0, "frp_sum": 0.0})
    )
    w = Window.partitionBy("cod_mpio").orderBy("fecha")
    w_ventana = w.rowsBetween(-(p.ventana_precip - 1), 0)
    base = (
        base.withColumn("_k", F.row_number().over(w))
        # Nulo mientras la ventana no esté completa (primeros días de la serie).
        .withColumn(
            "precip_30d",
            F.when(F.col("_k") >= p.ventana_precip, F.sum("precip").over(w_ventana)),
        )
        .withColumn("dia_seco", F.col("precip") < F.lit(p.umbral_dia_seco))
    )
    # Gaps-and-islands: dentro de (municipio, dia_seco) los días consecutivos comparten
    # _k − row_number. id_racha identifica cada racha seca; nulo en días húmedos.
    w_seco = Window.partitionBy("cod_mpio", "dia_seco").orderBy("fecha")
    base = base.withColumn("_g", F.col("_k") - F.row_number().over(w_seco)).withColumn(
        "id_racha",
        F.when(F.col("dia_seco"), F.concat_ws("-", "cod_mpio", F.col("_g").cast("string"))),
    )
    return base.select(*COLS_MUNICIPIO_DIA)


def agg_depto_mes(det: DataFrame, dpto: DataFrame, meses: DataFrame, p: Params) -> DataFrame:
    """Nacional: departamento × mes con ceros (alimenta K6). Sin fuentes estáticas."""
    real = (
        det.filter((~F.col("fuente_estatica")) & F.col("cod_dpto").isNotNull()
                   & _en_ventana("fecha", p))
        .withColumn("fecha_mes", F.trunc("fecha", "month"))
        .groupBy("cod_dpto", "fecha_mes")
        .agg(F.count(F.lit(1)).cast("int").alias("n_det"), F.sum("frp").alias("frp_total"))
    )
    return (
        dpto.crossJoin(meses)
        .join(real, ["cod_dpto", "fecha_mes"], "left")
        .fillna({"n_det": 0, "frp_total": 0.0})
        .select(*COLS_AGG_DEPTO_MES)
    )


def huella_contenido(df: DataFrame, decimales: int) -> str:
    """Huella independiente del orden y del particionado: Σ xxhash64(fila).

    Los double se redondean: la suma en punto flotante no es asociativa, y con otro
    número de particiones ``avg``/``sum`` pueden variar en el último bit.
    """
    cols = [
        F.round(F.col(f.name), decimales) if isinstance(f.dataType, T.DoubleType) else F.col(f.name)
        for f in df.schema.fields
    ]
    v = df.select(F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h")).first()["h"]
    return str(v)


# --------------------------------------------------------------------------- #
# Validación
# --------------------------------------------------------------------------- #
def validar(h, cd, md, ad, det, det_boy, mun, dpto, p: Params) -> tuple[Registro, dict]:
    """Ejecuta todos los checks; devuelve el registro y los conteos (no lanza)."""
    r = Registro()
    ini, fin = date.fromisoformat(p.inicio), date.fromisoformat(p.fin)
    n_mpio, n_dpto = mun.count(), dpto.count()
    esp_dia = n_mpio * p.n_dias

    # --- Entrada: silver.clima_horario --------------------------------------
    eh = h.agg(
        F.count(F.lit(1)).alias("filas"),
        F.countDistinct("cod_mpio").alias("mpios"),
        F.min("fecha").alias("fmin"), F.max("fecha").alias("fmax"),
        F.sum((F.col("fecha") != F.col("dia_ts")).cast("int")).alias("fecha_incoherente"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"nul_{c}")
          for c in ["cod_mpio", "ts_local", "fecha", *VARS_HORARIAS]],
    ).first()
    n_pk = h.select("cod_mpio", "ts_local").distinct().count()
    r.add("BD-1", "filas clima_horario", eh["filas"] == esp_dia * 24, eh["filas"], esp_dia * 24)
    r.add("BD-2", "PK (cod_mpio, ts_local) única", n_pk == eh["filas"], n_pk, eh["filas"])
    r.add("BD-3", "fecha = fecha local de ts_local",
          eh["fecha_incoherente"] == 0, eh["fecha_incoherente"], 0)
    r.add("BD-4", "rango de fechas = ventana", (eh["fmin"], eh["fmax"]) == (ini, fin),
          [eh["fmin"], eh["fmax"]], [ini, fin])
    nulos_h = {c: eh[f"nul_{c}"] for c in ["cod_mpio", "ts_local", "fecha", *VARS_HORARIAS]}
    r.add("BD-5", "0 nulos en claves y variables horarias", sum(nulos_h.values()) == 0, nulos_h, 0)

    # --- int.clima_diario ---------------------------------------------------
    ec = cd.agg(
        F.count(F.lit(1)).alias("filas"),
        F.sum((F.col("n_horas") != 24).cast("int")).alias("dias_no_24"),
        F.sum((F.col("tmax") < F.col("tmean")).cast("int")).alias("tmax_lt_tmean"),
        F.sum(((F.col("rh_min") < 0) | (F.col("rh_min") > 100)).cast("int")).alias("rh_fuera"),
        F.sum((F.col("precip") < 0).cast("int")).alias("precip_neg"),
        F.sum((F.col("et0") < 0).cast("int")).alias("et0_neg"),
        F.sum(((F.col("wind_max") < 0) | (F.col("wind_max") > p.viento_max_kmh))
              .cast("int")).alias("viento_fuera"),
        F.sum(((F.col("tmax") < -10) | (F.col("tmax") > 45)).cast("int")).alias("tmax_fuera"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"nul_{c}") for c in VARS_DIARIAS],
    ).first()
    serie = cd.groupBy("cod_mpio").agg(
        F.count(F.lit(1)).alias("n"), F.min("fecha").alias("a"), F.max("fecha").alias("b")
    )
    mpios_incompletos = serie.filter(
        (F.col("n") != p.n_dias) | (F.col("a") != F.lit(ini)) | (F.col("b") != F.lit(fin))
    ).count()
    r.add("BD-6", "filas clima_diario", ec["filas"] == esp_dia, ec["filas"], esp_dia)
    r.add("BD-7", "n_horas = 24 en todo municipio-día", ec["dias_no_24"] == 0, ec["dias_no_24"], 0)
    r.add("BD-8", "serie diaria completa y contigua por municipio",
          mpios_incompletos == 0, mpios_incompletos, 0)
    nulos_d = {c: ec[f"nul_{c}"] for c in VARS_DIARIAS}
    r.add("BD-9", "0 nulos en clima diario", sum(nulos_d.values()) == 0, nulos_d, 0)
    fisicos = {k: ec[k] for k in ("tmax_lt_tmean", "rh_fuera", "precip_neg", "et0_neg")}
    r.add("BD-10", "coherencia física (tmax≥tmean, rh∈[0,100], precip≥0, et0≥0)",
          sum(fisicos.values()) == 0, fisicos, 0)
    rangos = {"viento_fuera": ec["viento_fuera"], "tmax_fuera": ec["tmax_fuera"]}
    r.add("BD-11", "rangos V-M5 (viento ≤ 144 km/h, tmax ∈ [-10, 45])",
          sum(rangos.values()) == 0, rangos, 0, nivel="advertencia")
    mpios_cd = cd.select("cod_mpio").distinct()
    sin_dim = mpios_cd.join(mun, "cod_mpio", "left_anti").count()
    sin_clima = mun.join(mpios_cd, "cod_mpio", "left_anti").count()
    r.add("BD-12", "municipios clima = dim_municipio (ambos sentidos)",
          sin_dim == 0 and sin_clima == 0,
          {"sin_dim": sin_dim, "sin_clima": sin_clima}, {"sin_dim": 0, "sin_clima": 0})

    # --- int.municipio_dia --------------------------------------------------
    n_boy = det_boy.count()
    em = md.agg(
        F.count(F.lit(1)).alias("filas"),
        F.sum("n_det").alias("suma_det"),
        # coalesce: una comparación con nulo da nulo; se cuenta como fila mala.
        F.sum(F.coalesce(F.col("n_det") < 0, F.lit(True)).cast("int")).alias("ndet_mal"),
        F.sum(F.col("precip_30d").isNull().cast("int")).alias("p30_nulos"),
        F.sum((F.col("dia_seco") != F.col("id_racha").isNotNull()).cast("int")).alias("racha_incoh"),
    ).first()
    boy_fuera_grilla = det_boy.select("cod_mpio", "fecha").distinct().join(
        cd.select("cod_mpio", "fecha"), ["cod_mpio", "fecha"], "left_anti").count()
    esp_p30 = n_mpio * (p.ventana_precip - 1)
    r.add("BD-13", "filas municipio_dia", em["filas"] == esp_dia, em["filas"], esp_dia)
    r.add("BD-14", "Σ n_det = detecciones Boyacá en silver", em["suma_det"] == n_boy,
          em["suma_det"], n_boy)
    r.add("BD-15", "detecciones Boyacá fuera de la grilla", boy_fuera_grilla == 0,
          boy_fuera_grilla, 0)
    r.add("BD-16", "n_det sin nulos ni negativos", em["ndet_mal"] == 0, em["ndet_mal"], 0)
    r.add("BD-17", "precip_30d nulo solo en los primeros días", em["p30_nulos"] == esp_p30,
          em["p30_nulos"], esp_p30)
    r.add("BD-18", "id_racha no nulo ⇔ dia_seco", em["racha_incoh"] == 0, em["racha_incoh"], 0)

    # --- int.agg_depto_mes --------------------------------------------------
    n_nac = det.filter((~F.col("fuente_estatica")) & F.col("cod_dpto").isNotNull()
                       & _en_ventana("fecha", p)).count()
    ea = ad.agg(F.count(F.lit(1)).alias("filas"), F.sum("n_det").alias("suma")).first()
    boy_ad = ad.filter(F.col("cod_dpto") == p.dpto_boyaca).agg(F.sum("n_det")).first()[0]
    huerfanos = (det.filter(F.col("cod_dpto").isNotNull()).select("cod_dpto").distinct()
                 .join(dpto, "cod_dpto", "left_anti").count())
    r.add("BD-19", "filas agg_depto_mes", ea["filas"] == n_dpto * p.n_meses, ea["filas"],
          n_dpto * p.n_meses)
    r.add("BD-20", "Σ nacional = silver sin estáticas", ea["suma"] == n_nac, ea["suma"], n_nac)
    r.add("BD-21", "Boyacá en agregado = Σ n_det de municipio_dia", boy_ad == n_boy, boy_ad, n_boy)
    r.add("BD-22", "cod_dpto de detecciones ⊆ dim_departamento", huerfanos == 0, huerfanos, 0)

    conteos = {
        "clima_horario": eh["filas"], "clima_diario": ec["filas"], "municipio_dia": em["filas"],
        "agg_depto_mes": ea["filas"], "municipios": n_mpio, "departamentos": n_dpto,
        "dias": p.n_dias, "meses": p.n_meses, "detecciones_boyaca": n_boy,
        "detecciones_nacionales_sin_estaticas": n_nac,
    }
    return r, conteos


# --------------------------------------------------------------------------- #
# Publicación y orquestación
# --------------------------------------------------------------------------- #
def _escribir_tmp(df: DataFrame, ruta: Path, n_archivos: int | None = None) -> Path:
    tmp = ruta.with_name(ruta.name + ".tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    out = df.coalesce(n_archivos) if n_archivos else df
    out.write.mode("overwrite").parquet(str(tmp))
    return tmp


def _intercambiar(tmp: Path, ruta: Path) -> None:
    if ruta.exists():
        shutil.rmtree(ruta)
    os.replace(tmp, ruta)


def _escribir_json(obj: dict, ruta: Path) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tmp = ruta.with_suffix(ruta.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, ruta)


def ejecutar(spark: SparkSession, cfg: dict, publicar: bool = True) -> dict:
    """Corre build_daily completo y devuelve el informe (conteos, checks, tiempos)."""
    p = Params.desde_cfg(cfg)
    t: dict[str, float] = {}
    t_total = time.perf_counter()

    h, det, mun, dpto = leer_entradas(spark, p)
    meses = meses_ventana(spark, p)
    h_v = h.filter(_en_ventana("fecha", p))

    t0 = time.perf_counter()
    cd = clima_diario(h_v).cache()
    cd.count()  # materializa el shuffle principal
    t["clima_diario_s"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    det_boy = detecciones_boyaca(det, p).cache()
    md = municipio_dia(cd, det_boy, p).cache()
    ad = agg_depto_mes(det, dpto, meses, p).cache()
    md.count()
    ad.count()
    t["municipio_dia_agg_s"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    reg, conteos = validar(h, cd, md, ad, det, det_boy, mun, dpto, p)
    t["validacion_s"] = time.perf_counter() - t0

    informe = {
        "modulo": "firewatch.spark.build_daily",
        "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
        "spark": {
            "version": spark.version,
            "master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
        },
        "params": {k: str(v) for k, v in asdict(p).items()},
        "conteos": conteos,
        "esquemas": {
            "clima_diario": cd.schema.simpleString(),
            "municipio_dia": md.schema.simpleString(),
            "agg_depto_mes": ad.schema.simpleString(),
        },
        "checks": [asdict(c) for c in reg.checks],
        "publicado": False,
    }
    reporte = p.inter / "_reports" / "build_daily_validation.json"

    if reg.fallas:
        informe["tiempos"] = {k: round(v, 3) for k, v in t.items()}
        _escribir_json(informe, reporte)
        reg.exigir()  # lanza CheckError: no se publica nada

    informe["huellas"] = {
        "clima_diario": huella_contenido(cd, p.decimales_huella),
        "municipio_dia": huella_contenido(md, p.decimales_huella),
        "agg_depto_mes": huella_contenido(ad, p.decimales_huella),
    }

    if publicar:
        t0 = time.perf_counter()
        # Orden por PK: archivos deterministas y estadísticas min/max útiles por cod_mpio.
        tmps = {
            "clima_diario": _escribir_tmp(cd.orderBy("cod_mpio", "fecha"), p.inter / "clima_diario"),
            "municipio_dia": _escribir_tmp(md.orderBy("cod_mpio", "fecha"), p.inter / "municipio_dia"),
            "agg_depto_mes": _escribir_tmp(ad.orderBy("cod_dpto", "fecha_mes"),
                                           p.inter / "agg_depto_mes", n_archivos=1),
        }
        for nombre, tmp in tmps.items():  # solo se intercambia cuando las 3 escribieron bien
            _intercambiar(tmp, p.inter / nombre)
        t["escritura_s"] = time.perf_counter() - t0
        informe["publicado"] = True

    t["total_s"] = time.perf_counter() - t_total
    informe["tiempos"] = {k: round(v, 3) for k, v in t.items()}
    _escribir_json(informe, reporte)
    for df in (cd, det_boy, md, ad):
        df.unpersist()
    log.info("build_daily terminado · conteos=%s · tiempos=%s", conteos, informe["tiempos"])
    return informe


def main() -> None:
    ap = argparse.ArgumentParser(description="FireWatch · build_daily (L3)")
    ap.add_argument("--config", default="config/settings.yaml")
    ap.add_argument("--master", default=None, help="p. ej. local[4]")
    ap.add_argument("--particiones", type=int, default=None)
    ap.add_argument("--sin-aqe", action="store_true", help="desactiva AQE (benchmark)")
    ap.add_argument("--solo-validar", action="store_true", help="valida sin publicar")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = cargar_config(a.config)
    spark = crear_sesion(cfg, a.master, a.particiones, aqe=not a.sin_aqe)
    try:
        ejecutar(spark, cfg, publicar=not a.solo_validar)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
