"""L3 · Índice de Riesgo de Anomalías Térmicas (IRAT) por municipio-mes.

Lee solo ``int.municipio_mes`` (de ``climatology``). Si falla un check bloqueante no se
publica nada (D-L8).

Método (C23):
    1. Valores crudos orientados (mayor = más riesgo):
         H = mezcla · dens_loyo + (1 − mezcla) · coalesce(det_3m_previos, dens_loyo)
         P = −anom_precip   D = racha_seca_max   T = anom_tmax
         R = −anom_rh       W = wind_max_media
    2. c_X = percent_rank(X) sobre todo el panel (empates → mismo valor: determinista).
    3. irat = Σ w_X · c_X  (Σ w = 1, de ``irat.pesos``).
    4. Categorías por quintiles con cortes EXACTOS (``percentile``), no ``ntile``:
       con empates ``ntile`` reparte filas iguales entre grupos según el orden físico.

Salidas (Parquet, en ``rutas.intermedio``):

    municipio_mes_irat   int.municipio_mes + c_H, c_P, c_D, c_T, c_R, c_W, irat,
                         irat_categoria, irat_orden
    irat_sensibilidad    cod_mpio, fecha_mes, n_det, irat, irat_orden, irat_iguales,
                         orden_iguales, irat_solo_clima, orden_solo_clima
                         (``export_gold`` la copia a gold/_analisis; no es contrato gold)

Informe: ``rutas.intermedio/_reports/irat_validation.json`` (incluye cortes, distribución
por categoría y tasa de captura K5 por variante).

Uso::

    python -m firewatch.spark.irat
    python -m firewatch.spark.irat --solo-validar
"""
from __future__ import annotations

import argparse
import logging
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import reduce
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
from firewatch.spark.climatology import COLS_MUNICIPIO_MES, leer_informe
from firewatch.spark.session import cargar_config, crear_sesion

log = logging.getLogger(__name__)

COMPONENTES = ["H", "P", "D", "T", "R", "W"]
CATEGORIAS_DEFECTO = ["Muy bajo", "Bajo", "Moderado", "Alto", "Muy alto"]
CUANTILES = [0.2, 0.4, 0.6, 0.8]
COLS_IRAT = COLS_MUNICIPIO_MES + [f"c_{x}" for x in COMPONENTES] + [
    "irat", "irat_categoria", "irat_orden",
]
COLS_SENSIBILIDAD = [
    "cod_mpio", "fecha_mes", "n_det", "irat", "irat_orden",
    "irat_iguales", "orden_iguales", "irat_solo_clima", "orden_solo_clima",
]
# Nombres aceptados en irat.pesos para cada componente (sin tildes, minúsculas).
ALIAS = {
    "H": {"h", "historico", "propension", "historia", "humano"},
    "P": {"p", "precip", "precipitacion", "lluvia", "anom_precip"},
    "D": {"d", "sequia", "racha", "racha_seca", "racha_seca_max", "dias_secos"},
    "T": {"t", "temp", "temperatura", "tmax", "anom_tmax"},
    "R": {"r", "rh", "humedad", "humedad_relativa", "anom_rh"},
    "W": {"w", "viento", "wind", "wind_max"},
}


def _norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return s.strip().lower().replace(" ", "_")


# --------------------------------------------------------------------------- #
# Parámetros (todo sale de settings.yaml, C26)
# --------------------------------------------------------------------------- #
def _pesos(cfg_irat: dict) -> dict[str, float]:
    crudo = cfg_irat.get("pesos")
    if not isinstance(crudo, dict):
        raise KeyError("Falta irat.pesos (dict con H, P, D, T, R, W) en settings.yaml")
    pesos: dict[str, float] = {}
    for k, v in crudo.items():
        comp = next((c for c, al in ALIAS.items() if _norm(k) in al), None)
        if comp is None:
            raise KeyError(f"irat.pesos: clave no reconocida '{k}'")
        pesos[comp] = float(v)
    faltan = [c for c in COMPONENTES if c not in pesos]
    if faltan:
        raise KeyError(f"irat.pesos: faltan componentes {faltan}")
    if any(w < 0 for w in pesos.values()):
        raise ValueError(f"irat.pesos: hay pesos negativos {pesos}")
    if abs(sum(pesos.values()) - 1.0) > 1e-9:
        raise ValueError(f"irat.pesos debe sumar 1; suma {sum(pesos.values())}")
    return {c: pesos[c] for c in COMPONENTES}


def _mezcla_h(cfg_irat: dict) -> float:
    """Peso de dens_loyo dentro de H. Acepta un número o un dict {loyo, previos}."""
    v = cfg_irat.get("mezcla_h", 0.5)
    if isinstance(v, dict):
        loyo = next((float(x) for k, x in v.items() if any(s in _norm(k) for s in ("loyo", "hist"))), None)
        prev = next((float(x) for k, x in v.items()
                     if any(s in _norm(k) for s in ("previo", "rezago", "3m", "reciente"))), None)
        if loyo is None or prev is None or loyo + prev <= 0:
            raise ValueError(f"irat.mezcla_h no reconocida: {v}")
        v = loyo / (loyo + prev)
    v = float(v)
    if not 0.0 <= v <= 1.0:
        raise ValueError(f"irat.mezcla_h debe estar en [0, 1]; es {v}")
    return v


def _categorias(cfg_irat: dict) -> list[str]:
    v = cfg_irat.get("categorias", CATEGORIAS_DEFECTO)
    if isinstance(v, dict):  # {1: "Muy bajo", ...}
        v = [v[k] for k in sorted(v, key=lambda x: int(x))]
    v = [x.get("nombre", x.get("etiqueta")) if isinstance(x, dict) else x for x in v]
    if len(v) != 5:
        raise ValueError(f"irat.categorias debe tener 5 nombres (quintiles); tiene {v}")
    return [str(x) for x in v]


def _ordenes_alto(cfg_irat: dict, cats: list[str]) -> list[int]:
    v = cfg_irat.get("categorias_alto", cats[3:])
    ordenes = []
    for x in v:
        if isinstance(x, int) or str(x).isdigit():
            ordenes.append(int(x))
        else:
            idx = [_norm(c) for c in cats].index(_norm(x))  # ValueError si no existe
            ordenes.append(idx + 1)
    return sorted(set(ordenes))


@dataclass(frozen=True)
class ParamsIrat:
    inter: Path
    pesos: dict
    mezcla_h: float
    categorias: list
    ordenes_alto: list
    normalizacion: str
    decimales_huella: int

    @classmethod
    def desde_cfg(cls, cfg: dict) -> "ParamsIrat":
        if "intermedio" not in (cfg.get("rutas") or {}):
            raise KeyError("Faltan claves en settings.yaml: ['rutas.intermedio']")
        ci = cfg.get("irat") or {}
        cats = _categorias(ci)
        norm = str(ci.get("normalizacion", "percent_rank"))
        if "percent" not in _norm(norm):
            raise ValueError(f"irat.normalizacion = '{norm}': solo está implementado percent_rank (C23)")
        return cls(
            inter=Path(cfg["rutas"]["intermedio"]),
            pesos=_pesos(ci),
            mezcla_h=_mezcla_h(ci),
            categorias=cats,
            ordenes_alto=_ordenes_alto(ci, cats),
            normalizacion=norm,
            decimales_huella=int((cfg.get("spark") or {}).get("decimales_huella", 6)),
        )


# --------------------------------------------------------------------------- #
# Transformaciones
# --------------------------------------------------------------------------- #
def componentes_crudos(mm: DataFrame, mezcla_h: float) -> DataFrame:
    """Valores crudos orientados: mayor valor = más riesgo."""
    loyo = F.col("dens_loyo")
    return (
        mm.withColumn("H_raw", F.lit(mezcla_h) * loyo
                      + F.lit(1.0 - mezcla_h) * F.coalesce(F.col("det_3m_previos"), loyo))
        .withColumn("P_raw", -F.col("anom_precip"))
        .withColumn("D_raw", F.col("racha_seca_max").cast("double"))
        .withColumn("T_raw", F.col("anom_tmax"))
        .withColumn("R_raw", -F.col("anom_rh"))
        .withColumn("W_raw", F.col("wind_max_media"))
    )


def percentiles(df: DataFrame) -> DataFrame:
    """c_X = percent_rank global (7.134 filas: una sola partición es deliberado)."""
    for x in COMPONENTES:
        df = df.withColumn(f"c_{x}", F.percent_rank().over(Window.orderBy(f"{x}_raw")))
    return df


def suma_ponderada(pesos: dict[str, float]):
    return reduce(lambda a, b: a + b, [F.lit(w) * F.col(f"c_{x}") for x, w in pesos.items()])


def cortes_quintiles(df: DataFrame, col: str) -> list[float]:
    """Cortes exactos (``percentile``, no ``percentile_approx``)."""
    arr = ", ".join(str(q) for q in CUANTILES)
    return [float(v) for v in df.select(F.expr(f"percentile({col}, array({arr}))")).first()[0]]


def orden_por_cortes(col: str, cortes: list[float]):
    c = F.col(col)
    return (F.when(c <= cortes[0], 1).when(c <= cortes[1], 2).when(c <= cortes[2], 3)
            .when(c <= cortes[3], 4).otherwise(5))


def calcular(mm: DataFrame, p: ParamsIrat) -> tuple[DataFrame, DataFrame, dict]:
    """Devuelve (municipio_mes_irat, irat_sensibilidad, cortes por variante)."""
    base = percentiles(componentes_crudos(mm, p.mezcla_h))
    w_h = p.pesos["H"]
    variantes = {
        "irat": p.pesos,
        "irat_iguales": {x: 1.0 / len(COMPONENTES) for x in COMPONENTES},
        "irat_solo_clima": {x: (0.0 if x == "H" else w / (1.0 - w_h)) for x, w in p.pesos.items()},
    }
    for nombre, pesos in variantes.items():
        base = base.withColumn(nombre, suma_ponderada(pesos))
    base = base.cache()
    cortes = {nombre: cortes_quintiles(base, nombre) for nombre in variantes}
    base = (
        base.withColumn("irat_orden", orden_por_cortes("irat", cortes["irat"]))
        .withColumn("orden_iguales", orden_por_cortes("irat_iguales", cortes["irat_iguales"]))
        .withColumn("orden_solo_clima", orden_por_cortes("irat_solo_clima", cortes["irat_solo_clima"]))
        .withColumn("irat_categoria",
                    F.element_at(F.array(*[F.lit(c) for c in p.categorias]), F.col("irat_orden")))
    )
    return base.select(*COLS_IRAT), base.select(*COLS_SENSIBILIDAD), cortes


# --------------------------------------------------------------------------- #
# Validación
# --------------------------------------------------------------------------- #
def tasa_captura(df: DataFrame, col_orden: str, ordenes_alto: list[int]) -> float | None:
    """K5: fracción de detecciones que caen en municipio-mes de riesgo alto."""
    fila = df.agg(
        F.sum(F.when(F.col(col_orden).isin(ordenes_alto), F.col("n_det")).otherwise(0)).alias("alto"),
        F.sum("n_det").alias("total"),
    ).first()
    return None if not fila["total"] else fila["alto"] / fila["total"]


def validar(mm, out, sens, cortes, informe_cl, huella_reorden, p: ParamsIrat) -> tuple[Registro, dict]:
    r = Registro()
    publicado = bool(informe_cl and informe_cl.get("publicado"))
    r.add("IR-0", "climatology publicado (informe con publicado = true)", publicado, publicado, True)
    r.add("IR-1", "Σ pesos = 1 y pesos ≥ 0", abs(sum(p.pesos.values()) - 1) <= 1e-9,
          p.pesos, "Σ = 1")

    n_in = mm.count()
    filas_inf = (informe_cl or {}).get("conteos", {}).get("municipio_mes")
    eo = out.agg(
        F.count(F.lit(1)).alias("filas"),
        F.sum("n_det").alias("det"),
        *[F.sum(F.coalesce((F.col(f"c_{x}") < 0) | (F.col(f"c_{x}") > 1), F.lit(True)).cast("int"))
          .alias(f"mal_{x}") for x in COMPONENTES],
        F.sum(F.coalesce((F.col("irat") < 0) | (F.col("irat") > 1), F.lit(True)).cast("int")).alias("irat_mal"),
        F.sum(F.coalesce(~F.col("irat_orden").between(1, 5), F.lit(True)).cast("int")).alias("orden_mal"),
    ).first()
    n_pk = out.select("cod_mpio", "fecha_mes").distinct().count()
    r.add("IR-2", "filas = municipio_mes (= informe de climatology); PK única",
          eo["filas"] == n_in and n_pk == n_in and (filas_inf is None or filas_inf == n_in),
          {"filas": eo["filas"], "pk": n_pk, "informe": filas_inf}, n_in)
    mal_c = {x: eo[f"mal_{x}"] for x in COMPONENTES}
    r.add("IR-3", "c_X ∈ [0, 1] y sin nulos", sum(mal_c.values()) == 0, mal_c, 0)
    r.add("IR-4", "irat ∈ [0, 1] y sin nulos", eo["irat_mal"] == 0, eo["irat_mal"], 0)

    mapa = F.create_map(*[x for i, c in enumerate(p.categorias, 1) for x in (F.lit(i), F.lit(c))])
    incoh_cat = out.filter(F.coalesce(F.col("irat_categoria") != F.element_at(mapa, F.col("irat_orden")),
                                      F.lit(True))).count()
    r.add("IR-5", "irat_orden ∈ {1..5} y coherente con irat_categoria",
          eo["orden_mal"] == 0 and incoh_cat == 0,
          {"orden_fuera": eo["orden_mal"], "categoria_incoherente": incoh_cat}, 0)

    por_cat = {row["irat_orden"]: row for row in out.groupBy("irat_orden").agg(
        F.count(F.lit(1)).alias("n"), F.min("irat").alias("lo"), F.max("irat").alias("hi"),
        F.sum("n_det").alias("det")).collect()}
    ordenes = sorted(por_cat)
    monotona = all(por_cat[a]["hi"] < por_cat[b]["lo"] for a, b in zip(ordenes, ordenes[1:]))
    r.add("IR-6", "categorías monótonas: max(irat | k) < min(irat | k+1)", monotona,
          {k: [round(por_cat[k]["lo"], 6), round(por_cat[k]["hi"], 6)] for k in ordenes}, "creciente")
    proporciones = {p.categorias[k - 1]: round(por_cat[k]["n"] / eo["filas"], 4) for k in ordenes}
    fuera = {k: v for k, v in proporciones.items() if abs(v - 0.2) > 0.05}
    r.add("IR-7", "proporción por categoría en 20 % ± 5 pp (los empates la desvían)",
          not fuera and len(ordenes) == 5, proporciones, "0,15–0,25", nivel="advertencia")

    huella = huella_contenido(out, p.decimales_huella)
    r.add("IR-8", "determinismo: mismo resultado con otro orden físico y otro particionado",
          huella == huella_reorden, huella_reorden, huella)
    r.add("IR-9", "Σ n_det se conserva", eo["det"] == mm.agg(F.sum("n_det")).first()[0],
          eo["det"], "Σ municipio_mes")
    n_sens = sens.count()
    r.add("IR-10", "irat_sensibilidad tiene una fila por municipio-mes", n_sens == n_in, n_sens, n_in)

    h_min = out.agg(F.min("c_H")).first()[0]
    indicadores = {
        "cortes": cortes,
        "distribucion": {p.categorias[k - 1]: {"celdas": por_cat[k]["n"], "detecciones": por_cat[k]["det"]}
                         for k in ordenes},
        # Orden de lectura: primero "solo clima" y luego el oficial. H usa detecciones
        # pasadas (circularidad), así que la diferencia es lo que aporta la propensión.
        "K5_tasa_captura": {
            "irat_solo_clima": tasa_captura(sens, "orden_solo_clima", p.ordenes_alto),
            "irat": tasa_captura(out, "irat_orden", p.ordenes_alto),
            "irat_iguales": tasa_captura(sens, "orden_iguales", p.ordenes_alto),
        },
        "categorias_alto": [p.categorias[k - 1] for k in p.ordenes_alto],
        "celdas_c_H_minimo": out.filter(F.col("c_H") == h_min).count(),
    }
    conteos = {"municipio_mes": n_in, "municipio_mes_irat": eo["filas"],
               "irat_sensibilidad": n_sens, "detecciones": eo["det"]}
    return r, {"conteos": conteos, "indicadores": indicadores, "huella": huella}


# --------------------------------------------------------------------------- #
# Orquestación
# --------------------------------------------------------------------------- #
def ejecutar(spark: SparkSession, cfg: dict, publicar: bool = True) -> dict:
    p = ParamsIrat.desde_cfg(cfg)
    t: dict[str, float] = {}
    t_total = time.perf_counter()
    reportes = p.inter / "_reports"
    informe_cl = leer_informe(reportes / "climatology_validation.json")

    mm = spark.read.parquet(str(p.inter / "municipio_mes")).cache()

    t0 = time.perf_counter()
    out, sens, cortes = calcular(mm, p)
    out, sens = out.cache(), sens.cache()
    out.count()
    t["calculo_s"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    # Misma entrada con otro orden físico y otro número de particiones (IR-8).
    mm_reorden = mm.orderBy(F.col("cod_mpio").desc(), F.col("fecha_mes").desc()).repartition(5)
    out_2, _, _ = calcular(mm_reorden, p)
    huella_reorden = huella_contenido(out_2, p.decimales_huella)
    reg, res = validar(mm, out, sens, cortes, informe_cl, huella_reorden, p)
    t["validacion_s"] = time.perf_counter() - t0

    informe = {
        "modulo": "firewatch.spark.irat",
        "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
        "spark": {
            "version": spark.version,
            "master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
        },
        "params": {k: (v if isinstance(v, (dict, list, float)) else str(v))
                   for k, v in asdict(p).items()},
        "entrada_climatology": (informe_cl or {}).get("huellas"),
        "conteos": res["conteos"],
        "indicadores": res["indicadores"],
        "esquemas": {"municipio_mes_irat": out.schema.simpleString(),
                     "irat_sensibilidad": sens.schema.simpleString()},
        "checks": [asdict(c) for c in reg.checks],
        "publicado": False,
    }
    reporte = reportes / "irat_validation.json"

    if reg.fallas:
        informe["tiempos"] = {k: round(v, 3) for k, v in t.items()}
        _escribir_json(informe, reporte)
        reg.exigir()

    informe["huellas"] = {
        "municipio_mes_irat": res["huella"],
        "irat_sensibilidad": huella_contenido(sens, p.decimales_huella),
    }

    if publicar:
        t0 = time.perf_counter()
        tmps = {
            "municipio_mes_irat": _escribir_tmp(out.orderBy("cod_mpio", "fecha_mes"),
                                                p.inter / "municipio_mes_irat", n_archivos=1),
            "irat_sensibilidad": _escribir_tmp(sens.orderBy("cod_mpio", "fecha_mes"),
                                               p.inter / "irat_sensibilidad", n_archivos=1),
        }
        for nombre, tmp in tmps.items():
            _intercambiar(tmp, p.inter / nombre)
        t["escritura_s"] = time.perf_counter() - t0
        informe["publicado"] = True

    t["total_s"] = time.perf_counter() - t_total
    informe["tiempos"] = {k: round(v, 3) for k, v in t.items()}
    _escribir_json(informe, reporte)
    for df in (mm, out, sens):
        df.unpersist()
    log.info("irat terminado · conteos=%s · K5=%s", res["conteos"],
             res["indicadores"]["K5_tasa_captura"])
    return informe


def main() -> None:
    ap = argparse.ArgumentParser(description="FireWatch · IRAT (L3)")
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
