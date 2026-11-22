"""L3 → gold · Modelo estrella para Power BI, conciliación y manifiesto (data freeze).

Spark arma y filtra las tablas; como la más grande tiene 7.134 filas, cada tabla se
pasa a pandas y se escribe en UN archivo con nombre estable (``fact_municipio_mes.parquet``),
para que Power BI no pierda la conexión en cada corrida.

Si falla un check bloqueante no se publica nada. La publicación es atómica: se escribe
todo en ``<gold>.tmp`` y al final se intercambia la carpeta completa.

Salidas (``rutas.gold``, por defecto ``data/gold``):

    fact_municipio_mes.parquet   7.134   (contrato §6.6, orden exacto de columnas)
    fact_deteccion.parquet       detecciones de Boyacá sin fuentes estáticas
    agg_depto_mes.parquet        1.914
    dim_mes.parquet              58
    dim_municipio.parquet        123     (silver sin area_oficial_km2 ni metodo_centroide)
    dim_departamento.parquet     33
    _analisis/irat_sensibilidad.parquet
    csv/<tabla>.csv              copia de entrega (utf-8-sig). Power BI usa los Parquet.
    _reports/gold_validation.json  checks G-0…G-7 + KPI de control K1…K6
    _reports/gold_manifest.json    filas, columnas, SHA-256 y huella por tabla + entradas

Uso::

    python -m firewatch.spark.export_gold
    python -m firewatch.spark.export_gold --solo-validar
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from firewatch.spark.build_daily import CheckError, Registro, _escribir_json
from firewatch.spark.climatology import leer_informe
from firewatch.spark.irat import ParamsIrat
from firewatch.spark.session import cargar_config, crear_sesion

log = logging.getLogger(__name__)

NOMBRES_MES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
               "septiembre", "octubre", "noviembre", "diciembre"]

# Contratos gold (§6.6). Ninguna columna más, ninguna menos, en este orden.
COLS = {
    "fact_municipio_mes": [
        "cod_mpio", "fecha_mes", "n_det", "frp_total", "frp_medio", "det_100km2", "precip_mes",
        "tmax_media", "rh_min_media", "wind_max_media", "racha_seca_max", "anom_precip",
        "anom_tmax", "anom_rh", "det_3m_previos", "c_H", "c_P", "c_D", "c_T", "c_R", "c_W",
        "irat", "irat_categoria", "irat_orden",
    ],
    "fact_deteccion": ["id_det", "cod_mpio", "fecha", "fecha_mes", "hora_utc", "lat", "lon",
                       "frp", "confianza", "daynight", "satelite"],
    "agg_depto_mes": ["cod_dpto", "fecha_mes", "n_det", "frp_total"],
    "dim_mes": ["fecha_mes", "anio", "mes", "nombre_mes", "trimestre"],
    "dim_departamento": ["cod_dpto", "nombre_dpto"],
    # dim_municipio: columnas de silver menos las que no pasan a gold (se valida aparte)
}
EXCLUIR_DIM_MPIO = ["area_oficial_km2", "metodo_centroide"]
PK = {
    "fact_municipio_mes": ["cod_mpio", "fecha_mes"],
    "fact_deteccion": ["id_det"],
    "agg_depto_mes": ["cod_dpto", "fecha_mes"],
    "dim_mes": ["fecha_mes"],
    "dim_municipio": ["cod_mpio"],
    "dim_departamento": ["cod_dpto"],
    "irat_sensibilidad": ["cod_mpio", "fecha_mes"],
}
TABLAS = ["fact_municipio_mes", "fact_deteccion", "agg_depto_mes", "dim_mes",
          "dim_municipio", "dim_departamento"]


# --------------------------------------------------------------------------- #
# Parámetros
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ParamsGold:
    silver: Path
    inter: Path
    gold: Path
    inicio: str
    fin: str
    dpto_boyaca: str
    ordenes_alto: list
    decimales_huella: int

    CLAVES = (
        ("rutas", "silver"), ("rutas", "intermedio"),
        ("ventana", "fecha_inicio"), ("ventana", "fecha_fin"),
        ("territorio", "cod_dpto_foco"),
    )

    @classmethod
    def desde_cfg(cls, cfg: dict) -> "ParamsGold":
        faltan = [".".join(k) for k in cls.CLAVES if k[1] not in (cfg.get(k[0]) or {})]
        if faltan:
            raise KeyError(f"Faltan claves en settings.yaml: {faltan}")
        return cls(
            silver=Path(cfg["rutas"]["silver"]),
            inter=Path(cfg["rutas"]["intermedio"]),
            gold=Path(cfg["rutas"].get("gold", "data/gold")),
            inicio=str(cfg["ventana"]["fecha_inicio"]),
            fin=str(cfg["ventana"]["fecha_fin"]),
            dpto_boyaca=str(cfg["territorio"]["cod_dpto_foco"]),
            ordenes_alto=ParamsIrat.desde_cfg(cfg).ordenes_alto,
            decimales_huella=int((cfg.get("spark") or {}).get("decimales_huella", 6)),
        )

    @property
    def n_meses(self) -> int:
        a, b = date.fromisoformat(self.inicio), date.fromisoformat(self.fin)
        return (b.year - a.year) * 12 + (b.month - a.month) + 1


# --------------------------------------------------------------------------- #
# Construcción de las tablas (Spark)
# --------------------------------------------------------------------------- #
def dim_mes(spark: SparkSession, p: ParamsGold) -> DataFrame:
    """Meses de la ventana. nombre_mes en español sin depender del locale del sistema."""
    nombres = F.array(*[F.lit(n) for n in NOMBRES_MES])
    return (
        spark.range(1)
        .select(F.explode(F.sequence(
            F.trunc(F.lit(p.inicio).cast("date"), "month"),
            F.trunc(F.lit(p.fin).cast("date"), "month"),
            F.expr("interval 1 month"),
        )).alias("fecha_mes"))
        .withColumn("anio", F.year("fecha_mes").cast("int"))
        .withColumn("mes", F.month("fecha_mes").cast("int"))
        .withColumn("nombre_mes", F.element_at(nombres, F.col("mes")))
        .withColumn("trimestre", F.quarter("fecha_mes").cast("int"))
        .select(*COLS["dim_mes"])
    )


def fact_deteccion(det: DataFrame, p: ParamsGold) -> DataFrame:
    """Mismo filtro que build_daily: Boyacá, sin fuentes estáticas, con municipio, en ventana."""
    return (
        det.filter(
            (F.col("cod_dpto") == p.dpto_boyaca)
            & (~F.col("fuente_estatica"))
            & F.col("cod_mpio").isNotNull()
            & F.col("fecha").between(F.lit(p.inicio).cast("date"), F.lit(p.fin).cast("date"))
        )
        .withColumn("fecha_mes", F.trunc("fecha", "month"))
        .select(*COLS["fact_deteccion"])
    )


def construir(spark: SparkSession, p: ParamsGold) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, int]:
    """Devuelve (tablas gold en pandas, irat_sensibilidad, detecciones Boyacá en silver)."""
    s, i = (lambda n: str(p.silver / n)), (lambda n: str(p.inter / n))  # noqa: E731
    det = spark.read.parquet(s("detecciones")).select(
        F.col("id_det").cast("string"), F.col("cod_dpto").cast("string"),
        F.col("cod_mpio").cast("string"), F.col("fecha").cast("date"),
        F.col("hora_utc").cast("int"), F.col("lat").cast("double"), F.col("lon").cast("double"),
        F.col("frp").cast("double"), F.col("confianza").cast("string"),
        F.col("daynight").cast("string"), F.col("satelite").cast("string"),
        F.col("fuente_estatica").cast("boolean"),
    )
    dim_m = spark.read.parquet(s("dim_municipio"))
    dim_dpto = spark.read.parquet(s("dim_departamento"))
    sdf = {
        "fact_municipio_mes": spark.read.parquet(i("municipio_mes_irat")).select(*COLS["fact_municipio_mes"]),
        "fact_deteccion": fact_deteccion(det, p),
        "agg_depto_mes": spark.read.parquet(i("agg_depto_mes")).select(*COLS["agg_depto_mes"]),
        "dim_mes": dim_mes(spark, p),
        "dim_municipio": dim_m.select(*[c for c in dim_m.columns if c not in EXCLUIR_DIM_MPIO]),
        "dim_departamento": dim_dpto.select(
            F.col("cod_dpto").cast("string"),
            # silver real usa `nombre`; el contrato gold (§6.6) exige `nombre_dpto`.
            F.col("nombre_dpto" if "nombre_dpto" in dim_dpto.columns else "nombre")
            .cast("string").alias("nombre_dpto")),
    }
    tablas = {n: _a_pandas(df, PK[n]) for n, df in sdf.items()}
    sens = _a_pandas(spark.read.parquet(i("irat_sensibilidad")), PK["irat_sensibilidad"])
    # Detecciones de Boyacá en silver contadas sin pasar por ninguna tabla intermedia (G-3).
    n_silver = det.filter((F.col("cod_dpto") == p.dpto_boyaca) & (~F.col("fuente_estatica"))
                          & F.col("cod_mpio").isNotNull()
                          & F.col("fecha").between(F.lit(p.inicio).cast("date"),
                                                   F.lit(p.fin).cast("date"))).count()
    return tablas, sens, n_silver


def _a_pandas(df: DataFrame, pk: list[str]) -> pd.DataFrame:
    return df.toPandas().sort_values(pk, kind="mergesort").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Huellas
# --------------------------------------------------------------------------- #
def huella_pandas(df: pd.DataFrame, decimales: int) -> str:
    """SHA-256 del contenido (orden por PK; double redondeados para que el último bit no cuente)."""
    d = df.copy()
    for c in d.columns:
        if pd.api.types.is_float_dtype(d[c]):
            d[c] = d[c].round(decimales)
        elif d[c].dtype == object:
            d[c] = d[c].map(lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v))
    h = pd.util.hash_pandas_object(d, index=False).values
    return hashlib.sha256(h.tobytes() + ",".join(d.columns).encode()).hexdigest()


def sha256_archivo(ruta: Path) -> str:
    sha = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            sha.update(bloque)
    return sha.hexdigest()


# --------------------------------------------------------------------------- #
# Validación (sobre pandas: son tablas pequeñas)
# --------------------------------------------------------------------------- #
def validar(t: dict[str, pd.DataFrame], sens: pd.DataFrame, n_silver: int,
            informes: dict, p: ParamsGold) -> Registro:
    r = Registro()
    pub = {k: bool(v and v.get("publicado")) for k, v in informes.items()}
    r.add("G-0", "build_daily, climatology e irat publicados", all(pub.values()), pub, "todos true")

    dup = {n: int(df.duplicated(PK[n]).sum()) for n, df in t.items()}
    dup["irat_sensibilidad"] = int(sens.duplicated(PK["irat_sensibilidad"]).sum())
    r.add("G-1", "PK única en todas las tablas", sum(dup.values()) == 0, dup, 0)

    mpios, meses = set(t["dim_municipio"].cod_mpio), set(t["dim_mes"].fecha_mes)
    huerf = {
        "fact_municipio_mes.cod_mpio": int((~t["fact_municipio_mes"].cod_mpio.isin(mpios)).sum()),
        "fact_deteccion.cod_mpio": int((~t["fact_deteccion"].cod_mpio.isin(mpios)).sum()),
        "fact_municipio_mes.fecha_mes": int((~t["fact_municipio_mes"].fecha_mes.isin(meses)).sum()),
        "fact_deteccion.fecha_mes": int((~t["fact_deteccion"].fecha_mes.isin(meses)).sum()),
        "agg_depto_mes.fecha_mes": int((~t["agg_depto_mes"].fecha_mes.isin(meses)).sum()),
        "agg_depto_mes.cod_dpto": int((~t["agg_depto_mes"].cod_dpto
                                       .isin(set(t["dim_departamento"].cod_dpto))).sum()),
    }
    r.add("G-2", "integridad referencial hechos → dimensiones", sum(huerf.values()) == 0, huerf, 0)

    agg = t["agg_depto_mes"]
    vias = {
        "silver": int(n_silver),
        "fact_municipio_mes": int(t["fact_municipio_mes"].n_det.sum()),
        "fact_deteccion": int(len(t["fact_deteccion"])),
        "agg_depto_mes_boyaca": int(agg.loc[agg.cod_dpto == p.dpto_boyaca, "n_det"].sum()),
    }
    r.add("G-3", "conciliación de 4 vías de las detecciones de Boyacá (K1)",
          len(set(vias.values())) == 1, vias, f"iguales ({n_silver})")

    n_mpio, n_dpto = len(t["dim_municipio"]), len(t["dim_departamento"])
    filas = {n: len(df) for n, df in t.items()}
    esperado = {"fact_municipio_mes": n_mpio * p.n_meses, "agg_depto_mes": n_dpto * p.n_meses,
                "dim_mes": p.n_meses}
    r.add("G-4", "conteos de las grillas (municipios × meses, dptos × meses, meses)",
          all(filas[k] == v for k, v in esperado.items()) and len(sens) == filas["fact_municipio_mes"],
          filas, esperado)

    esquema_mal = {n: list(t[n].columns) for n in COLS if list(t[n].columns) != COLS[n]}
    sobran = [c for c in EXCLUIR_DIM_MPIO if c in t["dim_municipio"].columns]
    if sobran:
        esquema_mal["dim_municipio"] = sobran
    r.add("G-5", "esquema y orden de columnas = contrato §6.6", not esquema_mal, esquema_mal or "ok", "ok")

    nulos_pk = {n: int(df[PK[n]].isna().sum().sum()) for n, df in t.items()}
    r.add("G-6", "sin nulos en las claves", sum(nulos_pk.values()) == 0, nulos_pk, 0)

    prov = bool("provincia" in t["dim_municipio"].columns
                and t["dim_municipio"]["provincia"].notna().all())
    r.add("G-7", "dim_municipio con provincia (jerarquía del dashboard, R11)", prov, prov, True,
          nivel="advertencia")
    return r


def kpis(t: dict[str, pd.DataFrame], p: ParamsGold) -> dict:
    """Valores de control para comparar con las tarjetas de Power BI."""
    fm, agg = t["fact_municipio_mes"], t["agg_depto_mes"]
    k1 = int(fm.n_det.sum())
    ultimo = fm.fecha_mes.max()
    anio_u, mes_u = ultimo.year, ultimo.month
    anios = pd.to_datetime(fm.fecha_mes)
    ytd = lambda a: int(fm.loc[(anios.dt.year == a) & (anios.dt.month <= mes_u), "n_det"].sum())  # noqa: E731
    actual, previo = ytd(anio_u), ytd(anio_u - 1)
    alto = fm.irat_orden.isin(p.ordenes_alto)
    return {
        "K1_detecciones": k1,
        "K2_var_interanual": {"periodo": f"ene–{NOMBRES_MES[mes_u - 1]} {anio_u} vs {anio_u - 1}",
                              "actual": actual, "previo": previo,
                              "variacion": (actual - previo) / previo if previo else None},
        "K3_frp_medio_mw": float(t["fact_deteccion"].frp.mean()) if k1 else None,
        "K4_municipios_riesgo_alto_ultimo_mes": {
            "mes": ultimo.isoformat(),
            "municipios": int(fm.loc[alto & (fm.fecha_mes == ultimo), "cod_mpio"].nunique())},
        "K5_tasa_captura": float(fm.loc[alto, "n_det"].sum() / k1) if k1 else None,
        "K6_participacion_boyaca": float(agg.loc[agg.cod_dpto == p.dpto_boyaca, "n_det"].sum()
                                         / agg.n_det.sum()) if agg.n_det.sum() else None,
        "denominador_K6_nacional": int(agg.n_det.sum()),
    }


# --------------------------------------------------------------------------- #
# Escritura y orquestación
# --------------------------------------------------------------------------- #
def _commit_git() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                              text=True, timeout=10, check=True).stdout.strip()
    except Exception:  # noqa: BLE001 - sin git el manifiesto se escribe igual
        return None


def _escribir(df: pd.DataFrame, carpeta: Path, nombre: str) -> Path:
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{nombre}.parquet"
    df.to_parquet(ruta, index=False)
    return ruta


def _publicar_carpeta(tmp: Path, destino: Path) -> None:
    """Intercambia la carpeta gold completa; conserva archivos ocultos (.gitkeep, etc.)."""
    if destino.exists():
        for f in destino.iterdir():
            if f.name.startswith(".") and f.is_file():
                shutil.copy2(f, tmp / f.name)
    viejo = destino.with_name(destino.name + ".old")
    if viejo.exists():
        shutil.rmtree(viejo)
    if destino.exists():
        os.replace(destino, viejo)
    os.replace(tmp, destino)
    shutil.rmtree(viejo, ignore_errors=True)


def ejecutar(spark: SparkSession, cfg: dict, publicar: bool = True) -> dict:
    p = ParamsGold.desde_cfg(cfg)
    t0 = time.perf_counter()
    rep_int = p.inter / "_reports"
    informes = {m: leer_informe(rep_int / f"{m}_validation.json")
                for m in ("build_daily", "climatology", "irat")}

    tablas, sens, n_silver = construir(spark, p)
    reg = validar(tablas, sens, n_silver, informes, p)
    informe = {
        "modulo": "firewatch.spark.export_gold",
        "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
        "params": {k: str(v) for k, v in asdict(p).items()},
        "filas": {n: len(df) for n, df in tablas.items()} | {"irat_sensibilidad": len(sens)},
        "kpis_control": kpis(tablas, p),
        "checks": [asdict(c) for c in reg.checks],
        "publicado": False,
    }
    tmp = p.gold.with_name(p.gold.name + ".tmp")
    if tmp.exists():
        shutil.rmtree(tmp)

    if reg.fallas:
        informe["tiempo_s"] = round(time.perf_counter() - t0, 3)
        _escribir_json(informe, rep_int / "gold_validation_fallida.json")
        reg.exigir()

    manifiesto = {
        "fecha": informe["fecha_ejecucion"],
        "commit": _commit_git(),
        "entradas": {m: (inf or {}).get("huellas") for m, inf in informes.items()},
        "tablas": {},
    }
    if publicar:
        todas = dict(tablas) | {"irat_sensibilidad": sens}
        for nombre, df in todas.items():
            carpeta = tmp / "_analisis" if nombre == "irat_sensibilidad" else tmp
            ruta = _escribir(df, carpeta, nombre)
            (tmp / "csv").mkdir(parents=True, exist_ok=True)
            df.to_csv(tmp / "csv" / f"{nombre}.csv", index=False, encoding="utf-8-sig")
            # Se relee lo escrito: la huella del archivo debe ser la de la tabla en memoria.
            releida = pd.read_parquet(ruta)
            manifiesto["tablas"][nombre] = {
                "archivo": str(ruta.relative_to(tmp)).replace("\\", "/"),
                "filas": len(df),
                "columnas": list(df.columns),
                "sha256_archivo": sha256_archivo(ruta),
                "huella_contenido": huella_pandas(df, p.decimales_huella),
                "relectura_ok": len(releida) == len(df) and list(releida.columns) == list(df.columns),
            }
        if not all(v["relectura_ok"] for v in manifiesto["tablas"].values()):
            raise CheckError("La relectura de algún Parquet gold no coincide con lo escrito")
        informe["publicado"] = True
        informe["tiempo_s"] = round(time.perf_counter() - t0, 3)
        _escribir_json(informe, tmp / "_reports" / "gold_validation.json")
        _escribir_json(manifiesto, tmp / "_reports" / "gold_manifest.json")
        _publicar_carpeta(tmp, p.gold)
    else:
        informe["tiempo_s"] = round(time.perf_counter() - t0, 3)
    informe["manifiesto"] = manifiesto
    log.info("export_gold terminado · filas=%s · KPI=%s", informe["filas"], informe["kpis_control"])
    return informe


def main() -> None:
    ap = argparse.ArgumentParser(description="FireWatch · export_gold (L3 → gold)")
    ap.add_argument("--config", default="config/settings.yaml")
    ap.add_argument("--master", default=None)
    ap.add_argument("--particiones", type=int, default=None)
    ap.add_argument("--solo-validar", action="store_true", help="valida sin publicar")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = cargar_config(a.config)
    spark = crear_sesion(cfg, a.master, a.particiones)
    try:
        ejecutar(spark, cfg, publicar=not a.solo_validar)
    except CheckError as e:
        log.error("No se publicó: %s", e)
        raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
