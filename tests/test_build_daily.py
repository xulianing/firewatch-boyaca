"""Tests de firewatch.spark.build_daily (L3).

Usan una SparkSession local[2] y un silver sintético con la misma estructura que el real
(Hive por ``anio``, ``ts_local`` sin zona, ``fecha`` precalculada, ``wind`` en km/h).
Ventana de prueba: 2024-01-01 → 2024-02-10 (41 días, 2 meses), 2 municipios, 2 departamentos.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from firewatch.spark import build_daily as bd  # noqa: E402
from firewatch.spark.session import crear_sesion  # noqa: E402

INI, FIN = date(2024, 1, 1), date(2024, 2, 10)
N_DIAS = (FIN - INI).days + 1  # 41
MPIOS = ["15001", "15002"]
DPTOS = ["15", "05"]

ESQ_H = T.StructType([
    T.StructField("cod_mpio", T.StringType()),
    T.StructField("ts_local", T.TimestampNTZType()),
    T.StructField("fecha", T.DateType()),
    T.StructField("temp", T.DoubleType()),
    T.StructField("rh", T.DoubleType()),
    T.StructField("precip", T.DoubleType()),
    T.StructField("wind", T.DoubleType()),
    T.StructField("et0", T.DoubleType()),
    T.StructField("anio", T.IntegerType()),
])
ESQ_DET = T.StructType([
    T.StructField("cod_dpto", T.StringType()),
    T.StructField("cod_mpio", T.StringType()),
    T.StructField("fecha", T.DateType()),
    T.StructField("frp", T.DoubleType()),
    T.StructField("fuente_estatica", T.BooleanType()),
    T.StructField("anio", T.IntegerType()),
])


def cfg_prueba(base: Path) -> dict:
    return {
        "rutas": {"silver": str(base / "silver"), "intermedio": str(base / "int")},
        "ventana": {"fecha_inicio": INI.isoformat(), "fecha_fin": FIN.isoformat()},
        "territorio": {"cod_dpto_foco": "15"},
        "spark": {"master_produccion": "local[2]", "shuffle_partitions_produccion": 4,
                  "driver_memory": "1g", "ventana_precip_dias": 3, "umbral_dia_seco_mm": 1.0},
    }


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    s = crear_sesion(cfg_prueba(Path(".")))
    s.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    s.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    yield s
    s.stop()


def crear_df(spark, filas, esquema):
    """Crea un DataFrame sin workers de Python (pandas + Arrow → JVM).

    En el PC de ejecución (Windows + Python 3.12 + PySpark 3.5) los workers de Python
    mueren al arrancar ("Python worker exited unexpectedly"). El código de producción no
    los usa; los tests tampoco deben depender de ellos.
    """
    import pandas as pd

    st = esquema if isinstance(esquema, T.StructType) else T._parse_datatype_string(esquema)
    if not filas:  # vacío: se construye en la JVM, sin datos que transferir
        return spark.range(0).select(
            *[F.lit(None).cast(f.dataType).alias(f.name) for f in st.fields])
    pdf = pd.DataFrame([tuple(f) for f in filas], columns=[f.name for f in st.fields])
    return spark.createDataFrame(pdf, schema=st)


def params(base: Path = Path("."), **kw) -> bd.Params:
    p = bd.Params.desde_cfg(cfg_prueba(base))
    return bd.Params(**{**p.__dict__, **kw})


# --------------------------------------------------------------------------- #
# Generador de silver sintético
# --------------------------------------------------------------------------- #
def lluvia_diaria(mpio: str, d: int) -> float:
    """15001: llueve cada 5 días (rachas secas de 4); 15002: siempre llueve."""
    if mpio == "15002":
        return 2.4
    return 2.4 if d % 5 == 0 else 0.0


def filas_horarias() -> list[tuple]:
    filas = []
    for m in MPIOS:
        for d in range(N_DIAS):
            dia = INI + timedelta(days=d)
            p_h = lluvia_diaria(m, d) / 24
            for h in range(24):
                ts = datetime(dia.year, dia.month, dia.day, h)
                filas.append((m, ts, dia, 10.0 + h, 50.0 + h, p_h, float(h), 0.01, dia.year))
    return filas


def filas_detecciones() -> list[tuple]:
    return [
        ("15", "15001", date(2024, 1, 5), 10.0, False, 2024),
        ("15", "15001", date(2024, 1, 5), 20.0, False, 2024),
        ("15", "15002", date(2024, 2, 3), 5.0, False, 2024),
        ("15", "15001", date(2024, 1, 7), 99.0, True, 2024),   # fuente estática
        ("05", "05001", date(2024, 1, 10), 3.0, False, 2024),  # otro departamento
        (None, None, date(2024, 1, 12), 1.0, False, 2024),     # fuera de tierra
    ]


def escribir_silver(spark, base: Path, horarias=None, detecciones=None) -> None:
    s = base / "silver"
    h = crear_df(spark, horarias if horarias is not None else filas_horarias(), ESQ_H)
    h.write.mode("overwrite").partitionBy("anio").parquet(str(s / "clima_horario"))
    det = crear_df(spark,
        detecciones if detecciones is not None else filas_detecciones(), ESQ_DET)
    det.write.mode("overwrite").partitionBy("anio").parquet(str(s / "detecciones"))
    crear_df(spark, [(m,) for m in MPIOS], "cod_mpio string").write.mode("overwrite") \
        .parquet(str(s / "dim_municipio"))
    crear_df(spark, [(d,) for d in DPTOS], "cod_dpto string").write.mode("overwrite") \
        .parquet(str(s / "dim_departamento"))


# --------------------------------------------------------------------------- #
# Transformaciones puras
# --------------------------------------------------------------------------- #
def test_clima_diario_agregaciones(spark):
    dia = date(2024, 1, 1)
    filas = [("15001", None, dia, None, 10.0 + h, 50.0 + h, 0.1, float(h), 0.01) for h in range(24)]
    esq = "cod_mpio string, ts_local string, fecha date, dia_ts date, temp double, rh double, " \
          "precip double, wind double, et0 double"
    r = bd.clima_diario(crear_df(spark, filas, esq)).first()
    assert r["tmax"] == 33.0
    assert r["tmean"] == pytest.approx(21.5)
    assert r["precip"] == pytest.approx(2.4)
    assert r["rh_min"] == 50.0
    assert r["wind_max"] == 23.0
    assert r["et0"] == pytest.approx(0.24)
    assert r["n_horas"] == 24


def test_clima_diario_esquema(spark):
    esq = "cod_mpio string, fecha date, temp double, rh double, precip double, wind double, et0 double"
    df = bd.clima_diario(crear_df(spark, [("15001", date(2024, 1, 1), 1.0, 1.0, 1.0, 1.0, 1.0)], esq))
    assert df.columns == bd.COLS_CLIMA_DIARIO
    tipos = dict(df.dtypes)
    assert tipos["cod_mpio"] == "string" and tipos["fecha"] == "date" and tipos["n_horas"] == "int"
    assert all(tipos[c] == "double" for c in bd.VARS_DIARIAS)


def test_detecciones_boyaca_filtra_estaticas_otros_dptos_y_nulos(spark):
    det = crear_df(spark, filas_detecciones(), ESQ_DET)
    out = bd.detecciones_boyaca(det, params())
    assert out.count() == 3
    assert out.agg(F.sum("frp")).first()[0] == 35.0


def _clima_sintetico(spark, mpio="15001", n=10, lluvia=None):
    lluvia = lluvia or [0.0] * n
    filas = [(mpio, INI + timedelta(days=d), 20.0, 15.0, lluvia[d], 50.0, 5.0, 2.0)
             for d in range(n)]
    return crear_df(spark,
        filas, "cod_mpio string, fecha date, tmax double, tmean double, precip double, "
               "rh_min double, wind_max double, et0 double")


def test_municipio_dia_rellena_ceros_y_cuenta(spark):
    clima = _clima_sintetico(spark, n=5)
    det = crear_df(spark,
        [("15001", INI + timedelta(days=1), 10.0), ("15001", INI + timedelta(days=1), 2.5)],
        "cod_mpio string, fecha date, frp double")
    md = bd.municipio_dia(clima, det, params()).orderBy("fecha").collect()
    assert len(md) == 5
    assert [r["n_det"] for r in md] == [0, 2, 0, 0, 0]
    assert md[1]["frp_sum"] == 12.5 and md[0]["frp_sum"] == 0.0
    assert md[0].asDict().keys() == set(bd.COLS_MUNICIPIO_DIA)


def test_precip_30d_ventana_y_nulos_iniciales(spark):
    clima = _clima_sintetico(spark, n=6, lluvia=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    vacio = crear_df(spark, [], "cod_mpio string, fecha date, frp double")
    md = bd.municipio_dia(clima, vacio, params(ventana_precip=3)).orderBy("fecha").collect()
    assert [r["precip_30d"] for r in md] == [None, None, 6.0, 9.0, 12.0, 15.0]


def test_rachas_secas_gaps_and_islands(spark):
    # S S H S S S H  (S = seco < 1 mm, H = húmedo)
    lluvia = [0.0, 0.5, 5.0, 0.0, 0.0, 0.9, 3.0]
    clima = _clima_sintetico(spark, n=7, lluvia=lluvia)
    vacio = crear_df(spark, [], "cod_mpio string, fecha date, frp double")
    md = bd.municipio_dia(clima, vacio, params()).orderBy("fecha").collect()
    assert [r["dia_seco"] for r in md] == [True, True, False, True, True, True, False]
    ids = [r["id_racha"] for r in md]
    assert ids[2] is None and ids[6] is None
    assert ids[0] == ids[1] and ids[3] == ids[4] == ids[5] and ids[0] != ids[3]
    largos = sorted(sum(1 for x in ids if x == i) for i in {x for x in ids if x})
    assert largos == [2, 3]


def test_agg_depto_mes_grilla_completa_con_ceros(spark):
    p = params()
    det = crear_df(spark, filas_detecciones(), ESQ_DET)
    dpto = crear_df(spark, [(d,) for d in DPTOS], "cod_dpto string")
    ad = bd.agg_depto_mes(det, dpto, bd.meses_ventana(spark, p), p)
    assert ad.count() == len(DPTOS) * p.n_meses == 4
    assert ad.columns == bd.COLS_AGG_DEPTO_MES
    filas = {(r["cod_dpto"], r["fecha_mes"]): r["n_det"] for r in ad.collect()}
    assert filas[("15", date(2024, 1, 1))] == 2
    assert filas[("15", date(2024, 2, 1))] == 1
    assert filas[("05", date(2024, 1, 1))] == 1
    assert filas[("05", date(2024, 2, 1))] == 0


def test_huella_invariante_al_particionado(spark):
    esq = "cod_mpio string, fecha date, temp double, rh double, precip double, wind double, et0 double"
    filas = [(m, INI + timedelta(days=d), 10.0 + h / 7, 50.0, 0.1 / 3, 1.0, 0.01 / 3)
             for m in MPIOS for d in range(20) for h in range(24)]
    h = crear_df(spark, filas, esq)
    huellas = []
    for n in (1, 3, 7):
        spark.conf.set("spark.sql.shuffle.partitions", str(n))
        huellas.append(bd.huella_contenido(bd.clima_diario(h.repartition(n)), 6))
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    assert len(set(huellas)) == 1


def test_params_exige_claves(tmp_path):
    cfg = cfg_prueba(tmp_path)
    del cfg["spark"]["umbral_dia_seco_mm"]
    with pytest.raises(KeyError, match="umbral_dia_seco_mm"):
        bd.Params.desde_cfg(cfg)


# --------------------------------------------------------------------------- #
# Lectura de ts_local
# --------------------------------------------------------------------------- #
def test_ts_local_en_nanosegundos_desde_pandas(spark, tmp_path):
    """Reproduce el riesgo R1: pandas/pyarrow escriben TIMESTAMP(NANOS)."""
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({
        "cod_mpio": ["15001"] * 48,
        "ts_local": pd.date_range("2024-01-01 00:00", periods=48, freq="h"),
        "temp": 1.0, "rh": 1.0, "precip": 0.0, "wind": 1.0, "et0": 0.0,
    })
    df["fecha"] = df["ts_local"].dt.date
    destino = tmp_path / "silver" / "clima_horario" / "anio=2024"
    destino.mkdir(parents=True)
    df.to_parquet(destino / "part-0000.parquet", index=False)
    h = bd.leer_horario(spark, params(tmp_path))
    assert h.filter(F.col("fecha") != F.col("dia_ts")).count() == 0
    assert h.select("fecha").distinct().count() == 2


# --------------------------------------------------------------------------- #
# Punta a punta con escritura real
# --------------------------------------------------------------------------- #
def test_ejecutar_publica_y_reporta(spark, tmp_path):
    escribir_silver(spark, tmp_path)
    inf = bd.ejecutar(spark, cfg_prueba(tmp_path))
    assert inf["publicado"] is True
    assert all(c["ok"] for c in inf["checks"] if c["nivel"] == "bloqueante")
    assert inf["conteos"]["clima_horario"] == len(MPIOS) * N_DIAS * 24
    assert inf["conteos"]["clima_diario"] == len(MPIOS) * N_DIAS
    assert inf["conteos"]["detecciones_boyaca"] == 3
    assert inf["conteos"]["detecciones_nacionales_sin_estaticas"] == 4

    inter = tmp_path / "int"
    cd = spark.read.parquet(str(inter / "clima_diario"))
    assert cd.columns == bd.COLS_CLIMA_DIARIO and cd.count() == len(MPIOS) * N_DIAS
    md = spark.read.parquet(str(inter / "municipio_dia"))
    assert md.columns == bd.COLS_MUNICIPIO_DIA
    assert md.agg(F.sum("n_det")).first()[0] == 3
    ad = spark.read.parquet(str(inter / "agg_depto_mes"))
    assert ad.count() == 4
    rep = json.loads((inter / "_reports" / "build_daily_validation.json").read_text(encoding="utf-8"))
    assert rep["publicado"] is True and set(rep["huellas"]) == {
        "clima_diario", "municipio_dia", "agg_depto_mes"}
    assert not any(p.name.endswith(".tmp") for p in inter.iterdir())


def test_ejecutar_es_idempotente(spark, tmp_path):
    escribir_silver(spark, tmp_path)
    h1 = bd.ejecutar(spark, cfg_prueba(tmp_path))["huellas"]
    h2 = bd.ejecutar(spark, cfg_prueba(tmp_path))["huellas"]
    assert h1 == h2


def _falla(spark, tmp_path, horarias, id_check):
    escribir_silver(spark, tmp_path, horarias=horarias)
    with pytest.raises(bd.CheckError, match=id_check):
        bd.ejecutar(spark, cfg_prueba(tmp_path))
    assert not (tmp_path / "int" / "clima_diario").exists()
    rep = json.loads((tmp_path / "int" / "_reports" / "build_daily_validation.json")
                     .read_text(encoding="utf-8"))
    assert rep["publicado"] is False


def test_no_publica_si_falta_una_hora(spark, tmp_path):
    _falla(spark, tmp_path, filas_horarias()[1:], "BD-7")


def test_no_publica_si_hay_hueco_en_la_serie(spark, tmp_path):
    dia_faltante = INI + timedelta(days=10)
    filas = [f for f in filas_horarias() if not (f[0] == "15001" and f[2] == dia_faltante)]
    _falla(spark, tmp_path, filas, "BD-8")


def test_no_publica_si_hay_hora_duplicada(spark, tmp_path):
    filas = filas_horarias()
    _falla(spark, tmp_path, filas + [filas[0]], "BD-2")


def test_no_publica_si_fecha_incoherente_con_ts_local(spark, tmp_path):
    filas = filas_horarias()
    f = list(filas[5])
    f[2] = f[2] + timedelta(days=1)
    filas[5] = tuple(f)
    _falla(spark, tmp_path, filas, "BD-3")


def test_no_publica_si_hay_nulos(spark, tmp_path):
    filas = filas_horarias()
    f = list(filas[30])
    f[5] = None  # precip
    filas[30] = tuple(f)
    _falla(spark, tmp_path, filas, "BD-5")


def test_no_publica_si_detecciones_fuera_de_la_grilla(spark, tmp_path):
    det = filas_detecciones() + [("15", "15999", date(2024, 1, 5), 1.0, False, 2024)]
    escribir_silver(spark, tmp_path, detecciones=det)
    with pytest.raises(bd.CheckError, match="BD-15"):
        bd.ejecutar(spark, cfg_prueba(tmp_path))
