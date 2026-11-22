"""Tests de firewatch.spark.climatology (L3).

Datos sintéticos con la estructura de ``int.municipio_dia``. Ventana de prueba:
2022-01-01 → 2024-03-31 (821 días, 27 meses), línea base [2022, 2023], 2 municipios.

    15001 (50 km²):  llueve 2 mm todos los días salvo del 25-ene al 10-feb de 2022 (racha
                     seca de 17 días que cruza el cambio de mes). Detecciones el 5 de enero:
                     1 (2022), 3 (2023), 5 (2024) → densidades 2, 6, 10 por 100 km².
    15002 (200 km²): nunca llueve (todo el mes es una racha). Sin detecciones.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from firewatch.spark import climatology as cl  # noqa: E402
from firewatch.spark.build_daily import CheckError  # noqa: E402
from firewatch.spark.session import crear_sesion  # noqa: E402

INI, FIN = date(2022, 1, 1), date(2024, 3, 31)
N_DIAS = (FIN - INI).days + 1  # 821
N_MESES = 27
AREAS = {"15001": 50.0, "15002": 200.0}
DET_ENERO = {2022: 1, 2023: 3, 2024: 5}  # 15001, día 5 de enero

ESQ_MD = T.StructType([
    T.StructField("cod_mpio", T.StringType()), T.StructField("fecha", T.DateType()),
    T.StructField("tmax", T.DoubleType()), T.StructField("tmean", T.DoubleType()),
    T.StructField("precip", T.DoubleType()), T.StructField("rh_min", T.DoubleType()),
    T.StructField("wind_max", T.DoubleType()), T.StructField("et0", T.DoubleType()),
    T.StructField("n_det", T.IntegerType()), T.StructField("frp_sum", T.DoubleType()),
    T.StructField("precip_30d", T.DoubleType()), T.StructField("dia_seco", T.BooleanType()),
    T.StructField("id_racha", T.StringType()),
])
ESQ_DIM = T.StructType([T.StructField("cod_mpio", T.StringType()),
                        T.StructField("area_km2", T.DoubleType())])


def cfg_prueba(base: Path) -> dict:
    return {
        "rutas": {"silver": str(base / "silver"), "intermedio": str(base / "int")},
        "ventana": {"fecha_inicio": INI.isoformat(), "fecha_fin": FIN.isoformat(),
                    "anios_linea_base": [2022, 2023]},
        "territorio": {"cod_dpto_foco": "15"},
        "spark": {"master_produccion": "local[2]", "shuffle_partitions_produccion": 4,
                  "driver_memory": "1g", "ventana_precip_dias": 3, "umbral_dia_seco_mm": 1.0,
                  "meses_previos_h": 3},
        "irat": {"pesos": {"H": 0.30, "P": 0.25, "D": 0.15, "T": 0.15, "R": 0.10, "W": 0.05},
                 "mezcla_h": 0.5, "normalizacion": "percent_rank",
                 "categorias": ["Muy bajo", "Bajo", "Moderado", "Alto", "Muy alto"],
                 "categorias_alto": ["Alto", "Muy alto"]},
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
    """DataFrame vía pandas + Arrow, sin workers de Python (ver test_build_daily)."""
    import pandas as pd

    pdf = pd.DataFrame([tuple(f) for f in filas], columns=[f.name for f in esquema.fields])
    return spark.createDataFrame(pdf, schema=esquema)


def params(base: Path = Path(".")) -> cl.ParamsClima:
    return cl.ParamsClima.desde_cfg(cfg_prueba(base))


# --------------------------------------------------------------------------- #
# Generador de int.municipio_dia sintético
# --------------------------------------------------------------------------- #
def precip(mpio: str, dia: date) -> float:
    if mpio == "15002":
        return 0.0
    return 0.0 if date(2022, 1, 25) <= dia <= date(2022, 2, 10) else 2.0


def filas_md(quitar: tuple[str, date] | None = None) -> list[tuple]:
    filas = []
    for m in AREAS:
        k = secos = 0
        for d in range(N_DIAS):
            dia = INI + timedelta(days=d)
            k += 1
            pr = precip(m, dia)
            seco = pr < 1.0
            secos += seco
            # gaps-and-islands, igual que build_daily: k − row_number dentro de dia_seco
            id_racha = f"{m}-{k - secos}" if seco else None
            n = DET_ENERO.get(dia.year, 0) if (m == "15001" and dia.month == 1 and dia.day == 5) else 0
            tmax = 20.0 + (dia.year - 2022) + dia.month * 0.1
            fila = (m, dia, tmax, tmax - 5, pr, 40.0 + dia.month, 10.0 + dia.day % 7, 3.0,
                    n, 10.0 * n, 0.0, seco, id_racha)
            if quitar != (m, dia):
                filas.append(fila)
    return filas


def md_df(spark, **kw):
    return crear_df(spark, filas_md(**kw), ESQ_MD)


def dim_df(spark):
    return crear_df(spark, list(AREAS.items()), ESQ_DIM)


def escribir_entradas(spark, base: Path, publicado: bool = True, **kw) -> None:
    md_df(spark, **kw).write.mode("overwrite").parquet(str(base / "int" / "municipio_dia"))
    dim_df(spark).write.mode("overwrite").parquet(str(base / "silver" / "dim_municipio"))
    rep = base / "int" / "_reports" / "build_daily_validation.json"
    rep.parent.mkdir(parents=True, exist_ok=True)
    rep.write_text(json.dumps({"publicado": publicado,
                               "conteos": {"municipio_dia": len(filas_md(**kw))}}), encoding="utf-8")


def mes(df, mpio: str, anio: int, m: int):
    return df.filter((F.col("cod_mpio") == mpio) & (F.col("fecha_mes") == date(anio, m, 1))).first()


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_params_anios_en_varios_formatos():
    for v in ([2023, 2022], {"inicio": 2022, "fin": 2023}, "2022-2023"):
        cfg = cfg_prueba(Path("."))
        cfg["ventana"]["anios_linea_base"] = v
        assert cl.ParamsClima.desde_cfg(cfg).anios_lb == (2022, 2023)
    cfg = cfg_prueba(Path("."))
    del cfg["ventana"]["anios_linea_base"]
    with pytest.raises(KeyError):
        cl.ParamsClima.desde_cfg(cfg)


def test_racha_recortada_al_mes(spark):
    r = cl.racha_seca_mes(md_df(spark))
    assert mes(r, "15001", 2022, 1)["racha_seca_max"] == 7    # 25–31 ene
    assert mes(r, "15001", 2022, 2)["racha_seca_max"] == 10   # 1–10 feb
    assert mes(r, "15002", 2022, 2)["racha_seca_max"] == 28   # todo el mes
    assert mes(r, "15001", 2022, 3) is None                   # sin días secos → sin fila


def test_agregar_mes_conteos_densidad_y_frp_medio(spark):
    mm = cl.agregar_mes(md_df(spark), dim_df(spark))
    assert mm.count() == 2 * N_MESES
    ene = mes(mm, "15001", 2023, 1)
    assert (ene["n_dias"], ene["n_det"], ene["frp_total"]) == (31, 3, 30.0)
    assert ene["det_100km2"] == pytest.approx(6.0)
    assert ene["frp_medio"] == pytest.approx(10.0)
    feb = mes(mm, "15001", 2023, 2)
    assert feb["n_det"] == 0 and feb["frp_medio"] is None and feb["racha_seca_max"] == 0
    assert mes(mm, "15002", 2024, 2)["racha_seca_max"] == 29   # bisiesto


def test_linea_base_y_anomalias_media_cero(spark):
    p = params()
    mmf, lb = cl.calcular(md_df(spark), dim_df(spark), p)
    assert lb.count() == 2 * 12
    assert lb.agg(F.min("n_anios_lb"), F.max("n_anios_lb")).first() == (2, 2)
    medias = (mmf.filter(F.col("anio").isin(2022, 2023)).groupBy("cod_mpio", "mes")
              .agg(F.avg("anom_tmax").alias("a")).agg(F.max(F.abs("a"))).first()[0])
    assert medias == pytest.approx(0.0, abs=1e-9)
    # tmax sube 1 °C por año: 2022 queda 0,5 por debajo de la media y 2024 1,5 por encima
    assert mes(mmf, "15001", 2022, 6)["anom_tmax"] == pytest.approx(-0.5)
    assert mes(mmf, "15001", 2024, 2)["anom_tmax"] == pytest.approx(1.5)


def test_loyo_dentro_y_fuera_de_la_linea_base(spark):
    mmf, _ = cl.calcular(md_df(spark), dim_df(spark), params())
    # Enero 15001: densidades 2 (2022), 6 (2023), 10 (2024); línea base 2022–2023
    assert mes(mmf, "15001", 2022, 1)["dens_loyo"] == pytest.approx(6.0)  # excluye 2022
    assert mes(mmf, "15001", 2023, 1)["dens_loyo"] == pytest.approx(2.0)  # excluye 2023
    assert mes(mmf, "15001", 2024, 1)["dens_loyo"] == pytest.approx(4.0)  # (2 + 6) / 2


def test_det_3m_previos_borde_y_media(spark):
    mmf, _ = cl.calcular(md_df(spark), dim_df(spark), params())
    for m in (1, 2, 3):
        assert mes(mmf, "15001", 2022, m)["det_3m_previos"] is None
    assert mes(mmf, "15001", 2022, 4)["det_3m_previos"] == pytest.approx(2.0 / 3)
    assert mes(mmf, "15001", 2023, 2)["det_3m_previos"] == pytest.approx(2.0)  # nov, dic, ene
    assert mmf.filter(F.col("det_3m_previos").isNull()).count() == 2 * 3


def test_ejecutar_publica_y_reporta(spark, tmp_path):
    escribir_entradas(spark, tmp_path)
    inf = cl.ejecutar(spark, cfg_prueba(tmp_path))
    assert inf["publicado"] is True
    assert all(c["ok"] for c in inf["checks"] if c["nivel"] == "bloqueante")
    assert inf["conteos"]["municipio_mes"] == 2 * N_MESES
    assert inf["conteos"]["detecciones"] == sum(DET_ENERO.values())
    leido = spark.read.parquet(str(tmp_path / "int" / "municipio_mes"))
    assert leido.columns == cl.COLS_MUNICIPIO_MES
    assert spark.read.parquet(str(tmp_path / "int" / "linea_base")).columns == cl.COLS_LINEA_BASE
    rep = json.loads((tmp_path / "int" / "_reports" / "climatology_validation.json")
                     .read_text(encoding="utf-8"))
    assert rep["publicado"] is True and "huellas" in rep


def test_ejecutar_es_idempotente(spark, tmp_path):
    escribir_entradas(spark, tmp_path)
    h1 = cl.ejecutar(spark, cfg_prueba(tmp_path))["huellas"]
    h2 = cl.ejecutar(spark, cfg_prueba(tmp_path))["huellas"]
    assert h1 == h2


def _falla(spark, tmp_path, id_check, **kw):
    escribir_entradas(spark, tmp_path, **kw)
    with pytest.raises(CheckError):
        cl.ejecutar(spark, cfg_prueba(tmp_path))
    assert not (tmp_path / "int" / "municipio_mes").exists()
    rep = json.loads((tmp_path / "int" / "_reports" / "climatology_validation.json")
                     .read_text(encoding="utf-8"))
    fallas = {c["id"] for c in rep["checks"] if not c["ok"] and c["nivel"] == "bloqueante"}
    assert id_check in fallas and rep["publicado"] is False


def test_no_publica_si_build_daily_no_publico(spark, tmp_path):
    _falla(spark, tmp_path, "CL-0", publicado=False)


def test_no_publica_si_falta_un_dia(spark, tmp_path):
    _falla(spark, tmp_path, "CL-4", quitar=("15002", date(2023, 7, 14)))
