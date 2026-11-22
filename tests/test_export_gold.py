"""Tests de firewatch.spark.export_gold.

Arman la cadena completa sobre los datos sintéticos de ``test_climatology``
(municipio_dia → climatology → irat) y le añaden lo que gold lee aparte:
``silver.detecciones``, ``silver.dim_departamento`` e ``int.agg_depto_mes``.
Las 9 detecciones de Boyacá (15001, 5 de enero: 1 + 3 + 5) cuadran por las 4 vías.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

from firewatch.spark import climatology as cl  # noqa: E402
from firewatch.spark import export_gold as eg  # noqa: E402
from firewatch.spark import irat as ir  # noqa: E402
from firewatch.spark.build_daily import CheckError  # noqa: E402

try:
    from test_climatology import (  # noqa: E402
        DET_ENERO, N_MESES, cfg_prueba, crear_df, escribir_entradas, spark,  # noqa: F401
    )
except ImportError:
    from tests.test_climatology import (  # noqa: E402
        DET_ENERO, N_MESES, cfg_prueba, crear_df, escribir_entradas, spark,  # noqa: F401
    )

ESQ_DET = T.StructType([
    T.StructField("id_det", T.StringType()), T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()), T.StructField("fecha", T.DateType()),
    T.StructField("hora_utc", T.IntegerType()), T.StructField("frp", T.DoubleType()),
    T.StructField("confianza", T.StringType()), T.StructField("daynight", T.StringType()),
    T.StructField("satelite", T.StringType()), T.StructField("cod_dpto", T.StringType()),
    T.StructField("cod_mpio", T.StringType()), T.StructField("fuente_estatica", T.BooleanType()),
])
ESQ_DPTO = T.StructType([T.StructField("cod_dpto", T.StringType()),
                         T.StructField("nombre", T.StringType())])  # como el silver real
ESQ_AGG = T.StructType([
    T.StructField("cod_dpto", T.StringType()), T.StructField("fecha_mes", T.DateType()),
    T.StructField("n_det", T.IntegerType()), T.StructField("frp_total", T.DoubleType()),
])
MESES = [date(2022 + (m // 12), m % 12 + 1, 1) for m in range(N_MESES)]


def filas_det(quitar_una: bool = False) -> list[tuple]:
    filas, k = [], 0
    for anio, n in DET_ENERO.items():
        for _ in range(n):
            k += 1
            filas.append((f"d{k:03d}", 5.5, -73.3, date(anio, 1, 5), 630, 10.0, "n", "D", "N20",
                          "15", "15001", False))
    if quitar_una:
        filas = filas[1:]
    filas += [
        ("e001", 5.6, -73.2, date(2023, 3, 1), 630, 99.0, "h", "D", "N", "15", "15002", True),
        ("o001", 6.2, -75.5, date(2023, 3, 1), 1830, 7.0, "n", "N", "N", "05", "05001", False),
    ]
    return filas


def filas_agg() -> list[tuple]:
    filas = []
    for m in MESES:
        n15 = DET_ENERO[m.year] if m.month == 1 else 0
        n05 = 1 if m == date(2023, 3, 1) else 0
        filas += [("15", m, n15, 10.0 * n15), ("05", m, n05, 7.0 * n05)]
    return filas


def preparar(spark, base: Path, quitar_una: bool = False) -> dict:
    escribir_entradas(spark, base)
    cfg = cfg_prueba(base)
    cl.ejecutar(spark, cfg)
    ir.ejecutar(spark, cfg)
    crear_df(spark, filas_det(quitar_una), ESQ_DET).write.mode("overwrite").parquet(
        str(base / "silver" / "detecciones"))
    crear_df(spark, [("15", "Boyacá"), ("05", "Antioquia")], ESQ_DPTO).write.mode("overwrite").parquet(
        str(base / "silver" / "dim_departamento"))
    crear_df(spark, filas_agg(), ESQ_AGG).write.mode("overwrite").parquet(
        str(base / "int" / "agg_depto_mes"))
    cfg["rutas"]["gold"] = str(base / "gold")
    return cfg


def test_dim_mes_nombres_y_trimestre(spark):
    p = eg.ParamsGold.desde_cfg(cfg_prueba(Path(".")))
    d = eg.dim_mes(spark, p).toPandas()
    assert len(d) == N_MESES and list(d.columns) == eg.COLS["dim_mes"]
    fila = d[d.fecha_mes == date(2023, 9, 1)].iloc[0]
    assert (fila.nombre_mes, fila.trimestre, fila.anio, fila.mes) == ("septiembre", 3, 2023, 9)


def test_ejecutar_publica_gold_y_manifiesto(spark, tmp_path):
    cfg = preparar(spark, tmp_path)
    inf = eg.ejecutar(spark, cfg)
    assert inf["publicado"] is True
    assert all(c["ok"] for c in inf["checks"] if c["nivel"] == "bloqueante")
    gold = tmp_path / "gold"
    for n in eg.TABLAS:
        assert (gold / f"{n}.parquet").exists() and (gold / "csv" / f"{n}.csv").exists()
    assert (gold / "_analisis" / "irat_sensibilidad.parquet").exists()
    fm = pd.read_parquet(gold / "fact_municipio_mes.parquet")
    assert list(fm.columns) == eg.COLS["fact_municipio_mes"] and len(fm) == 2 * N_MESES
    fd = pd.read_parquet(gold / "fact_deteccion.parquet")
    assert len(fd) == 9 and list(fd.columns) == eg.COLS["fact_deteccion"]
    assert "area_oficial_km2" not in pd.read_parquet(gold / "dim_municipio.parquet").columns
    assert list(pd.read_parquet(gold / "dim_departamento.parquet").columns) == ["cod_dpto", "nombre_dpto"]
    k = inf["kpis_control"]
    assert k["K1_detecciones"] == 9 and k["denominador_K6_nacional"] == 10
    assert k["K6_participacion_boyaca"] == pytest.approx(0.9)
    assert k["K2_var_interanual"]["actual"] == 5 and k["K2_var_interanual"]["previo"] == 3
    man = json.loads((gold / "_reports" / "gold_manifest.json").read_text(encoding="utf-8"))
    assert set(man["tablas"]) == set(eg.TABLAS) | {"irat_sensibilidad"}
    assert all(v["relectura_ok"] for v in man["tablas"].values())
    assert not (tmp_path / "gold.tmp").exists()


def test_ejecutar_es_idempotente(spark, tmp_path):
    cfg = preparar(spark, tmp_path)
    m1 = eg.ejecutar(spark, cfg)["manifiesto"]["tablas"]
    m2 = eg.ejecutar(spark, cfg)["manifiesto"]["tablas"]
    assert {n: v["huella_contenido"] for n, v in m1.items()} == \
           {n: v["huella_contenido"] for n, v in m2.items()}


def test_no_publica_si_no_concilia(spark, tmp_path):
    cfg = preparar(spark, tmp_path, quitar_una=True)
    with pytest.raises(CheckError):
        eg.ejecutar(spark, cfg)
    assert not (tmp_path / "gold").exists()
    rep = json.loads((tmp_path / "int" / "_reports" / "gold_validation_fallida.json")
                     .read_text(encoding="utf-8"))
    assert "G-3" in {c["id"] for c in rep["checks"] if not c["ok"]}


def test_no_publica_si_irat_no_publico(spark, tmp_path):
    cfg = preparar(spark, tmp_path)
    rep = tmp_path / "int" / "_reports" / "irat_validation.json"
    d = json.loads(rep.read_text(encoding="utf-8"))
    d["publicado"] = False
    rep.write_text(json.dumps(d), encoding="utf-8")
    with pytest.raises(CheckError):
        eg.ejecutar(spark, cfg)
    assert not (tmp_path / "gold").exists()
