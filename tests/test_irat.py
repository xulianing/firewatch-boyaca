"""Tests de firewatch.spark.irat (L3).

Reutilizan el ``int.municipio_dia`` sintético de ``test_climatology`` (2 municipios ×
27 meses) y un panel pequeño hecho a mano para comprobar valores exactos.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from firewatch.spark import climatology as cl  # noqa: E402
from firewatch.spark import irat as ir  # noqa: E402
from firewatch.spark.build_daily import CheckError  # noqa: E402

try:  # tests/ sin __init__.py (pytest añade la carpeta al sys.path)
    from test_climatology import (  # noqa: E402
        N_MESES, cfg_prueba, crear_df, dim_df, escribir_entradas, md_df, spark,  # noqa: F401
    )
except ImportError:  # tests/ como paquete
    from tests.test_climatology import (  # noqa: E402
        N_MESES, cfg_prueba, crear_df, dim_df, escribir_entradas, md_df, spark,  # noqa: F401
    )

TIPOS_MM = {
    "cod_mpio": T.StringType(), "fecha_mes": T.DateType(), "anio": T.IntegerType(),
    "mes": T.IntegerType(), "n_dias": T.IntegerType(), "n_det": T.IntegerType(),
    "racha_seca_max": T.IntegerType(),
}
ESQ_MM = T.StructType([T.StructField(c, TIPOS_MM.get(c, T.DoubleType()))
                       for c in cl.COLS_MUNICIPIO_MES])


def params(**cambios) -> ir.ParamsIrat:
    cfg = cfg_prueba(Path("."))
    cfg["irat"].update(cambios)
    return ir.ParamsIrat.desde_cfg(cfg)


def fila_mm(mpio, m, n_det=0, loyo=0.0, previos=None, anom_p=0.0, racha=0, anom_t=0.0,
            anom_rh=0.0, viento=10.0):
    return (mpio, date(2024, m, 1), 2024, m, 30, n_det, 0.0, None, 0.0, 0.0, 20.0, 40.0,
            viento, racha, anom_p, anom_t, anom_rh, loyo, previos)


def mm_real(spark):
    mmf, _ = cl.calcular(md_df(spark), dim_df(spark), cl.ParamsClima.desde_cfg(cfg_prueba(Path("."))))
    return mmf


# --------------------------------------------------------------------------- #
# Parámetros
# --------------------------------------------------------------------------- #
def test_params_pesos_alias_suma_y_mezcla():
    p = params(pesos={"Histórico": 0.3, "precipitacion": 0.25, "sequia": 0.15,
                      "temperatura": 0.15, "humedad": 0.10, "viento": 0.05})
    assert p.pesos == {"H": 0.3, "P": 0.25, "D": 0.15, "T": 0.15, "R": 0.10, "W": 0.05}
    assert p.ordenes_alto == [4, 5]
    assert params(mezcla_h={"loyo": 1, "previos": 3}).mezcla_h == pytest.approx(0.25)
    with pytest.raises(ValueError):
        params(pesos={"H": 0.5, "P": 0.25, "D": 0.15, "T": 0.15, "R": 0.10, "W": 0.05})
    with pytest.raises(KeyError):
        params(pesos={"H": 0.35, "P": 0.25, "D": 0.15, "T": 0.15, "R": 0.10})


# --------------------------------------------------------------------------- #
# Transformaciones
# --------------------------------------------------------------------------- #
def test_componente_H_mezcla_y_coalesce(spark):
    mm = crear_df(spark, [fila_mm("15001", 1, loyo=4.0, previos=2.0),
                          fila_mm("15001", 2, loyo=4.0, previos=None)], ESQ_MM)
    h = {r["mes"]: r["H_raw"] for r in ir.componentes_crudos(mm, 0.5).collect()}
    assert h[1] == pytest.approx(3.0)   # 0,5·4 + 0,5·2
    assert h[2] == pytest.approx(4.0)   # sin pasado: solo LOYO


def test_percentiles_en_rango_y_empates_iguales(spark):
    filas = [fila_mm("15001", m, loyo=(5.0 if m == 12 else 0.0), anom_p=float(m)) for m in range(1, 13)]
    df = ir.percentiles(ir.componentes_crudos(crear_df(spark, filas, ESQ_MM), 0.5))
    ch = {r["mes"]: r["c_H"] for r in df.collect()}
    assert len({ch[m] for m in range(1, 12)}) == 1 and ch[1] == 0.0  # 11 empates en 0
    assert ch[12] == 1.0
    cp = {r["mes"]: r["c_P"] for r in df.collect()}                   # P = −anom_precip
    assert cp[1] == 1.0 and cp[12] == 0.0


def test_quintiles_deterministas_con_otro_orden_fisico(spark):
    mm = mm_real(spark)
    a, _, cortes_a = ir.calcular(mm, params())
    b, _, cortes_b = ir.calcular(mm.orderBy(F.rand(7)).repartition(3), params())
    clave = ["cod_mpio", "fecha_mes"]
    fa = a.select(*clave, "irat", "irat_orden").orderBy(*clave).collect()
    fb = b.select(*clave, "irat", "irat_orden").orderBy(*clave).collect()
    assert fa == fb and cortes_a == cortes_b


def test_categorias_monotonas_y_coherentes(spark):
    out, sens, _ = ir.calcular(mm_real(spark), params())
    rangos = (out.groupBy("irat_orden").agg(F.min("irat").alias("lo"), F.max("irat").alias("hi"))
              .orderBy("irat_orden").collect())
    assert all(x["hi"] < y["lo"] for x, y in zip(rangos, rangos[1:]))
    cats = dict(out.select("irat_orden", "irat_categoria").distinct().collect())
    nombres = ["Muy bajo", "Bajo", "Moderado", "Alto", "Muy alto"]
    assert all(cats[k] == nombres[k - 1] for k in cats)
    assert out.columns == ir.COLS_IRAT and sens.columns == ir.COLS_SENSIBILIDAD
    assert out.agg(F.min("irat"), F.max("irat")).first()[0] >= 0.0


def test_solo_clima_no_depende_de_H(spark):
    mm = mm_real(spark)
    _, s1, _ = ir.calcular(mm, params())
    _, s2, _ = ir.calcular(mm.withColumn("dens_loyo", F.col("dens_loyo") * 7 + 1), params())
    clave = ["cod_mpio", "fecha_mes"]
    assert (s1.select(*clave, "irat_solo_clima").orderBy(*clave).collect()
            == s2.select(*clave, "irat_solo_clima").orderBy(*clave).collect())


# --------------------------------------------------------------------------- #
# Punta a punta
# --------------------------------------------------------------------------- #
def test_ejecutar_publica_y_reporta(spark, tmp_path):
    escribir_entradas(spark, tmp_path)
    cfg = cfg_prueba(tmp_path)
    cl.ejecutar(spark, cfg)
    inf = ir.ejecutar(spark, cfg)
    assert inf["publicado"] is True
    assert all(c["ok"] for c in inf["checks"] if c["nivel"] == "bloqueante")
    assert inf["conteos"]["municipio_mes_irat"] == 2 * N_MESES
    k5 = inf["indicadores"]["K5_tasa_captura"]
    assert set(k5) == {"irat", "irat_solo_clima", "irat_iguales"}
    assert all(v is None or 0.0 <= v <= 1.0 for v in k5.values())
    leido = spark.read.parquet(str(tmp_path / "int" / "municipio_mes_irat"))
    assert leido.columns == ir.COLS_IRAT and leido.count() == 2 * N_MESES
    rep = json.loads((tmp_path / "int" / "_reports" / "irat_validation.json").read_text(encoding="utf-8"))
    assert rep["publicado"] is True and len(rep["indicadores"]["cortes"]["irat"]) == 4


def test_no_publica_si_climatology_no_publico(spark, tmp_path):
    escribir_entradas(spark, tmp_path)
    cfg = cfg_prueba(tmp_path)
    cl.ejecutar(spark, cfg)
    rep_cl = tmp_path / "int" / "_reports" / "climatology_validation.json"
    rep = json.loads(rep_cl.read_text(encoding="utf-8"))
    rep["publicado"] = False
    rep_cl.write_text(json.dumps(rep), encoding="utf-8")
    with pytest.raises(CheckError):
        ir.ejecutar(spark, cfg)
    assert not (tmp_path / "int" / "municipio_mes_irat").exists()
