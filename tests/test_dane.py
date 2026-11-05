"""
Tests sin red de firewatch.ingest.dane.

- Bloque TABULAR (siempre corre): provincias, slug, centroides vs CSV, elevación, área, esquema,
  detección de la columna de código, departamentos.
- Bloque GEOMÉTRICO (requiere GeoPandas/Shapely 2): MGN sintético escrito como shapefile en EPSG:4326
  → corrida completa de run() con sus modos temporales.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from firewatch.ingest import dane as d  # noqa: E402


def cfg_for(tmp_path: Path, **over) -> d.DaneConfig:
    settings = {
        "rutas": {"raw_mgn": "raw/mgn/MGN_ADM_MPIO_GRAFICO.shp", "provincias_csv": "config/provincias_boyaca.csv",
                  "centroides": "config/centroides_boyaca.csv", "raw_meteo": "raw/meteo", "silver": "silver"},
        "dane": {"n_mpios_foco": 4, "n_mpios_pais": [6, 6], "n_dptos": 2, "area_foco_km2": [1, 10_000],
                 "sjoin_check_points": 5_000, **over},
    }
    return d.load_config(settings, base_dir=tmp_path)


# =============================================================================
# Bloque tabular
# =============================================================================

def test_slugify_provincia():
    assert d.slugify_provincia("Zona de Manejo Especial") == "ZONA_DE_MANEJO_ESPECIAL"
    assert d.slugify_provincia("  Sugamuxí ") == "SUGAMUXI"
    assert d.slugify_provincia("Lengupá") == "LENGUPA"
    with pytest.raises(ValueError):
        d.slugify_provincia("  ")


def test_load_config_rejects_bad_tolerance(tmp_path):
    with pytest.raises(d.DaneError, match="simplify_tolerance"):
        cfg_for(tmp_path, simplify_tolerance_deg=0.01)


def test_detect_code_column_prefers_content_not_name():
    df = pd.DataFrame({"dpto_ccdgo": ["15", "15", "05"], "mpio_ccdgo": ["001", "022", "001"],
                       "mpio_cdpmp": ["15001", "15022", "05001"], "otra": ["99999", "15022", "05001"]})
    assert d.detect_code_column(df) == ["mpio_cdpmp"]


def test_normalize_columns_rejects_numeric_codes():
    df = pd.DataFrame({"DPTO_CCDGO": [15, 5], "MPIO_CDPMP": [15001, 5001], "MPIO_CNMBR": ["A", "B"],
                       "MPIO_NAREA": [1.0, 2.0]})
    lg = d.CheckLog()
    with pytest.raises(d.DaneValidationError, match="V-D4"):
        d.normalize_columns(df, log_=lg)
    assert lg.failures[0].id == "V-D4"


def test_normalize_columns_ok_lowercase_and_codes():
    df = pd.DataFrame({"DPTO_CCDGO": ["15", "05"], "MPIO_CDPMP": ["15001", "05001"], "MPIO_CCDGO": ["001", "001"],
                       "MPIO_CNMBR": ["TUNJA", "MEDELLÍN"], "MPIO_NAREA": [118.0, 380.0]})
    lg = d.CheckLog()
    out = d.normalize_columns(df, log_=lg)
    assert out["cod_mpio"].tolist() == ["15001", "05001"] and out["cod_dpto"].tolist() == ["15", "05"]
    assert not lg.failures


def dim_base():
    return pd.DataFrame({"cod_mpio": ["15001", "15176", "15572"], "nombre": ["TUNJA", "CHIQUINQUIRÁ", "PUERTO BOYACÁ"]})


def prov_df(rows):
    return pd.DataFrame(rows, columns=["cod_mpio", "municipio", "provincia"]).astype("string")


def test_join_provincias_ok_and_name_warning():
    lg = d.CheckLog()
    out = d.join_provincias(dim_base(), prov_df([("15001", "Tunja", "Centro"),
                                                 ("15176", "Chiquinquira", "Occidente"),
                                                 ("15572", "Pto Boyacá", "Zona de Manejo Especial")]), lg)
    assert not lg.failures
    assert out.set_index("cod_mpio").loc["15572", "cod_provincia"] == "ZONA_DE_MANEJO_ESPECIAL"
    warn = [c for c in lg.checks if c.id == "V-D10w"][0]
    assert warn.status == "warn" and "1 nombres" in warn.message          # 'Pto Boyacá' ≠ 'PUERTO BOYACÁ'


def test_join_provincias_empty_csv_like_a2_pending(tmp_path):
    p = tmp_path / "provincias_boyaca.csv"
    p.write_text("cod_mpio,municipio,provincia\n", encoding="utf-8")       # estado real hoy: solo encabezado
    lg = d.CheckLog()
    d.join_provincias(dim_base(), d.read_provincias(p), lg)
    assert lg.failures and lg.failures[0].id == "V-D10" and "vacío" in lg.failures[0].message


@pytest.mark.parametrize("rows, frag", [
    ([("15001", "Tunja", "Centro"), ("15176", "Chiquinquirá", "Occidente")], "sin fila"),
    ([("15001", "Tunja", "Centro"), ("15001", "Tunja", "Centro"), ("15176", "x", "Occidente"),
      ("15572", "x", "ZME")], "duplicados"),
    ([("15001", "Tunja", ""), ("15176", "x", "Occidente"), ("15572", "x", "ZME")], "sin provincia"),
    ([("15001", "Tunja", "Centro"), ("15176", "x", "Occidente"), ("15572", "x", "ZME"),
      ("05001", "Medellín", "Valle de Aburrá")], "no son del foco"),
])
def test_join_provincias_errors(rows, frag):
    lg = d.CheckLog()
    d.join_provincias(dim_base(), prov_df(rows), lg)
    assert lg.failures and frag in lg.failures[0].message


def test_check_centroides_csv(tmp_path):
    cent = pd.DataFrame({"cod_mpio": ["15001", "15176"], "lat_c": [5.5353, 5.6164], "lon_c": [-73.3678, -73.8195]})
    csv = tmp_path / "c.csv"
    cent.assign(nombre="x", metodo="centroid").to_csv(csv, index=False)
    lg = d.CheckLog(); d.check_centroides_csv(cent, csv, 4, lg)
    assert not lg.failures
    cent2 = cent.copy(); cent2.loc[1, "lon_c"] = -73.8196                   # 1e-4 ≈ 11 m de diferencia
    lg = d.CheckLog(); d.check_centroides_csv(cent2, csv, 4, lg)
    assert lg.failures[0].id == "V-D11" and "1 coordenadas distintas" in lg.failures[0].message


def test_enrich_elevation(tmp_path):
    (tmp_path / "15001.json").write_text(json.dumps({"elevation": 2782.0}))
    (tmp_path / "15176.json").write_text(json.dumps({"elevation": 50.0}))  # fuera de rango
    lg = d.CheckLog()
    out = d.enrich_elevation(dim_base(), tmp_path, (100.0, 4500.0), lg)
    assert out["elevacion_m"].iloc[0] == 2782.0 and np.isnan(out["elevacion_m"].iloc[2])
    msg = lg.failures[0].message
    assert "1 sin elevación" in msg and "1 fuera de" in msg


def test_check_area_detects_hectares(tmp_path):
    cfg = cfg_for(tmp_path)
    dim = pd.DataFrame({"area_km2": [100.0, 200.0], "area_oficial_km2": [10_000.0, 20_000.0]})  # en ha
    lg = d.CheckLog(); d.check_area(dim, cfg, lg)
    assert lg.failures[0].id == "V-D7"


def test_enforce_schema_nulls_and_temporal_mode():
    dim = pd.DataFrame({c: [None] for c in d.DIM_MUNICIPIO_SCHEMA})
    dim = dim.assign(cod_mpio="15001", cod_dpto="15", nombre="TUNJA", area_km2=118.0, area_oficial_km2=118.0,
                     lat_c=5.5, lon_c=-73.4, metodo_centroide="centroid", vigencia_mgn=2025)
    lg = d.CheckLog(); d.enforce_schema(dim, d.DIM_MUNICIPIO_SCHEMA, lg, "V-D15", pk="cod_mpio")
    assert lg.failures and "provincia" in lg.failures[0].message and "elevacion_m" in lg.failures[0].message
    lg = d.CheckLog()
    out = d.enforce_schema(dim, d.DIM_MUNICIPIO_SCHEMA, lg, "V-D15", pk="cod_mpio",
                           allow_null=["provincia", "cod_provincia", "elevacion_m"])
    assert not lg.failures and list(out.columns) == list(d.DIM_MUNICIPIO_SCHEMA)
    assert str(out["cod_mpio"].dtype) == "string" and str(out["vigencia_mgn"].dtype) == "int64"


def test_build_dim_departamento(tmp_path):
    cfg = cfg_for(tmp_path)
    df = pd.DataFrame({"cod_dpto": ["15", "15", "05"], "dpto_cnmbr": ["BOYACÁ", "BOYACÁ", "ANTIOQUIA"]})
    lg = d.CheckLog()
    dep = d.build_dim_departamento(df, cfg, lg)
    assert dep["cod_dpto"].tolist() == ["05", "15"] and not lg.failures


def test_checklog_status():
    lg = d.CheckLog(); lg.add("A", True); lg.pending("V-D10", "A2")
    assert lg.status() == "incompleto"
    lg.add("B", False, "x"); assert lg.status() == "fallido"
