"""
Tests geométricos sin red de firewatch.ingest.dane sobre un MGN SINTÉTICO (shapefile EPSG:4326, como el MGN real).
Requieren GeoPandas + Shapely 2 + pyogrio + pyarrow (entorno del proyecto).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from firewatch.ingest import dane as d  # noqa: E402
from test_dane import cfg_for, prov_df  # noqa: E402

# =============================================================================
# Bloque geométrico (MGN sintético)
# =============================================================================

gpd = pytest.importorskip("geopandas", reason="requiere GeoPandas (correr en el entorno del proyecto)")
from shapely.geometry import Polygon, box  # noqa: E402


def square(lon, lat, size=0.1):
    return box(lon, lat, lon + size, lat + size)


def u_shape(lon, lat, s=0.05):
    """Polígono en U: su centroide cae en el hueco → debe usarse representative_point."""
    pts = [(0, 0), (3, 0), (3, 3), (2, 3), (2, 1), (1, 1), (1, 3), (0, 3)]
    return Polygon([(lon + x * s, lat + y * s) for x, y in pts])


def bowtie(lon, lat, s=0.1):
    """Polígono autointersectado (inválido) → make_valid."""
    return Polygon([(lon, lat), (lon + s, lat + s), (lon + s, lat), (lon, lat + s)])


@pytest.fixture
def mgn(tmp_path):
    rows = [  # 4 de Boyacá (15) + 2 de Antioquia (05)
        # límites literales (no lon + 0.1) para que la frontera 15001|15022 sea exactamente x = -73.35
        ("15", "001", "15001", "TUNJA", box(-73.45, 5.50, -73.35, 5.60)),
        ("15", "022", "15022", "ALMEIDA", box(-73.35, 5.50, -73.25, 5.60)),
        ("15", "047", "15047", "AQUITANIA", box(-73.45, 5.60, -73.35, 5.70)),
        ("15", "051", "15051", "ARCABUCO", u_shape(-73.20, 5.70)),
        ("05", "001", "05001", "MEDELLÍN", square(-75.60, 6.20)),
        ("05", "002", "05002", "ABEJORRAL", bowtie(-75.45, 5.80)),
    ]
    g = gpd.GeoDataFrame(
        {"DPTO_CCDGO": [r[0] for r in rows], "MPIO_CCDGO": [r[1] for r in rows],
         "MPIO_CDPMP": [r[2] for r in rows], "MPIO_CNMBR": [r[3] for r in rows],
         "DPTO_CNMBR": ["BOYACÁ" if r[0] == "15" else "ANTIOQUIA" for r in rows],
         "MPIO_NANO": [2025] * len(rows)},
        geometry=[r[4] for r in rows], crs=4326)
    g["MPIO_NAREA"] = g.to_crs(9377).geometry.make_valid().area / 1e6       # área oficial coherente
    cfg = cfg_for(tmp_path)
    cfg.raw_mgn.parent.mkdir(parents=True)
    g.to_file(cfg.raw_mgn, engine="pyogrio")
    return cfg


def write_aux(cfg, provincias=True, meteo=True, centroides=True):
    raw = d.normalize_columns(d.read_mgn(cfg.raw_mgn))
    foco = raw[raw["cod_dpto"] == "15"]
    cfg.centroides_csv.parent.mkdir(parents=True, exist_ok=True)
    if centroides:   # lo que generó el puente para meteo
        d.compute_centroids(foco).rename(columns={"metodo_centroide": "metodo"}).assign(nombre="x") \
            .to_csv(cfg.centroides_csv, index=False)
    if provincias:
        prov_df([("15001", "Tunja", "Centro"), ("15022", "Almeida", "Oriente"),
                 ("15047", "Aquitania", "Sugamuxí"), ("15051", "Arcabuco", "Ricaurte")]) \
            .to_csv(cfg.provincias_csv, index=False)
    else:
        cfg.provincias_csv.write_text("cod_mpio,municipio,provincia\n", encoding="utf-8")
    if meteo:
        cfg.raw_meteo.mkdir(parents=True, exist_ok=True)
        for cod, elev in zip(["15001", "15022", "15047", "15051"], [2782.0, 2300.0, 3030.0, 2600.0]):
            (cfg.raw_meteo / f"{cod}.json").write_text(json.dumps({"elevation": elev}))


def test_compute_centroids_rep_point_for_u_shape(mgn):
    raw = d.normalize_columns(d.read_mgn(mgn.raw_mgn))
    c = d.compute_centroids(raw[raw["cod_dpto"] == "15"]).set_index("cod_mpio")
    assert c.loc["15051", "metodo_centroide"] == "rep_point"
    assert c.loc["15001", "metodo_centroide"] == "centroid"
    assert abs(c.loc["15001", "lat_c"] - 5.55) < 1e-3 and abs(c.loc["15001", "lon_c"] + 73.40) < 1e-3


def test_assign_points_boundary_tie_break(mgn):
    raw = d.normalize_columns(d.read_mgn(mgn.raw_mgn))          # mismo CRS (4326): sin errores de reproyección
    pts = gpd.GeoSeries(gpd.points_from_xy([-73.40, -73.35, -70.0], [5.55, 5.55, 5.0]), crs=raw.crs)
    codes = d.assign_points_to_polygons(pts, raw[["cod_mpio", "geometry"]])
    assert codes.iloc[0] == "15001"                 # interior
    assert codes.iloc[1] == "15001"                 # frontera 15001|15022 → menor código (D-G6)
    assert pd.isna(codes.iloc[2])                   # fuera de todo polígono


def test_run_final_complete(mgn):
    write_aux(mgn)
    rep = d.run(mgn)
    assert rep["status"] == "completo", [c for c in rep["checks"] if c["status"] != "ok"]
    dim = pd.read_parquet(mgn.out_dim_municipio)
    assert len(dim) == 4 and list(dim.columns) == list(d.DIM_MUNICIPIO_SCHEMA)
    assert dim["elevacion_m"].notna().all() and dim["provincia"].notna().all()
    assert set(dim["cod_provincia"]) == {"CENTRO", "ORIENTE", "SUGAMUXI", "RICAURTE"}
    geo = gpd.read_parquet(mgn.out_geo)
    assert len(geo) == 6 and geo.crs.to_epsg() == 4326 and geo.is_valid.all()     # bowtie corregido
    assert set(geo.geom_type) == {"MultiPolygon"}
    dep = pd.read_parquet(mgn.out_dim_departamento)
    assert dep["cod_dpto"].tolist() == ["05", "15"]
    v13 = [c for c in rep["checks"] if c["id"] == "V-D13"][0]
    assert v13["details"]["acuerdo"] >= 0.999


def test_run_temporal_modes_publish_incomplete(mgn):
    write_aux(mgn, provincias=False, meteo=False)       # estado real hoy: A2 vacío, clima pendiente
    rep = d.run(mgn, sin_provincias=True, sin_elevacion=True)
    assert rep["status"] == "incompleto"
    assert {c["id"] for c in rep["checks"] if c["status"] == "pending"} == {"V-D10", "V-D12"}
    dim = pd.read_parquet(mgn.out_dim_municipio)
    assert dim["provincia"].isna().all() and dim["elevacion_m"].isna().all()


def test_run_final_fails_without_provincias_and_publishes_nothing(mgn):
    write_aux(mgn, provincias=False)
    with pytest.raises(d.DaneValidationError, match="V-D10"):
        d.run(mgn)                                       # sin el flag, V-D10 es obligatoria
    assert not mgn.out_dim_municipio.exists() and not mgn.out_geo.exists()          # D-G7
    report = json.loads(mgn.out_report.read_text(encoding="utf-8"))
    assert report["status"] == "fallido"


def test_run_fails_if_centroids_differ_from_meteo_csv(mgn):
    write_aux(mgn)
    df = pd.read_csv(mgn.centroides_csv, dtype={"cod_mpio": str})
    df.loc[0, "lat_c"] += 0.001
    df.to_csv(mgn.centroides_csv, index=False)
    with pytest.raises(d.DaneValidationError, match="V-D11"):
        d.run(mgn)


def test_run_fails_if_mgn_crs_is_not_4326(mgn):
    write_aux(mgn)
    g = gpd.read_file(mgn.raw_mgn, engine="pyogrio").set_crs(4686, allow_override=True)
    g.to_file(mgn.raw_mgn, engine="pyogrio")
    with pytest.raises(d.DaneValidationError, match="V-D5"):
        d.run(mgn)
    assert not mgn.out_geo.exists()                      # D-G7


def test_load_config_foco_desde_territorio(tmp_path):
    settings = {"rutas": {}, "territorio": {"cod_dpto_foco": "15", "n_municipios_foco": 123}}
    cfg = d.load_config(settings, base_dir=tmp_path)
    assert cfg.cod_dpto_foco == "15" and cfg.n_mpios_foco == 123
    assert cfg.crs_origen == 4326 and cfg.simplify_tolerance_deg == 0.0003


def test_run_solo_geo(mgn):
    rep = d.run(mgn, solo_geo=True)                      # no necesita CSV ni clima
    assert rep["status"] == "incompleto" and mgn.out_geo.exists() and not mgn.out_dim_municipio.exists()


def test_run_rejects_wrong_count(mgn, tmp_path):
    write_aux(mgn)
    cfg = d.DaneConfig(**{**mgn.__dict__, "n_mpios_foco": 123})
    with pytest.raises(d.DaneValidationError, match="V-D2"):
        d.run(cfg)
