"""Tests de firewatch.parallel.spatial_join (requieren GeoPandas + Shapely 2 + pyarrow)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

gpd = pytest.importorskip("geopandas", reason="requiere GeoPandas (entorno del proyecto)")
pytest.importorskip("pyarrow", reason="requiere pyarrow (GeoParquet)")
from shapely.geometry import box

from firewatch.ingest.dane import CheckLog
from firewatch.parallel import spatial_join as sj


@pytest.fixture
def geo_dir(tmp_path):
    polys = gpd.GeoDataFrame(
        {"cod_mpio": ["15001", "15022", "05001"], "cod_dpto": ["15", "15", "05"]},
        geometry=[box(-73.45, 5.50, -73.35, 5.60), box(-73.35, 5.50, -73.25, 5.60), box(-75.7, 6.1, -75.5, 6.3)],
        crs=4326)
    d = tmp_path / "geo_municipios"
    d.mkdir()
    polys.to_parquet(d / "part-0000.parquet")
    return d


def detections(lats, lons, estatica=None):
    n = len(lats)
    return pd.DataFrame({"id_det": [f"{i:016x}" for i in range(n)], "lat": lats, "lon": lons,
                         "fuente_estatica": estatica if estatica is not None else [False] * n},
                        index=pd.RangeIndex(100, 100 + n))


def test_load_polygons_validates(geo_dir, tmp_path):
    polys = sj.load_polygons(geo_dir)
    assert len(polys) == 3 and polys.crs.to_epsg() == 4326
    with pytest.raises(sj.SpatialJoinError, match="No existe"):
        sj.load_polygons(tmp_path / "nada")
    bad = gpd.read_parquet(geo_dir / "part-0000.parquet").to_crs(3857)
    bad.to_parquet(geo_dir / "part-0000.parquet")
    with pytest.raises(sj.SpatialJoinError, match="EPSG:4326"):
        sj.load_polygons(geo_dir)


def test_spatial_join_assigns_keeps_rows_and_order(geo_dir):
    polys = sj.load_polygons(geo_dir)
    df = detections([5.55, 5.55, 6.2, 0.0], [-73.40, -73.35, -75.6, -70.0], estatica=[False, False, True, False])
    out = sj.spatial_join(df, polys)
    assert out.index.equals(df.index) and len(out) == 4                      # no filtra ni reordena
    assert out["cod_mpio"].iloc[0] == "15001"
    assert out["cod_mpio"].iloc[1] == "15001"                                # frontera → menor código (D-G6)
    assert out["cod_mpio"].iloc[2] == "05001" and out["cod_dpto"].iloc[2] == "05"   # estáticas también se cruzan
    assert pd.isna(out["cod_mpio"].iloc[3]) and pd.isna(out["cod_dpto"].iloc[3])


def test_spatial_join_empty(geo_dir):
    out = sj.spatial_join(detections([], []), sj.load_polygons(geo_dir))
    assert out.empty and {"cod_mpio", "cod_dpto"} <= set(out.columns)


def test_validate_join(geo_dir):
    polys = sj.load_polygons(geo_dir)
    out = sj.spatial_join(detections([5.55, 0.0], [-73.40, -70.0]), polys)
    lg = CheckLog()
    sj.validate_join(out, 2, 0.01, lg)                                       # 50 % sin municipio
    assert [c.id for c in lg.failures] == ["V-SJ2"]
    lg = CheckLog()
    sj.validate_join(out, 3, 0.9, lg)                                        # entrada ≠ salida
    assert [c.id for c in lg.failures] == ["V-SJ1"]
