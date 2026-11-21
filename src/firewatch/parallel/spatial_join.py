"""
firewatch.parallel.spatial_join — Cruce espacial punto-en-polígono (L2, parte 2).

Responsabilidad: agregar cod_mpio y cod_dpto a las detecciones limpias usando
silver/geo_municipios y la regla oficial dane.assign_points_to_polygons (D-G6: un punto en una
frontera compartida toma el menor cod_mpio).

Reglas de la interfaz (Diseño cleaning §6):
  - NO filtra filas: devuelve exactamente las mismas filas, en el mismo orden (conservación V-C7).
  - cod_mpio / cod_dpto = <NA> si el punto no cae en ningún municipio (mar, fuera de frontera).
  - cod_dpto se toma del polígono, no de FIRMS.
  - Las filas con fuente_estatica = true también se cruzan (el EDA las necesita por municipio).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from firewatch.ingest.dane import CheckLog, assign_points_to_polygons

if TYPE_CHECKING:                       # GeoPandas se importa dentro de las funciones que lo usan,
    import geopandas as gpd             # para que validate_join (pandas puro) no exija GIS

log = logging.getLogger(__name__)

GEO_COLUMNS = ("cod_mpio", "cod_dpto", "geometry")


class SpatialJoinError(Exception):
    """Capa de polígonos inválida o violación de la conservación de filas."""


def load_polygons(path: Path) -> "gpd.GeoDataFrame":
    """
    Lee silver/geo_municipios (directorio o archivo GeoParquet) y construye el índice espacial.
    En multiprocessing se llamará una sola vez por worker (initializer del Pool).
    """
    import geopandas as gpd

    path = Path(path)
    if not path.exists():
        raise SpatialJoinError(f"No existe {path}: ejecutar primero firewatch.ingest.dane")
    src = path / "part-0000.parquet" if path.is_dir() else path
    polys = gpd.read_parquet(src)
    missing = [c for c in GEO_COLUMNS if c not in polys.columns]
    if missing:
        raise SpatialJoinError(f"{src}: faltan columnas {missing}")
    if polys.crs is None or polys.crs.to_epsg() != 4326:
        raise SpatialJoinError(f"{src}: CRS {polys.crs}; se esperaba EPSG:4326")
    if polys["cod_mpio"].duplicated().any():
        raise SpatialJoinError(f"{src}: cod_mpio duplicado")
    polys = polys[list(GEO_COLUMNS)].copy()
    polys["cod_mpio"] = polys["cod_mpio"].astype("string")
    polys["cod_dpto"] = polys["cod_dpto"].astype("string")
    _ = polys.sindex                       # construye el STRtree ahora, no en el primer bloque
    log.info("Polígonos cargados: %d desde %s", len(polys), src)
    return polys.reset_index(drop=True)


def spatial_join(df: pd.DataFrame, polys: "gpd.GeoDataFrame") -> pd.DataFrame:
    """
    Agrega cod_mpio y cod_dpto a un bloque de detecciones limpias (columnas lat, lon en EPSG:4326).
    Devuelve las mismas filas, en el mismo orden.
    """
    import geopandas as gpd

    if df.empty:
        return df.assign(cod_mpio=pd.Series(dtype="string"), cod_dpto=pd.Series(dtype="string"))
    points = gpd.GeoSeries(gpd.points_from_xy(df["lon"].to_numpy(), df["lat"].to_numpy()),
                           index=df.index, crs=polys.crs)
    codes = assign_points_to_polygons(points, polys[["cod_mpio", "geometry"]], code_col="cod_mpio")
    dpto_by_mpio = dict(zip(polys["cod_mpio"], polys["cod_dpto"]))
    out = df.copy()
    out["cod_mpio"] = codes.astype("string")
    out["cod_dpto"] = out["cod_mpio"].map(dpto_by_mpio).astype("string")
    if len(out) != len(df) or not out.index.equals(df.index):
        raise SpatialJoinError("spatial_join alteró el número u orden de filas (viola la conservación)")
    return out


def validate_join(df: pd.DataFrame, n_entrada: int, max_sin_municipio: float, lg: CheckLog) -> None:
    """
    V-SJ1 conservación: filas de salida = filas de entrada              → bloqueante
    V-SJ2 detecciones sin municipio < max_sin_municipio                 → bloqueante
    V-SJ3 cod_dpto coincide con los 2 primeros dígitos de cod_mpio      → bloqueante
    """
    lg.add("V-SJ1", len(df) == n_entrada, f"entrada={n_entrada} salida={len(df)}")
    sin = int(df["cod_mpio"].isna().sum())
    share = sin / len(df) if len(df) else 0.0
    lg.add("V-SJ2", share < max_sin_municipio,
           f"{sin} detecciones sin municipio ({share:.2%}, máx {max_sin_municipio:.0%})",
           details={"sin_municipio": sin, "proporcion": share,
                    "sin_municipio_no_estaticas": int((df["cod_mpio"].isna() & ~df["fuente_estatica"]).sum())})
    con = df[df["cod_mpio"].notna()]
    bad = int((con["cod_dpto"] != con["cod_mpio"].str[:2]).sum())
    lg.add("V-SJ3", bad == 0, f"{bad} filas con cod_dpto ≠ prefijo de cod_mpio")
