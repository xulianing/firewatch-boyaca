"""
PUENTE TEMPORAL (3 nov 2026): genera config/centroides_boyaca.csv desde el MGN 2025 para que
ingest/meteo.py no espere a silver.dim_municipio (B3). Cuando B3 exista, dim_municipio debe
producir EXACTAMENTE las mismas coordenadas (misma regla) y este script se retira.

Regla (Guía B1 §4, V-D8):
  - Departamento y número de municipios desde settings.yaml (territorio.cod_dpto_foco,
    territorio.n_municipios_foco); código de 5 dígitos detectado por contenido (H3: mpio_cdpmp en MGN 2025).
  - Centroide calculado en EPSG:9377 (MAGNA-SIRGAS Origen Nacional), nunca en grados.
  - Si el centroide cae fuera del polígono → representative_point() y se marca metodo="rep_point".
  - Se reproyecta a EPSG:4326 y se redondea a 4 decimales (≈ 11 m).

Uso (desde la raíz del repo):
    python scripts/build_centroides_boyaca.py --mgn data/raw/dane/MGN2025_MPIO_GRAFICO/MGN_ADM_MPIO_GRAFICO.shp
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import yaml

ROOT = Path(__file__).resolve().parents[1]
CRS_METRICO = 9377      # MAGNA-SIRGAS Origen Nacional: parte de la regla de centroides (no está en settings.yaml)


def find_code_column(gdf: gpd.GeoDataFrame) -> str:
    """Columna de 5 dígitos que empieza por el código de departamento (H3)."""
    for col in gdf.columns:
        if col == "geometry":
            continue
        s = gdf[col].astype(str)
        if s.str.fullmatch(r"\d{5}").all() and (s.str[:2] == gdf["dpto_ccdgo"].astype(str)).all():
            return col
    raise SystemExit("No se encontró la columna DIVIPOLA de 5 dígitos")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mgn", required=True)
    ap.add_argument("--out", default=str(ROOT / "config" / "centroides_boyaca.csv"))
    ap.add_argument("--settings", default=str(ROOT / "config" / "settings.yaml"))
    args = ap.parse_args()

    territorio = yaml.safe_load(Path(args.settings).read_text(encoding="utf-8"))["territorio"]
    cod_dpto = str(territorio["cod_dpto_foco"])
    n_esperado = int(territorio["n_municipios_foco"])

    gdf = gpd.read_file(args.mgn)
    gdf = gdf.rename(columns={c: c.lower() for c in gdf.columns if c != gdf.geometry.name})
    gdf["dpto_ccdgo"] = gdf["dpto_ccdgo"].astype(str)
    boy = gdf[gdf["dpto_ccdgo"] == cod_dpto].copy()
    assert len(boy) == n_esperado, f"El departamento {cod_dpto} debe tener {n_esperado} municipios y tiene {len(boy)}"
    code = find_code_column(boy)

    boy = boy.to_crs(CRS_METRICO)
    boy["geometry"] = boy.geometry.make_valid()
    cent = boy.geometry.centroid
    inside = cent.within(boy.geometry)
    pts = cent.where(inside, boy.geometry.representative_point())
    pts = gpd.GeoSeries(pts, crs=CRS_METRICO).to_crs(4326)

    out = boy[[code, "mpio_cnmbr"]].rename(columns={code: "cod_mpio", "mpio_cnmbr": "nombre"})
    out["lat_c"] = pts.y.round(4).values
    out["lon_c"] = pts.x.round(4).values
    out["metodo"] = ["centroid" if i else "rep_point" for i in inside]
    out = out.sort_values("cod_mpio")
    out.to_csv(args.out, index=False, encoding="utf-8")
    print(f"{len(out)} centroides → {args.out} · rep_point en {int((~inside).sum())} municipios")
    print(out[out.cod_mpio == "15001"].to_string(index=False))


if __name__ == "__main__":
    main()
