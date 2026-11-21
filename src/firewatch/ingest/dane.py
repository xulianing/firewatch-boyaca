"""
firewatch.ingest.dane — Dimensiones territoriales desde el MGN 2025 (DANE).

Produce tres artefactos silver (diseño aprobado el 4 nov 2026):
    silver/geo_municipios/    ~1.122 polígonos de Colombia, EPSG:4326, simplificados → spatial_join.py (L2)
    silver/dim_municipio/     123 municipios de Boyacá, sin geometría             → Spark, gold, BI, EDA, meteo
    silver/dim_departamento/  33 departamentos                                    → agg_depto_mes (KPI K6)

Decisiones
    D-G1  Geometría y dimensión separadas: Spark/Power BI no cargan polígonos; el cruce espacial no
          depende del clima.
    D-G2  area_km2 se calcula en EPSG:9377 sobre la geometría SIN simplificar (denominador de la
          densidad); area_oficial_km2 (mpio_narea) solo audita (V-D7).
    D-G3  Centroide: regla ÚNICA en compute_centroids(); build_centroides_boyaca.py es un envoltorio.
          Secuencia exacta: raw → to_crs(9377) → make_valid → centroid (rep_point si cae fuera) →
          to_crs(4326) → round(4). V-D11 exige igualdad con config/centroides_boyaca.csv, porque el
          clima ya se descargó con esas coordenadas (y cambiarlas invalidaría la caché de meteo).
    D-G4  cod_provincia = slug ASCII en mayúsculas del nombre de la provincia.
    D-G5  elevacion_m = campo `elevation` de data/raw/meteo/<cod>.json (fuente F2 ya aprobada).
    D-G6  Fronteras compartidas en el cruce: desempate determinista por el menor cod_mpio
          (assign_points_to_polygons, reutilizada por spatial_join.py).
    D-G7  Si falla una validación bloqueante NO se publica ningún artefacto silver (solo el informe).

Modos temporales (salida marcada "incompleto"; la corrida final exige ambos completos):
    --sin-provincias   mientras A2 (config/provincias_boyaca.csv) está pendiente → V-D10 = pendiente
    --sin-elevacion    mientras el clima de los 123 no está descargado          → V-D12 = pendiente
    --solo-geo         solo geo_municipios (desbloquea B4)

Códigos de salida: 0 completo · 3 publicado en modo temporal (incompleto) · 1 falló una validación
bloqueante (no se publica nada) · 2 error de configuración o de lectura.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

try:                                    # el núcleo geométrico requiere GeoPandas + Shapely 2
    import geopandas as gpd
    import shapely
    from shapely.geometry import MultiPolygon, Polygon
except ImportError:                     # pragma: no cover — permite probar la lógica tabular sin GIS
    gpd = None
    shapely = None

from firewatch.utils.io import replace_atomic, sha256_file, write_atomic

log = logging.getLogger(__name__)

CODE_COLUMN_EXPECTED = "mpio_cdpmp"      # H3: en MGN 2025 el DIVIPOLA de 5 dígitos está aquí
RE_MPIO = re.compile(r"^\d{5}$")
RE_DPTO = re.compile(r"^\d{2}$")


# =============================================================================
# 1. Contratos de datos (fuente única: también los usa el diccionario de datos)
# =============================================================================
# columna → (dtype pandas, admite nulos en la corrida FINAL)

DIM_MUNICIPIO_SCHEMA: dict[str, tuple[str, bool]] = {
    "cod_mpio": ("string", False),
    "cod_dpto": ("string", False),
    "nombre": ("string", False),
    "cod_provincia": ("string", False),     # nulo solo con --sin-provincias
    "provincia": ("string", False),         # nulo solo con --sin-provincias
    "area_km2": ("float64", False),
    "area_oficial_km2": ("float64", False),
    "lat_c": ("float64", False),
    "lon_c": ("float64", False),
    "metodo_centroide": ("string", False),
    "elevacion_m": ("float64", False),      # nulo solo con --sin-elevacion
    "vigencia_mgn": ("int64", False),
}

DIM_DEPARTAMENTO_SCHEMA: dict[str, tuple[str, bool]] = {
    "cod_dpto": ("string", False),
    "nombre": ("string", False),
}

GEO_MUNICIPIOS_COLUMNS = ("cod_mpio", "cod_dpto", "geometry")


# =============================================================================
# 2. Configuración
# =============================================================================

@dataclass(frozen=True)
class DaneConfig:
    raw_mgn: Path                     # data/raw/dane/MGN2025_MPIO_GRAFICO/MGN_ADM_MPIO_GRAFICO.shp
    provincias_csv: Path              # config/provincias_boyaca.csv (A2)
    centroides_csv: Path              # config/centroides_boyaca.csv (puente meteo)
    raw_meteo: Path                   # data/raw/meteo
    silver_dir: Path                  # data/silver
    vigencia: int = 2025
    cod_dpto_foco: str = "15"
    n_mpios_foco: int = 123
    n_mpios_pais: tuple[int, int] = (1100, 1125)
    n_dptos: int = 33
    code_column: str = CODE_COLUMN_EXPECTED
    crs_origen: int = 4326            # V-D5: el .prj del MGN 2025 publicado declara WGS 84
    crs_area: int = 9377              # MAGNA-SIRGAS Origen Nacional
    crs_salida: int = 4326
    coord_decimals: int = 4
    simplify_tolerance_deg: float = 0.0003   # V-D13 con el MGN real: 0,001° no alcanza el acuerdo mínimo
    area_tol: float = 0.05            # V-D7: |calc − oficial| / oficial
    area_tol_share: float = 0.95      # V-D7: fracción mínima de municipios dentro de la tolerancia
    area_foco_km2: tuple[float, float] = (21_000.0, 25_000.0)   # suma de Boyacá ≈ 23.000 km²
    simplify_area_tol: float = 0.01   # V-D13: cambio de área máximo por polígono del foco
    sjoin_check_points: int = 100_000
    sjoin_agreement_min: float = 0.999
    sjoin_seed: int = 20261104
    elevation_range: tuple[float, float] = (100.0, 4_500.0)

    @property
    def out_geo(self) -> Path:
        return self.silver_dir / "geo_municipios" / "part-0000.parquet"

    @property
    def out_dim_municipio(self) -> Path:
        return self.silver_dir / "dim_municipio" / "part-0000.parquet"

    @property
    def out_dim_departamento(self) -> Path:
        return self.silver_dir / "dim_departamento" / "part-0000.parquet"

    @property
    def out_report(self) -> Path:
        return self.silver_dir / "_reports" / "dane_validation.json"


class DaneError(Exception):
    """Error de configuración o de lectura (exit 2)."""


class DaneValidationError(Exception):
    """Falló una validación bloqueante (exit 1). No se publica ningún artefacto."""


def load_config(settings: dict[str, Any], base_dir: str | Path | None = None) -> DaneConfig:
    """
    Construye DaneConfig desde settings.yaml:
        rutas: {raw_mgn, provincias_csv, centroides, raw_meteo, silver}
        territorio: {cod_dpto_foco, n_municipios_foco}
        dane:  {vigencia, n_mpios_pais: [min, max], crs_origen, simplify_tolerance_deg, ... (opcionales)}
    Rutas relativas → contra base_dir (raíz del repo).
    """
    if not isinstance(settings.get("rutas"), dict):
        raise DaneError("settings.yaml: falta la sección 'rutas'")
    r, d = settings["rutas"], settings.get("dane", {}) or {}
    t = settings.get("territorio", {}) or {}
    base = Path(base_dir) if base_dir is not None else Path.cwd()

    def p(key: str, default: str) -> Path:
        path = Path(r.get(key, default))
        return path if path.is_absolute() else base / path

    def pair(key: str, default: tuple) -> tuple:
        val = d.get(key, default)
        if not isinstance(val, (list, tuple)) or len(val) != 2:
            raise DaneError(f"dane.{key} debe ser [min, max]")
        return (type(default[0])(val[0]), type(default[1])(val[1]))

    defaults = DaneConfig(Path(), Path(), Path(), Path(), Path())
    cfg = DaneConfig(
        raw_mgn=p("raw_mgn", "data/raw/dane/MGN2025_MPIO_GRAFICO/MGN_ADM_MPIO_GRAFICO.shp"),
        provincias_csv=p("provincias_csv", "config/provincias_boyaca.csv"),
        centroides_csv=p("centroides", "config/centroides_boyaca.csv"),
        raw_meteo=p("raw_meteo", "data/raw/meteo"),
        silver_dir=p("silver", "data/silver"),
        vigencia=int(d.get("vigencia", defaults.vigencia)),
        cod_dpto_foco=str(d.get("cod_dpto_foco", t.get("cod_dpto_foco", defaults.cod_dpto_foco))).zfill(2),
        n_mpios_foco=int(d.get("n_mpios_foco", t.get("n_municipios_foco", defaults.n_mpios_foco))),
        n_mpios_pais=pair("n_mpios_pais", defaults.n_mpios_pais),
        n_dptos=int(d.get("n_dptos", defaults.n_dptos)),
        code_column=str(d.get("code_column", defaults.code_column)),
        crs_origen=int(d.get("crs_origen", defaults.crs_origen)),
        crs_area=int(d.get("crs_area", defaults.crs_area)),
        coord_decimals=int(d.get("coord_decimals", defaults.coord_decimals)),
        simplify_tolerance_deg=float(d.get("simplify_tolerance_deg", defaults.simplify_tolerance_deg)),
        area_tol=float(d.get("area_tol", defaults.area_tol)),
        area_tol_share=float(d.get("area_tol_share", defaults.area_tol_share)),
        area_foco_km2=pair("area_foco_km2", defaults.area_foco_km2),
        simplify_area_tol=float(d.get("simplify_area_tol", defaults.simplify_area_tol)),
        sjoin_check_points=int(d.get("sjoin_check_points", defaults.sjoin_check_points)),
        sjoin_agreement_min=float(d.get("sjoin_agreement_min", defaults.sjoin_agreement_min)),
        sjoin_seed=int(d.get("sjoin_seed", defaults.sjoin_seed)),
        elevation_range=pair("elevation_range", defaults.elevation_range),
    )
    if cfg.simplify_tolerance_deg <= 0 or cfg.simplify_tolerance_deg > 0.003:
        raise DaneError("dane.simplify_tolerance_deg debe estar en (0, 0.003] (píxel VIIRS ≈ 0,0034°)")
    return cfg


# =============================================================================
# 3. Registro de validaciones
# =============================================================================

@dataclass
class Check:
    id: str
    status: str                       # ok | fail | warn | pending | info
    blocking: bool
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)


class CheckLog:
    """Acumula los checks V-D* de una corrida y decide si se puede publicar."""

    def __init__(self) -> None:
        self.checks: list[Check] = []

    def add(self, cid: str, passed: bool, message: str = "", *, blocking: bool = True,
            details: dict[str, Any] | None = None) -> bool:
        status = "ok" if passed else ("fail" if blocking else "warn")
        self.checks.append(Check(cid, status, blocking, "" if passed else message, details or {}))
        (log.info if passed else log.error if blocking else log.warning)("%s %s %s", cid, status.upper(),
                                                                        "" if passed else message)
        return passed

    def info(self, cid: str, message: str = "", details: dict[str, Any] | None = None) -> None:
        self.checks.append(Check(cid, "info", False, message, details or {}))

    def pending(self, cid: str, message: str) -> None:
        self.checks.append(Check(cid, "pending", True, message))
        log.warning("%s PENDIENTE: %s", cid, message)

    @property
    def failures(self) -> list[Check]:
        return [c for c in self.checks if c.status == "fail"]

    @property
    def pendings(self) -> list[Check]:
        return [c for c in self.checks if c.status == "pending"]

    def status(self) -> str:
        if self.failures:
            return "fallido"
        return "incompleto" if self.pendings else "completo"

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status(), "checks": [asdict(c) for c in self.checks]}


def _require_gis() -> None:
    if gpd is None:
        raise DaneError("GeoPandas/Shapely no están instalados (pip install -r requirements.txt)")


# =============================================================================
# 4. Lectura y normalización
# =============================================================================

def read_mgn(path: Path, log_: CheckLog | None = None) -> "gpd.GeoDataFrame":
    """Lee la capa municipal (raw, inmutable). V-D1: existen los archivos del shapefile y abre sin error."""
    _require_gis()
    path = Path(path)
    if path.suffix.lower() == ".shp":
        missing = [ext for ext in (".shp", ".shx", ".dbf", ".prj") if not path.with_suffix(ext).exists()]
        if missing:
            raise DaneError(f"V-D1: faltan componentes del shapefile {missing} junto a {path}")
    if not path.exists():
        raise DaneError(f"V-D1: no existe {path}")
    try:
        gdf = gpd.read_file(path, engine="pyogrio")
    except Exception as exc:  # noqa: BLE001 — cualquier fallo de lectura es fatal
        raise DaneError(f"V-D1: no se pudo leer {path}: {exc}") from exc
    if log_ is not None:
        log_.add("V-D1", True, details={"path": str(path), "n": len(gdf)})
    return gdf


def detect_code_column(df: pd.DataFrame, dpto_col: str = "dpto_ccdgo") -> list[str]:
    """
    H3: columnas cuyo contenido es un DIVIPOLA de 5 dígitos (texto) que empieza por el código del
    departamento. Devuelve TODAS las candidatas (normalmente solo 'mpio_cdpmp').
    """
    dpto = df[dpto_col].astype("string")
    out = []
    for col in df.columns:
        if col in ("geometry", dpto_col) or not (pd.api.types.is_object_dtype(df[col])
                                                 or pd.api.types.is_string_dtype(df[col])):
            continue
        s = df[col].astype("string")
        if s.str.fullmatch(r"\d{5}").fillna(False).all() and (s.str[:2] == dpto).fillna(False).all():
            out.append(col)
    return out


def normalize_columns(df: pd.DataFrame, code_column: str = CODE_COLUMN_EXPECTED,
                      log_: CheckLog | None = None) -> pd.DataFrame:
    """
    Nombres en minúscula; agrega cod_mpio / cod_dpto como texto.
    V-D3: la columna de 5 dígitos detectada por contenido debe ser `code_column`.
    V-D4: los códigos deben venir como texto (si el DBF los trae numéricos se perdió el cero inicial).
    """
    out = df.rename(columns={c: c.lower() for c in df.columns if c != "geometry"})
    for col in ("dpto_ccdgo", code_column, "mpio_cnmbr", "mpio_narea"):
        if col not in out.columns:
            raise DaneError(f"V-D3: el MGN no tiene la columna '{col}'. Columnas: {list(out.columns)}")

    numeric_codes = [c for c in ("dpto_ccdgo", code_column) if pd.api.types.is_numeric_dtype(out[c])]
    v4 = not numeric_codes
    if log_ is not None:
        log_.add("V-D4", v4, f"códigos numéricos (se pierde el cero inicial): {numeric_codes}")
    if not v4:
        raise DaneValidationError(f"V-D4: códigos numéricos {numeric_codes}")

    candidates = detect_code_column(out)
    v3 = code_column in candidates
    if log_ is not None:
        log_.add("V-D3", v3, f"'{code_column}' no es un DIVIPOLA de 5 dígitos; candidatas: {candidates}",
                 details={"code_column": code_column, "candidates": candidates})
    if not v3:
        raise DaneValidationError(f"V-D3: columna de código inválida; candidatas {candidates}")

    out["cod_mpio"] = out[code_column].astype("string").str.strip()
    out["cod_dpto"] = out["dpto_ccdgo"].astype("string").str.strip()
    return out


def validate_raw(gdf: "gpd.GeoDataFrame", cfg: DaneConfig, log_: CheckLog) -> None:
    """V-D2 (conteos), V-D5 (CRS), V-D6 (geometrías vacías/ inválidas: se reportan; se corrigen después)."""
    n = len(gdf)
    n_foco = int((gdf["cod_dpto"] == cfg.cod_dpto_foco).sum())
    lo, hi = cfg.n_mpios_pais
    dups = int(gdf["cod_mpio"].duplicated().sum())
    log_.add("V-D2", lo <= n <= hi and n_foco == cfg.n_mpios_foco and dups == 0,
             f"total={n} (esperado {lo}–{hi}), foco {cfg.cod_dpto_foco}={n_foco} "
             f"(esperado {cfg.n_mpios_foco}), cod_mpio duplicados={dups}",
             details={"n_total": n, "n_foco": n_foco, "duplicados": dups})

    epsg = gdf.crs.to_epsg(min_confidence=25) if gdf.crs is not None else None   # .prj ESRI de MAGNA
    log_.add("V-D5", epsg == cfg.crs_origen, f"CRS EPSG:{epsg}, se esperaba EPSG:{cfg.crs_origen}",
             details={"epsg": epsg, "crs": str(gdf.crs)})

    empty = int((gdf.geometry.isna() | gdf.geometry.is_empty).sum())
    invalid = int((~gdf.geometry.is_valid).sum())
    log_.add("V-D6", empty == 0, f"{empty} geometrías vacías",
             details={"vacias": empty, "invalidas_raw": invalid})


# =============================================================================
# 5. Geometría
# =============================================================================

def _to_multipolygon(geom):
    """Conserva solo las partes poligonales (make_valid puede devolver colecciones con líneas)."""
    if geom is None or geom.is_empty:
        return geom
    if isinstance(geom, MultiPolygon):
        return geom
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    polys: list = []
    for part in shapely.get_parts(geom):
        if isinstance(part, Polygon):
            polys.append(part)
        elif isinstance(part, MultiPolygon):
            polys.extend(part.geoms)
    return MultiPolygon(polys) if polys else Polygon()


def _clean_geoms(geoms: "gpd.GeoSeries") -> "gpd.GeoSeries":
    """make_valid + solo partes poligonales, conservando índice y CRS."""
    return gpd.GeoSeries(geoms.make_valid().apply(_to_multipolygon), index=geoms.index, crs=geoms.crs)


def assign_points_to_polygons(points: "gpd.GeoSeries", polygons: "gpd.GeoDataFrame",
                              code_col: str = "cod_mpio") -> pd.Series:
    """
    Asigna a cada punto el código del polígono que lo contiene (predicado 'intersects' con STRtree).
    D-G6: un punto en una frontera compartida toma el MENOR código. Sin polígono → <NA>.
    Es la regla oficial del cruce: spatial_join.py debe usar esta función.
    """
    if points.crs != polygons.crs:
        raise DaneError(f"CRS distintos: puntos {points.crs} vs polígonos {polygons.crs}")
    idx_pt, idx_poly = polygons.sindex.query(points.values, predicate="intersects")
    codes = polygons[code_col].to_numpy()[idx_poly]
    best = pd.Series(codes, index=points.index[idx_pt]).groupby(level=0).min()
    return best.reindex(points.index).astype("string")


def build_geo_layer(gdf: "gpd.GeoDataFrame", cfg: DaneConfig, log_: CheckLog
                    ) -> tuple["gpd.GeoDataFrame", "gpd.GeoDataFrame"]:
    """
    Capa nacional para el cruce: make_valid (CRS origen) → to_crs(4326) → simplify → make_valid.
    Devuelve (simplificada, original_valida_4326). V-D6 (post) y V-D13.
    """
    g = gdf[["cod_mpio", "cod_dpto", "geometry"]].copy()
    g["geometry"] = _clean_geoms(g.geometry)
    g = g.to_crs(cfg.crs_salida)
    original = g.copy()

    g["geometry"] = _clean_geoms(g.geometry.simplify(cfg.simplify_tolerance_deg, preserve_topology=True))
    invalid = int((~g.geometry.is_valid).sum())
    empty = int((g.geometry.isna() | g.geometry.is_empty).sum())
    log_.add("V-D6b", invalid == 0 and empty == 0,
             f"tras simplificar: {invalid} inválidas, {empty} vacías",
             details={"invalidas": invalid, "vacias": empty})

    validate_simplification(original, g, cfg, log_)
    return g.sort_values("cod_mpio").reset_index(drop=True), original


def validate_simplification(original: "gpd.GeoDataFrame", simplified: "gpd.GeoDataFrame",
                            cfg: DaneConfig, log_: CheckLog) -> None:
    """
    V-D13:
      a) mismo número de polígonos;
      b) cambio de área < simplify_area_tol en TODOS los municipios del foco (bloqueante) y
         conteo nacional por encima del umbral (informativo: islas y municipios diminutos);
      c) acuerdo ≥ sjoin_agreement_min entre la asignación con la capa original y la simplificada
         sobre N puntos aleatorios (semilla fija) en la caja de Colombia.
    """
    a_orig = original.to_crs(cfg.crs_area).area.to_numpy()
    a_simp = simplified.to_crs(cfg.crs_area).area.to_numpy()      # mismo orden e índice que `original`
    rel = np.abs(a_simp - a_orig) / np.where(a_orig > 0, a_orig, np.nan)
    foco = (original["cod_dpto"] == cfg.cod_dpto_foco).to_numpy()
    max_foco = float(np.nanmax(rel[foco])) if foco.any() else 0.0
    n_over_nat = int(np.nansum(rel > cfg.simplify_area_tol))

    rng = np.random.default_rng(cfg.sjoin_seed)
    minx, miny, maxx, maxy = original.total_bounds
    pts = gpd.GeoSeries(gpd.points_from_xy(rng.uniform(minx, maxx, cfg.sjoin_check_points),
                                           rng.uniform(miny, maxy, cfg.sjoin_check_points)),
                        crs=original.crs)
    a = assign_points_to_polygons(pts, original)
    b = assign_points_to_polygons(pts, simplified)
    assigned = a.notna()
    n_assigned = int(assigned.sum())
    same = int((a[assigned] == b[assigned]).fillna(False).sum())
    agreement = same / n_assigned if n_assigned else 1.0
    lost = int((assigned & b.isna()).sum())
    gained = int((~assigned & b.notna()).sum())

    ok = (len(original) == len(simplified) and max_foco < cfg.simplify_area_tol
          and agreement >= cfg.sjoin_agreement_min)
    log_.add("V-D13", ok,
             f"n {len(original)}→{len(simplified)}; Δárea máx foco={max_foco:.4%} "
             f"(umbral {cfg.simplify_area_tol:.0%}); acuerdo={agreement:.5f} (mín {cfg.sjoin_agreement_min})",
             details={"n_original": len(original), "n_simplificada": len(simplified),
                      "delta_area_max_foco": max_foco, "n_pais_sobre_umbral": n_over_nat,
                      "puntos": cfg.sjoin_check_points, "asignados_original": n_assigned,
                      "acuerdo": agreement, "perdidos": lost, "ganados": gained,
                      "tolerancia_deg": cfg.simplify_tolerance_deg})


def compute_area_km2(gdf_area_crs: "gpd.GeoDataFrame") -> pd.Series:
    """Área en km² en un CRS proyectado (EPSG:9377). La geometría debe estar ya validada."""
    if gdf_area_crs.crs is None or not gdf_area_crs.crs.is_projected:
        raise DaneError("compute_area_km2 requiere un CRS proyectado (EPSG:9377)")
    return gdf_area_crs.geometry.area / 1e6


def compute_centroids(gdf_raw_foco: "gpd.GeoDataFrame", crs_area: int = 9377, decimals: int = 4,
                      crs_salida: int = 4326) -> pd.DataFrame:
    """
    D-G3 — REGLA ÚNICA DEL CENTROIDE (la usan dane.py y scripts/build_centroides_boyaca.py).
    Secuencia exacta (no alterar: V-D11 compara con el CSV con el que se descargó el clima):
        raw → to_crs(crs_area) → make_valid → centroid
        → si el centroide no está dentro del polígono: representative_point()
        → to_crs(crs_salida) → round(decimals)
    Entrada: GeoDataFrame con 'cod_mpio' en el CRS de origen. Salida: cod_mpio, lat_c, lon_c,
    metodo_centroide, más 'geometry_area_crs' no se devuelve (ver compute_area_km2).
    """
    _require_gis()
    g = gdf_raw_foco[["cod_mpio", "geometry"]].to_crs(crs_area)
    geom = g.geometry.make_valid()
    cent = geom.centroid
    inside = cent.within(geom)
    pts = gpd.GeoSeries(cent.where(inside, geom.representative_point()), crs=crs_area).to_crs(crs_salida)
    return pd.DataFrame({
        "cod_mpio": g["cod_mpio"].astype("string").to_numpy(),
        "lat_c": pts.y.round(decimals).to_numpy(),
        "lon_c": pts.x.round(decimals).to_numpy(),
        "metodo_centroide": np.where(inside.to_numpy(), "centroid", "rep_point"),
    })


# =============================================================================
# 6. Dimensiones (lógica tabular, testeable sin GIS)
# =============================================================================

def slugify_provincia(name: Any) -> str:
    """D-G4: 'Zona de Manejo Especial' → 'ZONA_DE_MANEJO_ESPECIAL'; 'Sugamuxí' → 'SUGAMUXI'."""
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").upper()
    if not s:
        raise ValueError(f"nombre de provincia vacío o inválido: {name!r}")
    return s


def _norm_name(s: Any) -> str:
    return slugify_provincia(s) if str(s).strip() else ""


def build_dim_departamento(df: pd.DataFrame, cfg: DaneConfig, log_: CheckLog) -> pd.DataFrame:
    """33 filas: cod_dpto, nombre (de dpto_cnmbr). V-D14."""
    if "dpto_cnmbr" not in df.columns:
        log_.add("V-D14", False, "el MGN no trae 'dpto_cnmbr' (nombre del departamento)")
        return pd.DataFrame(columns=list(DIM_DEPARTAMENTO_SCHEMA))
    dep = (df[["cod_dpto", "dpto_cnmbr"]].drop_duplicates()
           .rename(columns={"dpto_cnmbr": "nombre"}).sort_values("cod_dpto").reset_index(drop=True))
    dup = int(dep["cod_dpto"].duplicated().sum())           # un código con dos nombres distintos
    bad = int((~dep["cod_dpto"].astype("string").str.fullmatch(r"\d{2}").fillna(False)).sum())
    log_.add("V-D14", len(dep) == cfg.n_dptos and dup == 0 and bad == 0,
             f"{len(dep)} departamentos (esperado {cfg.n_dptos}), {dup} códigos con varios nombres, "
             f"{bad} códigos mal formados", details={"n": len(dep)})
    return dep


def read_provincias(path: Path) -> pd.DataFrame:
    """CSV de A2 (cod_mpio, municipio, provincia), todo como texto. Vacío (solo encabezado) es válido aquí."""
    if not Path(path).exists():
        return pd.DataFrame(columns=["cod_mpio", "municipio", "provincia"], dtype="string")
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]
    missing = {"cod_mpio", "municipio", "provincia"} - set(df.columns)
    if missing:
        raise DaneError(f"{path}: faltan columnas {sorted(missing)}")
    return df.apply(lambda s: s.str.strip()).astype("string")


def join_provincias(dim: pd.DataFrame, prov: pd.DataFrame, log_: CheckLog) -> pd.DataFrame:
    """
    V-D10 (bloqueante en la corrida final): join POR CÓDIGO; 123/123 con provincia, sin huérfanos,
    sin duplicados, provincia no vacía. Diferencias de nombre MGN↔CSV → advertencia V-D10w.
    """
    out = dim.drop(columns=["provincia", "cod_provincia"], errors="ignore")
    problems: list[str] = []
    if prov.empty:
        problems.append("el CSV de provincias está vacío (A2 pendiente)")
    bad_codes = prov.loc[~prov["cod_mpio"].str.fullmatch(r"\d{5}").fillna(False), "cod_mpio"].tolist()
    dups = prov.loc[prov["cod_mpio"].duplicated(), "cod_mpio"].tolist()
    empty_prov = prov.loc[prov["provincia"].fillna("") == "", "cod_mpio"].tolist()
    missing = sorted(set(out["cod_mpio"]) - set(prov["cod_mpio"]))
    orphans = sorted(set(prov["cod_mpio"]) - set(out["cod_mpio"]))
    for label, items in (("códigos mal formados", bad_codes), ("duplicados", dups),
                         ("sin provincia", empty_prov), ("municipios sin fila en el CSV", missing),
                         ("códigos del CSV que no son del foco", orphans)):
        if items and not (label == "municipios sin fila en el CSV" and prov.empty):
            problems.append(f"{label}: {items[:10]}{' …' if len(items) > 10 else ''} ({len(items)})")

    ok = log_.add("V-D10", not problems, "; ".join(problems),
                  details={"faltantes": len(missing), "huerfanos": len(orphans)})
    merged = out.merge(prov[["cod_mpio", "municipio", "provincia"]].drop_duplicates("cod_mpio"),
                       on="cod_mpio", how="left")
    if ok:
        mism = merged.loc[merged["nombre"].map(_norm_name) != merged["municipio"].map(_norm_name),
                          ["cod_mpio", "nombre", "municipio"]]
        log_.add("V-D10w", mism.empty, f"{len(mism)} nombres MGN≠CSV (se usa el del MGN): "
                 f"{mism.head(5).to_dict('records')}", blocking=False)
        log_.info("V-D10i", f"{merged['provincia'].nunique()} provincias",
                  {"provincias": sorted(merged["provincia"].unique().tolist())})
    merged["cod_provincia"] = merged["provincia"].map(lambda x: slugify_provincia(x) if pd.notna(x) and x else pd.NA)
    return merged.drop(columns=["municipio"])


def check_centroides_csv(cent: pd.DataFrame, csv_path: Path, decimals: int, log_: CheckLog) -> None:
    """V-D11 (bloqueante): lat_c/lon_c idénticos (a `decimals`) al CSV con el que se descargó el clima."""
    if not Path(csv_path).exists():
        log_.add("V-D11", False, f"no existe {csv_path} (debe existir: ancla de las coordenadas del clima)")
        return
    ref = pd.read_csv(csv_path, dtype={"cod_mpio": str}, encoding="utf-8-sig")
    m = cent.merge(ref[["cod_mpio", "lat_c", "lon_c"]], on="cod_mpio", how="outer",
                   suffixes=("", "_csv"), indicator=True)
    only = m[m["_merge"] != "both"]["cod_mpio"].tolist()
    both = m[m["_merge"] == "both"]
    tol = 0.5 * 10 ** (-decimals)
    diff = both[(np.abs(both["lat_c"].round(decimals) - both["lat_c_csv"].round(decimals)) > tol / 10)
                | (np.abs(both["lon_c"].round(decimals) - both["lon_c_csv"].round(decimals)) > tol / 10)]
    log_.add("V-D11", not only and diff.empty,
             f"{len(only)} códigos solo en un lado {only[:5]}; {len(diff)} coordenadas distintas "
             f"{diff[['cod_mpio', 'lat_c', 'lat_c_csv', 'lon_c', 'lon_c_csv']].head(5).to_dict('records')}",
             details={"solo_un_lado": len(only), "distintas": len(diff)})


def enrich_elevation(dim: pd.DataFrame, raw_meteo: Path, elev_range: tuple[float, float],
                     log_: CheckLog) -> pd.DataFrame:
    """V-D12: elevacion_m desde data/raw/meteo/<cod>.json (clave 'elevation'); 123 valores en rango."""
    vals, missing = [], []
    for cod in dim["cod_mpio"]:
        f = Path(raw_meteo) / f"{cod}.json"
        try:
            vals.append(float(json.loads(f.read_bytes()).get("elevation")))
        except (FileNotFoundError, TypeError, ValueError):
            vals.append(np.nan)
            missing.append(cod)
    out = dim.copy()
    out["elevacion_m"] = vals
    lo, hi = elev_range
    out_rng = out.loc[out["elevacion_m"].notna() & ~out["elevacion_m"].between(lo, hi), "cod_mpio"].tolist()
    log_.add("V-D12", not missing and not out_rng,
             f"{len(missing)} sin elevación {missing[:5]}; {len(out_rng)} fuera de [{lo}, {hi}] m {out_rng[:5]}",
             details={"faltantes": len(missing), "fuera_de_rango": len(out_rng),
                      "min": float(np.nanmin(vals)) if len(missing) < len(vals) else None,
                      "max": float(np.nanmax(vals)) if len(missing) < len(vals) else None})
    return out


def check_area(dim: pd.DataFrame, cfg: DaneConfig, log_: CheckLog) -> None:
    """V-D7: |área calculada − oficial| / oficial < area_tol en ≥ area_tol_share; suma del foco en rango."""
    rel = (dim["area_km2"] - dim["area_oficial_km2"]).abs() / dim["area_oficial_km2"]
    share = float((rel < cfg.area_tol).mean())
    total = float(dim["area_km2"].sum())
    lo, hi = cfg.area_foco_km2
    log_.add("V-D7", share >= cfg.area_tol_share and lo <= total <= hi,
             f"{share:.1%} dentro de ±{cfg.area_tol:.0%} (mín {cfg.area_tol_share:.0%}); "
             f"suma={total:,.0f} km² (esperado {lo:,.0f}–{hi:,.0f}); ¿mpio_narea en ha?",
             details={"share_ok": share, "suma_km2": total, "rel_max": float(rel.max())})


def enforce_schema(df: pd.DataFrame, schema: dict[str, tuple[str, bool]], log_: CheckLog, cid: str,
                   allow_null: Sequence[str] = (), pk: str | None = None) -> pd.DataFrame:
    """V-D15: columnas exactas y en orden, dtypes del contrato, PK única, nulos solo donde se permiten."""
    missing = [c for c in schema if c not in df.columns]
    if missing:
        log_.add(cid, False, f"faltan columnas {missing}")
        return df
    out = df[list(schema)].copy()
    problems = []
    for col, (dtype, nullable) in schema.items():
        try:
            out[col] = out[col].astype(dtype)
        except (TypeError, ValueError) as exc:
            problems.append(f"{col}: no convierte a {dtype} ({exc})")
            continue
        n_null = int(out[col].isna().sum())
        if n_null and not nullable and col not in allow_null:
            problems.append(f"{col}: {n_null} nulos")
    if pk is not None and out[pk].duplicated().any():
        problems.append(f"PK {pk} duplicada")
    log_.add(cid, not problems, "; ".join(problems), details={"filas": len(out)})
    return out.sort_values(pk).reset_index(drop=True) if pk else out


# =============================================================================
# 7. Escritura y orquestación
# =============================================================================

def _write_parquet(df: pd.DataFrame, dest: Path) -> None:
    tmp = dest.with_suffix(".parquet.part")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    if gpd is not None and isinstance(df, gpd.GeoDataFrame):
        df.to_parquet(tmp, index=False)                  # GeoParquet
    else:
        df.to_parquet(tmp, index=False)
    replace_atomic(tmp, dest)


def write_report(cfg: DaneConfig, log_: CheckLog, extra: dict[str, Any]) -> dict[str, Any]:
    report = {"etapa": "dane", "generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              **extra, **log_.to_dict()}
    write_atomic(cfg.out_report, json.dumps(report, ensure_ascii=False, indent=2, default=str).encode("utf-8"))
    return report


def run(cfg: DaneConfig, *, sin_provincias: bool = False, sin_elevacion: bool = False,
        solo_geo: bool = False) -> dict[str, Any]:
    """
    Flujo completo. Publica artefactos SOLO si no falla ningún check bloqueante (D-G7).
    Lanza DaneValidationError (tras escribir el informe) si algo bloqueante falla.
    """
    _require_gis()
    lg = CheckLog()
    modo = {"sin_provincias": sin_provincias, "sin_elevacion": sin_elevacion, "solo_geo": solo_geo}
    extra: dict[str, Any] = {"modo": modo, "fuente": str(cfg.raw_mgn), "vigencia": cfg.vigencia}
    try:
        raw = read_mgn(cfg.raw_mgn, lg)
        extra["fuente_sha256"] = sha256_file(cfg.raw_mgn)
        raw = normalize_columns(raw, cfg.code_column, lg)
        validate_raw(raw, cfg, lg)
        if "mpio_nano" in raw.columns:
            years = sorted(pd.to_numeric(raw["mpio_nano"], errors="coerce").dropna().astype(int).unique().tolist())
            lg.add("V-D16", years == [cfg.vigencia], f"vigencias en mpio_nano={years}, esperado [{cfg.vigencia}]",
                   blocking=False)

        geo, _ = build_geo_layer(raw, cfg, lg)

        dim = dep = None
        if not solo_geo:
            dep = enforce_schema(build_dim_departamento(raw, cfg, lg), DIM_DEPARTAMENTO_SCHEMA, lg,
                                 "V-D15d", pk="cod_dpto")

            foco = raw[raw["cod_dpto"] == cfg.cod_dpto_foco]
            cent = compute_centroids(foco, cfg.crs_area, cfg.coord_decimals, cfg.crs_salida)
            n_rep = int((cent["metodo_centroide"] == "rep_point").sum())
            lg.info("V-D8", f"{n_rep} municipios usan representative_point", {"rep_point": n_rep})
            check_centroides_csv(cent, cfg.centroides_csv, cfg.coord_decimals, lg)

            foco_area = foco[["cod_mpio", "geometry"]].to_crs(cfg.crs_area)
            foco_area["geometry"] = foco_area.geometry.make_valid()
            dim = pd.DataFrame({
                "cod_mpio": foco["cod_mpio"].to_numpy(),
                "cod_dpto": foco["cod_dpto"].to_numpy(),
                "nombre": foco["mpio_cnmbr"].astype("string").str.strip().to_numpy(),
                "area_km2": compute_area_km2(foco_area).to_numpy(),
                "area_oficial_km2": pd.to_numeric(foco["mpio_narea"], errors="coerce").to_numpy(),
                "vigencia_mgn": cfg.vigencia,
            }).merge(cent, on="cod_mpio", how="left")
            check_area(dim, cfg, lg)

            if sin_provincias:
                dim["provincia"] = pd.NA
                dim["cod_provincia"] = pd.NA
                lg.pending("V-D10", "modo --sin-provincias: provincias_boyaca.csv (A2) pendiente")
            else:
                dim = join_provincias(dim, read_provincias(cfg.provincias_csv), lg)

            if sin_elevacion:
                dim["elevacion_m"] = np.nan
                lg.pending("V-D12", "modo --sin-elevacion: clima de los 123 municipios pendiente")
            else:
                dim = enrich_elevation(dim, cfg.raw_meteo, cfg.elevation_range, lg)

            allow = (["provincia", "cod_provincia"] if sin_provincias else []) + \
                    (["elevacion_m"] if sin_elevacion else [])
            dim = enforce_schema(dim, DIM_MUNICIPIO_SCHEMA, lg, "V-D15", allow_null=allow, pk="cod_mpio")
        else:
            lg.pending("V-D10", "modo --solo-geo")
            lg.pending("V-D12", "modo --solo-geo")
    except DaneValidationError:
        pass                                   # ya quedó registrado en el CheckLog

    if lg.failures:
        report = write_report(cfg, lg, extra)
        raise DaneValidationError("; ".join(f"{c.id}: {c.message}" for c in lg.failures))

    _write_parquet(geo, cfg.out_geo)
    meta = {"vigencia": cfg.vigencia, "tolerancia_deg": cfg.simplify_tolerance_deg, "crs": f"EPSG:{cfg.crs_salida}",
            "n": len(geo), "fuente_sha256": extra.get("fuente_sha256")}
    write_atomic(cfg.out_geo.parent / "_metadata.json", json.dumps(meta, indent=2).encode("utf-8"))
    extra["salidas"] = {"geo_municipios": str(cfg.out_geo)}
    if dim is not None:
        _write_parquet(dim, cfg.out_dim_municipio)
        _write_parquet(dep, cfg.out_dim_departamento)
        extra["salidas"].update({"dim_municipio": str(cfg.out_dim_municipio),
                                 "dim_departamento": str(cfg.out_dim_departamento)})
    return write_report(cfg, lg, extra)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="firewatch.ingest.dane", description="Dimensiones territoriales (MGN 2025)")
    ap.add_argument("--settings", default="config/settings.yaml")
    ap.add_argument("--sin-provincias", action="store_true", help="temporal: A2 pendiente (V-D10 pendiente)")
    ap.add_argument("--sin-elevacion", action="store_true", help="temporal: clima pendiente (V-D12 pendiente)")
    ap.add_argument("--solo-geo", action="store_true", help="solo silver/geo_municipios (desbloquea B4)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    try:
        import yaml
        settings = yaml.safe_load(Path(args.settings).read_text(encoding="utf-8"))
        cfg = load_config(settings, base_dir=Path(args.settings).resolve().parent.parent)
        report = run(cfg, sin_provincias=args.sin_provincias, sin_elevacion=args.sin_elevacion,
                     solo_geo=args.solo_geo)
    except DaneError as exc:
        log.error("%s", exc)
        return 2
    except DaneValidationError as exc:
        log.error("Validación bloqueante fallida; no se publicó nada. Ver %s\n%s", "dane_validation.json", exc)
        return 1
    print(json.dumps({"status": report["status"], "salidas": report.get("salidas"),
                      "pendientes": [c["id"] for c in report["checks"] if c["status"] == "pending"]},
                     ensure_ascii=False, indent=2))
    return 0 if report["status"] == "completo" else 3


if __name__ == "__main__":
    sys.exit(main())
