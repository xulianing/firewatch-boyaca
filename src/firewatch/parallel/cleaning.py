"""
firewatch.parallel.cleaning — Limpieza y validación de FIRMS antes del cruce espacial (L2, parte 1).

Base: firewatch_l2_secuencial_v0.1 (5 nov 2026), ajustado a la política CONGELADA de CLAUDE.md §4 y
Arquitectura §"Tratamiento SP frente a NRT":
  - type == firms.filtros.type_permitido (0) en SP; NRT no trae type y no se filtra por él.
  - Máscara de fuentes estáticas = celdas de firms.mascara_estatica.tamano_celda_grados con al menos una
    detección SP type == firms.mascara_estatica.type_estatico (2) en anios_fuente. Sin umbral mínimo de
    detecciones ni dilatación: la máscara ampliada de la v0.1 (type ∈ {1,2,3}, min_det, 3×3) requiere ADR.
  - La máscara se aplica a TODO (incluido NRT 2026) y solo MARCA fuente_estatica; gold excluye esas filas.
  - confidence ∉ firms.filtros.confianza_excluida.
  - Lectura de los CSV reales con firewatch.ingest.firms (rutas, alias brightness/bright_t31, SNPP).

  Fase 0 (secuencial, global):  listar_archivos_raw → read_raw → validate_schema (V-C1) → normalize (F0)
                                → validate_files (V-C2…V-C6) → compute_sp_cutoffs → build_static_mask
                                → mask_quality (V-C13) → build_context  ⇒  CleaningContext inmutable
  Fase 1 (pura, por bloque):    clean_chunk(df_normalizado, ctx) → (detecciones_limpias, CleaningMetrics)
                                reglas F1…F7 en orden fijo + marca F8 + id_det
  Reducción:                    Σ CleaningMetrics (suma asociativa) → validate_output (V-C7…V-C12, V-C14)

Invariante de bloque (la garantiza runner.plan_chunks): un bloque contiene TODAS las filas de cada
(satélite, fecha) que toca → la deduplicación F7 es exacta sin paso global. clean_chunk no lee ni escribe
archivos ni usa estado global: misma salida en secuencial y en paralelo (V-C14).

Este módulo solo depende de pandas y numpy. Dueño: B · Tareas: B4, B6
"""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from firewatch.ingest.dane import CheckLog
from firewatch.ingest.firms import (ALIAS_COLUMNAS, COLUMNAS_REQUERIDAS, KINDS_SP, ArchivoRaw,
                                    instrumentos_invalidos, listar_archivos_raw, normalizar_encabezados,
                                    rutas_raw)

log = logging.getLogger(__name__)


# =============================================================================
# 1. Constantes y contratos
# =============================================================================

RAW_FLOAT = ("latitude", "longitude", "bright_ti4", "bright_ti5", "scan", "track", "frp")
RAW_TEXT = ("acq_date", "acq_time", "satellite", "instrument", "confidence", "version", "daynight", "type")

SAT_MAP = {                     # F0: valores que usan los distintos productos de FIRMS
    "N": "N", "NPP": "N", "SNPP": "N", "SUOMI NPP": "N", "SUOMI-NPP": "N",
    "N20": "N20", "1": "N20", "J1": "N20", "NOAA-20": "N20", "NOAA20": "N20",
    "N21": "N21", "2": "N21", "J2": "N21", "NOAA-21": "N21", "NOAA21": "N21",   # se rechaza en V-C2
}
CONF_MAP = {"l": "l", "low": "l", "n": "n", "nominal": "n", "h": "h", "high": "h"}

RULES = ("F1_ventana", "F2_bbox", "F3_rango", "F4_corte_sp_nrt", "F5_tipo", "F6_confianza", "F7_duplicado")

# Contrato de salida de clean_chunk = silver.detecciones (diccionario de datos) sin cod_dpto/cod_mpio
LIMPIAS_COLUMNS: dict[str, str] = {
    "id_det": "string",
    "lat": "float64",
    "lon": "float64",
    "fecha": "datetime64[ns]",      # se escribe como date32 en Parquet (runner)
    "hora_utc": "int32",
    "anio": "int16",
    "mes": "int16",
    "daynight": "string",
    "confianza": "string",
    "frp": "float64",
    "bright_ti4": "float64",
    "satelite": "string",
    "proceso": "string",
    "fuente_estatica": "bool",
}
NULLABLES = {"bright_ti4"}      # diccionario: única columna de detecciones (sin códigos) que admite nulos
DOMINIOS = {"confianza": {"n", "h"}, "satelite": {"N", "N20"}, "proceso": {"SP", "NRT"}, "daynight": {"D", "N"}}

_CELL_J_OFFSET = 500_000        # celda_id = i·1e6 + (j + 500000); j ∈ [−16400, −13360] en Colombia


class FirmsError(Exception):
    """Error de configuración o de lectura (exit 2)."""


# =============================================================================
# 2. Configuración (todo sale de settings.yaml; sin valores por defecto en el código)
# =============================================================================

@dataclass(frozen=True)
class FirmsConfig:
    raw_firms_dir: Path                   # rutas.raw_firms (CSV anual SP)
    raw_firms_2026_dir: Path              # rutas.raw_firms_2026 (Archive Download: archive/ y nrt/)
    silver_dir: Path                      # rutas.silver
    window_start: date
    window_end: date
    anios_linea_base: tuple[int, ...]     # ventana.anios_linea_base (referencia de V-C12)
    bbox_lat: tuple[float, float]         # territorio.bbox_colombia
    bbox_lon: tuple[float, float]
    satelites: tuple[str, ...]
    type_permitido: int                   # firms.filtros
    confianza_excluida: tuple[str, ...]
    frp_minimo: float
    type_estatico: int                    # firms.mascara_estatica
    celda_deg: float
    mascara_anios: tuple[int, ...]
    bloque_filas: int                     # paralelo.tamano_bloque_filas
    sin_municipio_max: float              # calidad.max_pct_sin_municipio / 100 (fracción)
    umbral_descarte_rango: float          # firms.validacion
    vc11_ratio: tuple[float, float]
    vc12_tol: float
    vc13_sobre_max: float
    zona_control_lat: tuple[float, float]
    zona_control_lon: tuple[float, float]

    @property
    def geo_path(self) -> Path:
        return self.silver_dir / "geo_municipios"

    @property
    def out_detecciones(self) -> Path:
        return self.silver_dir / "detecciones"

    @property
    def out_reports(self) -> Path:
        return self.silver_dir / "_reports"

    @property
    def out_mascara(self) -> Path:
        """Artefacto de auditoría (no es tabla del contrato silver)."""
        return self.out_reports / "mascara_fuentes_estaticas.parquet"

    @property
    def out_report(self) -> Path:
        return self.out_reports / "cleaning_metrics.json"


def _to_date(v: Any, key: str) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v))
    except ValueError as exc:
        raise FirmsError(f"settings.yaml: '{key}' no es una fecha ISO: {v!r}") from exc


def _get(settings: dict[str, Any], dotted: str) -> Any:
    cur: Any = settings
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise FirmsError(f"settings.yaml: falta la clave '{dotted}'")
        cur = cur[part]
    return cur


def _pair(settings: dict[str, Any], dotted: str) -> tuple[float, float]:
    val = _get(settings, dotted)
    if not isinstance(val, (list, tuple)) or len(val) != 2:
        raise FirmsError(f"settings.yaml: '{dotted}' debe ser [min, max]")
    return float(val[0]), float(val[1])


def load_config(settings: dict[str, Any], base_dir: str | Path | None = None) -> FirmsConfig:
    """Construye FirmsConfig desde settings.yaml; las rutas relativas se resuelven contra base_dir."""
    base = Path(base_dir) if base_dir is not None else Path.cwd()
    try:
        dir_anual, dir_2026 = rutas_raw(settings, base)
    except Exception as exc:                                       # FirmsRawError → error de configuración
        raise FirmsError(str(exc)) from exc
    silver = Path(_get(settings, "rutas.silver"))
    bbox = _get(settings, "territorio.bbox_colombia")
    try:
        bbox_lat = (float(bbox["lat_min"]), float(bbox["lat_max"]))
        bbox_lon = (float(bbox["lon_min"]), float(bbox["lon_max"]))
    except (KeyError, TypeError) as exc:
        raise FirmsError("territorio.bbox_colombia debe tener lon_min, lat_min, lon_max, lat_max") from exc

    cfg = FirmsConfig(
        raw_firms_dir=dir_anual,
        raw_firms_2026_dir=dir_2026,
        silver_dir=silver if silver.is_absolute() else base / silver,
        window_start=_to_date(_get(settings, "ventana.fecha_inicio"), "ventana.fecha_inicio"),
        window_end=_to_date(_get(settings, "ventana.fecha_fin"), "ventana.fecha_fin"),
        anios_linea_base=tuple(int(y) for y in _get(settings, "ventana.anios_linea_base")),
        bbox_lat=bbox_lat,
        bbox_lon=bbox_lon,
        satelites=tuple(str(s) for s in _get(settings, "firms.satelites")),
        type_permitido=int(_get(settings, "firms.filtros.type_permitido")),
        confianza_excluida=tuple(str(c) for c in _get(settings, "firms.filtros.confianza_excluida")),
        frp_minimo=float(_get(settings, "firms.filtros.frp_minimo")),
        type_estatico=int(_get(settings, "firms.mascara_estatica.type_estatico")),
        celda_deg=float(_get(settings, "firms.mascara_estatica.tamano_celda_grados")),
        mascara_anios=tuple(int(y) for y in _get(settings, "firms.mascara_estatica.anios_fuente")),
        bloque_filas=int(_get(settings, "paralelo.tamano_bloque_filas")),
        sin_municipio_max=float(_get(settings, "calidad.max_pct_sin_municipio")) / 100.0,
        umbral_descarte_rango=float(_get(settings, "firms.validacion.umbral_descarte_rango")),
        vc11_ratio=_pair(settings, "firms.validacion.ratio_n20_n"),
        vc12_tol=float(_get(settings, "firms.validacion.tol_salto_sp_nrt")),
        vc13_sobre_max=float(_get(settings, "firms.validacion.sobre_enmascaramiento_max")),
        zona_control_lat=_pair(settings, "firms.validacion.zona_control.lat"),
        zona_control_lon=_pair(settings, "firms.validacion.zona_control.lon"),
    )
    if cfg.window_start >= cfg.window_end:
        raise FirmsError("ventana: fecha_inicio debe ser anterior a fecha_fin")
    if set(cfg.satelites) - {"N", "N20"}:
        raise FirmsError(f"firms.satelites solo admite N y N20; recibido {cfg.satelites}")
    if cfg.celda_deg <= 0 or cfg.bloque_filas < 1 or not cfg.mascara_anios:
        raise FirmsError("firms.mascara_estatica / paralelo.tamano_bloque_filas con valores inválidos")
    return cfg


# =============================================================================
# 3. Fase 0 — inventario, lectura y normalización
# =============================================================================

def inventory_raw(cfg: FirmsConfig) -> list[ArchivoRaw]:
    """Lista los CSV raw en orden determinista (delegado en firewatch.ingest.firms)."""
    try:
        return listar_archivos_raw(cfg.raw_firms_dir, cfg.raw_firms_2026_dir)
    except Exception as exc:
        raise FirmsError(str(exc)) from exc


def read_raw(f: ArchivoRaw) -> pd.DataFrame:
    """
    Lee un CSV con tipos fijos (nunca inferidos) y encabezados canónicos (brightness → bright_ti4,
    bright_t31 → bright_ti5). Agrega _archivo y _kind para la trazabilidad.
    """
    wanted = set(RAW_FLOAT) | set(RAW_TEXT)
    try:
        header = list(pd.read_csv(f.path, nrows=0).columns)
        cols = [c for c in header if ALIAS_COLUMNAS.get(c, c) in wanted]
        dtypes = {c: ("float64" if ALIAS_COLUMNAS.get(c, c) in RAW_FLOAT else "string") for c in cols}
        df = pd.read_csv(f.path, usecols=cols, dtype=dtypes)
        df = normalizar_encabezados(df)
    except Exception as exc:
        raise FirmsError(f"No se pudo leer {f.path}: {exc}") from exc
    df["_archivo"] = f.path.name
    df["_kind"] = f.kind
    return df


def validate_schema(f: ArchivoRaw, df: pd.DataFrame) -> list[str]:
    """V-C1 para un archivo: columnas canónicas faltantes (type es obligatoria en SP)."""
    missing = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if f.kind in KINDS_SP and "type" not in df.columns:
        missing.append("type")
    return missing


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    F0 — agrega columnas normalizadas sin tocar las raw:
      satelite (N/N20/N21/NA) · confianza (l/n/h/NA) · proceso (SP/NRT, desde version) ·
      fecha (datetime64, NaT si no parsea) · hora_utc (Int32, NA si no es entero) · tipo (Int8, NA en NRT)
    """
    out = df.copy()
    sat = out["satellite"].astype("string").str.strip().str.upper()
    out["satelite"] = sat.map(SAT_MAP).astype("string")
    out["confianza"] = out["confidence"].astype("string").str.strip().str.lower().map(CONF_MAP).astype("string")
    out["proceso"] = np.where(out["version"].astype("string").str.upper().str.contains("NRT", na=False), "NRT", "SP")
    out["proceso"] = out["proceso"].astype("string")
    out["fecha"] = pd.to_datetime(out["acq_date"], format="%Y-%m-%d", errors="coerce")
    out["hora_utc"] = pd.to_numeric(out["acq_time"].astype("string").str.strip(), errors="coerce").astype("Int32")
    if "type" in out.columns:
        out["tipo"] = pd.to_numeric(out["type"], errors="coerce").astype("Int8")
    else:
        out["tipo"] = pd.array([pd.NA] * len(out), dtype="Int8")
    return out


def _months(start: date, end: date) -> list[tuple[int, int]]:
    return [(p.year, p.month) for p in pd.period_range(start, end, freq="M")]


def validate_files(files: list[ArchivoRaw], frames: dict[str, pd.DataFrame], cfg: FirmsConfig,
                   lg: CheckLog) -> None:
    """
    V-C2…V-C6 sobre los frames YA normalizados (V-C1 se valida antes de normalizar).
      V-C2  producto VIIRS (firms.instrumentos_invalidos) · un solo satélite por archivo, permitido ·
            confianza en letras (numérica = MODIS) y mapeable
      V-C3  version coherente con el tipo de archivo
      V-C4  cobertura mensual por satélite en toda la ventana (conteo > 0)
      V-C6  0 errores de conversión en acq_date / acq_time
    """
    p2, p3, p6 = [], [], []
    for f in files:
        name = f.path.name
        df = frames[name]
        bad_inst = instrumentos_invalidos(df)
        if bad_inst:
            p2.append(f"{name}: (instrument, satellite) no VIIRS {bad_inst[:5]}")
        sats = set(df["satelite"].dropna())
        unmapped = sorted(set(df.loc[df["satelite"].isna(), "satellite"].dropna().astype(str)))
        if unmapped:
            p2.append(f"{name}: satellite sin mapeo {unmapped[:5]}")
        if len(sats) != 1:
            p2.append(f"{name}: {len(sats)} satélites distintos {sorted(sats)}")
        bad = sats - set(cfg.satelites)
        if bad:
            p2.append(f"{name}: satélite no permitido {sorted(bad)} (solo {list(cfg.satelites)})")
        conf_raw = df["confidence"].dropna().astype(str).str.strip()
        if conf_raw.str.fullmatch(r"\d+(\.\d+)?").any():
            p2.append(f"{name}: confianza numérica (producto MODIS, no VIIRS)")
        elif df["confianza"].isna().sum() > df["confidence"].isna().sum():
            p2.append(f"{name}: confianza sin mapeo {sorted(set(conf_raw[df['confianza'].isna()]))[:5]}")

        share_nrt = float((df["proceso"] == "NRT").mean()) if len(df) else 0.0
        expects_nrt = f.kind == "archive_nrt"
        if expects_nrt and share_nrt < 1.0:
            p3.append(f"{name}: {share_nrt:.1%} NRT (se esperaba 100 %)")
        if not expects_nrt and share_nrt > 0.0:
            p3.append(f"{name}: {share_nrt:.1%} NRT (se esperaba 0 %)")

        n_date = int(df["fecha"].isna().sum())          # incluye acq_date vacía
        n_time = int(df["hora_utc"].isna().sum())
        if n_date or n_time:
            p6.append(f"{name}: {n_date} fechas y {n_time} horas no convertibles")

    lg.add("V-C2", not p2, "; ".join(p2))
    lg.add("V-C3", not p3, "; ".join(p3))
    lg.add("V-C6", not p6, "; ".join(p6))

    allm = pd.concat([frames[f.path.name][["satelite", "fecha"]] for f in files], ignore_index=True)
    allm = allm[(allm["fecha"] >= pd.Timestamp(cfg.window_start)) & (allm["fecha"] <= pd.Timestamp(cfg.window_end))]
    expected = set(_months(cfg.window_start, cfg.window_end))
    p4, det = [], {}
    for sat in cfg.satelites:
        s = allm.loc[allm["satelite"] == sat, "fecha"]
        present = set(zip(s.dt.year, s.dt.month))
        missing = sorted(expected - present)
        det[sat] = {"meses_presentes": len(present & expected), "meses_esperados": len(expected)}
        if missing:
            p4.append(f"{sat}: faltan {len(missing)} meses {[f'{y}-{m:02d}' for y, m in missing[:6]]}")
    lg.add("V-C4", not p4, "; ".join(p4), details=det)


def compute_sp_cutoffs(norm: pd.DataFrame, satelites: Iterable[str]) -> dict[str, pd.Timestamp | None]:
    """V-C5 / F4: D_sat = última fecha con datos SP por satélite (None si no hay SP)."""
    out: dict[str, pd.Timestamp | None] = {}
    for sat in satelites:
        s = norm.loc[(norm["satelite"] == sat) & (norm["proceso"] == "SP"), "fecha"]
        out[sat] = None if s.empty else pd.Timestamp(s.max())
    return out


# =============================================================================
# 4. Fase 0 — máscara de fuentes estáticas (política congelada)
# =============================================================================

def cell_ids(lat: np.ndarray | pd.Series, lon: np.ndarray | pd.Series, celda: float) -> np.ndarray:
    """Celda de `celda` grados: id = floor(lat/c)·1e6 + floor(lon/c) + 500000."""
    i = np.floor(np.asarray(lat, dtype="float64") / celda).astype(np.int64)
    j = np.floor(np.asarray(lon, dtype="float64") / celda).astype(np.int64)
    return i * 1_000_000 + (j + _CELL_J_OFFSET)


def decode_cells(cells: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    cells = np.asarray(cells, dtype=np.int64)
    i = cells // 1_000_000
    j = cells - i * 1_000_000 - _CELL_J_OFFSET
    return i, j


def _sp_static(norm: pd.DataFrame, cfg: FirmsConfig) -> pd.DataFrame:
    """Detecciones SP de mascara_anios con type == type_estatico."""
    return norm[(norm["proceso"] == "SP") & (norm["tipo"] == cfg.type_estatico).fillna(False)
                & norm["fecha"].dt.year.isin(cfg.mascara_anios)]


def build_static_mask(norm: pd.DataFrame, cfg: FirmsConfig) -> tuple[np.ndarray, pd.DataFrame]:
    """
    Celdas de celda_deg con al menos una detección SP type == type_estatico en mascara_anios.
    Devuelve (celda_ids ordenados y únicos, tabla de auditoría con conteos por celda).
    """
    sp = _sp_static(norm, cfg)
    cols = ["celda_id", "i", "j", "lat_celda", "lon_celda", "n_det", "n_dias", "n_anios"]
    if sp.empty:
        return np.array([], dtype=np.int64), pd.DataFrame(columns=cols)
    tmp = pd.DataFrame({"celda_id": cell_ids(sp["latitude"], sp["longitude"], cfg.celda_deg),
                        "fecha": sp["fecha"].to_numpy()})
    tmp["anio"] = tmp["fecha"].dt.year
    table = tmp.groupby("celda_id").agg(n_det=("fecha", "size"), n_dias=("fecha", "nunique"),
                                        n_anios=("anio", "nunique")).reset_index()
    ti, tj = decode_cells(table["celda_id"].to_numpy())
    table["i"], table["j"] = ti, tj
    table["lat_celda"] = (ti + 0.5) * cfg.celda_deg
    table["lon_celda"] = (tj + 0.5) * cfg.celda_deg
    table = table[cols].sort_values("celda_id").reset_index(drop=True)
    return table["celda_id"].to_numpy(dtype=np.int64), table


def mask_quality(norm: pd.DataFrame, cells: np.ndarray, table: pd.DataFrame, cfg: FirmsConfig,
                 lg: CheckLog) -> None:
    """
    V-C13  (a) hay celdas en la zona de control (Nobsa–Sogamoso)             → bloqueante
           (b) cobertura = 100 % de las SP type_estatico (por construcción)   → bloqueante
    V-C13c sobre-enmascaramiento < vc13_sobre_max de las SP type_permitido   → advertencia
    """
    sp = norm[(norm["proceso"] == "SP") & norm["fecha"].dt.year.isin(cfg.mascara_anios)]
    st = sp[(sp["tipo"] == cfg.type_estatico).fillna(False)]
    veg = sp[(sp["tipo"] == cfg.type_permitido).fillna(False)]
    cov = float(np.isin(cell_ids(st["latitude"], st["longitude"], cfg.celda_deg), cells).mean()) if len(st) else 0.0
    over = float(np.isin(cell_ids(veg["latitude"], veg["longitude"], cfg.celda_deg), cells).mean()) if len(veg) else 0.0
    (la0, la1), (lo0, lo1) = cfg.zona_control_lat, cfg.zona_control_lon
    n_zona = int((table["lat_celda"].between(la0, la1) & table["lon_celda"].between(lo0, lo1)).sum())
    det = {"celdas": int(len(cells)), "celdas_zona_control": n_zona, "cobertura": cov,
           "sobre_enmascaramiento": over, "n_sp_estaticas": int(len(st)), "n_sp_veg": int(len(veg))}
    lg.add("V-C13", n_zona > 0 and cov == 1.0,
           f"celdas en zona de control={n_zona}; cobertura={cov:.2%} (se esperaba 100 %)", details=det)
    lg.add("V-C13c", over < cfg.vc13_sobre_max,
           f"sobre-enmascaramiento={over:.2%} (máx {cfg.vc13_sobre_max:.0%})",
           blocking=False, details={"sobre_enmascaramiento": over})


# =============================================================================
# 5. Contexto inmutable (se pasa a cada bloque / worker)
# =============================================================================

@dataclass(frozen=True)
class CleaningContext:
    window_start: pd.Timestamp
    window_end: pd.Timestamp
    bbox_lat: tuple[float, float]
    bbox_lon: tuple[float, float]
    frp_minimo: float
    type_permitido: int
    confianza_excluida: tuple[str, ...]
    cutoffs: dict[str, pd.Timestamp | None]
    mask_cells: np.ndarray
    celda_deg: float


def build_context(cfg: FirmsConfig, cutoffs: dict[str, pd.Timestamp | None],
                  mask_cells: np.ndarray) -> CleaningContext:
    return CleaningContext(
        window_start=pd.Timestamp(cfg.window_start), window_end=pd.Timestamp(cfg.window_end),
        bbox_lat=cfg.bbox_lat, bbox_lon=cfg.bbox_lon, frp_minimo=cfg.frp_minimo,
        type_permitido=cfg.type_permitido, confianza_excluida=cfg.confianza_excluida,
        cutoffs=dict(cutoffs), mask_cells=np.sort(np.asarray(mask_cells, dtype=np.int64)), celda_deg=cfg.celda_deg,
    )


# =============================================================================
# 6. Fase 1 — limpieza pura por bloque
# =============================================================================

@dataclass
class CleaningMetrics:
    """Contadores sumables entre bloques (reducción asociativa)."""
    raw: Counter = field(default_factory=Counter)             # archivo → n
    descartes: Counter = field(default_factory=Counter)       # (archivo, regla) → n
    salida: Counter = field(default_factory=Counter)          # archivo → n
    detalle: Counter = field(default_factory=Counter)         # (satelite, proceso, anio, mes, estatica) → n

    def __add__(self, other: "CleaningMetrics") -> "CleaningMetrics":
        return CleaningMetrics(self.raw + other.raw, self.descartes + other.descartes,
                               self.salida + other.salida, self.detalle + other.detalle)

    def __radd__(self, other: Any) -> "CleaningMetrics":
        return self if other == 0 else self.__add__(other)

    def descartes_por_regla(self) -> dict[str, int]:
        out = {r: 0 for r in RULES}
        for (_, regla), n in self.descartes.items():
            out[regla] += n
        return out

    def to_dict(self) -> dict[str, Any]:
        archivos = sorted(self.raw)
        return {
            "n_raw": sum(self.raw.values()),
            "n_salida": sum(self.salida.values()),
            "descartes": self.descartes_por_regla(),
            "por_archivo": {a: {"raw": self.raw[a], "salida": self.salida[a],
                                **{r: self.descartes[(a, r)] for r in RULES}} for a in archivos},
            "salida_detalle": [{"satelite": s, "proceso": p, "anio": int(y), "mes": int(m),
                                "fuente_estatica": bool(e), "n": n}
                               for (s, p, y, m, e), n in sorted(self.detalle.items())],
        }


def make_id_det(lat: pd.Series, lon: pd.Series, fecha: pd.Series, hora: pd.Series, sat: pd.Series) -> pd.Series:
    """SHA-1 (16 hex) de f"{lat:.5f}|{lon:.5f}|{fecha}|{hhmm:04d}|{satelite}"."""
    keys = [f"{a:.5f}|{b:.5f}|{c}|{int(d):04d}|{e}" for a, b, c, d, e in
            zip(lat.to_numpy(), lon.to_numpy(), fecha.dt.strftime("%Y-%m-%d").to_numpy(),
                hora.to_numpy(), sat.to_numpy())]
    return pd.Series([hashlib.sha1(k.encode("ascii")).hexdigest()[:16] for k in keys],
                     index=lat.index, dtype="string")


def clean_chunk(df: pd.DataFrame, ctx: CleaningContext) -> tuple[pd.DataFrame, CleaningMetrics]:
    """
    Aplica F1…F7 en orden fijo a un bloque YA normalizado (F0) y devuelve
    (detecciones_limpias con LIMPIAS_COLUMNS + _archivo, métricas del bloque).
      F1 ventana · F2 caja de Colombia · F3 rangos (frp > frp_minimo, hora HHMM válida) ·
      F4 corte SP/NRT · F5 type == type_permitido (solo SP) · F6 confianza ·
      F7 duplicados (SP > NRT, h > n, mayor frp) · F8 marca fuente_estatica por la máscara (no descarta)
    """
    m = CleaningMetrics()
    m.raw.update(df["_archivo"].value_counts().to_dict())
    cur = df

    def drop(regla: str, mask: pd.Series) -> None:
        nonlocal cur
        mask = mask.fillna(True).astype(bool)
        if mask.any():
            m.descartes.update({(a, regla): n for a, n in cur.loc[mask, "_archivo"].value_counts().items()})
            cur = cur.loc[~mask]

    drop("F1_ventana", (cur["fecha"] < ctx.window_start) | (cur["fecha"] > ctx.window_end))
    drop("F2_bbox", ~cur["latitude"].between(*ctx.bbox_lat) | ~cur["longitude"].between(*ctx.bbox_lon))
    hh, mm = cur["hora_utc"] // 100, cur["hora_utc"] % 100
    drop("F3_rango", (cur["frp"] <= ctx.frp_minimo) | cur["frp"].isna()
         | (cur["hora_utc"] < 0) | (hh > 23) | (mm > 59))
    cutoff = cur["satelite"].map(lambda s: ctx.cutoffs.get(s)).astype("datetime64[ns]")
    drop("F4_corte_sp_nrt", (cur["proceso"] == "NRT") & cutoff.notna() & (cur["fecha"] <= cutoff))
    drop("F5_tipo", ((cur["proceso"] == "SP") & (cur["tipo"] != ctx.type_permitido)).fillna(True))
    drop("F6_confianza", cur["confianza"].isin(ctx.confianza_excluida))

    # F7: clave redondeada a 5 decimales (la misma que id_det) y desempate determinista
    if len(cur):
        cur = cur.assign(_lat5=cur["latitude"].round(5), _lon5=cur["longitude"].round(5),
                         _p_proc=(cur["proceso"] != "SP").astype("int8"),
                         _p_conf=(cur["confianza"] != "h").astype("int8"))
        cur = cur.sort_values(["_lat5", "_lon5", "fecha", "hora_utc", "satelite", "_p_proc", "_p_conf", "frp"],
                              ascending=[True, True, True, True, True, True, True, False], kind="mergesort")
        drop("F7_duplicado", cur.duplicated(["_lat5", "_lon5", "fecha", "hora_utc", "satelite"], keep="first"))

    # F8: marca por la máscara (SP y NRT por igual)
    estatica = np.isin(cell_ids(cur["latitude"], cur["longitude"], ctx.celda_deg), ctx.mask_cells)

    out = pd.DataFrame({
        "id_det": make_id_det(cur["latitude"], cur["longitude"], cur["fecha"], cur["hora_utc"], cur["satelite"]),
        "lat": cur["latitude"].to_numpy(),
        "lon": cur["longitude"].to_numpy(),
        "fecha": cur["fecha"].to_numpy(),
        "hora_utc": cur["hora_utc"].to_numpy(dtype="int32", na_value=0) if len(cur) else np.array([], dtype="int32"),
        "anio": cur["fecha"].dt.year.to_numpy(),
        "mes": cur["fecha"].dt.month.to_numpy(),
        "daynight": cur["daynight"].astype("string").str.strip().str.upper().to_numpy(),
        "confianza": cur["confianza"].to_numpy(),
        "frp": cur["frp"].to_numpy(),
        "bright_ti4": cur["bright_ti4"].to_numpy(),
        "satelite": cur["satelite"].to_numpy(),
        "proceso": cur["proceso"].to_numpy(),
        "fuente_estatica": estatica,
    }, index=cur.index).astype(LIMPIAS_COLUMNS)
    out["_archivo"] = cur["_archivo"].to_numpy()

    m.salida.update(out["_archivo"].value_counts().to_dict())
    m.detalle.update(Counter(zip(out["satelite"], out["proceso"], out["anio"].astype(int), out["mes"].astype(int),
                                 out["fuente_estatica"])))
    return out.reset_index(drop=True), m


# =============================================================================
# 7. Validación de la salida completa
# =============================================================================

def content_hash(df: pd.DataFrame, columns: Iterable[str]) -> str:
    """Hash de contenido de la salida ordenada por id_det (V-C14: secuencial = paralelo)."""
    cols = list(columns)
    d = df[cols].sort_values("id_det").reset_index(drop=True)
    return hashlib.sha256(pd.util.hash_pandas_object(d, index=False).to_numpy().tobytes()).hexdigest()


def validate_output(df: pd.DataFrame, metrics: CleaningMetrics, cfg: FirmsConfig, lg: CheckLog) -> str:
    """V-C7…V-C12 y V-C14 (hash) sobre la salida concatenada. Devuelve el hash de contenido."""
    # V-C7 conservación exacta por archivo
    bad = []
    for a, n in metrics.raw.items():
        disc = sum(metrics.descartes[(a, r)] for r in RULES)
        if n != disc + metrics.salida[a]:
            bad.append(f"{a}: raw={n} ≠ descartes={disc} + salida={metrics.salida[a]}")
    if sum(metrics.salida.values()) != len(df):
        bad.append(f"Σ salida={sum(metrics.salida.values())} ≠ filas={len(df)}")
    lg.add("V-C7", not bad, "; ".join(bad), details={"n_raw": sum(metrics.raw.values()), "n_salida": len(df),
                                                     "descartes": metrics.descartes_por_regla()})
    # V-C8
    dups = int(df["id_det"].duplicated().sum())
    lg.add("V-C8", dups == 0, f"{dups} id_det duplicados")
    # V-C9 nulos y dominios según el diccionario
    probs = []
    for c in LIMPIAS_COLUMNS:
        n = int(df[c].isna().sum())
        if n and c not in NULLABLES:
            probs.append(f"{c}: {n} nulos")
    for c, dom in DOMINIOS.items():
        extra = set(df[c].dropna().unique()) - dom
        if extra:
            probs.append(f"{c}: valores fuera de dominio {sorted(extra)}")
    lg.add("V-C9", not probs, "; ".join(probs))
    # V-C10 descartes por rango (F2 + F3) por archivo
    over = []
    for a, n in metrics.raw.items():
        r = (metrics.descartes[(a, "F2_bbox")] + metrics.descartes[(a, "F3_rango")]) / n if n else 0.0
        if r >= cfg.umbral_descarte_rango:
            over.append(f"{a}: {r:.2%}")
    lg.add("V-C10", not over, f"descartes por rango ≥ {cfg.umbral_descarte_rango:.0%}: {over}")
    # V-C11 razón N20/N por año
    cnt = df.groupby(["anio", "satelite"]).size().unstack(fill_value=0)
    ratios, out_r = {}, []
    if {"N", "N20"} <= set(cnt.columns):
        for y, row in cnt.iterrows():
            r = row["N20"] / row["N"] if row["N"] else float("inf")
            ratios[int(y)] = round(float(r), 3)
            if not (cfg.vc11_ratio[0] <= r <= cfg.vc11_ratio[1]):
                out_r.append(f"{int(y)}: {r:.2f}")
    lg.add("V-C11", not out_r, f"razón N20/N fuera de {cfg.vc11_ratio}: {out_r}", blocking=False,
           details={"ratios": ratios})
    # V-C12 salto SP→NRT frente a la línea base
    veg = df[~df["fuente_estatica"]]
    monthly = veg.groupby(["anio", "mes"]).size()
    nrt = df["proceso"] == "NRT"
    nrt_months = sorted(set(zip(df.loc[nrt, "anio"].astype(int), df.loc[nrt, "mes"].astype(int))))
    jumps, det12 = [], {}
    for (y, mth) in nrt_months:
        ref = [monthly.get((yy, mth), 0) for yy in cfg.anios_linea_base]
        mean = float(np.mean(ref)) if ref else 0.0
        val = int(monthly.get((y, mth), 0))
        rel = (val - mean) / mean if mean else float("inf")
        det12[f"{y}-{mth:02d}"] = {"n": val, "media_ref": round(mean, 1), "rel": round(rel, 3)}
        if abs(rel) > cfg.vc12_tol:
            jumps.append(f"{y}-{mth:02d}: {rel:+.0%}")
    if nrt_months:
        lg.add("V-C12", not jumps, f"meses NRT fuera de ±{cfg.vc12_tol:.0%} frente a la línea base: {jumps} "
               "(verificar explicación climática antes de declararlo limitación)", blocking=False, details=det12)
    else:
        lg.info("V-C12", "no hay filas NRT en la salida")
    # V-C14
    h = content_hash(df, LIMPIAS_COLUMNS)
    lg.info("V-C14", "hash de contenido (comparar secuencial vs paralelo)", {"content_sha256": h})
    return h


def fase0(cfg: FirmsConfig, lg: CheckLog) -> tuple[pd.DataFrame, CleaningContext | None, pd.DataFrame,
                                                   dict[str, Any]]:
    """
    Ejecuta la Fase 0 completa. Devuelve (tabla normalizada completa en orden determinista,
    contexto o None si hubo fallos bloqueantes, tabla de la máscara, info de archivos).
    """
    files = inventory_raw(cfg)
    raw = {f.path.name: read_raw(f) for f in files}
    schema_bad = {f.path.name: validate_schema(f, raw[f.path.name]) for f in files}
    schema_bad = {k: v for k, v in schema_bad.items() if v}
    lg.add("V-C1", not schema_bad, f"columnas faltantes: {schema_bad}")
    info = {"archivos": [{"archivo": f.path.name, "tipo": f.kind, "filas": int(len(raw[f.path.name]))}
                         for f in files]}
    empty = pd.DataFrame()
    if schema_bad:
        return empty, None, empty, info

    frames = {name: normalize(df) for name, df in raw.items()}
    validate_files(files, frames, cfg, lg)
    norm = pd.concat([frames[f.path.name] for f in files], ignore_index=True)
    cutoffs = compute_sp_cutoffs(norm, cfg.satelites)
    lg.info("V-C5", "fecha de corte SP→NRT por satélite (D_sat)",
            {s: (str(d.date()) if d is not None else None) for s, d in cutoffs.items()})
    cells, table = build_static_mask(norm, cfg)
    mask_quality(norm, cells, table, cfg, lg)
    if lg.failures:
        return norm, None, table, info
    return norm, build_context(cfg, cutoffs, cells), table, info
