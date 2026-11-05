"""
firewatch.ingest.meteo — Ingesta L1 de clima horario (Open-Meteo, ERA5-Seamless).

Responsabilidad única
    Descargar, de forma reproducible, reanudable y respetando la cuota gratuita de Open-Meteo,
    la serie HORARIA 2022-01-01 → 2026-10-31 de 5 variables para los 123 centroides municipales
    de Boyacá, guardar cada respuesta SIN modificar en data/raw/meteo/ y validar su calidad.

Fuera de alcance (pertenece a otras capas)
    - Calcular centroides            → B3 (silver.dim_municipio). Puente temporal: config/centroides_boyaca.csv
    - Agregar horario → diario       → Spark L3 (build_daily.py).

Pendiente en este módulo (B3)
    - a_silver_horario: JSON → silver/clima_horario (Parquet por anio), con el viento convertido
      de m/s a km/h (×3,6) según docs/diccionario_datos.md, y la `elevation` devuelta por la API
      para silver/dim_municipio.

Decisiones de diseño (ver Guía B1 §5 y ADR-0003/0004)
    D-M1  Una ubicación por petición: el peso de la cuota escala con las ubicaciones; así la unidad
          de caché = unidad de reintento = un archivo.
    D-M2  Ejecución SECUENCIAL con ritmo fijo (pace). El cuello de botella es la cuota de la API,
          no la CPU: paralelizar no acelera nada y dispararía el 429. (No confundir con R3: el
          paralelismo del proyecto vive en src/firewatch/parallel/.)
    D-M3  Raw inmutable: se guardan los bytes EXACTOS de la respuesta; la validación parsea una copia.
          Las unidades son las que devuelve Open-Meteo (viento en m/s vía wind_speed_unit=ms).
    D-M4  Caché por huella (fingerprint) de parámetros: si cambia el modelo, las variables o las
          fechas, la caché antigua deja de ser válida automáticamente (lección era5_land → seamless).
    D-M5  Estado derivado del sistema de archivos + manifiesto JSONL de solo-anexado; no hay
          base de datos de estado ni archivos de "lock".
    D-M6  Descarga en dos tramos por el rezago de ~5 días de ERA5: 'base' (→ 2026-10-25) y
          'cola' (2026-10-26 → 2026-10-31, ejecutar desde el 6 nov). Silver concatena.
    D-M7  Reproducibilidad a nivel de CONTENIDO: el sha256 de los bytes cambia entre corridas
          (la respuesta incluye generationtime_ms), así que se compara `content_sha256`, el hash
          canónico del bloque `hourly` + `hourly_units` + celda devuelta.

Estado de implementación
    v0.1 (3 nov): load_config, load_locations, make_session, fetch, validate_payload, process_task
                  (+ helpers puros).
    v0.2 (4 nov): RateLimiter, classify_response, fetch_with_retry, manifiesto (append/read/latest),
                  is_cached, run_download, main → descarga reanudable de los 123 municipios.
    Pendiente:    QuotaBudget (la salvaguarda es el ritmo fijo + el 429 del servidor), validate_dataset.

Uso (CLI, v0.2)
    python -m firewatch.ingest.meteo --tramo base --dry-run          # plan + presupuesto, sin red
    python -m firewatch.ingest.meteo --tramo base --only 15001       # un municipio (prueba)
    python -m firewatch.ingest.meteo --tramo base --limit 5          # primeros 5 pendientes
    python -m firewatch.ingest.meteo --tramo base                    # corrida completa (reanudable)
    python -m firewatch.ingest.meteo --validate-only                 # solo validación de dataset
Prueba de humo de Tunja:
    python scripts/smoke_meteo_tunja.py
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Sequence

import requests

log = logging.getLogger(__name__)

# Constantes congeladas (cambiarlas exige ADR)
APPROVED_MODEL = "era5_seamless"
REQUIRED_HOURLY_VARS: tuple[str, ...] = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "et0_fao_evapotranspiration",
)
APPROVED_TIMEZONE = "America/Bogota"
APPROVED_WIND_UNIT = "ms"
BOGOTA_UTC_OFFSET_S = -18_000
COD_MPIO_BOYACA = re.compile(r"^15\d{3}$")
BOYACA_BBOX = {"lat": (4.5, 7.2), "lon": (-74.7, -71.9)}   # con margen; detecta lat/lon invertidas


# =============================================================================
# 1. Modelos de datos
# =============================================================================

@dataclass(frozen=True)
class MeteoConfig:
    """Parámetros congelados de la ingesta. Única fuente: config/settings.yaml (sección `meteo`)."""
    base_url: str                       # https://archive-api.open-meteo.com/v1/archive
    model: str                          # "era5_seamless"
    hourly_vars: tuple[str, ...]        # 5 variables de ADR-0003, en orden fijo
    timezone: str                       # "America/Bogota"
    wind_speed_unit: str                # "ms"
    window_start: date                  # 2022-01-01  (ADR-0004)
    window_end: date                    # 2026-10-31  (ADR-0004)
    raw_dir: Path                       # data/raw/meteo
    manifest_path: Path                 # data/raw/meteo/_manifest.jsonl
    dim_municipio_path: Path            # data/silver/dim_municipio (Parquet) → centroides (definitivo)
    pace_seconds: float = 60.0          # 1 petición por minuto (H1)
    max_attempts: int = 5               # por tarea y corrida
    backoff_base_s: float = 30.0        # 30, 60, 120, 240 s…
    backoff_cap_s: float = 600.0
    timeout_connect_s: float = 10.0
    timeout_read_s: float = 180.0
    daily_budget: float = 9_000.0       # margen bajo el límite de 10.000/día
    hourly_budget: float = 4_500.0      # margen bajo 5.000/h
    minute_budget: float = 500.0        # margen bajo 600/min
    coord_decimals: int = 4
    user_agent: str = "firewatch-boyaca/1.0 (academic project)"
    expected_n_locations: int = 123
    centroides_path: Path | None = None  # puente temporal: config/centroides_boyaca.csv (prioridad sobre Parquet)
    cola_start: date | None = None      # inicio del tramo 'cola' (D-M6); obligatorio en settings.yaml


@dataclass(frozen=True)
class Location:
    """Un centroide municipal. cod_mpio SIEMPRE texto de 5 dígitos."""
    cod_mpio: str
    nombre: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Segment:
    """Tramo temporal de descarga (D-M6)."""
    name: str           # "base" | "cola"
    start: date
    end: date
    subdir: str         # "" para base, "_cola" para cola

    @property
    def n_days(self) -> int:
        return (self.end - self.start).days + 1

    @property
    def n_hours(self) -> int:
        return self.n_days * 24     # America/Bogota: sin horario de verano


@dataclass(frozen=True)
class DownloadTask:
    location: Location
    segment: Segment


class TaskStatus(str, Enum):
    CACHED = "cached"           # ya existía y es válido con la misma huella
    OK = "ok"                   # descargado y validado
    INVALID = "invalid"         # descargado pero falla validación (no cuenta como caché)
    FAILED = "failed"           # agotó reintentos (red / 5xx / 429 minutal-horario)
    SKIPPED_QUOTA = "skipped_quota"   # no se intentó: presupuesto diario agotado
    PLANNED = "planned"         # dry-run


@dataclass
class ValidationReport:
    ok: bool
    checks: dict[str, bool] = field(default_factory=dict)   # id → resultado (V-M2, V-M3…)
    details: dict[str, Any] = field(default_factory=dict)   # conteos, rangos, celda devuelta
    errors: list[str] = field(default_factory=list)


@dataclass
class DownloadResult:
    task: DownloadTask
    status: TaskStatus
    attempts: int = 0
    weight: float = 0.0
    http_status: int | None = None
    bytes: int | None = None
    sha256: str | None = None
    validation: ValidationReport | None = None
    error: str | None = None
    path: Path | None = None            # archivo final (OK) o archivo en _rejected/ (INVALID)
    content_sha256: str | None = None   # D-M7: hash de contenido, estable entre corridas
    fingerprint: str | None = None      # huella de parámetros (D-M4)
    elapsed_s: float | None = None


@dataclass
class RunSummary:
    segment: str
    total: int
    by_status: dict[str, int]
    weight_spent: float
    elapsed_s: float
    pending: list[str]          # cod_mpio que siguen pendientes → próxima corrida
    stop_reason: str | None = None   # None | "fatal" | "quota" | "interrupted" | "consecutive_failures"
    planned_weight: float = 0.0      # peso estimado de lo pendiente (dry-run y log)


# =============================================================================
# 2. Excepciones (determinan la política de reintento)
# =============================================================================

class MeteoError(Exception):
    """Base."""

class FatalRequestError(MeteoError):
    """HTTP 400 / {"error": true}. Error de configuración → ABORTA la corrida (afecta a todas las tareas)."""

class TransientRequestError(MeteoError):
    """Timeout, conexión, 5xx, 429 minutal/horario → se reintenta con backoff."""

    def __init__(self, message: str, attempts: int = 1, http_status: int | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.http_status = http_status

class DailyQuotaExceeded(MeteoError):
    """429 por límite diario (o presupuesto local agotado) → detener con gracia y reanudar mañana."""


# =============================================================================
# 3. Configuración y entradas
# =============================================================================

def _to_date(value: Any, key: str) -> date:
    """YAML puede entregar date (sin comillas) o str (con comillas)."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise MeteoError(f"settings.yaml: '{key}' no es una fecha ISO válida: {value!r}") from exc


def load_config(settings: dict[str, Any], base_dir: str | Path | None = None) -> MeteoConfig:
    """
    Construye MeteoConfig desde settings.yaml (sección `meteo` + `ventana` + `rutas`).

    Estructura esperada:
        ventana: {fecha_inicio: 2022-01-01, fecha_fin: 2026-10-31}
        rutas:   {raw_meteo: data/raw/meteo, dim_municipio: data/silver/dim_municipio,
                  centroides: config/centroides_boyaca.csv}
        meteo:   {base_url, model, hourly_vars, timezone, wind_speed_unit, cola_start,
                  pace_seconds, max_attempts, budget: {daily, hourly, minute}, ...}

    Las rutas relativas se resuelven contra `base_dir` (por defecto, el directorio actual = raíz del repo).

    Valida (falla rápido con MeteoError):
      - model == "era5_seamless" (decisión aprobada; cualquier otro valor exige ADR).
      - exactamente las 5 variables de ADR-0003, sin duplicados (se guardan en orden canónico
        para que la huella no dependa del orden escrito en el YAML).
      - timezone == America/Bogota y wind_speed_unit == ms.
      - window_start < cola_start ≤ window_end.
      - pace_seconds × peso de una tarea base coherente con los presupuestos por minuto y hora.
    """
    for section in ("meteo", "ventana", "rutas"):
        if not isinstance(settings.get(section), dict):
            raise MeteoError(f"settings.yaml: falta la sección '{section}'")
    m, v, r = settings["meteo"], settings["ventana"], settings["rutas"]
    base = Path(base_dir) if base_dir is not None else Path.cwd()

    def path_of(key: str, default: str | None) -> Path | None:
        raw = r.get(key, default)
        if raw is None:
            return None
        p = Path(raw)
        return p if p.is_absolute() else base / p

    model = str(m.get("model", ""))
    if model != APPROVED_MODEL:
        raise MeteoError(f"meteo.model = {model!r}; el modelo congelado es {APPROVED_MODEL!r} (cambiarlo exige ADR)")

    vars_cfg = list(m.get("hourly_vars", []))
    if len(vars_cfg) != len(set(vars_cfg)) or set(vars_cfg) != set(REQUIRED_HOURLY_VARS):
        raise MeteoError(f"meteo.hourly_vars debe ser exactamente {list(REQUIRED_HOURLY_VARS)}; recibido {vars_cfg}")

    tz = str(m.get("timezone", APPROVED_TIMEZONE))
    if tz != APPROVED_TIMEZONE:
        raise MeteoError(f"meteo.timezone = {tz!r}; debe ser {APPROVED_TIMEZONE!r} (contrato ts_local)")
    wind = str(m.get("wind_speed_unit", APPROVED_WIND_UNIT))
    if wind != APPROVED_WIND_UNIT:
        raise MeteoError(f"meteo.wind_speed_unit = {wind!r}; debe ser {APPROVED_WIND_UNIT!r} (m/s)")

    w_start = _to_date(v.get("fecha_inicio"), "ventana.fecha_inicio")
    w_end = _to_date(v.get("fecha_fin"), "ventana.fecha_fin")
    if m.get("cola_start") is None:
        raise MeteoError("settings.yaml: falta 'meteo.cola_start' (inicio del tramo cola, D-M6)")
    cola_start = _to_date(m["cola_start"], "meteo.cola_start")
    if not (w_start < cola_start <= w_end):
        raise MeteoError(f"Se requiere fecha_inicio < cola_start ≤ fecha_fin; recibido {w_start}, {cola_start}, {w_end}")

    raw_dir = path_of("raw_meteo", "data/raw/meteo")
    budget = m.get("budget", {}) or {}
    cfg = MeteoConfig(
        base_url=str(m.get("base_url", "https://archive-api.open-meteo.com/v1/archive")),
        model=model,
        hourly_vars=REQUIRED_HOURLY_VARS,
        timezone=tz,
        wind_speed_unit=wind,
        window_start=w_start,
        window_end=w_end,
        raw_dir=raw_dir,
        manifest_path=raw_dir / "_manifest.jsonl",
        dim_municipio_path=path_of("dim_municipio", "data/silver/dim_municipio"),
        pace_seconds=float(m.get("pace_seconds", 60)),
        max_attempts=int(m.get("max_attempts", 5)),
        backoff_base_s=float(m.get("backoff_base_s", 30)),
        backoff_cap_s=float(m.get("backoff_cap_s", 600)),
        timeout_connect_s=float(m.get("timeout_connect_s", 10)),
        timeout_read_s=float(m.get("timeout_read_s", 180)),
        daily_budget=float(budget.get("daily", 9_000)),
        hourly_budget=float(budget.get("hourly", 4_500)),
        minute_budget=float(budget.get("minute", 500)),
        coord_decimals=int(m.get("coord_decimals", 4)),
        user_agent=str(m.get("user_agent", "firewatch-boyaca/1.0 (academic project)")),
        expected_n_locations=int(m.get("expected_n_locations", 123)),
        centroides_path=path_of("centroides", None),
        cola_start=cola_start,
    )

    # Coherencia ritmo ↔ cuota (con el tramo base, que es el más pesado)
    base_seg = build_segments(cfg, cfg.cola_start)["base"]
    w_base = max(1.0, base_seg.n_days / 14 * len(cfg.hourly_vars) / 10)
    per_hour = 3600.0 / cfg.pace_seconds * w_base
    if w_base > cfg.minute_budget or per_hour > cfg.hourly_budget:
        raise MeteoError(f"pace_seconds={cfg.pace_seconds} con peso {w_base:.1f}/petición consume "
                         f"{per_hour:.0f}/h (> presupuesto {cfg.hourly_budget:.0f}/h). Aumentar pace_seconds.")
    return cfg


def load_locations(cfg: MeteoConfig, require_complete: bool = True) -> list[Location]:
    """
    Devuelve los centroides municipales como Location, ordenados por cod_mpio.

    Fuente (v0.1): cfg.centroides_path (CSV puente, columnas cod_mpio,nombre,lat_c,lon_c, UTF-8).
    Fuente definitiva (B3): silver.dim_municipio en Parquet (mismas columnas), cuando exista.

    Reglas:
      - cod_mpio leído como str; regex ^15\\d{3}$; únicos.
      - Exactamente cfg.expected_n_locations si require_complete=True (la prueba de Tunja puede
        usar require_complete=False con un CSV parcial).
      - lat ∈ [4.5, 7.2], lon ∈ [−74.7, −71.9] (caja de Boyacá con margen) → detecta lat/lon invertidas.
      - Coordenadas redondeadas a cfg.coord_decimals (forman parte de la huella de caché).
      - Orden determinista por cod_mpio (reproducibilidad del log y de la reanudación).
    """
    rows: list[dict[str, str]]
    if cfg.centroides_path is not None and cfg.centroides_path.exists():
        source = cfg.centroides_path
        with open(source, newline="", encoding="utf-8-sig") as fh:   # -sig tolera BOM de Excel
            reader = csv.DictReader(fh)
            missing = {"cod_mpio", "nombre", "lat_c", "lon_c"} - set(reader.fieldnames or [])
            if missing:
                raise MeteoError(f"{source}: faltan columnas {sorted(missing)}")
            rows = [dict(r) for r in reader]
    elif cfg.dim_municipio_path.exists():
        import pandas as pd   # solo para la fuente definitiva
        source = cfg.dim_municipio_path
        df = pd.read_parquet(source, columns=["cod_mpio", "nombre", "lat_c", "lon_c"])
        rows = df.astype({"cod_mpio": "string"}).astype(str).to_dict("records")
    else:
        raise MeteoError(f"No hay fuente de centroides: ni {cfg.centroides_path} ni {cfg.dim_municipio_path}")

    errors: list[str] = []
    seen: set[str] = set()
    locations: list[Location] = []
    for i, r in enumerate(rows, start=2):           # línea 1 = encabezado
        cod = str(r["cod_mpio"]).strip()
        if not COD_MPIO_BOYACA.match(cod):
            errors.append(f"línea {i}: cod_mpio {cod!r} no cumple ^15\\d{{3}}$ (¿se perdió el formato texto?)")
            continue
        if cod in seen:
            errors.append(f"línea {i}: cod_mpio duplicado {cod}")
            continue
        try:
            lat, lon = float(r["lat_c"]), float(r["lon_c"])
        except ValueError:
            errors.append(f"línea {i}: lat/lon no numéricos ({r['lat_c']!r}, {r['lon_c']!r})")
            continue
        (la0, la1), (lo0, lo1) = BOYACA_BBOX["lat"], BOYACA_BBOX["lon"]
        if not (la0 <= lat <= la1 and lo0 <= lon <= lo1):
            errors.append(f"línea {i}: {cod} ({lat}, {lon}) fuera de la caja de Boyacá (¿lat/lon invertidas?)")
            continue
        seen.add(cod)
        locations.append(Location(cod, str(r["nombre"]).strip(),
                                  round(lat, cfg.coord_decimals), round(lon, cfg.coord_decimals)))

    if require_complete and len(locations) != cfg.expected_n_locations:
        errors.append(f"se esperaban {cfg.expected_n_locations} municipios válidos y hay {len(locations)}")
    if errors:
        raise MeteoError(f"{source}: centroides inválidos:\n  - " + "\n  - ".join(errors))

    locations.sort(key=lambda loc: loc.cod_mpio)
    log.info("Centroides cargados: %d desde %s", len(locations), source)
    return locations


def build_segments(cfg: MeteoConfig, cola_start: date | None = None) -> dict[str, Segment]:
    """
    Tramos D-M6. 'base' = window_start → cola_start − 1 día; 'cola' = cola_start → window_end.
    Si no se pasa cola_start se usa cfg.cola_start (settings.yaml).
    Invariante: base.n_hours + cola.n_hours == horas de la ventana completa (42.360).
    """
    cola_start = cola_start or cfg.cola_start
    if cola_start is None:
        raise MeteoError("No hay cola_start: definir meteo.cola_start en settings.yaml")
    base = Segment("base", cfg.window_start, cola_start - timedelta(days=1), "")
    cola = Segment("cola", cola_start, cfg.window_end, "_cola")
    total_days = (cfg.window_end - cfg.window_start).days + 1
    assert base.n_days + cola.n_days == total_days, "Los tramos no cubren la ventana exacta"
    return {"base": base, "cola": cola}


def check_segment_available(segment: Segment, today: date | None = None, lag_days: int = 6) -> None:
    """
    Impide pedir datos que ERA5 aún no publica (rezago ~5 días; se usan 6 de margen).
    Si segment.end > hoy − lag_days → MeteoError con la fecha a partir de la cual sí se puede correr.
    """
    today = today or date.today()
    if segment.end > today - timedelta(days=lag_days):
        ready = segment.end + timedelta(days=lag_days)
        raise MeteoError(f"Tramo '{segment.name}' termina {segment.end}; ERA5 no estará "
                         f"completo hasta ~{ready}. Ejecutar a partir de esa fecha.")


def build_tasks(locations: Sequence[Location], segment: Segment,
                only: Iterable[str] | None = None) -> list[DownloadTask]:
    """Producto locations × segment, filtrado opcionalmente por cod_mpio (--only)."""
    only_set = set(only) if only else None
    return [DownloadTask(loc, segment) for loc in locations
            if only_set is None or loc.cod_mpio in only_set]


# =============================================================================
# 4. Petición: parámetros, peso, huella y rutas (funciones puras, testeables sin red)
# =============================================================================

def build_params(task: DownloadTask, cfg: MeteoConfig) -> dict[str, str]:
    """Parámetros exactos de la API. Orden y formato estables (entran en la huella)."""
    return {
        "latitude": f"{round(task.location.lat, cfg.coord_decimals):.{cfg.coord_decimals}f}",
        "longitude": f"{round(task.location.lon, cfg.coord_decimals):.{cfg.coord_decimals}f}",
        "start_date": task.segment.start.isoformat(),
        "end_date": task.segment.end.isoformat(),
        "hourly": ",".join(cfg.hourly_vars),
        "models": cfg.model,
        "timezone": cfg.timezone,
        "wind_speed_unit": cfg.wind_speed_unit,
    }


def request_weight(task: DownloadTask, cfg: MeteoConfig) -> float:
    """
    Peso Open-Meteo: n_ubicaciones × (n_días/14) × (n_variables/10).
    Se aplica un mínimo de 1 por petición (supuesto conservador para el presupuesto).
    Base (1.759 días, 5 vars) ≈ 62,8 · Cola (6 días) → 1.
    """
    w = 1 * (task.segment.n_days / 14) * (len(cfg.hourly_vars) / 10)
    return max(1.0, w)


def params_fingerprint(params: dict[str, str]) -> str:
    """SHA-256 (16 hex) de los parámetros ordenados. Cambia si cambia cualquier parámetro (D-M4)."""
    canon = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]


def target_path(task: DownloadTask, cfg: MeteoConfig) -> Path:
    """data/raw/meteo/<cod_mpio>.json (base) · data/raw/meteo/_cola/<cod_mpio>.json (cola)."""
    folder = cfg.raw_dir / task.segment.subdir if task.segment.subdir else cfg.raw_dir
    return folder / f"{task.location.cod_mpio}.json"


def rejected_path(task: DownloadTask, cfg: MeteoConfig, ts: datetime | None = None) -> Path:
    """data/raw/meteo/_rejected/<cod_mpio>_<tramo>_<YYYYmmddTHHMMSSZ>.json (diagnóstico, no es caché)."""
    ts = ts or datetime.now(timezone.utc)
    return cfg.raw_dir / "_rejected" / f"{task.location.cod_mpio}_{task.segment.name}_{ts:%Y%m%dT%H%M%SZ}.json"


def content_fingerprint(payload: dict[str, Any]) -> str:
    """
    D-M7: SHA-256 del contenido científico (hourly + hourly_units + celda + elevación), con JSON
    canónico. Excluye generationtime_ms, que cambia en cada respuesta. Dos descargas con la
    misma huella de parámetros deben producir el mismo content_sha256 (salvo revisión de ERA5T).
    """
    core = {k: payload.get(k) for k in ("latitude", "longitude", "elevation", "hourly_units", "hourly")}
    canon = json.dumps(core, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


# =============================================================================
# 5. Manifiesto (estado de reanudación + trazabilidad)
# =============================================================================
# Una línea JSON por intento terminado (append-only):
# {"ts_utc": "...", "cod_mpio": "15001", "segment": "base", "fingerprint": "ab12…",
#  "status": "ok", "attempts": 1, "weight": 62.86, "http_status": 200, "bytes": 2101234,
#  "sha256": "…", "path": "data/raw/meteo/15001.json", "cell_lat": 5.5, "cell_lon": -73.4,
#  "elevation": 2810.0, "checks": {"V-M2": true, ...}, "error": null}

def append_manifest(cfg: MeteoConfig, result: DownloadResult, fingerprint: str) -> None:
    """Anexa una línea (abrir en modo 'a', flush + fsync). Nunca reescribe líneas anteriores."""
    rep = result.validation
    cell = (rep.details.get("cell") or {}).get("returned") if rep else None
    entry = {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cod_mpio": result.task.location.cod_mpio,
        "segment": result.task.segment.name,
        "start": result.task.segment.start.isoformat(),
        "end": result.task.segment.end.isoformat(),
        "fingerprint": fingerprint,
        "status": result.status.value,
        "attempts": result.attempts,
        "weight": round(result.weight, 3),
        "http_status": result.http_status,
        "bytes": result.bytes,
        "sha256": result.sha256,
        "content_sha256": result.content_sha256,
        "path": str(result.path) if result.path else None,
        "cell_lat": cell[0] if cell else None,
        "cell_lon": cell[1] if cell else None,
        "elevation": rep.details.get("elevation") if rep else None,
        "checks": rep.checks if rep else None,
        "elapsed_s": round(result.elapsed_s, 2) if result.elapsed_s is not None else None,
        "error": result.error,
    }
    cfg.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cfg.manifest_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def read_manifest(cfg: MeteoConfig) -> list[dict[str, Any]]:
    """Lee el JSONL tolerando una última línea truncada (corte de energía) → se descarta con warning."""
    if not cfg.manifest_path.exists():
        return []
    entries: list[dict[str, Any]] = []
    lines = cfg.manifest_path.read_text(encoding="utf-8").splitlines()
    for i, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            if i == len(lines):
                log.warning("Manifiesto: última línea truncada (se descarta): %r", line[:80])
            else:
                raise MeteoError(f"Manifiesto corrupto en la línea {i} de {cfg.manifest_path}")
    return entries


def latest_entries(manifest: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    """Último registro por (cod_mpio, segment). Es la 'vista de estado' para reanudar."""
    state: dict[tuple[str, str], dict[str, Any]] = {}
    for e in manifest:                      # el orden del archivo es cronológico
        state[(e["cod_mpio"], e["segment"])] = e
    return state


# =============================================================================
# 6. Caché
# =============================================================================

def is_cached(task: DownloadTask, cfg: MeteoConfig,
              state: dict[tuple[str, str], dict[str, Any]]) -> bool:
    """
    Una tarea está en caché si y solo si se cumplen LAS TRES condiciones:
      1. Existe target_path(task) y su tamaño > 0.
      2. El último registro del manifiesto para (cod_mpio, segment) tiene status 'ok' y la MISMA
         huella que build_params(task) actual (D-M4) y el MISMO sha256 que el archivo en disco.
      3. (Opcional, --revalidate) validate_payload vuelve a pasar sobre el archivo.
    Si el archivo existe pero no cumple 2 → se trata como pendiente y se sobrescribe (con log WARNING).
    """
    return _is_cached(task, cfg, state, revalidate=False)


def _is_cached(task: DownloadTask, cfg: MeteoConfig, state: dict[tuple[str, str], dict[str, Any]],
               revalidate: bool) -> bool:
    path = target_path(task, cfg)
    if not path.exists() or path.stat().st_size == 0:
        return False
    entry = state.get((task.location.cod_mpio, task.segment.name))
    fp = params_fingerprint(build_params(task, cfg))
    if entry is None or entry.get("status") != TaskStatus.OK.value:
        log.warning("[%s/%s] existe %s pero no hay registro 'ok' en el manifiesto → se vuelve a descargar",
                    task.location.cod_mpio, task.segment.name, path.name)
        return False
    if entry.get("fingerprint") != fp:
        log.warning("[%s/%s] la huella cambió (%s → %s) → se vuelve a descargar",
                    task.location.cod_mpio, task.segment.name, entry.get("fingerprint"), fp)
        return False
    data = path.read_bytes()
    if sha256_bytes(data) != entry.get("sha256"):
        log.warning("[%s/%s] el archivo no coincide con el sha256 del manifiesto → se vuelve a descargar",
                    task.location.cod_mpio, task.segment.name)
        return False
    if revalidate:
        rep = validate_payload(json.loads(data), task, cfg)
        if not rep.ok:
            log.warning("[%s/%s] la revalidación falla: %s", task.location.cod_mpio, task.segment.name, rep.errors)
            return False
    return True


def write_atomic(path: Path, data: bytes) -> None:
    """
    Escritura atómica: escribe en <path>.part, fsync y os.replace → nunca queda un JSON a medias
    con el nombre final (un corte durante la escritura no contamina la caché).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# =============================================================================
# 7. Control de cuota y ritmo
# =============================================================================

class RateLimiter:
    """Ritmo fijo: garantiza ≥ pace_seconds entre el INICIO de dos peticiones consecutivas."""

    def __init__(self, pace_seconds: float) -> None:
        self.pace = pace_seconds
        self._last: float | None = None

    def wait(self) -> None:
        """Duerme lo necesario (time.monotonic, inmune a cambios de reloj)."""
        now = time.monotonic()
        if self._last is not None:
            remaining = self.pace - (now - self._last)
            if remaining > 0:
                time.sleep(remaining)
                now = time.monotonic()
        self._last = now


class QuotaBudget:
    """
    Contabilidad local de peso por ventanas deslizantes (24 h, 60 min, 60 s).

    - Se inicializa con el peso de los registros del manifiesto de las últimas 24 h
      (incluye intentos fallidos con respuesta: Open-Meteo también los cuenta).
    - can_spend(w) antes de cada petición; si el diario no alcanza → DailyQuotaExceeded
      (la corrida termina con gracia: lo pendiente queda para mañana).
    - Es una salvaguarda local; la autoridad final es el 429 del servidor.
    """

    def __init__(self, cfg: MeteoConfig, manifest: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    def can_spend(self, weight: float) -> bool:
        raise NotImplementedError

    def spend(self, weight: float) -> None:
        raise NotImplementedError

    def report(self) -> dict[str, float]:
        """Peso usado/disponible en cada ventana (para el log y el dry-run)."""
        raise NotImplementedError


# =============================================================================
# 8. Red
# =============================================================================

def make_session(cfg: MeteoConfig) -> requests.Session:
    """
    Session con User-Agent genérico (sin datos personales) y keep-alive.
    Sin reintentos automáticos de urllib3: la política de reintento es nuestra
    (fetch_with_retry), para que cada intento se pueda contabilizar en la cuota.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": cfg.user_agent,
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    })
    adapter = requests.adapters.HTTPAdapter(max_retries=0, pool_connections=1, pool_maxsize=1)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def classify_response(status: int, body: bytes) -> None:
    """
    Traduce la respuesta HTTP en la política de reintento:
      200                       → ok (retorna None)
      400 / {"error": true}     → FatalRequestError(reason)        # parámetros mal: abortar todo
      429 + reason diario       → DailyQuotaExceeded(reason)       # parar y reanudar mañana
      429 (minutal/horario)     → TransientRequestError            # backoff
      5xx                       → TransientRequestError
    Nota: el texto exacto de `reason` se registra tal cual en el manifiesto; la detección de
    "diario" se hace por palabra clave (p. ej. 'daily') y se ajusta tras ver el primer 429 real.
    Otros 4xx (403, 404…) → FatalRequestError: no se arreglan reintentando.
    """
    if status == 200:
        return
    reason = _http_reason(body)
    msg = f"HTTP {status}: {reason}"
    if status == 429:
        if "daily" in reason.lower():
            raise DailyQuotaExceeded(msg)
        raise TransientRequestError(msg, http_status=status)
    if status >= 500:
        raise TransientRequestError(msg, http_status=status)
    raise FatalRequestError(msg)


def fetch(session: requests.Session, params: dict[str, str], cfg: MeteoConfig) -> tuple[int, bytes]:
    """
    Una petición GET con timeout (connect, read). Devuelve (status, bytes) SIN parsear.
    Excepciones de red (timeout, ConnectionError) → TransientRequestError.
    No interpreta el status: eso es responsabilidad de classify_response / process_task.
    """
    try:
        resp = session.get(cfg.base_url, params=params,
                           timeout=(cfg.timeout_connect_s, cfg.timeout_read_s))
    except requests.Timeout as exc:
        raise TransientRequestError(f"timeout: {exc}") from exc
    except requests.ConnectionError as exc:
        raise TransientRequestError(f"error de conexión: {exc}") from exc
    log.debug("GET %s → HTTP %s (%d bytes)", resp.url, resp.status_code, len(resp.content))
    return resp.status_code, resp.content


def fetch_with_retry(session, task: DownloadTask, cfg: MeteoConfig,
                     limiter: RateLimiter | None = None,
                     budget: QuotaBudget | None = None) -> tuple[int, bytes, int]:
    """
    Bucle de reintentos de UNA tarea. Devuelve (status, bytes, attempts).
      for attempt in 1..max_attempts:
          budget.can_spend(w) o DailyQuotaExceeded
          limiter.wait(); status, body = fetch(...); budget.spend(w)
          classify_response(status, body)
          Transient → sleep(min(cap, base·2^(attempt−1)) + jitter) y continuar
      Agotado → TransientRequestError (la tarea queda FAILED y se reintenta en la próxima corrida).
    v0.2: budget es opcional (None); la salvaguarda de cuota es el ritmo fijo + el 429 del servidor.
    """
    params = build_params(task, cfg)
    weight = request_weight(task, cfg)
    cod, seg = task.location.cod_mpio, task.segment.name
    last: TransientRequestError | None = None
    for attempt in range(1, cfg.max_attempts + 1):
        if budget is not None and not budget.can_spend(weight):
            raise DailyQuotaExceeded(f"[{cod}/{seg}] presupuesto local diario agotado")
        if limiter is not None:
            limiter.wait()
        try:
            status, body = fetch(session, params, cfg)
            if budget is not None:
                budget.spend(weight)
            classify_response(status, body)
            return status, body, attempt
        except TransientRequestError as exc:
            last = exc
            if attempt == cfg.max_attempts:
                break
            delay = min(cfg.backoff_cap_s, cfg.backoff_base_s * 2 ** (attempt - 1))
            delay += random.uniform(0, 0.1 * delay)
            log.warning("[%s/%s] intento %d/%d falló (%s); reintento en %.0f s",
                        cod, seg, attempt, cfg.max_attempts, exc, delay)
            time.sleep(delay)
    raise TransientRequestError(f"agotados {cfg.max_attempts} intentos; último: {last}",
                                attempts=cfg.max_attempts, http_status=last.http_status if last else None)


# =============================================================================
# 9. Validación de calidad
# =============================================================================

PHYSICAL_RANGES: dict[str, tuple[float, float]] = {
    "temperature_2m": (-10.0, 45.0),          # °C (páramo ↔ Puerto Boyacá)
    "relative_humidity_2m": (0.0, 100.0),     # %
    "precipitation": (0.0, 150.0),            # mm/h
    "wind_speed_10m": (0.0, 40.0),            # m/s
    "et0_fao_evapotranspiration": (0.0, 2.0), # mm/h
}

EXPECTED_UNITS: dict[str, str] = {
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "wind_speed_10m": "m/s",
    "et0_fao_evapotranspiration": "mm",
}

REQUIRED_TOP_KEYS = ("latitude", "longitude", "elevation", "utc_offset_seconds",
                     "timezone", "hourly_units", "hourly")
BLOCKING_CHECKS = ("V-M0", "V-M2", "V-M3", "V-M4", "V-M7")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def validate_payload(payload: dict[str, Any], task: DownloadTask, cfg: MeteoConfig) -> ValidationReport:
    """
    Validación POR ARCHIVO (se ejecuta antes de aceptar la descarga como caché).

    Bloqueantes (ok=False si falla alguna):
      V-M0  Estructura: claves latitude, longitude, elevation, utc_offset_seconds, timezone,
            hourly_units, hourly; sin "error".
      V-M2  Longitud: len(hourly.time) == segment.n_hours y len(var) igual para las 5 variables.
            Primer time == f"{start}T00:00", último == f"{end}T23:00".
      V-M3  Continuidad: paso constante de 1 h, sin duplicados.
      V-M4  Nulos: 0 nulos en las 5 variables (en el tramo pedido).
      V-M7  Metadatos: timezone == cfg.timezone, utc_offset_seconds == −18000,
            hourly_units == EXPECTED_UNITS.
    No bloqueantes (se registran en details):
      V-M5  Fuera de PHYSICAL_RANGES: conteo por variable (se reporta; raw no se corrige).
      V-M6* Celda devuelta: (latitude, longitude) y distancia a la coordenada pedida.
      V-M8* elevation devuelta.
    """
    rep = ValidationReport(ok=False)

    def check(cid: str, passed: bool, msg: str = "") -> None:
        rep.checks[cid] = bool(passed)
        if not passed:
            rep.errors.append(f"{cid}: {msg}")

    # --- V-M0 Estructura (si falla, no tiene sentido seguir) ---
    if not isinstance(payload, dict):
        check("V-M0", False, f"la respuesta no es un objeto JSON ({type(payload).__name__})")
        return rep
    if payload.get("error"):
        check("V-M0", False, f"la API devolvió error: {payload.get('reason')!r}")
        return rep
    missing_top = [k for k in REQUIRED_TOP_KEYS if k not in payload]
    hourly = payload.get("hourly") if isinstance(payload.get("hourly"), dict) else {}
    missing_vars = [k for k in ("time", *cfg.hourly_vars) if k not in hourly]
    check("V-M0", not missing_top and not missing_vars,
          f"faltan claves {missing_top} / variables horarias {missing_vars}")
    if not rep.checks["V-M0"]:
        return rep

    times: list[str] = hourly["time"]
    seg = task.segment
    n_exp = seg.n_hours

    # --- V-M2 Longitud y extremos ---
    lengths = {k: len(hourly[k]) for k in ("time", *cfg.hourly_vars)}
    first_exp, last_exp = f"{seg.start.isoformat()}T00:00", f"{seg.end.isoformat()}T23:00"
    first_got = times[0] if times else None
    last_got = times[-1] if times else None
    rep.details["n_hours"] = {"expected": n_exp, "got": lengths}
    rep.details["time_range"] = {"first": first_got, "last": last_got}
    check("V-M2",
          all(n == n_exp for n in lengths.values()) and first_got == first_exp and last_got == last_exp,
          f"esperado {n_exp} h de {first_exp} a {last_exp}; recibido {lengths} de {first_got} a {last_got}")

    # --- V-M3 Continuidad horaria ---
    try:
        parsed = [datetime.fromisoformat(t) for t in times]
        steps = [(b - a).total_seconds() for a, b in zip(parsed, parsed[1:])]
        bad_steps = sum(1 for s in steps if s != 3600.0)
        dups = len(parsed) - len(set(parsed))
        rep.details["continuity"] = {"bad_steps": bad_steps, "duplicates": dups}
        check("V-M3", bad_steps == 0 and dups == 0,
              f"{bad_steps} saltos distintos de 1 h y {dups} horas duplicadas")
    except (TypeError, ValueError) as exc:
        check("V-M3", False, f"marcas de tiempo no ISO 8601: {exc}")

    # --- V-M4 Nulos ---
    nulls = {k: sum(1 for x in hourly[k] if x is None) for k in cfg.hourly_vars}
    rep.details["nulls"] = nulls
    check("V-M4", sum(nulls.values()) == 0,
          "nulos por variable " + str({k: n for k, n in nulls.items() if n}))

    # --- V-M7 Metadatos y unidades ---
    units = payload.get("hourly_units") or {}
    wrong_units = {k: units.get(k) for k, u in EXPECTED_UNITS.items() if units.get(k) != u}
    tz_ok = payload.get("timezone") == cfg.timezone
    off_ok = payload.get("utc_offset_seconds") == BOGOTA_UTC_OFFSET_S
    rep.details["metadata"] = {"timezone": payload.get("timezone"),
                               "utc_offset_seconds": payload.get("utc_offset_seconds"),
                               "hourly_units": {k: units.get(k) for k in cfg.hourly_vars}}
    check("V-M7", tz_ok and off_ok and not wrong_units,
          f"timezone={payload.get('timezone')!r} offset={payload.get('utc_offset_seconds')!r} "
          f"unidades distintas={wrong_units}")

    # --- V-M5 Rangos físicos (no bloqueante) ---
    out_of_range: dict[str, int] = {}
    stats: dict[str, dict[str, float]] = {}
    for k, (lo, hi) in PHYSICAL_RANGES.items():
        vals = [x for x in hourly.get(k, []) if x is not None]
        out_of_range[k] = sum(1 for x in vals if x < lo or x > hi)
        if vals:
            stats[k] = {"min": min(vals), "mean": round(sum(vals) / len(vals), 3), "max": max(vals)}
    rep.details["out_of_range"] = out_of_range
    rep.details["stats"] = stats
    rep.checks["V-M5"] = sum(out_of_range.values()) == 0     # informativo

    # --- V-M6* / V-M8* Celda devuelta y elevación (informativos) ---
    try:
        cell_lat, cell_lon = float(payload["latitude"]), float(payload["longitude"])
        rep.details["cell"] = {
            "requested": [task.location.lat, task.location.lon],
            "returned": [cell_lat, cell_lon],
            "distance_km": round(_haversine_km(task.location.lat, task.location.lon, cell_lat, cell_lon), 2),
        }
    except (TypeError, ValueError):
        rep.details["cell"] = None
    rep.details["elevation"] = payload.get("elevation")

    rep.ok = all(rep.checks.get(c, False) for c in BLOCKING_CHECKS)
    return rep


def validate_dataset(cfg: MeteoConfig, locations: Sequence[Location],
                     segments: dict[str, Segment]) -> dict[str, Any]:
    """
    Validación DE CONJUNTO (después de la corrida; también vía --validate-only).

      V-M1   123 archivos 'ok' por tramo; lista de faltantes.
      V-M2b  base + cola concatenados = 42.360 h por municipio, sin solape ni hueco en la frontera
             (2026-10-25T23:00 → 2026-10-26T00:00). Si la cola aún no existe → estado 'pendiente_cola'.
      V-M6   Celdas compartidas: nº de celdas distintas y grupos de municipios con serie idéntica,
             medido POR SEPARADO para (temperature_2m, relative_humidity_2m) [ERA5-Land 0,1°] y
             (precipitation, wind_speed_10m) [ERA5 0,25°] → limitación del IEEE.
      V-M8   Sanidad física: correlación de Spearman elevación vs. temperatura media < −0,7.
      V-M9   Régimen bimodal de precipitación en Tunja (15001): picos en AMJ y OND.
      V-M10  Filas horarias totales = 123 × 42.360 = 5.210.280.

    Escribe: data/raw/meteo/_validation_report.json y un resumen Markdown para
    docs/notas/manifest_raw.md (este sí se versiona).
    """
    raise NotImplementedError


# =============================================================================
# 10. Orquestación
# =============================================================================

def _http_reason(body: bytes) -> str:
    try:
        data = json.loads(body)
        return str(data.get("reason", data))[:300] if isinstance(data, dict) else str(data)[:300]
    except (ValueError, UnicodeDecodeError):
        return body[:300].decode("utf-8", errors="replace")


def process_task(session: requests.Session, task: DownloadTask, cfg: MeteoConfig,
                 limiter: RateLimiter | None = None,
                 budget: QuotaBudget | None = None) -> DownloadResult:
    """
    Una tarea de punta a punta:
      params → fetch (v0.1: un intento; v0.2: fetch_with_retry) → json.loads(copia) → validate_payload
        ok      → write_atomic(bytes originales) → DownloadResult(OK)
        invalid → NO se escribe con el nombre final; se guarda en <raw>/_rejected/<cod>_<ts>.json
                  para diagnóstico → DownloadResult(INVALID)
    FatalRequestError y DailyQuotaExceeded se propagan (los maneja run_download).
    v0.2: usa fetch_with_retry (reintentos con backoff para red, 5xx y 429 no diario);
    agotados los intentos → FAILED. limiter y budget siguen siendo opcionales.
    """
    t0 = time.monotonic()
    params = build_params(task, cfg)
    fp = params_fingerprint(params)
    weight = request_weight(task, cfg)
    result = DownloadResult(task=task, status=TaskStatus.FAILED, attempts=1, weight=weight, fingerprint=fp)
    cod, seg = task.location.cod_mpio, task.segment.name
    log.info("[%s/%s] GET %s → %s (peso≈%.1f, huella %s)", cod, seg, params["start_date"],
             params["end_date"], weight, fp)

    try:
        status, body, attempts = fetch_with_retry(session, task, cfg, limiter, budget)
    except TransientRequestError as exc:
        result.attempts = exc.attempts
        result.http_status = exc.http_status
        result.error = str(exc)
        result.elapsed_s = time.monotonic() - t0
        log.warning("[%s/%s] FAILED: %s", cod, seg, exc)
        return result
    except (FatalRequestError, DailyQuotaExceeded) as exc:
        raise type(exc)(f"[{cod}/{seg}] {exc}") from exc
    result.attempts = attempts
    result.http_status = status

    result.bytes = len(body)
    result.sha256 = sha256_bytes(body)

    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        report = ValidationReport(ok=False, checks={"V-M0": False}, errors=[f"V-M0: JSON inválido: {exc}"])
        payload = None
    else:
        report = validate_payload(payload, task, cfg)
    result.validation = report

    if report.ok:
        dest = target_path(task, cfg)
        write_atomic(dest, body)                   # bytes ORIGINALES (D-M3)
        result.status = TaskStatus.OK
        result.path = dest
        result.content_sha256 = content_fingerprint(payload)
        log.info("[%s/%s] OK → %s (%d bytes)", cod, seg, dest, result.bytes)
    else:
        dest = rejected_path(task, cfg)
        write_atomic(dest, body)
        result.status = TaskStatus.INVALID
        result.path = dest
        result.error = "; ".join(report.errors)
        log.error("[%s/%s] INVALID → %s: %s", cod, seg, dest, result.error)

    result.elapsed_s = time.monotonic() - t0
    return result


def run_download(cfg: MeteoConfig, segment: Segment, *, only: Iterable[str] | None = None,
                 limit: int | None = None, dry_run: bool = False,
                 revalidate: bool = False, require_complete: bool = True,
                 max_consecutive_failures: int = 3) -> RunSummary:
    """
    Flujo principal (idempotente: ejecutarlo N veces converge al mismo estado final):

      1. check_segment_available(segment)
      2. locations = load_locations(cfg); tasks = build_tasks(...)
      3. manifest = read_manifest(cfg); state = latest_entries(manifest)
      4. pending = [t for t in tasks if not is_cached(t, cfg, state)]; aplicar --limit
      5. budget = QuotaBudget(cfg, manifest); estimar peso total de pending
         dry_run → imprimir plan (n tareas, peso, duración estimada, ¿cabe hoy?) y salir
      6. session = make_session(cfg); limiter = RateLimiter(cfg.pace_seconds)
      7. for task in pending:
             try:   result = process_task(...)
             except FatalRequestError → append_manifest(failed) y ABORTAR (exit 2)
             except DailyQuotaExceeded → marcar resto como SKIPPED_QUOTA y TERMINAR (exit 3)
             except TransientRequestError → result FAILED, continuar con la siguiente
             append_manifest(result)   # siempre, también en fallos
             log de progreso: k/N, ETA, peso diario restante
      8. Ctrl+C (KeyboardInterrupt): termina la tarea en curso de forma limpia (el .part no se
         promueve), escribe el resumen y sale con exit 130. Volver a ejecutar = reanudar.
      9. Retorna RunSummary (pendientes = FAILED + INVALID + SKIPPED_QUOTA + no intentadas).
    v0.2: sin QuotaBudget (stub). Salvaguardas: ritmo fijo, 429 diario → parada limpia, y
    corte tras `max_consecutive_failures` tareas FAILED seguidas (evita quemar cuota a ciegas).
    """
    t0 = time.monotonic()
    check_segment_available(segment)
    locations = load_locations(cfg, require_complete=require_complete)
    tasks = build_tasks(locations, segment, only)
    if only and len(tasks) != len(set(only)):
        missing = sorted(set(only) - {t.location.cod_mpio for t in tasks})
        raise MeteoError(f"--only: códigos que no están en los centroides: {missing}")

    state = latest_entries(read_manifest(cfg))
    cached = [t for t in tasks if _is_cached(t, cfg, state, revalidate)]
    cached_ids = {t.location.cod_mpio for t in cached}
    pending = [t for t in tasks if t.location.cod_mpio not in cached_ids]
    if limit is not None:
        pending = pending[:limit]
    planned = sum(request_weight(t, cfg) for t in pending)
    eta_min = len(pending) * cfg.pace_seconds / 60
    log.info("Tramo %s (%s → %s): %d tareas · %d en caché · %d pendientes · peso≈%.0f llamadas · ETA≈%.0f min",
             segment.name, segment.start, segment.end, len(tasks), len(cached), len(pending), planned, eta_min)
    if planned > cfg.daily_budget:
        log.warning("El peso pendiente (%.0f) supera el presupuesto diario (%.0f): la corrida se cortará "
                    "por cuota y deberá reanudarse mañana.", planned, cfg.daily_budget)

    by_status: dict[str, int] = {TaskStatus.CACHED.value: len(cached)}
    summary = RunSummary(segment=segment.name, total=len(tasks), by_status=by_status, weight_spent=0.0,
                         elapsed_s=0.0, pending=[], planned_weight=planned)
    if dry_run:
        by_status[TaskStatus.PLANNED.value] = len(pending)
        summary.pending = [t.location.cod_mpio for t in pending]
        summary.elapsed_s = time.monotonic() - t0
        return summary

    session = make_session(cfg)
    limiter = RateLimiter(cfg.pace_seconds)
    not_done = [t.location.cod_mpio for t in pending]
    consecutive_failures = 0
    try:
        for k, task in enumerate(pending, start=1):
            cod = task.location.cod_mpio
            fp = params_fingerprint(build_params(task, cfg))
            try:
                result = process_task(session, task, cfg, limiter, None)
            except FatalRequestError as exc:
                append_manifest(cfg, DownloadResult(task, TaskStatus.FAILED, weight=request_weight(task, cfg),
                                                    error=str(exc), fingerprint=fp), fp)
                log.error("ERROR FATAL (se aborta la corrida): %s", exc)
                summary.stop_reason = "fatal"
                break
            except DailyQuotaExceeded as exc:
                append_manifest(cfg, DownloadResult(task, TaskStatus.SKIPPED_QUOTA, error=str(exc),
                                                    fingerprint=fp), fp)
                log.error("Cuota diaria agotada: %s. Reanudar mañana con el mismo comando.", exc)
                summary.stop_reason = "quota"
                break

            append_manifest(cfg, result, fp)
            by_status[result.status.value] = by_status.get(result.status.value, 0) + 1
            if result.status == TaskStatus.OK:
                summary.weight_spent += result.weight
                not_done.remove(cod)
                consecutive_failures = 0
            else:
                consecutive_failures += 1
            left = len(pending) - k
            log.info("Progreso %d/%d · %s=%s · restan %d (≈%.0f min) · peso usado≈%.0f",
                     k, len(pending), cod, result.status.value, left, left * cfg.pace_seconds / 60,
                     summary.weight_spent)
            if consecutive_failures >= max_consecutive_failures:
                log.error("%d tareas fallidas seguidas: se detiene la corrida para no gastar cuota. "
                          "Revisar el log y reanudar con el mismo comando.", consecutive_failures)
                summary.stop_reason = "consecutive_failures"
                break
    except KeyboardInterrupt:
        log.warning("Interrumpido por el usuario. Lo descargado queda guardado; reanudar con el mismo comando.")
        summary.stop_reason = "interrupted"
    finally:
        session.close()

    summary.pending = not_done
    summary.elapsed_s = time.monotonic() - t0
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    """
    CLI. Códigos de salida: 0 todo ok · 1 quedan pendientes (reintentar) · 2 error fatal de
    configuración · 3 cuota diaria agotada (reanudar mañana) · 130 interrumpido por el usuario.
    """
    p = argparse.ArgumentParser(prog="firewatch.ingest.meteo", description=__doc__.splitlines()[1])
    p.add_argument("--settings", default="config/settings.yaml")
    p.add_argument("--tramo", choices=["base", "cola"], default="base")
    p.add_argument("--only", nargs="*", help="cod_mpio específicos (p. ej. 15001)")
    p.add_argument("--limit", type=int, help="máximo de tareas pendientes a procesar")
    p.add_argument("--dry-run", action="store_true", help="plan y presupuesto, sin red")
    p.add_argument("--revalidate", action="store_true", help="re-validar archivos en caché")
    p.add_argument("--validate-only", action="store_true", help="solo validate_dataset")
    p.add_argument("--allow-partial", action="store_true", help="no exigir los 123 municipios en los centroides")
    p.add_argument("--max-fallos-seguidos", type=int, default=3)
    args = p.parse_args(argv)

    import yaml
    settings_path = Path(args.settings).resolve()
    repo_root = settings_path.parent.parent
    try:
        settings = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
        cfg = load_config(settings, base_dir=repo_root)
    except (OSError, MeteoError, yaml.YAMLError) as exc:
        logging.basicConfig(level=logging.INFO)
        log.error("Configuración inválida: %s", exc)
        return 2

    # Log a consola y a archivo (útil para la corrida nocturna)
    log_dir = cfg.raw_dir / "_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"meteo_{args.tramo}_{datetime.now():%Y%m%dT%H%M%S}.log"
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in (logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")):
        h.setFormatter(fmt)
        root.addHandler(h)

    if args.validate_only:
        log.error("--validate-only (validate_dataset) aún no está implementado en v0.2")
        return 2

    try:
        segment = build_segments(cfg, cfg.cola_start)[args.tramo]
        summary = run_download(cfg, segment, only=args.only, limit=args.limit, dry_run=args.dry_run,
                               revalidate=args.revalidate, require_complete=not args.allow_partial,
                               max_consecutive_failures=args.max_fallos_seguidos)
    except MeteoError as exc:
        log.error("%s: %s", type(exc).__name__, exc)
        return 2

    log.info("=" * 70)
    log.info("RESUMEN tramo=%s total=%d estados=%s peso usado≈%.0f duración=%.1f min motivo de parada=%s",
             summary.segment, summary.total, summary.by_status, summary.weight_spent,
             summary.elapsed_s / 60, summary.stop_reason)
    if summary.pending:
        log.info("Pendientes (%d): %s", len(summary.pending), " ".join(summary.pending))
    log.info("Log completo: %s · Manifiesto: %s", log_file, cfg.manifest_path)

    if args.dry_run:
        return 0
    return {"fatal": 2, "quota": 3, "interrupted": 130}.get(summary.stop_reason or "",
                                                              1 if summary.pending else 0)


if __name__ == "__main__":      # obligatorio en Windows (método spawn) y para `python -m`
    raise SystemExit(main())
