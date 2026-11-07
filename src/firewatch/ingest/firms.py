"""L1 · Ingesta de NASA FIRMS (VIIRS S-NPP + NOAA-20, Colombia).

Fuentes (ver settings.yaml → rutas.raw_firms y rutas.raw_firms_2026):
    - 2022–2025: CSV anual por país (procesamiento estándar, SP) → data/raw/firms/
    - 2026-01-01 → 2026-10-31: Archive Download (SP + NRT) → data/raw/firms_2026/{archive,nrt}/
      La solicitud se hace manualmente en la web (código por correo); este módulo solo
      valida y registra los archivos recibidos.

Diferencias reales entre productos que este módulo absorbe:
    - Archive Download nombra brightness/bright_t31 lo que el CSV anual nombra bright_ti4/bright_ti5
      (las distribuciones coinciden: mediana ≈ 337 K y p99 = 367 K en I-4; mediana ≈ 295 K en I-5).
    - Archive Download escribe instrument=SNPP (y satellite=SNPP) en los archivos de S-NPP.

A implementar:
    resumen_volumen() -> pandas.DataFrame
        Conteo por año, satélite y tipo de proceso (SP/NRT). Alimenta el gate
        (docs/notas/gate_volumen.md).

Salida: archivos raw sin modificar + log de validación.
Dueño: B · Tareas: B1, B5
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

COLUMNAS_REQUERIDAS = ("latitude", "longitude", "bright_ti4", "scan", "track", "acq_date", "acq_time",
                       "satellite", "instrument", "confidence", "version", "bright_ti5", "frp", "daynight")
ALIAS_COLUMNAS = {"brightness": "bright_ti4", "bright_t31": "bright_ti5"}
KINDS_SP = ("sp_anual", "archive_sp")
PREFIJOS_2026 = {"fire_archive": "archive_sp", "fire_nrt": "archive_nrt"}


class FirmsRawError(Exception):
    """Configuración inválida o archivo raw de FIRMS que no cumple lo esperado."""


@dataclass(frozen=True)
class ArchivoRaw:
    """Un CSV raw y su producto: 'sp_anual', 'archive_sp' o 'archive_nrt'."""

    path: Path
    kind: str


def rutas_raw(settings: dict[str, Any], base_dir: str | Path) -> tuple[Path, Path]:
    """Devuelve (carpeta del CSV anual, carpeta de 2026) desde settings; las relativas van contra base_dir."""
    rutas = settings.get("rutas") or {}
    faltan = [k for k in ("raw_firms", "raw_firms_2026") if k not in rutas]
    if faltan:
        raise FirmsRawError(f"settings.yaml: faltan las claves rutas.{faltan}")
    return Path(base_dir) / rutas["raw_firms"], Path(base_dir) / rutas["raw_firms_2026"]


def listar_archivos_raw(dir_anual: Path, dir_2026: Path) -> list[ArchivoRaw]:
    """
    Lista los CSV en orden determinista: anuales, Archive SP y NRT, cada grupo por nombre.
    En 2026 el producto se reconoce por el prefijo del archivo; otro CSV es un error.
    """
    anuales = sorted(Path(dir_anual).glob("*.csv"))
    if not anuales:
        raise FirmsRawError(f"No hay CSV anuales de FIRMS en {dir_anual}")

    por_kind: dict[str, list[Path]] = {kind: [] for kind in PREFIJOS_2026.values()}
    for path in Path(dir_2026).rglob("*.csv"):
        kind = next((k for prefijo, k in PREFIJOS_2026.items() if path.name.lower().startswith(prefijo)), None)
        if kind is None:
            raise FirmsRawError(f"Archivo no reconocido en {dir_2026}: {path.name}")
        por_kind[kind].append(path)
    if not any(por_kind.values()):
        raise FirmsRawError(f"No hay CSV de Archive Download en {dir_2026}")

    archivos = [ArchivoRaw(p, "sp_anual") for p in anuales]
    for kind, paths in por_kind.items():
        archivos += [ArchivoRaw(p, kind) for p in sorted(paths, key=lambda p: p.name)]
    return archivos


def normalizar_encabezados(df: pd.DataFrame) -> pd.DataFrame:
    """Lleva los nombres de Archive Download a los canónicos, sin tocar los valores."""
    choques = [alias for alias, canonica in ALIAS_COLUMNAS.items() if alias in df.columns and canonica in df.columns]
    if choques:
        raise FirmsRawError(f"Columnas duplicadas por alias: {choques}")
    return df.rename(columns=ALIAS_COLUMNAS)


def validar_columnas(archivo: ArchivoRaw) -> None:
    """Exige las columnas canónicas tras normalizar; `type` solo es obligatoria en SP."""
    columnas = normalizar_encabezados(pd.read_csv(archivo.path, nrows=0)).columns
    faltan = [c for c in COLUMNAS_REQUERIDAS if c not in columnas]
    if archivo.kind in KINDS_SP and "type" not in columnas:
        faltan.append("type")
    if faltan:
        raise FirmsRawError(f"{archivo.path.name}: faltan columnas {faltan}")
    log.info("%s: columnas válidas (%s)", archivo.path.name, archivo.kind)


def instrumentos_invalidos(df: pd.DataFrame) -> list[tuple[str, str]]:
    """
    Pares (instrument, satellite) no admitidos, en mayúsculas y ordenados.
    Vale VIIRS con cualquier satélite, y SNPP solo junto a satellite=SNPP (Archive Download).
    El satélite permitido se valida aparte: aquí solo se decide si el producto es VIIRS.
    """
    instrumento = df["instrument"].astype("string").str.strip().str.upper().fillna("<NA>")
    satelite = df["satellite"].astype("string").str.strip().str.upper().fillna("<NA>")
    validos = (instrumento == "VIIRS") | ((instrumento == "SNPP") & (satelite == "SNPP"))
    return sorted(set(zip(instrumento[~validos], satelite[~validos])))
