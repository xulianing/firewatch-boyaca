"""Tests sin red de firewatch.ingest.meteo.a_silver_horario (B3: JSON Open-Meteo → silver/clima_horario).

Ventana sintética corta que cruza el cambio de año para probar las particiones:
    base = 2025-12-30 → 2026-01-01 (72 h) · cola = 2026-01-02 (24 h) · total 96 h por municipio.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from firewatch.ingest import meteo as m

LOCS = [m.Location("15001", "Tunja", 5.5353, -73.3678),
        m.Location("15022", "Almeida", 4.9706, -73.3792)]
H_TOTAL = 96


# ----------------------------------------------------------------------------- fixtures

@pytest.fixture
def cfg(tmp_path):
    settings = {
        "ventana": {"fecha_inicio": "2025-12-30", "fecha_fin": "2026-01-02"},
        "rutas": {"raw_meteo": "data/raw/meteo", "silver": "data/silver",
                  "dim_municipio": "data/silver/dim_municipio"},
        "meteo": {"model": "era5_seamless", "hourly_vars": list(m.REQUIRED_HOURLY_VARS),
                  "timezone": "America/Bogota", "wind_speed_unit": "ms", "cola_start": "2026-01-02"},
    }
    return m.load_config(settings, base_dir=tmp_path)


def make_payload(seg: m.Segment, offset: float = 0.0, shift_h: int = 0) -> dict:
    """Payload válido; los valores llevan la hora absoluta para poder verificar el orden en la costura."""
    n = seg.n_hours
    t0 = datetime.fromisoformat(f"{seg.start}T00:00") + timedelta(hours=shift_h)
    times = [t0 + timedelta(hours=i) for i in range(n)]
    return {
        "latitude": 5.5, "longitude": -73.4, "generationtime_ms": 1.0, "utc_offset_seconds": -18000,
        "timezone": "America/Bogota", "timezone_abbreviation": "GMT-5", "elevation": 2790.0,
        "hourly_units": {"time": "iso8601", **m.EXPECTED_UNITS},
        "hourly": {
            "time": [t.strftime("%Y-%m-%dT%H:%M") for t in times],
            "temperature_2m": [offset + t.hour for t in times],
            "relative_humidity_2m": [80] * n,
            "precipitation": [0.1] * n,
            "wind_speed_10m": [2.5] * n,
            "et0_fao_evapotranspiration": [0.05] * n,
        },
    }


def write_raw(cfg: m.MeteoConfig, loc: m.Location, seg: m.Segment, payload: dict) -> None:
    """Escribe el JSON raw y su línea 'ok' en el manifiesto (igual que process_task)."""
    task = m.DownloadTask(loc, seg)
    data = json.dumps(payload).encode("utf-8")
    m.write_atomic(m.target_path(task, cfg), data)
    entry = {"cod_mpio": loc.cod_mpio, "segment": seg.name, "status": "ok",
             "fingerprint": m.params_fingerprint(m.build_params(task, cfg)), "sha256": m.sha256_bytes(data)}
    cfg.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cfg.manifest_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


@pytest.fixture
def raw(cfg):
    segs = m.build_segments(cfg)
    for i, loc in enumerate(LOCS):
        write_raw(cfg, loc, segs["base"], make_payload(segs["base"], offset=100 * i))
        write_raw(cfg, loc, segs["cola"], make_payload(segs["cola"], offset=100 * i + 50))
    return segs


def out_dir(cfg) -> Path:
    return cfg.silver_dir / "clima_horario"


def read_silver(cfg) -> pd.DataFrame:
    return pd.read_parquet(out_dir(cfg))


# ----------------------------------------------------------------------------- conteos

def test_conteo_total_y_por_anio(cfg, raw):
    rep = m.a_silver_horario(cfg, LOCS)
    df = read_silver(cfg)
    assert len(df) == rep["n_filas"] == len(LOCS) * H_TOTAL
    # 2025-12-30 y 31 → 48 h; 2026-01-01 y 02 → 48 h, por municipio
    assert rep["filas_por_anio"] == {2025: 48 * len(LOCS), 2026: 48 * len(LOCS)}
    assert df.groupby("cod_mpio").size().to_dict() == {"15001": H_TOTAL, "15022": H_TOTAL}


def test_falta_archivo_cola_falla(cfg, raw):
    m.target_path(m.DownloadTask(LOCS[1], raw["cola"]), cfg).unlink()
    with pytest.raises(m.MeteoError, match="15022"):
        m.a_silver_horario(cfg, LOCS)
    assert not out_dir(cfg).exists()


def test_archivo_sin_registro_ok_en_manifiesto_falla(cfg, raw):
    cfg.manifest_path.unlink()
    with pytest.raises(m.MeteoError, match="manifiesto"):
        m.a_silver_horario(cfg, LOCS)


def test_payload_invalido_falla(cfg, raw):
    bad = make_payload(raw["base"])
    bad["hourly"]["precipitation"][5] = None                      # V-M4
    write_raw(cfg, LOCS[0], raw["base"], bad)
    with pytest.raises(m.MeteoError, match="V-M4"):
        m.a_silver_horario(cfg, LOCS)


# ----------------------------------------------------------------------------- costura base → cola

def test_costura_base_cola_continua(cfg, raw):
    m.a_silver_horario(cfg, LOCS)
    t = read_silver(cfg).query("cod_mpio == '15001'").sort_values("ts_local").reset_index(drop=True)
    pasos = t["ts_local"].diff().dropna().unique()
    assert list(pasos) == [pd.Timedelta(hours=1)]
    assert t["ts_local"].iloc[0] == pd.Timestamp("2025-12-30 00:00")
    assert t["ts_local"].iloc[-1] == pd.Timestamp("2026-01-02 23:00")
    frontera = t.set_index("ts_local")["temp"]
    assert frontera[pd.Timestamp("2026-01-01 23:00")] == 23.0     # último valor del tramo base
    assert frontera[pd.Timestamp("2026-01-02 00:00")] == 50.0     # primer valor del tramo cola


def test_costura_con_solape_falla(cfg, raw):
    # la cola empieza una hora antes (solape con la última hora de base)
    write_raw(cfg, LOCS[0], raw["cola"], make_payload(raw["cola"], shift_h=-1))
    with pytest.raises(m.MeteoError):
        m.a_silver_horario(cfg, LOCS)
    assert not out_dir(cfg).exists()


# ----------------------------------------------------------------------------- viento y tipos

def test_viento_convertido_a_kmh(cfg, raw):
    m.a_silver_horario(cfg, LOCS)
    df = read_silver(cfg)
    assert df["wind"].tolist() == pytest.approx([2.5 * 3.6] * len(df))   # 2,5 m/s = 9 km/h


def test_esquema_y_tipos(cfg, raw):
    m.a_silver_horario(cfg, LOCS)
    files = sorted(out_dir(cfg).rglob("*.parquet"))
    schema = pq.read_schema(files[0])
    assert schema.field("ts_local").type == pa.timestamp("us")          # sin zona horaria, en µs
    assert schema.field("fecha").type == pa.date32()
    assert schema.field("cod_mpio").type == pa.string()
    for c in ("temp", "rh", "precip", "wind", "et0"):
        assert schema.field(c).type == pa.float64()
    assert set(schema.names) == {"cod_mpio", "ts_local", "fecha", "temp", "rh", "precip", "wind", "et0"}
    df = read_silver(cfg)
    assert (df["fecha"].astype(str) == df["ts_local"].dt.strftime("%Y-%m-%d")).all()
    assert df["cod_mpio"].str.fullmatch(r"15\d{3}").all()


# ----------------------------------------------------------------------------- particiones

def test_particiones_hive_por_anio(cfg, raw):
    m.a_silver_horario(cfg, LOCS)
    parts = sorted(p.name for p in out_dir(cfg).iterdir() if p.is_dir())
    assert parts == ["anio=2025", "anio=2026"]
    for p in out_dir(cfg).iterdir():
        anio = int(p.name.split("=")[1])
        t = pq.read_table(p).to_pandas()
        assert (t["ts_local"].dt.year == anio).all()


def test_idempotente_y_hash_estable(cfg, raw):
    r1 = m.a_silver_horario(cfg, LOCS)
    files1 = sorted(str(p.relative_to(out_dir(cfg))) for p in out_dir(cfg).rglob("*.parquet"))
    r2 = m.a_silver_horario(cfg, LOCS)
    files2 = sorted(str(p.relative_to(out_dir(cfg))) for p in out_dir(cfg).rglob("*.parquet"))
    assert r1["content_sha256"] == r2["content_sha256"]
    assert files1 == files2
    # el hash recalculado sobre lo que quedó en disco coincide con el reportado
    assert m.hash_clima(read_silver(cfg)) == r2["content_sha256"]
    assert (cfg.silver_dir / "_reports" / "clima_horario.json").exists()


# ----------------------------------------------------------------------------- unicidad

def test_clave_unica(cfg, raw):
    m.a_silver_horario(cfg, LOCS)
    df = read_silver(cfg)
    assert not df.duplicated(["cod_mpio", "ts_local"]).any()


def test_municipio_duplicado_falla(cfg, raw):
    with pytest.raises(m.MeteoError, match="duplicad"):
        m.a_silver_horario(cfg, [LOCS[0], LOCS[0]])
