"""Tests sin red de firewatch.ingest.meteo v0.1 (load_config, load_locations, validate_payload, process_task)."""

from __future__ import annotations

import copy
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from firewatch.ingest import meteo as m

TUNJA = ("15001", "Tunja", 5.5353, -73.3678)


# ----------------------------------------------------------------------------- fixtures

def settings_dict(**meteo_overrides):
    s = {
        "ventana": {"fecha_inicio": date(2022, 1, 1), "fecha_fin": date(2026, 10, 31)},
        "rutas": {"raw_meteo": "data/raw/meteo", "dim_municipio": "data/silver/dim_municipio",
                  "centroides": "config/centroides_boyaca.csv"},
        "meteo": {"model": "era5_seamless", "hourly_vars": list(m.REQUIRED_HOURLY_VARS),
                  "timezone": "America/Bogota", "wind_speed_unit": "ms", "cola_start": "2026-10-26"},
    }
    s["meteo"].update(meteo_overrides)
    return s


@pytest.fixture
def cfg(tmp_path):
    return m.load_config(settings_dict(), base_dir=tmp_path)


@pytest.fixture
def short_task():
    """Tramo corto (3 días) para que los payloads sintéticos sean pequeños."""
    seg = m.Segment("base", date(2025, 1, 1), date(2025, 1, 3), "")
    return m.DownloadTask(m.Location(*TUNJA), seg)


def make_payload(task, units=None, **hourly_overrides):
    n = task.segment.n_hours
    t0 = datetime.fromisoformat(f"{task.segment.start}T00:00")
    hourly = {
        "time": [(t0 + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(n)],
        "temperature_2m": [12.0] * n,
        "relative_humidity_2m": [80] * n,
        "precipitation": [0.1] * n,
        "wind_speed_10m": [2.5] * n,
        "et0_fao_evapotranspiration": [0.05] * n,
    }
    hourly.update(hourly_overrides)
    return {
        "latitude": 5.5, "longitude": -73.4, "generationtime_ms": 12.3, "utc_offset_seconds": -18000,
        "timezone": "America/Bogota", "timezone_abbreviation": "GMT-5", "elevation": 2790.0,
        "hourly_units": units or {"time": "iso8601", **m.EXPECTED_UNITS},
        "hourly": hourly,
    }


def write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["cod_mpio,nombre,lat_c,lon_c,metodo"] + [",".join(map(str, r)) + ",centroid" for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class FakeResponse:
    def __init__(self, status, body: bytes):
        self.status_code, self.content, self.url = status, body, "https://fake"


class FakeSession:
    def __init__(self, status, body: bytes):
        self.response, self.calls = FakeResponse(status, body), []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        return self.response


# ----------------------------------------------------------------------------- load_config

def test_load_config_ok(cfg, tmp_path):
    assert cfg.model == "era5_seamless" and cfg.wind_speed_unit == "ms"
    assert cfg.raw_dir == tmp_path / "data/raw/meteo"
    assert cfg.centroides_path == tmp_path / "config/centroides_boyaca.csv"
    segs = m.build_segments(cfg, cfg.cola_start)
    assert segs["base"].end == date(2026, 10, 25)
    assert segs["base"].n_hours + segs["cola"].n_hours == 42_360


@pytest.mark.parametrize("override, msg", [
    ({"model": "era5_land"}, "congelado"),
    ({"hourly_vars": ["temperature_2m"]}, "exactamente"),
    ({"wind_speed_unit": "kmh"}, "m/s"),
    ({"timezone": "UTC"}, "ts_local"),
    ({"pace_seconds": 5}, "pace_seconds"),
])
def test_load_config_rejects(override, msg, tmp_path):
    with pytest.raises(m.MeteoError, match=msg):
        m.load_config(settings_dict(**override), base_dir=tmp_path)


def test_hourly_vars_order_does_not_change_fingerprint(tmp_path, short_task):
    a = m.load_config(settings_dict(), base_dir=tmp_path)
    b = m.load_config(settings_dict(hourly_vars=list(reversed(m.REQUIRED_HOURLY_VARS))), base_dir=tmp_path)
    assert m.params_fingerprint(m.build_params(short_task, a)) == m.params_fingerprint(m.build_params(short_task, b))


# ----------------------------------------------------------------------------- load_locations

def test_load_locations_partial_and_complete(cfg):
    write_csv(cfg.centroides_path, [TUNJA])
    locs = m.load_locations(cfg, require_complete=False)
    assert locs == [m.Location("15001", "Tunja", 5.5353, -73.3678)]
    with pytest.raises(m.MeteoError, match="123"):
        m.load_locations(cfg, require_complete=True)


@pytest.mark.parametrize("row, msg", [
    (("15001", "Tunja", -73.3678, 5.5353), "invertidas"),     # lat/lon invertidas
    (("5001", "Tunja", 5.5353, -73.3678), "texto"),           # se perdió formato (no es Boyacá/5 dígitos)
    (("05001", "Medellín", 6.25, -75.56), r"\^15"),           # otro departamento
])
def test_load_locations_rejects(cfg, row, msg):
    write_csv(cfg.centroides_path, [row])
    with pytest.raises(m.MeteoError, match=msg):
        m.load_locations(cfg, require_complete=False)


def test_load_locations_rejects_duplicates(cfg):
    write_csv(cfg.centroides_path, [TUNJA, TUNJA])
    with pytest.raises(m.MeteoError, match="duplicado"):
        m.load_locations(cfg, require_complete=False)


# ----------------------------------------------------------------------------- validate_payload

def test_validate_ok(cfg, short_task):
    rep = m.validate_payload(make_payload(short_task), short_task, cfg)
    assert rep.ok, rep.errors
    assert all(rep.checks[c] for c in m.BLOCKING_CHECKS)
    assert rep.details["n_hours"]["expected"] == 72
    assert rep.details["cell"]["distance_km"] < 15


def test_validate_null_variable_like_era5_land(cfg, short_task):
    """Reproduce el fallo real del 2 nov: precipitación, viento y ET0 100 % nulos."""
    n = short_task.segment.n_hours
    p = make_payload(short_task, precipitation=[None] * n, wind_speed_10m=[None] * n,
                     et0_fao_evapotranspiration=[None] * n)
    rep = m.validate_payload(p, short_task, cfg)
    assert not rep.ok and rep.checks["V-M4"] is False
    assert rep.details["nulls"]["precipitation"] == n


def test_validate_wrong_length_and_gap(cfg, short_task):
    p = make_payload(short_task)
    for k in p["hourly"]:
        p["hourly"][k] = p["hourly"][k][:-1]                       # falta la última hora
    rep = m.validate_payload(p, short_task, cfg)
    assert rep.checks["V-M2"] is False

    p = make_payload(short_task)
    p["hourly"]["time"][10] = p["hourly"]["time"][9]               # hora duplicada
    rep = m.validate_payload(p, short_task, cfg)
    assert rep.checks["V-M3"] is False


def test_validate_units_and_timezone(cfg, short_task):
    p = make_payload(short_task, units={"time": "iso8601", **m.EXPECTED_UNITS, "wind_speed_10m": "km/h"})
    assert m.validate_payload(p, short_task, cfg).checks["V-M7"] is False
    p = make_payload(short_task)
    p["utc_offset_seconds"] = 0
    assert m.validate_payload(p, short_task, cfg).checks["V-M7"] is False


def test_validate_api_error_and_missing_keys(cfg, short_task):
    rep = m.validate_payload({"error": True, "reason": "Cannot initialize WeatherVariable"}, short_task, cfg)
    assert rep.checks == {"V-M0": False} and "Cannot initialize" in rep.errors[0]
    p = make_payload(short_task)
    del p["hourly"]["et0_fao_evapotranspiration"]
    assert m.validate_payload(p, short_task, cfg).checks["V-M0"] is False


def test_out_of_range_is_not_blocking(cfg, short_task):
    n = short_task.segment.n_hours
    rep = m.validate_payload(make_payload(short_task, relative_humidity_2m=[101] * n), short_task, cfg)
    assert rep.ok and rep.checks["V-M5"] is False and rep.details["out_of_range"]["relative_humidity_2m"] == n


# ----------------------------------------------------------------------------- process_task (E2E sin red)

def test_process_task_ok_writes_original_bytes(cfg, short_task):
    body = json.dumps(make_payload(short_task), ensure_ascii=False).encode("utf-8")
    session = FakeSession(200, body)
    res = m.process_task(session, short_task, cfg)
    assert res.status is m.TaskStatus.OK, res.error
    assert res.path == cfg.raw_dir / "15001.json"
    assert res.path.read_bytes() == body                           # D-M3: bytes exactos
    assert res.sha256 == m.sha256_bytes(body) and res.bytes == len(body)
    assert not list(cfg.raw_dir.glob("*.part"))                    # escritura atómica
    url, params, timeout = session.calls[0]
    assert params["models"] == "era5_seamless" and params["wind_speed_unit"] == "ms"
    assert timeout == (cfg.timeout_connect_s, cfg.timeout_read_s)


def test_content_hash_is_reproducible_despite_generationtime(cfg, short_task):
    p1 = make_payload(short_task)
    p2 = copy.deepcopy(p1); p2["generationtime_ms"] = 999.9
    r1 = m.process_task(FakeSession(200, json.dumps(p1).encode()), short_task, cfg)
    r2 = m.process_task(FakeSession(200, json.dumps(p2).encode()), short_task, cfg)
    assert r1.sha256 != r2.sha256                                  # bytes distintos…
    assert r1.content_sha256 == r2.content_sha256                  # …contenido idéntico (D-M7)


def test_process_task_invalid_goes_to_rejected(cfg, short_task):
    n = short_task.segment.n_hours
    body = json.dumps(make_payload(short_task, precipitation=[None] * n)).encode()
    res = m.process_task(FakeSession(200, body), short_task, cfg)
    assert res.status is m.TaskStatus.INVALID
    assert res.path.parent.name == "_rejected" and res.path.exists()
    assert not (cfg.raw_dir / "15001.json").exists()               # no contamina la caché


def test_process_task_http_errors(cfg, short_task):
    with pytest.raises(m.FatalRequestError, match="Cannot initialize"):
        m.process_task(FakeSession(400, b'{"error":true,"reason":"Cannot initialize WeatherVariable"}'),
                       short_task, cfg)
    with pytest.raises(m.DailyQuotaExceeded):
        m.process_task(FakeSession(429, b'{"error":true,"reason":"Daily API request limit exceeded"}'),
                       short_task, cfg)
    res = m.process_task(FakeSession(503, b"Service Unavailable"), short_task, cfg)
    assert res.status is m.TaskStatus.FAILED and "503" in res.error
