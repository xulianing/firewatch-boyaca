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

@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Los reintentos y el ritmo usan time.sleep: en los tests se registran pero no se esperan."""
    calls = []
    monkeypatch.setattr(m.time, "sleep", lambda s: calls.append(s))
    return calls


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


# ============================================================================= v0.2

class SeqSession:
    """Devuelve una secuencia de (status, body); el último se repite."""
    def __init__(self, seq):
        self.seq, self.calls = list(seq), []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        status, body = self.seq.pop(0) if len(self.seq) > 1 else self.seq[0]
        return FakeResponse(status, body)

    def close(self):
        pass


class FakeAPI:
    """Simula Open-Meteo: construye un payload válido para las fechas pedidas.
    `plan` mapea latitud pedida → lista de respuestas especiales ('429d', '400', '503', 'nulls')."""
    def __init__(self, plan=None):
        self.plan, self.calls = plan or {}, []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        special = self.plan.get(params["latitude"], [])
        kind = special.pop(0) if special else "ok"
        if kind == "429d":
            return FakeResponse(429, b'{"error":true,"reason":"Daily API request limit exceeded. Please try again tomorrow."}')
        if kind == "400":
            return FakeResponse(400, b'{"error":true,"reason":"Cannot initialize WeatherVariable"}')
        if kind == "503":
            return FakeResponse(503, b"Service Unavailable")
        start, end = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
        task = m.DownloadTask(m.Location("15001", "x", float(params["latitude"]), float(params["longitude"])),
                              m.Segment("base", start, end, ""))
        n = task.segment.n_hours
        payload = make_payload(task, **({"precipitation": [None] * n} if kind == "nulls" else {}))
        payload["latitude"], payload["longitude"] = float(params["latitude"]), float(params["longitude"])
        return FakeResponse(200, json.dumps(payload).encode())

    def close(self):
        pass


LOCS = [("15001", "Tunja", 5.5353, -73.3678), ("15022", "Almeida", 4.9706, -73.3786),
        ("15047", "Aquitania", 5.5186, -72.8833)]
SHORT = m.Segment("base", date(2025, 1, 1), date(2025, 1, 3), "")


@pytest.fixture
def run_env(cfg, monkeypatch):
    write_csv(cfg.centroides_path, LOCS)
    api = FakeAPI()
    monkeypatch.setattr(m, "make_session", lambda c: api)
    return cfg, api


def test_classify_response():
    m.classify_response(200, b"{}")
    with pytest.raises(m.DailyQuotaExceeded):
        m.classify_response(429, b'{"error":true,"reason":"Daily API request limit exceeded"}')
    with pytest.raises(m.TransientRequestError):
        m.classify_response(429, b'{"error":true,"reason":"Minutely API request limit exceeded"}')
    with pytest.raises(m.TransientRequestError):
        m.classify_response(502, b"Bad Gateway")
    for st in (400, 403, 404):
        with pytest.raises(m.FatalRequestError):
            m.classify_response(st, b'{"error":true,"reason":"x"}')


def test_rate_limiter(monkeypatch, no_sleep):
    # lecturas del reloj: wait1→100 · wait2→110 (+ tras dormir 160) · wait3→200 (+ tras dormir 220)
    clock = iter([100.0, 110.0, 160.0, 200.0, 220.0])
    monkeypatch.setattr(m.time, "monotonic", lambda: next(clock))
    rl = m.RateLimiter(60)
    rl.wait()            # primera petición: no espera
    rl.wait()            # 10 s después de la anterior → duerme 50
    rl.wait()            # 40 s después de la anterior (160) → duerme 20
    assert no_sleep == [50.0, 20.0]


def test_fetch_with_retry_backoff_then_ok(cfg, short_task, no_sleep):
    body = json.dumps(make_payload(short_task)).encode()
    s = SeqSession([(503, b"x"), (429, b'{"reason":"Minutely API request limit exceeded"}'), (200, body)])
    status, got, attempts = m.fetch_with_retry(s, short_task, cfg)
    assert (status, attempts, got) == (200, 3, body)
    assert 30 <= no_sleep[0] <= 33 and 60 <= no_sleep[1] <= 66          # backoff 30, 60 (+ jitter ≤10 %)


def test_fetch_with_retry_exhausted(cfg, short_task):
    with pytest.raises(m.TransientRequestError) as ei:
        m.fetch_with_retry(SeqSession([(503, b"down")]), short_task, cfg)
    assert ei.value.attempts == cfg.max_attempts and ei.value.http_status == 503


def test_manifest_roundtrip_and_truncated_line(cfg, short_task):
    res = m.process_task(FakeSession(200, json.dumps(make_payload(short_task)).encode()), short_task, cfg)
    m.append_manifest(cfg, res, res.fingerprint)
    with open(cfg.manifest_path, "a", encoding="utf-8") as fh:
        fh.write('{"cod_mpio": "15001", "seg')                         # corte de energía
    entries = m.read_manifest(cfg)
    assert len(entries) == 1 and entries[0]["status"] == "ok" and entries[0]["cell_lat"] == 5.5
    state = m.latest_entries(entries)
    assert m.is_cached(short_task, cfg, state)
    res.path.write_bytes(res.path.read_bytes() + b" ")                 # archivo alterado → no es caché
    assert not m.is_cached(short_task, cfg, state)


def test_is_cached_rejects_changed_fingerprint(cfg, short_task):
    res = m.process_task(FakeSession(200, json.dumps(make_payload(short_task)).encode()), short_task, cfg)
    m.append_manifest(cfg, res, res.fingerprint)
    moved = m.DownloadTask(m.Location("15001", "Tunja", 5.5354, -73.3678), short_task.segment)
    assert not m.is_cached(moved, cfg, m.latest_entries(m.read_manifest(cfg)))


def test_run_download_full_then_resume_is_noop(run_env):
    cfg, api = run_env
    s1 = m.run_download(cfg, SHORT, require_complete=False)
    assert s1.by_status.get("ok") == 3 and s1.pending == [] and s1.stop_reason is None
    assert len(api.calls) == 3
    s2 = m.run_download(cfg, SHORT, require_complete=False)             # relanzar = no gastar cuota
    assert s2.by_status == {"cached": 3} and len(api.calls) == 3


def test_run_download_daily_quota_then_resume(run_env):
    cfg, api = run_env
    api.plan = {"4.9706": ["429d"]}                                    # Almeida choca con la cuota diaria
    s1 = m.run_download(cfg, SHORT, require_complete=False)
    assert s1.stop_reason == "quota" and s1.by_status.get("ok") == 1
    assert s1.pending == ["15022", "15047"]
    s2 = m.run_download(cfg, SHORT, require_complete=False)             # "mañana"
    assert s2.by_status.get("cached") == 1 and s2.by_status.get("ok") == 2 and not s2.pending
    assert [c["latitude"] for c in api.calls] == ["5.5353", "4.9706", "4.9706", "5.5186"]


def test_run_download_fatal_aborts(run_env):
    cfg, api = run_env
    api.plan = {"5.5353": ["400"]}
    s = m.run_download(cfg, SHORT, require_complete=False)
    assert s.stop_reason == "fatal" and len(api.calls) == 1 and len(s.pending) == 3


def test_run_download_invalid_and_consecutive_failures(run_env):
    cfg, api = run_env
    api.plan = {"5.5353": ["nulls"], "4.9706": ["503"] * 9, "5.5186": ["503"] * 9}
    s = m.run_download(cfg, SHORT, require_complete=False, max_consecutive_failures=3)
    assert s.by_status.get("invalid") == 1 and s.by_status.get("failed") == 2
    assert s.stop_reason == "consecutive_failures" and s.pending == ["15001", "15022", "15047"]
    assert list((cfg.raw_dir / "_rejected").glob("15001_base_*.json"))


def test_run_download_dry_run_and_limit(run_env):
    cfg, api = run_env
    s = m.run_download(cfg, SHORT, require_complete=False, dry_run=True)
    assert s.by_status.get("planned") == 3 and api.calls == [] and s.planned_weight == 3.0
    s = m.run_download(cfg, SHORT, require_complete=False, limit=1)
    assert s.by_status.get("ok") == 1 and len(api.calls) == 1


def test_run_download_only_unknown_code(run_env):
    cfg, _ = run_env
    with pytest.raises(m.MeteoError, match="15999"):
        m.run_download(cfg, SHORT, require_complete=False, only=["15999"])


def test_run_download_refuses_unpublished_segment(run_env):
    cfg, _ = run_env
    future = m.Segment("cola", date.today() - timedelta(days=2), date.today(), "_cola")
    with pytest.raises(m.MeteoError, match="ERA5 no estará"):
        m.run_download(cfg, future, require_complete=False)
