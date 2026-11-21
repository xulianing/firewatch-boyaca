"""
Tests sin red ni GIS de firewatch.parallel.cleaning (solo pandas/numpy).

Contiene también el GENERADOR de FIRMS sintético que reutilizan test_runner.py y test_spatial_join.py.
Reproduce la estructura REAL de rutas (rutas.raw_firms con el CSV anual; rutas.raw_firms_2026 con
archive/ y nrt/) y los encabezados reales: el CSV anual usa bright_ti4/bright_ti5 y Archive Download usa
brightness/bright_t31, con instrument=SNPP y satellite=SNPP en S-NPP.

Ventana corta 2025-11-01 → 2026-02-28, dos satélites, SP anual 2025 + Archive 2026 (SP enero, NRT con
solapamiento en la última semana de enero), un foco industrial en Nobsa y casos borde para cada regla.
"""

from __future__ import annotations

import copy
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from firewatch.ingest.dane import CheckLog
from firewatch.parallel import cleaning as cl

NOBSA = (5.7700, -72.9400)
HEADER_SP = ["latitude", "longitude", "bright_ti4", "scan", "track", "acq_date", "acq_time", "satellite",
             "instrument", "confidence", "version", "bright_ti5", "frp", "daynight", "type"]
HEADER_ARCHIVE = [{"bright_ti4": "brightness", "bright_ti5": "bright_t31"}.get(c, c) for c in HEADER_SP]
HEADER_NRT = [c for c in HEADER_ARCHIVE if c != "type"]


# =============================================================================
# Generador sintético
# =============================================================================

def row(lat, lon, d, t="0630", sat="N", conf="n", frp=5.0, ti4=330.0, typ=0, version="2.0", dn="N", inst="VIIRS"):
    return {"latitude": lat, "longitude": lon, "bright_ti4": ti4, "scan": 0.4, "track": 0.4, "acq_date": d,
            "acq_time": t, "satellite": sat, "instrument": inst, "confidence": conf, "version": version,
            "bright_ti5": 290.0, "frp": frp, "daynight": dn, "type": typ}


def veg_rows(rng, n, days, sat, version="2.0", conf_choices=("n", "h"), with_type=True, inst="VIIRS"):
    out = []
    for _ in range(n):
        d = days[rng.integers(len(days))]
        r = row(round(float(rng.uniform(4.6, 7.0)), 5), round(float(rng.uniform(-74.5, -72.2)), 5),
                d.strftime("%Y-%m-%d"), t=str(rng.choice(["0630", "1824", "0712", "1806"])), sat=sat,
                conf=str(rng.choice(conf_choices)), frp=round(float(rng.uniform(0.5, 40)), 2), version=version,
                inst=inst)
        if not with_type:
            r.pop("type")
        out.append(r)
    return out


def static_rows(days, sat, version="2.0", with_type=True, typ=2, inst="VIIRS"):
    out = []
    for k, d in enumerate(days):
        jitter = (k % 3) * 0.0004
        r = row(round(NOBSA[0] + jitter, 5), round(NOBSA[1] - jitter, 5), d.strftime("%Y-%m-%d"),
                t="1830", sat=sat, conf="h", frp=25.0, typ=typ, version=version, dn="D", inst=inst)
        if not with_type:
            r.pop("type")
        out.append(r)
    return out


def write_csv(path: Path, rows: list[dict], header: list[str]) -> None:
    """Escribe con el encabezado del producto: renombra bright_ti4/5 si el encabezado es de Archive."""
    path.parent.mkdir(parents=True, exist_ok=True)
    ren = {"bright_ti4": "brightness", "bright_ti5": "bright_t31"} if "brightness" in header else {}
    pd.DataFrame(rows).rename(columns=ren).reindex(columns=header).to_csv(path, index=False)


def build_synthetic_firms(cfg: cl.FirmsConfig, seed: int = 7, overrides: dict | None = None) -> dict:
    """
    Escribe el árbol raw de FIRMS sintético en las rutas de cfg y devuelve los casos borde esperados.
    overrides: {clave_archivo: list[dict]} para inyectar fallos (clave = "anual/…", "archive/…" o "nrt/…").
    """
    rng = np.random.default_rng(seed)
    nov_dec = list(pd.date_range("2025-11-01", "2025-12-31"))
    oct25 = list(pd.date_range("2025-10-01", "2025-10-31"))
    jan = list(pd.date_range("2026-01-01", "2026-01-31"))
    jan_late = list(pd.date_range("2026-01-25", "2026-01-31"))
    feb = list(pd.date_range("2026-02-01", "2026-02-28"))

    snpp25 = veg_rows(rng, 300, nov_dec, "N") + veg_rows(rng, 20, oct25, "N") + static_rows(nov_dec[::5], "N")
    snpp25 += [
        row(5.1, -73.1, "2025-11-15", sat="N", conf="l"),                    # F6 confianza baja
        row(5.2, -73.2, "2025-11-16", sat="N", frp=0.0),                     # F3 frp ≤ frp_minimo
        row(5.3, -73.3, "2025-11-17", sat="N", t="2575"),                    # F3 hora inválida
        row(20.0, -73.0, "2025-11-19", sat="N"),                             # F2 fuera de Colombia
        row(5.5, -73.5, "2025-11-20", sat="N", conf="n", frp=10.0),          # F7 par duplicado:
        row(5.5, -73.5, "2025-11-20", sat="N", conf="h", frp=3.0),           #   gana la 'h'
        row(4.9, -73.6, "2025-12-02", sat="N", typ=1, conf="h"),             # volcán (type 1) → F5 descarta
        row(5.0, -73.7, "2025-12-03", sat="N", ti4=500.0),                   # brillo raro: NO se descarta
    ]
    noaa25 = veg_rows(rng, 300, nov_dec, "N20") + static_rows(nov_dec[2::5], "N20")
    arch_sp_n = (veg_rows(rng, 120, jan, "SNPP", inst="SNPP")
                 + static_rows(jan[::4], "SNPP", inst="SNPP"))               # Archive: SNPP/SNPP (real)
    arch_sp_n20 = veg_rows(rng, 120, jan, "N20")
    nrt_n = (veg_rows(rng, 40, jan_late, "SNPP", version="2.0NRT", with_type=False, inst="SNPP")
             + veg_rows(rng, 150, feb, "SNPP", version="2.0NRT", conf_choices=("n", "h", "l"), with_type=False,
                        inst="SNPP")
             + static_rows(feb[::3], "SNPP", version="2.0NRT", with_type=False, inst="SNPP"))
    nrt_n20 = veg_rows(rng, 150, feb, "N20", version="2.0NRT", with_type=False)

    a, b = cfg.raw_firms_dir, cfg.raw_firms_2026_dir
    files = {
        "anual/viirs-snpp_2025_Colombia.csv": (a / "viirs-snpp_2025_Colombia.csv", snpp25, HEADER_SP),
        "anual/viirs-jpss1_2025_Colombia.csv": (a / "viirs-jpss1_2025_Colombia.csv", noaa25, HEADER_SP),
        "archive/fire_archive_SV-C2_1.csv": (b / "archive/fire_archive_SV-C2_1.csv", arch_sp_n, HEADER_ARCHIVE),
        "archive/fire_archive_J1V-C2_2.csv": (b / "archive/fire_archive_J1V-C2_2.csv", arch_sp_n20, HEADER_ARCHIVE),
        "nrt/fire_nrt_SV-C2_1.csv": (b / "nrt/fire_nrt_SV-C2_1.csv", nrt_n, HEADER_NRT),
        "nrt/fire_nrt_J1V-C2_2.csv": (b / "nrt/fire_nrt_J1V-C2_2.csv", nrt_n20, HEADER_NRT),
    }
    for key, rows in (overrides or {}).items():
        path, _, hdr = files[key]
        files[key] = (path, rows, hdr)
    for path, rows, hdr in files.values():
        write_csv(path, rows, hdr)
    return {"n_raw": sum(len(r) for _, r, _ in files.values()), "n_oct": 20, "n_nrt_overlap_n": 40}


BASE_SETTINGS = {
    "ventana": {"fecha_inicio": date(2025, 11, 1), "fecha_fin": date(2026, 2, 28), "anios_linea_base": [2025]},
    "territorio": {"bbox_colombia": {"lon_min": -79.1, "lat_min": -4.3, "lon_max": -66.8, "lat_max": 13.5}},
    "rutas": {"raw_firms": "raw/firms", "raw_firms_2026": "raw/firms_2026", "silver": "silver"},
    "firms": {
        "satelites": ["N", "N20"],
        "filtros": {"type_permitido": 0, "confianza_excluida": ["l"], "frp_minimo": 0.0},
        "mascara_estatica": {"type_estatico": 2, "tamano_celda_grados": 0.005, "anios_fuente": [2025]},
        # umbral_descarte_rango 5 %: el sintético tiene 3 filas inválidas por rango en ~340 (0,9 %) + 1 F2
        "validacion": {"umbral_descarte_rango": 0.05, "ratio_n20_n": [0.3, 3.0], "tol_salto_sp_nrt": 0.30,
                       "sobre_enmascaramiento_max": 0.02,
                       "zona_control": {"lat": [5.6, 5.8], "lon": [-73.0, -72.8]}},
    },
    "paralelo": {"tamano_bloque_filas": 150},
    "calidad": {"max_pct_sin_municipio": 1.0},
}


def settings_for(tmp_path: Path, **over) -> dict:
    """Copia de BASE_SETTINGS; over usa claves con puntos: settings_for(p, **{"firms.satelites": [...]})."""
    s = copy.deepcopy(BASE_SETTINGS)
    for dotted, val in over.items():
        cur = s
        *parents, last = dotted.split(".")
        for p in parents:
            cur = cur[p]
        cur[last] = val
    return s


@pytest.fixture
def synth(tmp_path):
    cfg = cl.load_config(settings_for(tmp_path), base_dir=tmp_path)
    expected = build_synthetic_firms(cfg)
    return cfg, expected


def run_fase0(cfg):
    lg = CheckLog()
    norm, ctx, table, info = cl.fase0(cfg, lg)
    return lg, norm, ctx, table, info


def status_of(lg: CheckLog, cid: str) -> str:
    return [c for c in lg.checks if c.id == cid][-1].status


# =============================================================================
# Configuración y normalización
# =============================================================================

def test_load_config_reads_frozen_settings(tmp_path):
    cfg = cl.load_config(settings_for(tmp_path), base_dir=tmp_path)
    assert cfg.type_permitido == 0 and cfg.type_estatico == 2 and cfg.celda_deg == 0.005
    assert cfg.bbox_lon == (-79.1, -66.8) and cfg.sin_municipio_max == pytest.approx(0.01)
    assert cfg.raw_firms_2026_dir == tmp_path / "raw/firms_2026"


def test_load_config_rejects_noaa21_bad_window_and_missing_key(tmp_path):
    with pytest.raises(cl.FirmsError, match="N y N20"):
        cl.load_config(settings_for(tmp_path, **{"firms.satelites": ["N", "N21"]}), base_dir=tmp_path)
    with pytest.raises(cl.FirmsError, match="anterior"):
        cl.load_config(settings_for(tmp_path, **{"ventana.fecha_fin": date(2025, 1, 1)}), base_dir=tmp_path)
    s = settings_for(tmp_path)
    del s["firms"]["mascara_estatica"]["type_estatico"]
    with pytest.raises(cl.FirmsError, match="type_estatico"):
        cl.load_config(s, base_dir=tmp_path)


def test_normalize_maps_products():
    raw = pd.DataFrame([row(5, -73, "2026-02-01", sat="1", conf="nominal", version="2.0NRT"),
                        row(5, -73, "2026-02-01", sat="SNPP", conf="HIGH"),
                        row(5, -73, "2026-02-01", sat="N21", conf="low", t="645")]).astype({"acq_time": "string"})
    n = cl.normalize(raw.drop(columns=["type"]))
    assert n["satelite"].tolist() == ["N20", "N", "N21"]
    assert n["confianza"].tolist() == ["n", "h", "l"]
    assert n["proceso"].tolist() == ["NRT", "SP", "SP"]
    assert n["hora_utc"].tolist() == [630, 630, 645] and n["tipo"].isna().all()


def test_read_raw_renames_archive_headers(synth):
    cfg, _ = synth
    f = [x for x in cl.inventory_raw(cfg) if x.kind == "archive_sp"][0]
    df = cl.read_raw(f)
    assert {"bright_ti4", "bright_ti5"} <= set(df.columns) and "brightness" not in df.columns
    assert df["bright_ti4"].dtype == "float64" and not cl.validate_schema(f, df)


def test_inventory_rejects_unknown_2026_file(synth):
    cfg, _ = synth
    (cfg.raw_firms_2026_dir / "archive" / "otra_cosa.csv").write_text("a\n1\n")
    with pytest.raises(cl.FirmsError, match="no reconocido"):
        cl.inventory_raw(cfg)


# =============================================================================
# Fase 0
# =============================================================================

def test_fase0_ok_cutoffs_and_mask(synth):
    cfg, _ = synth
    lg, norm, ctx, table, info = run_fase0(cfg)
    assert not lg.failures, [(c.id, c.message) for c in lg.failures]
    assert ctx.cutoffs["N"] == pd.Timestamp("2026-01-31") and ctx.cutoffs["N20"] == pd.Timestamp("2026-01-31")
    assert len(info["archivos"]) == 6
    v13 = [c for c in lg.checks if c.id == "V-C13"][0]
    assert v13.details["cobertura"] == 1.0 and v13.details["celdas_zona_control"] >= 1
    assert status_of(lg, "V-C13c") == "ok"


def test_fase0_vc1_missing_type_in_sp(synth):
    cfg, _ = synth
    rows = veg_rows(np.random.default_rng(1), 5, list(pd.date_range("2026-01-01", "2026-01-31")), "SNPP",
                    with_type=False, inst="SNPP")
    write_csv(cfg.raw_firms_2026_dir / "archive/fire_archive_SV-C2_1.csv", rows, HEADER_NRT)
    lg, _, ctx, _, _ = run_fase0(cfg)
    assert ctx is None and lg.failures[0].id == "V-C1"


@pytest.mark.parametrize("sat, conf, inst, frag", [("N21", "n", "VIIRS", "no permitido"),
                                                   ("N", "85", "VIIRS", "MODIS"),
                                                   ("N20", "n", "SNPP", "no VIIRS")])
def test_fase0_vc2_noaa21_modis_and_snpp_mismatch(synth, sat, conf, inst, frag):
    cfg, _ = synth
    rows = [row(5.0 + k / 100, -73.0, "2026-01-10", sat=sat, conf=conf, inst=inst) for k in range(4)]
    write_csv(cfg.raw_firms_2026_dir / "archive/fire_archive_SV-C2_1.csv", rows, HEADER_ARCHIVE)
    lg, _, ctx, _, _ = run_fase0(cfg)
    assert ctx is None
    v2 = [c for c in lg.checks if c.id == "V-C2"][0]
    assert v2.status == "fail" and frag in v2.message


def test_fase0_vc3_nrt_rows_in_sp_file(synth):
    cfg, _ = synth
    rows = veg_rows(np.random.default_rng(2), 10, list(pd.date_range("2026-01-01", "2026-01-31")), "SNPP",
                    version="2.0NRT", inst="SNPP")
    write_csv(cfg.raw_firms_2026_dir / "archive/fire_archive_SV-C2_1.csv", rows, HEADER_ARCHIVE)
    lg, _, ctx, _, _ = run_fase0(cfg)
    assert ctx is None and status_of(lg, "V-C3") == "fail"


def test_fase0_vc4_missing_month(synth):
    cfg, _ = synth
    write_csv(cfg.raw_firms_2026_dir / "nrt/fire_nrt_J1V-C2_2.csv",
              veg_rows(np.random.default_rng(3), 5, list(pd.date_range("2026-01-28", "2026-01-31")), "N20",
                       version="2.0NRT", with_type=False), HEADER_NRT)       # N20 se queda sin febrero
    lg, _, ctx, _, _ = run_fase0(cfg)
    assert ctx is None and "N20: faltan 1 meses" in [c for c in lg.checks if c.id == "V-C4"][0].message


def test_fase0_vc6_bad_date(synth):
    cfg, _ = synth
    rows = veg_rows(np.random.default_rng(4), 5, list(pd.date_range("2026-01-01", "2026-01-31")), "SNPP",
                    inst="SNPP")
    rows[0]["acq_date"] = "31/01/2026"
    write_csv(cfg.raw_firms_2026_dir / "archive/fire_archive_SV-C2_1.csv", rows, HEADER_ARCHIVE)
    lg, _, ctx, _, _ = run_fase0(cfg)
    assert ctx is None and status_of(lg, "V-C6") == "fail"


def test_fase0_vc13_fails_without_cells_in_control_zone(tmp_path):
    s = settings_for(tmp_path, **{"firms.validacion.zona_control": {"lat": [0.0, 0.1], "lon": [-70.0, -69.9]}})
    cfg = cl.load_config(s, base_dir=tmp_path)
    build_synthetic_firms(cfg)
    lg, _, ctx, _, _ = run_fase0(cfg)
    assert ctx is None and status_of(lg, "V-C13") == "fail"


# =============================================================================
# Máscara (política congelada: type == 2, sin mínimo ni dilatación)
# =============================================================================

def test_cell_ids_roundtrip():
    lat = np.array([5.7712, -4.2, 13.5]); lon = np.array([-72.9401, -69.9, -78.7])
    i, j = cl.decode_cells(cl.cell_ids(lat, lon, 0.005))
    assert (i == np.floor(lat / 0.005)).all() and (j == np.floor(lon / 0.005)).all()


def test_build_static_mask_type2_only_no_min_no_dilation(tmp_path):
    cfg = cl.load_config(settings_for(tmp_path), base_dir=tmp_path)
    raw = pd.DataFrame([row(5.7701, -72.9401, "2025-11-01", typ=2), row(5.7702, -72.9402, "2025-11-02", typ=2),
                        row(6.5012, -73.5012, "2025-11-03", typ=2),             # 1 sola detección: entra igual
                        row(4.9012, -73.6012, "2025-11-04", typ=1),             # volcán: no forma máscara
                        row(4.8012, -73.7012, "2025-11-05", typ=3),             # costa afuera: no forma máscara
                        row(4.7012, -73.8012, "2024-11-05", typ=2)])            # fuera de anios_fuente
    cells, table = cl.build_static_mask(cl.normalize(raw.astype({"acq_time": "string"})), cfg)
    assert len(cells) == 2 and len(table) == 2                                  # sin vecinos 3×3
    assert sorted(table["n_det"].tolist()) == [1, 2]


# =============================================================================
# Fase 1: reglas
# =============================================================================

@pytest.fixture
def ctx_norm(synth):
    cfg, expected = synth
    lg, norm, ctx, table, _ = run_fase0(cfg)
    assert ctx is not None, [(c.id, c.message) for c in lg.failures]
    return cfg, norm, ctx, expected


def test_clean_chunk_rules_and_conservation(ctx_norm):
    cfg, norm, ctx, expected = ctx_norm
    out, m = cl.clean_chunk(norm, ctx)
    d = m.descartes_por_regla()
    assert d["F1_ventana"] == expected["n_oct"]
    assert d["F2_bbox"] == 1 and d["F3_rango"] == 2
    assert d["F4_corte_sp_nrt"] == expected["n_nrt_overlap_n"]
    assert d["F5_tipo"] > 0 and d["F6_confianza"] >= 1 and d["F7_duplicado"] >= 1
    assert sum(m.raw.values()) == expected["n_raw"] == sum(d.values()) + len(out)
    assert list(out.columns)[:-1] == list(cl.LIMPIAS_COLUMNS)
    assert set(out["confianza"]) <= {"n", "h"} and set(out["satelite"]) == {"N", "N20"}


def test_f3_does_not_filter_on_brightness(ctx_norm):
    _, norm, ctx, _ = ctx_norm
    out, _ = cl.clean_chunk(norm, ctx)
    assert ((out["lat"] == 5.0) & (out["lon"] == -73.7)).sum() == 1             # bright_ti4 = 500 se conserva


def test_f4_keeps_nrt_after_cutoff_only(ctx_norm):
    _, norm, ctx, _ = ctx_norm
    out, _ = cl.clean_chunk(norm, ctx)
    nrt = out[out["proceso"] == "NRT"]
    assert len(nrt) and (nrt["fecha"] > pd.Timestamp("2026-01-31")).all()


def test_f5_drops_sp_type_not_zero_and_keeps_nrt(ctx_norm):
    _, norm, ctx, _ = ctx_norm
    out, m = cl.clean_chunk(norm, ctx)
    assert ((out["lat"] == 4.9) & (out["lon"] == -73.6)).sum() == 0             # volcán SP type 1
    n_sp_no0 = int(((norm["proceso"] == "SP") & (norm["tipo"] != 0)).sum())
    assert m.descartes_por_regla()["F5_tipo"] <= n_sp_no0                       # algunas caen antes por F1–F4
    assert (out["proceso"] == "NRT").any()                                     # NRT sin type no se filtra por F5


def test_f7_tie_break_prefers_high_confidence(ctx_norm):
    _, norm, ctx, _ = ctx_norm
    out, _ = cl.clean_chunk(norm, ctx)
    dup = out[(out["lat"] == 5.5) & (out["lon"] == -73.5)]
    assert len(dup) == 1 and dup["confianza"].iloc[0] == "h" and dup["frp"].iloc[0] == 3.0


def test_f8_mask_flags_nrt_static_without_dropping(ctx_norm):
    _, norm, ctx, _ = ctx_norm
    out, _ = cl.clean_chunk(norm, ctx)
    near = (out["lat"].sub(NOBSA[0]).abs() < 0.002) & (out["lon"].sub(NOBSA[1]).abs() < 0.002)
    assert out.loc[near, "fuente_estatica"].all()
    assert out.loc[near & (out["proceso"] == "NRT")].shape[0] > 0            # NRT sin type marcadas por la máscara
    assert out.loc[~near, "fuente_estatica"].mean() < 0.05


def test_id_det_is_deterministic_and_unique(ctx_norm):
    _, norm, ctx, _ = ctx_norm
    a, _ = cl.clean_chunk(norm, ctx)
    b, _ = cl.clean_chunk(norm.sample(frac=1, random_state=3), ctx)            # otro orden de entrada
    assert a["id_det"].is_unique and a["id_det"].str.fullmatch(r"[0-9a-f]{16}").all()
    assert cl.content_hash(a, cl.LIMPIAS_COLUMNS) == cl.content_hash(b, cl.LIMPIAS_COLUMNS)


def test_metrics_are_associative_across_chunks(ctx_norm):
    _, norm, ctx, _ = ctx_norm
    whole, m_whole = cl.clean_chunk(norm, ctx)
    parts, metrics = [], []
    for _, g in norm.groupby(["satelite", "fecha"]):                         # bloques que respetan el invariante
        o, m = cl.clean_chunk(g, ctx)
        parts.append(o); metrics.append(m)
    total = sum(metrics)
    assert total.raw == m_whole.raw and total.descartes == m_whole.descartes and total.salida == m_whole.salida
    joined = pd.concat(parts, ignore_index=True)
    assert cl.content_hash(joined, cl.LIMPIAS_COLUMNS) == cl.content_hash(whole, cl.LIMPIAS_COLUMNS)


def test_empty_chunk(ctx_norm):
    _, norm, ctx, _ = ctx_norm
    out, m = cl.clean_chunk(norm.iloc[0:0], ctx)
    assert out.empty and sum(m.raw.values()) == 0


# =============================================================================
# Validación de salida
# =============================================================================

def test_validate_output_ok_and_detects_conservation_break(ctx_norm):
    cfg, norm, ctx, _ = ctx_norm
    out, m = cl.clean_chunk(norm, ctx)
    lg = CheckLog()
    h = cl.validate_output(out, m, cfg, lg)
    assert not lg.failures, [(c.id, c.message) for c in lg.failures]
    assert len(h) == 64 and status_of(lg, "V-C12") in ("ok", "warn")
    lg2 = CheckLog()
    cl.validate_output(out.iloc[1:], m, cfg, lg2)                            # una fila "perdida"
    assert status_of(lg2, "V-C7") == "fail"


def test_validate_output_detects_duplicate_ids_and_domains(ctx_norm):
    cfg, norm, ctx, _ = ctx_norm
    out, m = cl.clean_chunk(norm, ctx)
    bad = out.copy()
    bad.loc[1, "id_det"] = bad.loc[0, "id_det"]
    bad["confianza"] = bad["confianza"].astype(object)
    bad.loc[2, "confianza"] = "l"
    lg = CheckLog()
    cl.validate_output(bad, m, cfg, lg)
    assert status_of(lg, "V-C8") == "fail" and status_of(lg, "V-C9") == "fail"


def test_vc9_allows_null_bright_ti4(ctx_norm):
    cfg, norm, ctx, _ = ctx_norm
    out, m = cl.clean_chunk(norm, ctx)
    out.loc[0, "bright_ti4"] = np.nan
    lg = CheckLog()
    cl.validate_output(out, m, cfg, lg)
    assert status_of(lg, "V-C9") == "ok"


def test_vc10_trips_with_production_threshold(tmp_path):
    """El ~0,9 % de F2+F3 del sintético en snpp_2025 debe bloquear si el umbral baja a 0,5 %."""
    cfg = cl.load_config(settings_for(tmp_path, **{"firms.validacion.umbral_descarte_rango": 0.005}),
                         base_dir=tmp_path)
    build_synthetic_firms(cfg)
    lg0, norm, ctx, _, _ = run_fase0(cfg)
    out, m = cl.clean_chunk(norm, ctx)
    lg = CheckLog()
    cl.validate_output(out, m, cfg, lg)
    assert status_of(lg, "V-C10") == "fail" and "viirs-snpp_2025" in [c for c in lg.checks if c.id == "V-C10"][0].message
