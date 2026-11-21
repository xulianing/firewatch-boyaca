"""
Tests del runner secuencial de L2.

- Nivel 1 (siempre corre): plan de bloques y orquestación completa con DOBLES para el cruce espacial
  y la escritura Parquet → valida flujo, conservación, determinismo y política de no-publicación.
- Nivel 2 (requiere GeoPandas + pyarrow): de punta a punta real → silver/detecciones particionado.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from firewatch.parallel import cleaning as cl
from firewatch.parallel import runner as rn
from test_cleaning import build_synthetic_firms, settings_for


@pytest.fixture
def cfg(tmp_path):
    c = cl.load_config(settings_for(tmp_path), base_dir=tmp_path)
    build_synthetic_firms(c)
    return c


def fake_loader(path):
    return "POLIGONOS"


def fake_join(df, polys):
    """Boyacá ≈ caja lat 4.6–7.0 / lon −74.5…−72.2 → todo cae en 15001 o 15176; fuera → 05001."""
    out = df.copy()
    boy = df["lat"].between(4.6, 7.0) & df["lon"].between(-74.5, -72.2)
    out["cod_mpio"] = np.where(boy, np.where(df["lon"] < -73.3, "15176", "15001"), "05001")
    out["cod_mpio"] = out["cod_mpio"].astype("string")
    out["cod_dpto"] = out["cod_mpio"].str[:2].astype("string")
    return out


@pytest.fixture
def captured(monkeypatch):
    """Sustituye la escritura Parquet (no hay pyarrow en todos los entornos) y registra lo publicado."""
    store: dict = {}
    monkeypatch.setattr(rn, "write_detecciones", lambda df, root: store.__setitem__("detecciones", df.copy()))
    monkeypatch.setattr(rn, "_write_parquet", lambda df, dest: store.__setitem__("mascara", df.copy()))
    return store


# ----------------------------------------------------------------------------- plan

def test_plan_chunks_respects_day_invariant_and_lpt(cfg):
    from firewatch.ingest.dane import CheckLog
    norm, ctx, _, _ = cl.fase0(cfg, CheckLog())
    chunks = rn.plan_chunks(norm, target_rows=37)
    allidx = np.concatenate(chunks)
    assert len(allidx) == len(norm) and len(np.unique(allidx)) == len(norm)       # cada fila exactamente una vez
    owner = {}
    for k, idx in enumerate(chunks):
        for key in set(zip(norm.iloc[idx]["satelite"], norm.iloc[idx]["fecha"])):
            assert key not in owner, f"{key} repartido entre bloques {owner[key]} y {k}"
            owner[key] = k
    sizes = [len(c) for c in chunks]
    assert sizes == sorted(sizes, reverse=True)                                    # LPT


# ----------------------------------------------------------------------------- orquestación con dobles

def test_run_sequential_with_doubles(cfg, captured):
    rep = rn.run(cfg, polygons_loader=fake_loader, joiner=fake_join)
    assert rep["status"] in ("completo",), [(c["id"], c["message"]) for c in rep["checks"] if c["status"] == "fail"]
    det = captured["detecciones"]
    assert set(rn.SILVER_COLUMNS) <= set(det.columns)
    assert det["id_det"].is_monotonic_increasing                                    # salida ordenada
    assert rep["resumen"]["n_salida"] == len(det) == rep["metricas"]["n_salida"]
    assert rep["resumen"]["n_fuente_estatica"] > 0 and rep["resumen"]["n_boyaca_vegetacion"] > 0
    assert 0 < rep["tiempos"]["fraccion_serial"] < 1
    on_disk = json.loads(cfg.out_report.read_text(encoding="utf-8"))
    assert on_disk["content_sha256"] == rep["content_sha256"]


def test_output_independent_of_chunk_size(cfg, captured):
    """V-C14 en su forma secuencial: el tamaño de bloque no puede cambiar el resultado."""
    a = rn.run(cfg, polygons_loader=fake_loader, joiner=fake_join, bloque_filas=25)["content_sha256"]
    b = rn.run(cfg, polygons_loader=fake_loader, joiner=fake_join, bloque_filas=100_000)["content_sha256"]
    assert a == b


def test_run_does_not_publish_when_join_fails_vsj2(cfg, captured):
    def bad_join(df, polys):
        out = fake_join(df, polys)
        out.loc[out.index[: len(out) // 2], ["cod_mpio", "cod_dpto"]] = pd.NA      # 50 % sin municipio
        return out
    with pytest.raises(rn.RunnerValidationError, match="V-SJ2"):
        rn.run(cfg, polygons_loader=fake_loader, joiner=bad_join)
    assert captured == {}                                                          # D-G7: nada publicado
    assert json.loads(cfg.out_report.read_text(encoding="utf-8"))["status"] == "fallido"


def test_run_stops_after_fase0_failure(tmp_path, captured):
    c = cl.load_config(settings_for(tmp_path), base_dir=tmp_path)
    build_synthetic_firms(c, overrides={
        "archive/fire_archive_SV-C2_1.csv": [
            {"latitude": 5.0, "longitude": -73.0, "bright_ti4": 330, "scan": 0.4, "track": 0.4,
             "acq_date": "2026-01-05", "acq_time": "0630", "satellite": "N21", "instrument": "VIIRS",
             "confidence": "n", "version": "2.0", "bright_ti5": 290, "frp": 5, "daynight": "N", "type": 0}]})
    with pytest.raises(rn.RunnerValidationError, match="V-C2"):
        rn.run(c, polygons_loader=fake_loader, joiner=fake_join)
    assert captured == {}


def test_solo_fase0_writes_only_the_fase0_report(cfg, captured):
    rep = rn.run(cfg, solo_fase0=True, polygons_loader=fake_loader, joiner=fake_join)
    assert rep["status"] == "fase0_ok" and rep["plan"]["n_bloques"] >= 1
    assert captured == {} and (cfg.out_report.parent / "cleaning_fase0.json").exists()


def test_ejecutar_silver_rejects_parallel_until_integrated():
    with pytest.raises(NotImplementedError):
        rn.ejecutar_silver(4)


def test_main_config_error_exit_2(tmp_path):
    bad = tmp_path / "config" / "settings.yaml"
    bad.parent.mkdir(parents=True)
    bad.write_text("rutas: {}\n", encoding="utf-8")                                 # sin 'ventana'
    assert rn.main(["--settings", str(bad)]) == 2


# ----------------------------------------------------------------------------- de punta a punta real

def test_end_to_end_real_join_and_parquet(cfg):
    gpd = pytest.importorskip("geopandas")
    pytest.importorskip("pyarrow")
    import pyarrow.dataset as ds
    from shapely.geometry import box

    polys = gpd.GeoDataFrame(
        {"cod_mpio": ["15001", "15176", "05001"], "cod_dpto": ["15", "15", "05"]},
        geometry=[box(-73.3, 4.0, -72.0, 7.5), box(-75.0, 4.0, -73.3, 7.5), box(-80.0, -4.5, -66.0, 4.0)],
        crs=4326)
    cfg.geo_path.mkdir(parents=True)
    polys.to_parquet(cfg.geo_path / "part-0000.parquet")

    rep = rn.run(cfg)
    assert rep["status"] == "completo", [(c["id"], c["message"]) for c in rep["checks"] if c["status"] == "fail"]
    dset = ds.dataset(cfg.out_detecciones, format="parquet", partitioning="hive")
    tbl = dset.to_table()
    assert tbl.num_rows == rep["resumen"]["n_salida"]
    names = set(tbl.schema.names)
    assert set(rn.SILVER_COLUMNS) <= names
    assert str(tbl.schema.field("fecha").type) == "date32[day]"
    assert sorted(p.name for p in cfg.out_detecciones.iterdir()) == ["anio=2025", "anio=2026"]
    assert cfg.out_mascara.exists()
    rep2 = rn.run(cfg)                                                             # relanzar: idempotente
    assert rep2["content_sha256"] == rep["content_sha256"]
