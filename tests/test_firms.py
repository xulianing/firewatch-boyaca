"""
Tests sin red de firewatch.ingest.firms.

Los encabezados y las filas de ejemplo son LITERALES de los CSV recibidos de FIRMS (noviembre de 2026):
el CSV anual por país usa bright_ti4/bright_ti5, mientras que Archive Download usa brightness/bright_t31
y escribe instrument=SNPP en los archivos de S-NPP.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from firewatch.ingest import firms

HEADER_ANUAL = ("latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,"
                "confidence,version,bright_ti5,frp,daynight,type")
HEADER_ARCHIVE = ("latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,"
                  "confidence,version,bright_t31,frp,daynight,type")
HEADER_NRT = ("latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,"
              "confidence,version,bright_t31,frp,daynight")

CSV_REALES = {
    "firms/viirs-jpss1_2022_Colombia.csv":
        (HEADER_ANUAL, "10.94083,-74.84477,307.75,0.42,0.38,2022-01-01,0639,N20,VIIRS,n,2,294.55,0.99,N,0"),
    "firms/viirs-snpp_2022_Colombia.csv":
        (HEADER_ANUAL, "11.1347,-72.58459,314.35,0.6,0.53,2022-01-01,0551,N,VIIRS,n,2,291.78,4.08,N,2"),
    "firms_2026/archive/fire_archive_J1V-C2_43071.csv":
        (HEADER_ARCHIVE, "11.1363,-72.57448,313.49,0.38,0.36,2026-01-01,0622,N20,VIIRS,n,2,294.07,3.95,N,2"),
    "firms_2026/archive/fire_archive_SV-C2_43072.csv":
        (HEADER_ARCHIVE, "10.31877,-75.49037,316.39,0.36,0.57,2026-01-01,0602,SNPP,SNPP,n,2,290.16,2.15,N,2"),
    "firms_2026/nrt/fire_nrt_J1V-C2_43071.csv":
        (HEADER_NRT, "11.08203,-72.6794,307.25,0.47,0.48,2026-08-01,0652,N20,VIIRS,n,2.0NRT,292.48,4.23,N"),
    "firms_2026/nrt/fire_nrt_SV-C2_43072.csv":
        (HEADER_NRT, "11.1471,-72.52658,306.77,0.38,0.36,2026-08-01,0631,SNPP,SNPP,n,2.0NRT,293.7,2.77,N"),
}


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    """Árbol raw con la disposición real, incluido el Readme que FIRMS entrega junto a los CSV."""
    for rel, (header, fila) in CSV_REALES.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{header}\n{fila}\n", encoding="utf-8")
    readme = tmp_path / "firms_2026" / "readme" / "Readme_43071.txt"
    readme.parent.mkdir()
    readme.write_text("To view Readme.txt please visit FIRMS\n", encoding="utf-8")
    return tmp_path


def listar(raw: Path) -> list[firms.ArchivoRaw]:
    return firms.listar_archivos_raw(raw / "firms", raw / "firms_2026")


# =============================================================================
# Bloqueo 1: rutas reales
# =============================================================================

def test_rutas_raw_del_settings_del_repo_apuntan_a_la_disposicion_real(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    settings = yaml.safe_load((repo / "config" / "settings.yaml").read_text(encoding="utf-8"))
    assert firms.rutas_raw(settings, base_dir=tmp_path) == (tmp_path / "data/raw/firms",
                                                            tmp_path / "data/raw/firms_2026")


def test_rutas_raw_conserva_rutas_absolutas(tmp_path):
    anual, r2026 = tmp_path / "a", tmp_path / "b"
    settings = {"rutas": {"raw_firms": str(anual), "raw_firms_2026": str(r2026)}}
    assert firms.rutas_raw(settings, base_dir=tmp_path / "otra") == (anual, r2026)


@pytest.mark.parametrize("rutas", [{}, {"raw_firms": "x"}, {"raw_firms_2026": "y"}])
def test_rutas_raw_exige_ambas_claves(rutas, tmp_path):
    with pytest.raises(firms.FirmsRawError, match="raw_firms"):
        firms.rutas_raw({"rutas": rutas}, base_dir=tmp_path)


def test_listar_clasifica_la_disposicion_real_en_orden_determinista(raw):
    assert [(a.path.name, a.kind) for a in listar(raw)] == [
        ("viirs-jpss1_2022_Colombia.csv", "sp_anual"),
        ("viirs-snpp_2022_Colombia.csv", "sp_anual"),
        ("fire_archive_J1V-C2_43071.csv", "archive_sp"),
        ("fire_archive_SV-C2_43072.csv", "archive_sp"),
        ("fire_nrt_J1V-C2_43071.csv", "archive_nrt"),
        ("fire_nrt_SV-C2_43072.csv", "archive_nrt"),
    ]


def test_listar_rechaza_csv_desconocido_en_2026(raw):
    (raw / "firms_2026" / "archive" / "otra_cosa.csv").write_text("a\n1\n", encoding="utf-8")
    with pytest.raises(firms.FirmsRawError, match="otra_cosa.csv"):
        listar(raw)


@pytest.mark.parametrize("vaciar", ["firms", "firms_2026"])
def test_listar_falla_si_falta_una_de_las_dos_fuentes(raw, vaciar):
    for csv in (raw / vaciar).rglob("*.csv"):
        csv.unlink()
    with pytest.raises(firms.FirmsRawError, match=vaciar):
        listar(raw)


# =============================================================================
# Bloqueo 2: encabezados reales
# =============================================================================

def test_normalizar_lleva_archive_al_nombre_canonico_sin_tocar_valores(raw):
    df = pd.read_csv(raw / "firms_2026/archive/fire_archive_J1V-C2_43071.csv")
    out = firms.normalizar_encabezados(df)
    assert "brightness" not in out.columns and "bright_t31" not in out.columns
    assert out["bright_ti4"].iloc[0] == 313.49 and out["bright_ti5"].iloc[0] == 294.07


def test_normalizar_no_altera_el_csv_anual(raw):
    df = pd.read_csv(raw / "firms/viirs-snpp_2022_Colombia.csv")
    assert firms.normalizar_encabezados(df).columns.tolist() == HEADER_ANUAL.split(",")


def test_normalizar_rechaza_columna_duplicada_por_alias():
    df = pd.DataFrame({"brightness": [300.0], "bright_ti4": [310.0]})
    with pytest.raises(firms.FirmsRawError, match="brightness"):
        firms.normalizar_encabezados(df)


def test_validar_columnas_acepta_todos_los_archivos_reales(raw):
    for archivo in listar(raw):
        firms.validar_columnas(archivo)


def test_validar_columnas_rechaza_columna_faltante(raw):
    path = raw / "firms_2026/nrt/fire_nrt_SV-C2_43072.csv"
    path.write_text(HEADER_NRT.replace(",frp", "") + "\n", encoding="utf-8")
    with pytest.raises(firms.FirmsRawError, match="frp"):
        firms.validar_columnas(firms.ArchivoRaw(path, "archive_nrt"))


def test_validar_columnas_exige_type_en_sp_pero_no_en_nrt(raw):
    nrt = raw / "firms_2026/nrt/fire_nrt_SV-C2_43072.csv"
    firms.validar_columnas(firms.ArchivoRaw(nrt, "archive_nrt"))
    with pytest.raises(firms.FirmsRawError, match="type"):
        firms.validar_columnas(firms.ArchivoRaw(nrt, "archive_sp"))


# =============================================================================
# Bloqueo 3: instrument = SNPP
# =============================================================================

def test_instrumentos_de_todos_los_archivos_reales_son_validos(raw):
    for archivo in listar(raw):
        assert firms.instrumentos_invalidos(pd.read_csv(archivo.path, dtype=str)) == []


@pytest.mark.parametrize("instrument, satellite, invalidos", [
    ("VIIRS", "N", []),
    ("VIIRS", "N20", []),
    ("SNPP", "SNPP", []),
    (" snpp ", "SNPP", []),
    ("MODIS", "Terra", [("MODIS", "TERRA")]),          # MODIS está fuera del alcance
    ("SNPP", "N20", [("SNPP", "N20")]),                # SNPP solo vale junto a S-NPP
    ("SNPP", "N", [("SNPP", "N")]),                    # esa combinación no existe en FIRMS
])
def test_instrumentos_invalidos(instrument, satellite, invalidos):
    df = pd.DataFrame({"instrument": [instrument], "satellite": [satellite]})
    assert firms.instrumentos_invalidos(df) == invalidos


def test_instrument_vacio_es_invalido():
    df = pd.DataFrame({"instrument": [None], "satellite": ["N"]})
    assert firms.instrumentos_invalidos(df) == [("<NA>", "N")]
