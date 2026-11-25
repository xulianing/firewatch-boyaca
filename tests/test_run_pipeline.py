"""Tests de scripts/run_pipeline.py (B10).

No ejecutan L2 ni Spark: inyectan etapas falsas para probar la orquestación, que es lo que aporta
el script — orden de las etapas, corte en la primera falla, cierre de la sesión, conciliación K1 y
códigos de salida.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml

RAIZ = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("run_pipeline", RAIZ / "scripts" / "run_pipeline.py")
rp = importlib.util.module_from_spec(_spec)
sys.modules["run_pipeline"] = rp          # dataclasses resuelven tipos vía sys.modules
_spec.loader.exec_module(rp)


class SparkFalso:
    def __init__(self):
        self.detenido = False
        self.sparkContext = type("SC", (), {"master": "local[2]"})()

    def stop(self):
        self.detenido = True


@pytest.fixture
def settings(tmp_path: Path) -> Path:
    silver = tmp_path / "silver"
    (silver / "_reports").mkdir(parents=True)
    (silver / "_reports" / "cleaning_metrics.json").write_text(
        json.dumps({"resumen": {"n_boyaca_vegetacion": 3908}}), encoding="utf-8")
    ruta = tmp_path / "settings.yaml"
    ruta.write_text(yaml.safe_dump({"paralelo": {"procesos_produccion": 4},
                                    "rutas": {"silver": str(silver)}}), encoding="utf-8")
    return ruta


def silver_ok(cfg, settings_path, n):
    return {"n_raw": 10, "n_silver": 8, "k1_silver": 3908, "procesos": n}


def silver_falla(cfg, settings_path, n):
    raise rp.EtapaFallida("V-SJ2 en FALLA")


def gold_falso(llamadas: list, k1: int = 3908, falla_en: str | None = None):
    def ejecutar_de(nombre):
        def ejecutar(spark, cfg):
            llamadas.append(nombre)
            if nombre == falla_en:
                raise RuntimeError("check bloqueante")
            if nombre == "export_gold":
                return {"filas": {"fact_municipio_mes": 7134}, "kpis_control": {"K1_detecciones": k1}}
            return {"conteos": {"n": 1}}
        return ejecutar
    return lambda cfg: [(n, ejecutar_de(n)) for n in rp.ETAPAS_GOLD]


def test_silver_ok_usa_procesos_de_produccion(settings):
    vistos = {}
    def silver(cfg, sp, n):
        vistos["n"] = n
        return silver_ok(cfg, sp, n)
    assert rp.main(["--stage", "silver", "--config", str(settings)], silver_fn=silver) == 0
    assert vistos["n"] == 4


def test_silver_falla_da_codigo_1_y_no_corre_gold(settings):
    llamadas: list = []
    code = rp.main(["--stage", "all", "--config", str(settings)], silver_fn=silver_falla,
                   gold_fn=gold_falso(llamadas), sesion_fn=lambda cfg: SparkFalso())
    assert code == 1 and llamadas == []


def test_gold_ok_respeta_orden_cierra_sesion_y_concilia(settings, tmp_path):
    llamadas: list = []
    spark = SparkFalso()
    salida = tmp_path / "resumen.json"
    code = rp.main(["--stage", "gold", "--config", str(settings), "--resumen-json", str(salida)],
                   gold_fn=gold_falso(llamadas), sesion_fn=lambda cfg: spark)
    assert code == 0
    assert llamadas == list(rp.ETAPAS_GOLD) and spark.detenido
    conc = json.loads(salida.read_text(encoding="utf-8"))["conciliacion"]
    assert conc == {"evaluada": True, "k1_silver": 3908, "k1_gold": 3908, "ok": True}


def test_gold_se_detiene_en_la_primera_falla_y_cierra_sesion(settings):
    llamadas: list = []
    spark = SparkFalso()
    code = rp.main(["--stage", "gold", "--config", str(settings)],
                   gold_fn=gold_falso(llamadas, falla_en="irat"), sesion_fn=lambda cfg: spark)
    assert code == 1
    assert llamadas == ["build_daily", "climatology", "irat"] and spark.detenido


def test_conciliacion_distinta_da_codigo_1(settings):
    code = rp.main(["--stage", "all", "--config", str(settings)], silver_fn=silver_ok,
                   gold_fn=gold_falso([], k1=4294), sesion_fn=lambda cfg: SparkFalso())
    assert code == 1


def test_sesion_que_no_arranca_da_codigo_1(settings):
    def sin_java(cfg):
        raise RuntimeError("JAVA_HOME is not set")
    code = rp.main(["--stage", "gold", "--config", str(settings)],
                   gold_fn=gold_falso([]), sesion_fn=sin_java)
    assert code == 1


def test_procesos_invalidos_da_codigo_2(settings):
    assert rp.main(["--stage", "silver", "--config", str(settings), "--procesos", "0"],
                   silver_fn=silver_ok) == 2
