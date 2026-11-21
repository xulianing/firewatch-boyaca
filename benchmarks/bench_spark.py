"""Benchmark de Spark (C7): etapa horario → diario de build_daily.

Mide el código de producción (``build_daily.clima_diario``) sobre ``silver.clima_horario``
(5.210.280 filas) variando solo el número de núcleos (``local[N]``) y, opcionalmente,
las particiones de shuffle. Calcula speedup, eficiencia y la fracción serial de Karp–Flatt.

Decisiones de medición:
- AQE desactivado: si no, Spark fusiona particiones y el barrido de particiones mide lo mismo.
- Acción ``write.format("noop")``: fuerza el cálculo completo sin medir la escritura a disco.
- 1 corrida de calentamiento descartada por configuración (JIT de la JVM, caché del SO).
- El arranque de la sesión se mide aparte (no entra en T(p)): es la parte serial fija.
- Dos modos por corrida:
    con_lectura → lee Parquet + agrega (lo que pasa en producción);
    en_memoria  → entrada en caché, solo la agregación (aísla el paralelismo de CPU).

Uso (PowerShell, con (.venv) y HADOOP_HOME):
    python benchmarks/bench_spark.py --repeticiones 1          # prueba rápida (~2 min)
    python benchmarks/bench_spark.py                           # masters × 5 rep (avance)
    python benchmarks/bench_spark.py --barrido-particiones     # + particiones {4,8,16,200}
    python benchmarks/bench_spark.py --pandas                  # + línea base en pandas
    python benchmarks/bench_spark.py --ui                      # deja la Spark UI abierta
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import logging
import os
import platform
import statistics
import time
from datetime import datetime
from pathlib import Path

from firewatch.spark import build_daily as bd
from firewatch.spark.session import cargar_config, crear_sesion

log = logging.getLogger("bench_spark")


def nucleos(master: str) -> int:
    """local[N] → N; local[*] → núcleos lógicos del equipo."""
    dentro = master[master.index("[") + 1: master.index("]")].split(",")[0]
    return os.cpu_count() if dentro == "*" else int(dentro)


def entrada(spark, p: bd.Params):
    h = bd.leer_horario(spark, p).filter(bd._en_ventana("fecha", p))
    return h.select("cod_mpio", "fecha", *bd.VARS_HORARIAS)


def medir(df) -> float:
    t0 = time.perf_counter()
    bd.clima_diario(df).write.format("noop").mode("overwrite").save()
    return time.perf_counter() - t0


def plan_fisico(df) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        bd.clima_diario(df).explain(mode="formatted")
    return buf.getvalue()


def correr_config(cfg, p, master, particiones, reps, filas_ref, salida):
    t0 = time.perf_counter()
    spark = crear_sesion(cfg, master=master, particiones=particiones, aqe=False)
    t_sesion = time.perf_counter() - t0
    paralelismo = spark.sparkContext.defaultParallelism
    filas = []
    try:
        h = entrada(spark, p)
        medir(h)  # calentamiento, descartado
        for r in range(1, reps + 1):
            filas.append(("con_lectura", r, medir(h)))
        hc = h.cache()
        n = hc.count()
        if filas_ref is not None and n != filas_ref:
            raise RuntimeError(f"Filas de entrada {n} ≠ {filas_ref}: no se compara lo mismo")
        medir(hc)  # calentamiento en memoria
        for r in range(1, reps + 1):
            filas.append(("en_memoria", r, medir(hc)))
        hc.unpersist()
    finally:
        spark.stop()
    res = []
    for modo, r, seg in filas:
        fila = {
            "master": master, "p": nucleos(master), "paralelismo_spark": paralelismo,
            "particiones": particiones, "aqe": False, "modo": modo, "rep": r,
            "segundos": round(seg, 4), "filas_entrada": n, "t_sesion_s": round(t_sesion, 3),
        }
        res.append(fila)
        salida.writerow(fila)
    log.info("%-9s part=%-3s · con_lectura med=%.2fs · en_memoria med=%.2fs · sesión=%.1fs",
             master, particiones,
             statistics.median(f["segundos"] for f in res if f["modo"] == "con_lectura"),
             statistics.median(f["segundos"] for f in res if f["modo"] == "en_memoria"),
             t_sesion)
    return res, n


def resumir(filas: list[dict]) -> list[dict]:
    grupos: dict[tuple, list[float]] = {}
    for f in filas:
        grupos.setdefault((f["modo"], f["particiones"], f["p"], f["master"]), []).append(f["segundos"])
    out = []
    for (modo, part, p_, master), ts in sorted(grupos.items()):
        base = [v for (m, pa, pp, _), v in grupos.items() if m == modo and pa == part and pp == 1]
        t1 = statistics.median(base[0]) if base else None
        tp = statistics.median(ts)
        s = t1 / tp if t1 else None
        e = s / p_ if s else None
        kf = ((1 / s - 1 / p_) / (1 - 1 / p_)) if (s and p_ > 1) else None
        out.append({
            "modo": modo, "particiones": part, "master": master, "p": p_, "n_rep": len(ts),
            "t_mediana_s": round(tp, 4), "t_min_s": round(min(ts), 4),
            "t_desv_s": round(statistics.stdev(ts), 4) if len(ts) > 1 else 0.0,
            "speedup": round(s, 3) if s else None, "eficiencia": round(e, 3) if e else None,
            "karp_flatt": round(kf, 4) if kf is not None else None,
            "filas_por_s": round(filas[0]["filas_entrada"] / tp),
        })
    return out


def graficar(resumen: list[dict], part_prod: int, carpeta: Path, sello: str) -> list[Path]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rutas = []
    datos = {m: sorted([r for r in resumen if r["modo"] == m and r["particiones"] == part_prod
                        and r["speedup"]], key=lambda r: r["p"])
             for m in ("con_lectura", "en_memoria")}
    ps = sorted({r["p"] for v in datos.values() for r in v})
    if not ps:
        return rutas
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
    for modo, estilo in (("con_lectura", "o-"), ("en_memoria", "s--")):
        v = datos[modo]
        a1.plot([r["p"] for r in v], [r["speedup"] for r in v], estilo, label=modo)
        a2.plot([r["p"] for r in v], [r["eficiencia"] for r in v], estilo, label=modo)
    a1.plot(ps, ps, ":", color="gray", label="ideal (S = p)")
    a2.axhline(1.0, ls=":", color="gray", label="ideal (E = 1)")
    for a, t, y in ((a1, "Speedup S(p) = T(1) / T(p)", "Speedup"),
                    (a2, "Eficiencia E(p) = S(p) / p", "Eficiencia")):
        a.set_title(t)
        a.set_xlabel("Núcleos (p)")
        a.set_ylabel(y)
        a.set_xticks(ps)
        a.grid(alpha=0.3)
        a.legend()
    fig.suptitle(f"Spark local · build_daily (horario → diario, 5,21 M filas) · {part_prod} particiones · AQE off")
    fig.tight_layout()
    r = carpeta / f"spark_speedup_eficiencia_{sello}.png"
    fig.savefig(r, dpi=150)
    plt.close(fig)
    rutas.append(r)

    barrido = sorted([x for x in resumen if x["modo"] == "en_memoria" and x["p"] == max(ps)],
                     key=lambda x: x["particiones"])
    if len(barrido) > 1:
        fig, a = plt.subplots(figsize=(6, 4))
        a.bar([str(x["particiones"]) for x in barrido], [x["t_mediana_s"] for x in barrido])
        a.set_xlabel("spark.sql.shuffle.partitions")
        a.set_ylabel("Tiempo mediano (s)")
        a.set_title(f"Efecto de las particiones · {barrido[0]['master']} · en memoria")
        a.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        r = carpeta / f"spark_particiones_{sello}.png"
        fig.savefig(r, dpi=150)
        plt.close(fig)
        rutas.append(r)
    return rutas


def linea_base_pandas(p: bd.Params, reps: int) -> list[dict]:
    import pandas as pd

    cols = ["cod_mpio", "fecha", *bd.VARS_HORARIAS]
    out = []
    for r in range(1, reps + 1):
        t0 = time.perf_counter()
        df = pd.read_parquet(p.silver / "clima_horario", columns=cols)
        t_lect = time.perf_counter() - t0
        t1 = time.perf_counter()
        d = df.groupby(["cod_mpio", "fecha"]).agg(
            tmax=("temp", "max"), tmean=("temp", "mean"), precip=("precip", "sum"),
            rh_min=("rh", "min"), wind_max=("wind", "max"), et0=("et0", "sum"),
            n_horas=("temp", "size"))
        t_agg = time.perf_counter() - t1
        out.append({"rep": r, "filas_entrada": len(df), "filas_salida": len(d),
                    "t_lectura_s": round(t_lect, 4), "t_agregacion_s": round(t_agg, 4),
                    "t_total_s": round(t_lect + t_agg, 4)})
        log.info("pandas rep %d · lectura %.2fs · agregación %.2fs", r, t_lect, t_agg)
    return out


def entorno(cfg) -> dict:
    spark = crear_sesion(cfg, master="local[1]", aqe=False)
    try:
        jvm = spark.sparkContext._jvm
        info = {
            "fecha": datetime.now().isoformat(timespec="seconds"),
            "sistema": platform.platform(), "procesador": platform.processor(),
            "nucleos_logicos": os.cpu_count(), "python": platform.python_version(),
            "spark": spark.version, "java": jvm.java.lang.System.getProperty("java.version"),
            "driver_memory": cfg["spark"]["driver_memory"],
        }
    finally:
        spark.stop()
    return info


def main() -> None:
    ap = argparse.ArgumentParser(description="FireWatch · benchmark de Spark (C7)")
    ap.add_argument("--config", default="config/settings.yaml")
    ap.add_argument("--repeticiones", type=int, default=None)
    ap.add_argument("--masters", nargs="*", default=None)
    ap.add_argument("--barrido-particiones", action="store_true")
    ap.add_argument("--pandas", action="store_true")
    ap.add_argument("--ui", action="store_true", help="una corrida y deja la Spark UI abierta")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = cargar_config(a.config)
    p = bd.Params.desde_cfg(cfg)
    sb = cfg["spark"]["benchmark"]
    masters = a.masters or sb["masters"]
    reps = a.repeticiones or int(sb["repeticiones"])
    part_prod = int(cfg["spark"]["shuffle_partitions_produccion"])
    grilla_part = [int(x) for x in sb["shuffle_partitions"]] if a.barrido_particiones else [part_prod]

    res_dir = Path(cfg["rutas"]["benchmarks_resultados"])
    fig_dir = Path(cfg["rutas"]["benchmarks_figuras"])
    res_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)
    sello = datetime.now().strftime("%Y%m%d_%H%M")

    if a.ui:
        spark = crear_sesion(cfg, master=masters[-1], particiones=part_prod, aqe=False)
        h = entrada(spark, p)
        log.info("Corrida para la Spark UI: %.2fs", medir(h))
        (res_dir / f"spark_plan_fisico_{sello}.txt").write_text(plan_fisico(h), encoding="utf-8")
        input("Spark UI en http://localhost:4040 (pestañas Jobs, Stages, SQL, Executors). "
              "Toma las capturas y presiona Enter para cerrar...")
        spark.stop()
        return

    info = entorno(cfg)
    (res_dir / f"spark_entorno_{sello}.json").write_text(json.dumps(info, indent=2, ensure_ascii=False),
                                                         encoding="utf-8")
    log.info("Entorno: %s", info)

    crudo = res_dir / f"spark_bench_crudo_{sello}.csv"
    campos = ["master", "p", "paralelismo_spark", "particiones", "aqe", "modo", "rep",
              "segundos", "filas_entrada", "t_sesion_s"]
    todas, filas_ref = [], None
    with open(crudo, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        for part in grilla_part:
            for m in masters:
                res, filas_ref = correr_config(cfg, p, m, part, reps, filas_ref, w)
                todas += res
                f.flush()

    resumen = resumir(todas)
    ruta_res = res_dir / f"spark_bench_resumen_{sello}.csv"
    with open(ruta_res, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(resumen[0].keys()))
        w.writeheader()
        w.writerows(resumen)

    if a.pandas:
        pdres = linea_base_pandas(p, reps)
        with open(res_dir / f"pandas_bench_{sello}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(pdres[0].keys()))
            w.writeheader()
            w.writerows(pdres)

    figs = graficar(resumen, part_prod, fig_dir, sello)
    print("\nModo         part  master      p   T_med(s)  speedup  eficiencia  Karp-Flatt")
    for r in resumen:
        print(f"{r['modo']:<12} {r['particiones']:<5} {r['master']:<10} {r['p']:<3} "
              f"{r['t_mediana_s']:<9} {str(r['speedup']):<8} {str(r['eficiencia']):<11} {r['karp_flatt']}")
    print(f"\nCrudo: {crudo}\nResumen: {ruta_res}\nFiguras: {[str(x) for x in figs]}")


if __name__ == "__main__":
    main()
