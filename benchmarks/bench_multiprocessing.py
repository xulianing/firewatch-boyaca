"""
Benchmark L2 (B7): escalado fuerte y débil de la etapa silver con multiprocessing.

Protocolo (benchmarks/README.md): misma máquina para todas las configuraciones, conectada a la corriente,
modo de máximo rendimiento y sin otras aplicaciones; corridas de calentamiento descartadas; se reporta la
MEDIANA de las repeticiones (igual que bench_spark.py, para que las tablas del IEEE sean comparables).

Experimentos
  Fuerte:  el MISMO problema completo (960.830 filas raw) con p = 1, 2, 4, 8 → runner.ejecutar_silver(p),
           el código de producción completo (Fase 0 + Fase 1 + validación + escritura).
           Métricas: T(p), speedup S = T1/Tp, eficiencia E = S/p, Karp–Flatt e = (1/S − 1/p)/(1 − 1/p),
           más el speedup de la Fase 1 sola (la parte paralelizable).
  Débil:   el trabajo CRECE con p (p × bloques_por_proceso bloques) → runner.ejecutar_fase1, la misma
           función que usa run(). Solo la Fase 1: la Fase 0 siempre lee todo el raw, no escala con p.
           Métrica: eficiencia débil = T(1) / T(p) (ideal = 1: más procesos resuelven más datos en igual tiempo).
  Control: el content_sha256 debe ser idéntico en TODAS las corridas del escalado fuerte; si no, se aborta.

Salidas (benchmarks/results y benchmarks/figures, se versionan):
  mp_entorno_<sello>.json · mp_bench_crudo_<sello>.csv · mp_bench_resumen_<sello>.csv · mp_bench_debil_<sello>.csv
  mp_speedup_eficiencia_<sello>.png · mp_karp_flatt_<sello>.png · mp_escalado_debil_<sello>.png

Uso (desde la raíz del repo, con el .venv activado):
  python benchmarks/bench_multiprocessing.py --rapido        # prueba de humo: 1 repetición, sin calentamiento
  python benchmarks/bench_multiprocessing.py                 # corrida oficial (settings: paralelo.benchmark)

Se ejecuta como script (`if __name__ == "__main__":`), nunca desde Jupyter: con spawn, cada proceso hijo
reimporta este archivo y, sin esa guarda, cada hijo volvería a lanzar el benchmark completo.

Dueño: B · Tarea: B7
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import yaml  # noqa: E402

from firewatch.ingest.dane import CheckLog  # noqa: E402
from firewatch.parallel import cleaning as cl  # noqa: E402
from firewatch.parallel import runner as rn  # noqa: E402

FASES = ("fase0_s", "plan_s", "carga_poligonos_s", "fase1_s", "reduccion_validacion_s", "escritura_s", "total_s")
PERFIL = ("suma_tareas_s", "spawn_max_s", "arranque_max_s", "desbalance", "ocupacion")
CAMPOS_CRUDO = ("experimento", "p", "rep", "n_filas", "n_bloques", *FASES, *PERFIL, "content_sha256")


# =============================================================================
# Entorno (va a la tabla de configuración experimental del IEEE)
# =============================================================================

def _cpu_nombre() -> str:
    try:
        import winreg
        k = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0")
        return str(winreg.QueryValueEx(k, "ProcessorNameString")[0]).strip()
    except Exception:  # noqa: BLE001 — fuera de Windows o sin permiso
        return platform.processor()


def _nucleos_fisicos() -> int | None:
    try:
        cmd = "(Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfCores -Sum).Sum"
        out = subprocess.run(["powershell", "-NoProfile", "-Command", cmd], capture_output=True, text=True, timeout=30)
        return int(out.stdout.strip())
    except Exception:  # noqa: BLE001
        return None


def _ram_gb() -> float | None:
    try:
        import ctypes

        class _MemStatus(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
        st = _MemStatus()
        st.dwLength = ctypes.sizeof(st)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))
        return round(st.ullTotalPhys / 2**30, 1)
    except Exception:  # noqa: BLE001
        return None


def entorno(bloque_filas: int) -> dict:
    import geopandas
    import numpy
    import pandas
    import shapely
    return {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "sistema": platform.platform(),
        "procesador": _cpu_nombre(),
        "nucleos_fisicos": _nucleos_fisicos(),
        "nucleos_logicos": os.cpu_count(),
        "ram_gb": _ram_gb(),
        "python": platform.python_version(),
        "metodo_inicio": "spawn",
        "pandas": pandas.__version__, "numpy": numpy.__version__,
        "geopandas": geopandas.__version__, "shapely": shapely.__version__,
        "tamano_bloque_filas": bloque_filas,
    }


# =============================================================================
# Experimentos
# =============================================================================

def _guardar_fila(ruta: Path, fila: dict) -> None:
    """Agrega una fila al CSV crudo apenas termina cada corrida: si se interrumpe, no se pierde lo medido."""
    nuevo = not ruta.exists()
    with open(ruta, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_CRUDO, extrasaction="ignore")
        if nuevo:
            w.writeheader()
        w.writerow(fila)


def escalado_fuerte(settings_path: Path, procesos: list[int], reps: int, calent: int, crudo: Path) -> list[dict]:
    filas: list[dict] = []
    hash_ref: str | None = None
    for p in procesos:
        for i in range(calent + reps):
            es_calentamiento = i < calent
            etiqueta = "calentamiento" if es_calentamiento else f"rep {i - calent + 1}/{reps}"
            print(f"  fuerte · p={p} · {etiqueta} ...", end=" ", flush=True)
            rep = rn.ejecutar_silver(p, settings_path=settings_path)
            t = rep["tiempos"]
            h = rep["content_sha256"]
            print(f"{t['total_s']:.1f} s (Fase 1 {t['fase1_s']:.1f} s)", flush=True)
            if hash_ref is None:
                hash_ref = h
            elif h != hash_ref:
                raise SystemExit(f"ABORTADO: content_sha256 distinto con p={p} ({h[:12]}… ≠ {hash_ref[:12]}…). "
                                 "El paralelo no reproduce el resultado secuencial: revisar antes de medir.")
            if es_calentamiento:
                continue
            fila = {"experimento": "fuerte", "p": p, "rep": i - calent + 1,
                    "n_filas": rep["metricas"]["n_raw"], "n_bloques": rep["plan"]["n_bloques"],
                    **{f: round(t[f], 4) for f in FASES},
                    **{k: rep.get("perfil_fase1", {}).get(k) for k in PERFIL}, "content_sha256": h}
            _guardar_fila(crudo, fila)
            filas.append(fila)
    return filas


def escalado_debil(settings_path: Path, procesos: list[int], bloques_por_proceso: int, reps: int, calent: int,
                   crudo: Path) -> list[dict]:
    from firewatch.parallel import spatial_join as sj
    settings, base_dir = rn._load_settings(settings_path)
    cfg = cl.load_config(settings, base_dir=base_dir)
    print("  débil · preparando Fase 0 (una sola vez, no se mide) ...", flush=True)
    norm, ctx, _, _ = cl.fase0(cfg, CheckLog())
    if ctx is None:
        raise SystemExit("ABORTADO: la Fase 0 falló; corre primero python -m firewatch.parallel.runner")
    chunks = rn.plan_chunks(norm, cfg.bloque_filas)
    filas: list[dict] = []
    for p in procesos:
        k = p * bloques_por_proceso
        if k > len(chunks):
            print(f"  débil · p={p}: hacen falta {k} bloques y hay {len(chunks)}; se omite", flush=True)
            continue
        sub = chunks[:k]
        n_filas = int(sum(len(c) for c in sub))
        for i in range(calent + reps):
            es_calentamiento = i < calent
            etiqueta = "calentamiento" if es_calentamiento else f"rep {i - calent + 1}/{reps}"
            print(f"  débil · p={p} · {k} bloques ({n_filas:,} filas) · {etiqueta} ...", end=" ", flush=True)
            t0 = time.perf_counter()
            *_, perfil = rn.ejecutar_fase1(norm, sub, ctx, cfg.geo_path, p, sj.load_polygons, sj.spatial_join)
            dt = time.perf_counter() - t0
            print(f"{dt:.1f} s", flush=True)
            if es_calentamiento:
                continue
            fila = {"experimento": "debil", "p": p, "rep": i - calent + 1, "n_filas": n_filas, "n_bloques": k,
                    "fase1_s": round(dt, 4), **{c: perfil.get(c) for c in PERFIL}}
            _guardar_fila(crudo, fila)
            filas.append(fila)
    return filas


# =============================================================================
# Resumen y métricas
# =============================================================================

def _mediana(xs: list[float]) -> float:
    return statistics.median(xs)


def _desv(xs: list[float]) -> float:
    return statistics.stdev(xs) if len(xs) > 1 else 0.0


def resumir_fuerte(filas: list[dict]) -> list[dict]:
    ps = sorted({f["p"] for f in filas})
    med = {p: {c: _mediana([f[c] for f in filas if f["p"] == p]) for c in (*FASES, *PERFIL)} for p in ps}
    t1, f1 = med[ps[0]]["total_s"], med[ps[0]]["fase1_s"]
    sum1 = med[ps[0]]["suma_tareas_s"]
    out = []
    for p in ps:
        tot = [f["total_s"] for f in filas if f["p"] == p]
        s, s_f1 = t1 / med[p]["total_s"], f1 / med[p]["fase1_s"]
        kf = (1 / s - 1 / p) / (1 - 1 / p) if p > 1 else None
        out.append({"p": p, "n_rep": len(tot),
                    "t_mediana_s": round(med[p]["total_s"], 3), "t_min_s": round(min(tot), 3),
                    "t_desv_s": round(_desv(tot), 3), "cv": round(_desv(tot) / med[p]["total_s"], 4),
                    "speedup": round(s, 3), "eficiencia": round(s / p, 3),
                    "karp_flatt": round(kf, 4) if kf is not None else None,
                    "fase1_mediana_s": round(med[p]["fase1_s"], 3), "speedup_fase1": round(s_f1, 3),
                    "eficiencia_fase1": round(s_f1 / p, 3),
                    "serial_mediana_s": round(med[p]["total_s"] - med[p]["fase1_s"], 3),
                    # Perfil (medido dentro de los procesos): ¿cada proceso calcula igual de rápido que uno solo?
                    "suma_tareas_s": round(med[p]["suma_tareas_s"], 3),
                    "velocidad_relativa": round(sum1 / med[p]["suma_tareas_s"], 3),
                    "spawn_max_s": round(med[p]["spawn_max_s"], 3),
                    "arranque_max_s": round(med[p]["arranque_max_s"], 3),
                    "desbalance": round(med[p]["desbalance"], 3),
                    "ocupacion": round(med[p]["ocupacion"], 3)})
    return out


def fraccion_amdahl(resumen: list[dict]) -> float:
    """f de Amdahl = parte NO paralelizable de la corrida con p = 1 (todo lo que no es Fase 1)."""
    base = resumen[0]
    return base["serial_mediana_s"] / base["t_mediana_s"]


def resumir_debil(filas: list[dict]) -> list[dict]:
    ps = sorted({f["p"] for f in filas})
    med = {p: _mediana([f["fase1_s"] for f in filas if f["p"] == p]) for p in ps}
    cpu = {p: _mediana([f["suma_tareas_s"] / f["n_filas"] for f in filas if f["p"] == p]) for p in ps}
    out = []
    for p in ps:
        ts = [f["fase1_s"] for f in filas if f["p"] == p]
        n = next(f["n_filas"] for f in filas if f["p"] == p)
        out.append({"p": p, "n_rep": len(ts), "n_filas": n, "t_mediana_s": round(med[p], 3),
                    "t_desv_s": round(_desv(ts), 3), "eficiencia_debil": round(med[ps[0]] / med[p], 3),
                    "filas_por_s": int(n / med[p]),
                    "velocidad_relativa": round(cpu[ps[0]] / cpu[p], 3),
                    "spawn_max_s": round(_mediana([f["spawn_max_s"] for f in filas if f["p"] == p]), 3),
                    "arranque_max_s": round(_mediana([f["arranque_max_s"] for f in filas if f["p"] == p]), 3)})
    return out


# =============================================================================
# Figuras
# =============================================================================

def graficar(fuerte: list[dict], debil: list[dict], f_amdahl: float | None, nucleos_fisicos: int | None,
             carpeta: Path, sello: str) -> list[Path]:
    import matplotlib
    matplotlib.use("Agg")                    # sin ventana: solo archivos
    import matplotlib.pyplot as plt

    carpeta.mkdir(parents=True, exist_ok=True)
    rutas: list[Path] = []

    def marcar_fisicos(ax):
        if nucleos_fisicos:
            ax.axvline(nucleos_fisicos, color="grey", ls=":", lw=1)
            ax.text(nucleos_fisicos, ax.get_ylim()[1] * 0.97, f" {nucleos_fisicos} núcleos físicos",
                    color="grey", fontsize=8, va="top")

    if fuerte:
        ps = [r["p"] for r in fuerte]
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
        a1.plot(ps, ps, "k--", lw=1, label="Ideal (S = p)")
        if f_amdahl is not None:
            xs = [1 + i * (max(ps) - 1) / 100 for i in range(101)]
            a1.plot(xs, [1 / (f_amdahl + (1 - f_amdahl) / x) for x in xs], color="tab:orange", lw=1.2,
                    label=f"Amdahl (f = {f_amdahl:.2f})")
        a1.plot(ps, [r["speedup"] for r in fuerte], "o-", color="tab:blue", label="Medido · total")
        a1.plot(ps, [r["speedup_fase1"] for r in fuerte], "s--", color="tab:green", label="Medido · Fase 1")
        a1.set(xlabel="Procesos (p)", ylabel="Speedup S = T₁/Tₚ", title="Escalado fuerte: speedup", xticks=ps)
        a1.grid(alpha=0.3)
        marcar_fisicos(a1)
        a1.legend(fontsize=8)
        a2.axhline(1, color="k", ls="--", lw=1, label="Ideal (E = 1)")
        a2.plot(ps, [r["eficiencia"] for r in fuerte], "o-", color="tab:blue", label="Total")
        a2.plot(ps, [r["eficiencia_fase1"] for r in fuerte], "s--", color="tab:green", label="Fase 1")
        a2.set(xlabel="Procesos (p)", ylabel="Eficiencia E = S/p", title="Escalado fuerte: eficiencia",
               xticks=ps, ylim=(0, 1.1))
        a2.grid(alpha=0.3)
        marcar_fisicos(a2)
        a2.legend(fontsize=8)
        fig.tight_layout()
        r = carpeta / f"mp_speedup_eficiencia_{sello}.png"
        fig.savefig(r, dpi=150)
        plt.close(fig)
        rutas.append(r)

        # Descomposición de la Fase 1: ¿dónde se va el tiempo respecto del ideal?
        fig, ax = plt.subplots(figsize=(7, 4))
        w = 0.27
        xs = list(range(len(ps)))
        t1f = fuerte[0]["fase1_mediana_s"]
        ax.bar([x - w for x in xs], [r["fase1_mediana_s"] for r in fuerte], w, label="Fase 1 medida (pared)",
               color="tab:blue")
        ax.bar(xs, [r["suma_tareas_s"] / r["p"] for r in fuerte], w, label="Cómputo por proceso (Σ tareas / p)",
               color="tab:green")
        ax.bar([x + w for x in xs], [t1f / r["p"] for r in fuerte], w, label="Ideal (T₁ / p)", color="lightgrey",
               edgecolor="grey")
        ax.set_xticks(xs, [str(p_) for p_ in ps])
        ax.set(xlabel="Procesos (p)", ylabel="Segundos", title="Fase 1: tiempo medido frente al ideal")
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=8)
        fig.tight_layout()
        r = carpeta / f"mp_descomposicion_fase1_{sello}.png"
        fig.savefig(r, dpi=150)
        plt.close(fig)
        rutas.append(r)

        kf = [x for x in fuerte if x["karp_flatt"] is not None]
        if kf:
            fig, ax = plt.subplots(figsize=(5.5, 3.8))
            ax.bar([str(x["p"]) for x in kf], [x["karp_flatt"] for x in kf], color="tab:purple")
            for i, x in enumerate(kf):
                ax.text(i, x["karp_flatt"], f"{x['karp_flatt']:.3f}", ha="center", va="bottom", fontsize=9)
            ax.set(xlabel="Procesos (p)", ylabel="e (fracción serial experimental)",
                   title="Métrica de Karp–Flatt")
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            r = carpeta / f"mp_karp_flatt_{sello}.png"
            fig.savefig(r, dpi=150)
            plt.close(fig)
            rutas.append(r)

    if debil:
        ps = [r["p"] for r in debil]
        fig, ax = plt.subplots(figsize=(5.5, 3.8))
        ax.axhline(1, color="k", ls="--", lw=1, label="Ideal")
        ax.plot(ps, [r["eficiencia_debil"] for r in debil], "o-", color="tab:red", label="Medido")
        for r_ in debil:
            ax.annotate(f"{r_['n_filas'] / 1000:.0f}k filas", (r_["p"], r_["eficiencia_debil"]),
                        textcoords="offset points", xytext=(0, 7), ha="center", fontsize=8)
        ax.set(xlabel="Procesos (p) · datos ∝ p", ylabel="Eficiencia débil T₁/Tₚ",
               title="Escalado débil (Fase 1)", xticks=ps, ylim=(0, 1.15))
        ax.grid(alpha=0.3)
        marcar_fisicos(ax)
        ax.legend(fontsize=8)
        fig.tight_layout()
        r = carpeta / f"mp_escalado_debil_{sello}.png"
        fig.savefig(r, dpi=150)
        plt.close(fig)
        rutas.append(r)
    return rutas


def _escribir_csv(ruta: Path, filas: list[dict]) -> None:
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark L2 multiprocessing (B7)")
    ap.add_argument("--config", default=str(ROOT / "config" / "settings.yaml"))
    ap.add_argument("--procesos", type=int, nargs="*", default=None)
    ap.add_argument("--repeticiones", type=int, default=None)
    ap.add_argument("--calentamiento", type=int, default=None)
    ap.add_argument("--bloques-por-proceso", type=int, default=4, help="escalado débil: bloques por proceso")
    ap.add_argument("--solo-fuerte", action="store_true")
    ap.add_argument("--solo-debil", action="store_true")
    ap.add_argument("--rapido", action="store_true", help="prueba de humo: 1 repetición y sin calentamiento")
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING)          # el runner informa en INFO: aquí solo el progreso
    settings_path = Path(args.config).resolve()
    settings = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
    b = settings["paralelo"]["benchmark"]
    procesos = args.procesos or list(b["procesos"])
    reps = 1 if args.rapido else (args.repeticiones or int(b["repeticiones"]))
    calent = 0 if args.rapido else (args.calentamiento if args.calentamiento is not None else int(b["calentamiento"]))
    hacer_debil = bool(b.get("escalado_debil", True)) and not args.solo_fuerte
    hacer_fuerte = not args.solo_debil
    if 1 not in procesos:
        raise SystemExit("La lista de procesos debe incluir p = 1 (línea base del speedup)")
    procesos = sorted(procesos)

    sello = datetime.now().strftime("%Y%m%d_%H%M")
    res_dir, fig_dir = ROOT / "benchmarks" / "results", ROOT / "benchmarks" / "figures"
    res_dir.mkdir(parents=True, exist_ok=True)
    info = entorno(int(settings["paralelo"]["tamano_bloque_filas"]))
    info.update({"procesos": procesos, "repeticiones": reps, "calentamiento": calent,
                 "bloques_por_proceso_debil": args.bloques_por_proceso, "rapido": args.rapido})
    (res_dir / f"mp_entorno_{sello}.json").write_text(json.dumps(info, indent=2, ensure_ascii=False),
                                                       encoding="utf-8")
    crudo = res_dir / f"mp_bench_crudo_{sello}.csv"
    print(f"Benchmark L2 · {info['procesador']} · {info['nucleos_fisicos']} físicos / {info['nucleos_logicos']} "
          f"lógicos · {info['ram_gb']} GB · Python {info['python']}")
    print(f"p = {procesos} · {calent} calentamiento + {reps} repeticiones · sello {sello}\n")

    t_ini = time.perf_counter()
    fuerte = escalado_fuerte(settings_path, procesos, reps, calent, crudo) if hacer_fuerte else []
    debil = escalado_debil(settings_path, procesos, args.bloques_por_proceso, reps, calent, crudo) if hacer_debil else []

    res_f = resumir_fuerte(fuerte) if fuerte else []
    res_d = resumir_debil(debil) if debil else []
    f_am = fraccion_amdahl(res_f) if res_f else None
    if res_f:
        _escribir_csv(res_dir / f"mp_bench_resumen_{sello}.csv", res_f)
    if res_d:
        _escribir_csv(res_dir / f"mp_bench_debil_{sello}.csv", res_d)
    figuras = graficar(res_f, res_d, f_am, info["nucleos_fisicos"], fig_dir, sello)

    print(f"\nDuración total: {(time.perf_counter() - t_ini) / 60:.1f} min")
    if res_f:
        print(f"\nEscalado fuerte (mediana) · f de Amdahl = {f_am:.3f} → techo teórico 1/f = {1 / f_am:.2f}")
        print(f"{'p':>3} {'T (s)':>8} {'S':>6} {'E':>6} {'Karp-Flatt':>11} {'S Fase1':>8} {'CV':>6}")
        for r in res_f:
            kf = f"{r['karp_flatt']:.3f}" if r["karp_flatt"] is not None else "—"
            print(f"{r['p']:>3} {r['t_mediana_s']:>8.2f} {r['speedup']:>6.2f} {r['eficiencia']:>6.2f} "
                  f"{kf:>11} {r['speedup_fase1']:>8.2f} {r['cv']:>6.1%}")
        print("\nPerfil de la Fase 1 (medido dentro de los procesos)")
        print(f"{'p':>3} {'Σ tareas (s)':>13} {'vel. relativa':>14} {'spawn (s)':>10} {'init (s)':>9} "
              f"{'desbalance':>11} {'ocupación':>10}")
        for r in res_f:
            print(f"{r['p']:>3} {r['suma_tareas_s']:>13.2f} {r['velocidad_relativa']:>14.2f} "
                  f"{r['spawn_max_s']:>10.2f} {r['arranque_max_s']:>9.2f} {r['desbalance']:>11.2f} "
                  f"{r['ocupacion']:>10.2f}")
    if res_d:
        print("\nEscalado débil (Fase 1, mediana)")
        print(f"{'p':>3} {'filas':>9} {'T (s)':>8} {'E débil':>8} {'vel. relativa':>14} {'spawn (s)':>10}")
        for r in res_d:
            print(f"{r['p']:>3} {r['n_filas']:>9,} {r['t_mediana_s']:>8.2f} {r['eficiencia_debil']:>8.2f} "
                  f"{r['velocidad_relativa']:>14.2f} {r['spawn_max_s']:>10.2f}")
    print("\nArchivos:")
    for ruta in [res_dir / f"mp_entorno_{sello}.json", crudo, *figuras]:
        print(f"  {ruta.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
