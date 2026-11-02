"""Benchmark L2: escalado fuerte y débil de la etapa silver (protocolo en benchmarks/README.md).

Llama a firewatch.parallel.runner.ejecutar_silver(n_procesos) para cada p de
settings.yaml → paralelo.benchmark. Escribe results/mp_<fecha>.csv y genera las 4 figuras.
Se ejecuta como script (`if __name__ == "__main__":`), nunca desde Jupyter.

Dueño: B · Tarea: B7
"""
