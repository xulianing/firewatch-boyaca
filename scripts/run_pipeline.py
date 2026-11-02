"""Punto de entrada L2 + L3: pipeline reproducible de punta a punta.

Uso:
    python scripts/run_pipeline.py --stage silver   # L2: runner.ejecutar_silver(p de producción)
    python scripts/run_pipeline.py --stage gold     # L3: session → build_daily → climatology
                                                    #     → irat → export_gold
    python scripts/run_pipeline.py --stage all

Al final imprime un resumen: conteos por capa, tiempos por etapa y resultado de la
reconciliación. Ese resumen se pega en docs/notas/ como evidencia.
Dueño: B (orquestación) + C (etapa gold) · Tarea: B10
"""
