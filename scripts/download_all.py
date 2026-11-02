"""Punto de entrada L1: descarga y valida todas las fuentes en data/raw.

Uso:
    python scripts/download_all.py [--solo firms|meteo|dane]

Pasos:
    1. dane.cargar_municipios → centroides (los necesita meteo).
    2. firms: valida los archivos presentes en raw/firms y raw/firms_2026 y escribe
       el resumen de volumen (gate) en el log.
    3. meteo.descargar_todos (reanudable).

Debe ejecutarse con `if __name__ == "__main__":`.
Dueño: B · Tareas: B1, B2
"""
