"""L2 · Procesamiento paralelo (multiprocessing + GeoPandas) → data/silver.

Regla spawn (Windows): todas las funciones que recibe Pool viven a nivel de módulo
en este paquete. Ningún lambda ni función anidada se pasa a Pool.
"""
