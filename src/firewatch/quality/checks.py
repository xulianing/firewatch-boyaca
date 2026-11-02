"""Controles de calidad reutilizables (pandas y Spark) — Arquitectura §3.1.

A implementar:
    exigir_columnas(df, columnas: list[str]) -> None
    exigir_unicidad(df, claves: list[str]) -> None
    exigir_sin_nulos(df, columnas: list[str]) -> None
    exigir_rango(df, columna: str, minimo, maximo) -> None
    exigir_conteo(df, esperado: int, nombre: str) -> None
    reconciliar_detecciones(silver, fact_mes) -> None   # mismo total de Boyacá

Cada control lanza una excepción con un mensaje claro y deja registro en el log.
Lo usan runner.py, build_daily.py y export_gold.py.
Dueño: B + C
"""
