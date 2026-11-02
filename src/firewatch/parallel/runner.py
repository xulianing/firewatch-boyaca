"""L2 · Orquestación de la etapa silver: secuencial (p = 1) o paralela (p > 1).

Flujo:
    raw FIRMS → bloques de tamaño fijo (paralelo.tamano_bloque_filas)
    → Pool(p, initializer=init_worker): limpiar_bloque → filtrar_calidad → asignar_municipio
    → silver/detecciones (Parquet particionado por anio, esquema del diccionario).

A implementar:
    generar_bloques() -> Iterator[pandas.DataFrame]
        Bloques de igual tamaño (no por año, para balancear la carga).
    procesar_bloque(df: pandas.DataFrame) -> pandas.DataFrame
        Cadena completa de L2 para un bloque (función de nivel de módulo).
    ejecutar_silver(n_procesos: int) -> dict
        Si n_procesos == 1, ejecuta sin Pool (línea base de la medición).
        Devuelve tiempos por fase y conteos. La usan run_pipeline y el benchmark.

Garantía: la salida con p = 1 y con p > 1 es idéntica (tests/test_cleaning.py).
Dueño: B · Tareas: B4, B6, B7
"""
