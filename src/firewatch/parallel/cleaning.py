"""L2 · Limpieza y filtros de calidad de las detecciones FIRMS (funciones puras por bloque).

Parámetros: settings.yaml → firms.filtros, firms.mascara_estatica, territorio.bbox_colombia

A implementar:
    limpiar_bloque(df: pandas.DataFrame) -> pandas.DataFrame
        Tipos, rangos (lat/lon dentro de la caja de Colombia, frp > 0), satélites
        permitidos, deduplicación por firms.dedup_claves y columna `proceso` (SP/NRT).
    construir_mascara_estatica(df_sp: pandas.DataFrame) -> set[tuple[int, int]]
        Celdas de ~0,005° con type == 2 en SP 2022–2025. Se construye UNA vez, antes
        del Pool, y se pasa a los procesos mediante el initializer.
    filtrar_calidad(df: pandas.DataFrame, mascara: set) -> pandas.DataFrame
        type == 0 (si existe la columna), confidence no en {'l'} y marca
        `fuente_estatica` según la máscara (incluye NRT 2026, que no trae type).

Log obligatorio: filas de entrada y descartadas por cada filtro (van al IEEE).
Dueño: B · Tareas: B4, B6
"""
