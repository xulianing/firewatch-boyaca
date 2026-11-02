"""L3 · Exportación del modelo estrella a data/gold (Parquet + CSV para Power BI).

Tablas (esquemas exactos en docs/diccionario_datos.md):
    fact_municipio_mes (7.134 filas) · fact_deteccion (solo Boyacá) · agg_depto_mes
    dim_municipio (123) · dim_departamento · dim_mes (58)

A implementar:
    exportar_todo(spark) -> None
        Un único archivo CSV por tabla (coalesce(1); son tablas pequeñas), con
        separador de coma, codificación UTF-8 y encabezado.
    verificar_integridad() -> None
        Cada clave foránea existe en su dimensión; la reconciliación de conteos
        silver = fact_municipio_mes = valor esperado de K1.

Al terminar el sábado 7: tag `data-freeze`.
Dueño: C · Tareas: C4, C9
"""
