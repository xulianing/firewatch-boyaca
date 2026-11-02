"""L3 · Creación de la SparkSession (único punto de configuración de Spark).

A implementar:
    crear_sesion(master: str | None = None, shuffle_partitions: int | None = None) -> SparkSession
        Valores por defecto tomados de settings.yaml → spark (master_produccion,
        driver_memory, shuffle_partitions_produccion). El benchmark los sobrescribe.
        Registra en el log la versión de Spark, el master y los núcleos efectivos.

Dueño: C · Tarea: C3
"""
