"""Benchmark L3: escalado por núcleos y particiones de las etapas Spark, y comparación con pandas.

Para cada master y valor de shuffle_partitions de settings.yaml → spark.benchmark:
crea la sesión con session.crear_sesion(...), ejecuta agregar_horario_a_diario y el gold
completo, y mide los tiempos (con una acción que materialice el resultado, por ejemplo
una escritura). Escribe results/spark_<fecha>.csv y genera las figuras.

Dueño: C · Tarea: C7
"""
