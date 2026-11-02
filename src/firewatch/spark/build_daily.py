"""L3 · Clima horario → diario y grilla completa municipio-día.

Etapas (Arquitectura §9.2):
    1. int.clima_diario   groupBy(cod_mpio, fecha) sobre ~5,2 M filas horarias:
                          tmax, tmean, precip (suma), rh_min, wind_max, et0 (suma).
                          Es la carga con shuffle más pesada y la etapa principal del
                          benchmark de Spark.
    2. int.municipio_dia  base = clima diario (todos los municipio-día) LEFT JOIN
                          detecciones de Boyacá (silver, sin fuente_estatica) → n_det = 0
                          donde no hubo. Ventanas Window.partitionBy(cod_mpio).orderBy(fecha):
                          precip_30d, dia_seco e id_racha (gaps-and-islands).
    3. Agregado nacional por departamento y mes (para K6), sobre silver de Colombia.

A implementar:
    agregar_horario_a_diario(spark) -> DataFrame
    construir_grilla_municipio_dia(spark, clima_diario) -> DataFrame
    agregar_departamento_mes(spark) -> DataFrame

Controles: unicidad (cod_mpio, fecha); 24 horas por día; la suma de n_det coincide con silver.
Dueño: C · Tareas: C4, C6
"""
