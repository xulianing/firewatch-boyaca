"""L2 · Asignación espacial punto-en-polígono (detección → municipio), por bloque.

Diseño (Arquitectura §1.4 y §3.2):
    - Colombia completa contra ~1.120 polígonos simplificados.
    - Los polígonos se cargan UNA vez por proceso (variable global de módulo que llena
      el initializer del Pool); no se serializan con cada tarea.
    - geopandas.sjoin(predicate="within") con índice STRtree (Shapely 2).

A implementar:
    init_worker(ruta_geometrias: str) -> None
        Initializer del Pool: carga las geometrías en la variable global del proceso.
    asignar_municipio(df_bloque: pandas.DataFrame) -> pandas.DataFrame
        Añade cod_dpto y cod_mpio (texto); cod_mpio nulo si el punto no cae en tierra.

Control: % sin municipio < calidad.max_pct_sin_municipio.
Dueño: B · Tareas: B4, B6
"""
