"""L1 · DANE MGN (municipios) + tabla de provincias → dimensión de municipios.

Entradas: data/raw/dane/ (capa de municipios) · config/provincias_boyaca.csv
Parámetros: settings.yaml → territorio (CRS, tolerancia de simplificación, cod_dpto_foco)

A implementar:
    cargar_municipios() -> geopandas.GeoDataFrame
        Lee la capa, aplica make_valid, reproyecta a EPSG:4326 y guarda los códigos
        como texto (cod_dpto de 2 caracteres, cod_mpio de 5).
    geometria_simplificada() -> geopandas.GeoDataFrame
        Versión simplificada (≈110 m) para el cruce espacial de L2 (Colombia completa).
    construir_dim_municipio() -> None
        Boyacá (123 filas): cod_mpio, nombre, cod_provincia, provincia, area_km2
        (calculada en una proyección métrica), lat_c, lon_c, elevacion_m
        → silver/dim_municipio.

Control: exactamente 123 municipios con cod_dpto == "15"; todos con provincia.
Dueño: B · Tarea: B3 (la tabla de provincias la entrega A en A2)
"""
