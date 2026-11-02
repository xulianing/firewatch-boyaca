"""L1 · Ingesta de Open-Meteo Historical Weather API (ERA5, horario) — ADR-0003.

Entrada: centroides de los 123 municipios (silver/dim_municipio).
Parámetros: settings.yaml → meteo (endpoint, 5 variables horarias, zona horaria, pausas).

A implementar:
    descargar_municipio(cod_mpio: str, lat: float, lon: float) -> Path
        Una petición por municipio para la ventana completa. Caché: si el archivo existe
        y está completo, no se vuelve a pedir. Reintentos ante HTTP 429.
    descargar_todos() -> None
        Recorre los 123 municipios y reanuda donde quedó.
    a_silver_horario() -> None
        JSON → silver/clima_horario (Parquet particionado por anio) con el esquema
        de docs/diccionario_datos.md (cod_mpio texto, ts_local, fecha, temp, rh,
        precip, wind, et0). Guarda también la `elevation` devuelta por la API para
        dim_municipio.

Control: 42.360 horas por municipio; sin duplicados en (cod_mpio, ts_local).
Dueño: B · Tareas: B2, B3
"""
