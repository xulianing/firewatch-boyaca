"""L1 · Ingesta de NASA FIRMS (VIIRS S-NPP + NOAA-20, Colombia).

Fuentes (ver settings.yaml → firms):
    - 2022–2025: CSV anual por país (procesamiento estándar, SP) → data/raw/firms/
    - 2026-01-01 → 2026-10-31: Archive Download (SP + NRT) → data/raw/firms_2026/
      La solicitud se hace manualmente en la web (código por correo); este módulo solo
      valida y registra los archivos recibidos.

A implementar:
    listar_archivos_raw() -> list[Path]
    validar_columnas(ruta: Path) -> None
        Exige latitude, longitude, acq_date, acq_time, satellite, confidence, frp,
        bright_ti4, daynight; registra si existe `type` (solo en SP).
    resumen_volumen() -> pandas.DataFrame
        Conteo por año, satélite y tipo de proceso (SP/NRT). Alimenta el gate
        (docs/notas/gate_volumen.md).

Salida: archivos raw sin modificar + log de validación.
Dueño: B · Tareas: B1, B5
"""
