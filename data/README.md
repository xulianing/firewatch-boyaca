# data/ — capas de datos (NO versionadas)

Los datos se regeneran con `python scripts/download_all.py` y `python scripts/run_pipeline.py`. Git solo guarda los `.gitkeep`.

| Carpeta | Capa | Contenido | Formato | Produce |
|---|---|---|---|---|
| `raw/firms/` | Raw | CSV anual por país, Colombia, VIIRS S-NPP y NOAA-20, 2022–2025 | CSV | `ingest/firms.py` (B) |
| `raw/firms_2026/` | Raw | Archive Download 2026-01-01 → 2026-10-31 (SP + NRT) | CSV | B (descarga manual con código por correo) |
| `raw/meteo/` | Raw | Un JSON por municipio (`<cod_mpio>.json`), horario | JSON | `ingest/meteo.py` (B) |
| `raw/dane/` | Raw | Capa de municipios del MGN | SHP/GPKG | `ingest/dane.py` (B) |
| `raw/ungrd/` | Raw | Consolidado de emergencias (opcional, P1) | CSV/XLSX | A |
| `silver/detecciones/` | Silver | Detecciones limpias y con municipio asignado, particionadas por `anio` | Parquet | `parallel/runner.py` (B) |
| `silver/clima_horario/` | Silver | Clima horario por municipio, particionado por `anio` | Parquet | `ingest/meteo.py` (B) |
| `silver/dim_municipio/` | Silver | 123 municipios de Boyacá con provincia, área y centroide | Parquet | `ingest/dane.py` (B) |
| `gold/` | Gold | Modelo estrella para Power BI y EDA | Parquet + CSV | `spark/export_gold.py` (C) |

**Reglas:**
- `raw/` es inmutable: nunca se edita a mano.
- Los esquemas de silver y gold están en `docs/diccionario_datos.md`.
- Para compartir datos entre integrantes: carpeta compartida en Drive con la misma estructura, nunca Git.
