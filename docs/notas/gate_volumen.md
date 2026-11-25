# Gate de volumen (entregable B1)

Conteo de filas **raw** de NASA FIRMS (VIIRS 375 m, Colombia) por año, satélite y nivel de procesamiento, y
decisión sobre cómo presentar el benchmark de `multiprocessing`.

Fuente: `data/silver/_reports/cleaning_metrics.json` (`metricas.por_archivo` y `archivos`), generado por
`python -m firewatch.parallel.runner` (corrida del 21 nov 2026, silver `content_sha256` `a41f8fb8…`).
Satélites: `N` = S-NPP (archivos `viirs-snpp_*`, `*_SV-*`) · `N20` = NOAA-20 (archivos `viirs-jpss1_*`, `*_J1V-*`).

| Año | Satélite | Proceso | Archivo | Filas raw | Filas tras limpieza | ¿Columna `type`? |
|---|---|---|---|---:|---:|---|
| 2022 | N | SP | `viirs-snpp_2022_Colombia.csv` | 121.650 | 110.369 | Sí |
| 2022 | N20 | SP | `viirs-jpss1_2022_Colombia.csv` | 124.840 | 112.262 | Sí |
| 2023 | N | SP | `viirs-snpp_2023_Colombia.csv` | 99.568 | 89.156 | Sí |
| 2023 | N20 | SP | `viirs-jpss1_2023_Colombia.csv` | 98.684 | 87.555 | Sí |
| 2024 | N | SP | `viirs-snpp_2024_Colombia.csv` | 107.010 | 96.956 | Sí |
| 2024 | N20 | SP | `viirs-jpss1_2024_Colombia.csv` | 104.438 | 93.593 | Sí |
| 2025 | N | SP | `viirs-snpp_2025_Colombia.csv` | 80.802 | 73.674 | Sí |
| 2025 | N20 | SP | `viirs-jpss1_2025_Colombia.csv` | 81.875 | 74.211 | Sí |
| 2026 (ene–jul) | N | SP | `fire_archive_SV-C2_43072.csv` | 56.490 | 52.381 | Sí |
| 2026 (ene–jul) | N20 | SP | `fire_archive_J1V-C2_43071.csv` | 57.373 | 51.781 | Sí |
| 2026 (ago–oct) | N | NRT | `fire_nrt_SV-C2_43072.csv` | 14.147 | 13.158 | No (NRT no trae `type`) |
| 2026 (ago–oct) | N20 | NRT | `fire_nrt_J1V-C2_43071.csv` | 13.953 | 12.998 | No (NRT no trae `type`) |
| **Total** | | | 12 archivos | **960.830** | **868.094** | |

Subtotales raw por año: 2022 = 246.490 · 2023 = 198.252 · 2024 = 211.448 · 2025 = 162.677 · 2026 = 141.963.

**Descartes de la limpieza** (mismo informe, `metricas.descartes`): F1 ventana 0 · F2 caja de Colombia 6 ·
F3 rangos físicos 94 · F4 corte SP/NRT 0 · F5 `type ≠ 0` (solo SP) 36.060 · F6 confianza baja 56.576 ·
F7 duplicados 0 → **92.736 descartadas (9,65 %)**. La regla F8 (fuentes estáticas, ADR-0005) marca y no descarta:
11.439 filas marcadas; 856.655 de vegetación; **3.908 de vegetación en Boyacá**.

**Decisión** (Plan de Implementación §4.1):
- [x] **≥ 300k → escalado fuerte y débil según el plan** (960.830 filas raw)
- [ ] 100k–300k → medir la etapa L2 completa y dar más peso al escalado débil
- [ ] < 100k → reportar con el análisis de overhead

Se midió la etapa L2 completa (no solo el cruce espacial) con escalado fuerte y débil:
`benchmarks/results/mp_bench_resumen_20261121_0947.csv` y `mp_bench_debil_20261121_0947.csv`.

**Observaciones:**
- Los meses ago–oct de 2026 provienen de NRT: no traen `type` (F5 no aplica) y se tratan como provisionales.
- R4 (Spark sobre ≥ 100.000 registros) queda cubierto además por el clima horario: 5.210.280 filas
  (`docs/evidencias/build_daily_validation.json`).
