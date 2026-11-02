# docs/ieee/ — Artículo IEEE Conference (R8) · editor: A

**Herramienta:** Overleaf, plantilla oficial *IEEE Conference Template* (dos columnas). El proyecto de Overleaf se comparte con B y C como editores. En cada hito se descarga el ZIP fuente y se versiona aquí.

```
docs/ieee/
├── en/                 Fuente LaTeX en inglés (main.tex + secciones) y firewatch_en.pdf
├── es/                 Fuente LaTeX en español y firewatch_es.pdf
├── figuras/            Figuras compartidas por EN y ES (PNG/PDF a 300 dpi)
└── referencias.bib     Bibliografía común (estilo IEEEtran)
```

## Secciones, autor responsable y fuente de los datos

| Sección | Autor | Se alimenta de | Fecha |
|---|---|---|---|
| Abstract + Index Terms | A | Todo | Mié 11 |
| I. Introduction (contexto, ODS 13) | A | Evidencia del problema (A6) | Dom 8 |
| II. Problem Statement | A | Evidencia + usuarios | Dom 8 |
| III. Related Work | A | `referencias.bib` | Dom 8 |
| IV. Proposed System | A | `docs/arquitectura.md` §1–3 | Lun 9 |
| V. Network Infrastructure | A | `network/` | Lun 9 |
| VI-A. Parallel Processing | B → A | `benchmarks/results/mp_*` | Lun 9 |
| VI-B. Distributed Processing | C → A | `benchmarks/results/spark_*`, demo de tolerancia a fallos | Mar 10 |
| VII. Visualization & Dashboards | B (EDA) + C (BI) → A | `notebooks/01_eda`, `dashboard/capturas/` | Mar 10 |
| VIII. Methodology / Work Plan (CRISP-DM) | A | `docs/plan/` | Mar 10 |
| IX. Results & Expected Outcomes (incluye la validación del IRAT) | A + B | `notebooks/02_validacion_irat` | Mié 11 |
| Limitations + Conclusions + Future Work | A | Arquitectura §5 (limitaciones) | Mié 11 |
| References (≥8, preferiblemente indexadas) | A | `referencias.bib` | Mié 11 |

## Figuras obligatorias (en `figuras/`)

| Archivo | Contenido | Autor |
|---|---|---|
| `fig_arquitectura.pdf` | Diagrama de bloques del sistema (red + procesamiento + visualización) | A |
| `fig_topologia.png` | Topología de Packet Tracer | A |
| `fig_mp_speedup.png`, `fig_mp_eficiencia.png` | Escalado de multiprocessing | B |
| `fig_spark_escalado.png` | Escalado de Spark por núcleos y particiones | C |
| `fig_eda_serie.png`, `fig_eda_heatmap.png` | Dos figuras clave del EDA | B |
| `fig_dashboard.png` | Captura de la página 3 del dashboard | C |
| `fig_spark_ui_reintento.png` | Evidencia de tolerancia a fallos (P1) | C |

## Reglas

- La versión en español es una **traducción revisada** de la inglesa (no se reescribe). Las cifras son idénticas en ambas.
- Toda cifra del artículo sale de `benchmarks/results/`, de un notebook o de `docs/notas/`. Ningún número se escribe "de memoria".
- Terminología: *thermal anomaly / hotspot detection*, nunca *confirmed fire*.
- Hitos: EN v1.0 el **mié 11** · ES + PDFs finales el **jue 12**.
