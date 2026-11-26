# CLAUDE.md — Contexto permanente de FireWatch Boyacá

> Este archivo lo leen los asistentes de IA (Claude Code u otros) y los integrantes nuevos antes de tocar el repositorio.
> **Si algo aquí contradice una petición, se pregunta antes de actuar.**

## 1. Qué es el proyecto

Proyecto Integrador de cuarto semestre (Ingeniería de Datos e IA, FIDIA, Universidad Santo Tomás, Tunja). Integra tres asignaturas en un solo sistema:
- **Redes e Infraestructura:** topología en Packet Tracer, VLSM, OSPF, VLAN, DHCP, DNS y HTTP.
- **Computación Paralela y Distribuida:** `multiprocessing` (L2) + PySpark local (L3), con speedup y eficiencia medidos.
- **Analítica y Visualización:** EDA en Matplotlib/Seaborn + dashboard en Power BI.

**Tema:** anomalías térmicas (NASA FIRMS) + clima (ERA5 vía Open-Meteo) → Índice de Riesgo Relativo de Anomalías Térmicas (IRAT) por municipio y mes en Boyacá. ODS 13.

## 2. Estado: ENTREGADO

- Entrega oficial: **26 de noviembre de 2026**. La versión entregada es el *tag* **`v1.0-entrega`**. Las tablas gold que usan el IEEE y el dashboard son las del *tag* **`data-freeze`**.
- El código, los informes de `docs/evidencias/` y los resultados de `benchmarks/results/` son la fuente de verdad. Las decisiones posteriores al diseño inicial están en `docs/adr/` (0001 a 0005).
- No se proponen tecnologías, fuentes de datos ni funcionalidades nuevas. Cualquier cambio de fondo requiere un ADR y la aprobación de los otros dos integrantes.

## 3. Restricciones duras

**Permitido:** Python 3.11–3.12, pandas, GeoPandas, multiprocessing, PySpark 3.5 (**solo modo local**), Power BI, Packet Tracer, GitHub, NASA FIRMS, datos meteorológicos abiertos.

**Prohibido:** apps móviles o web, Kubernetes, microservicios, nube, Docker multinodo, deep learning, modelos de IA complejos, Apache Sedona.

## 4. Decisiones que NO se rediscuten

| Tema | Decisión |
|---|---|
| Ventana temporal | `2022-01-01 → 2026-10-31` (ADR-0004). Se lee de `settings.yaml`. **Nunca se escriben años en el código.** |
| Satélites | VIIRS S-NPP (`N`) + NOAA-20 (`N20`). Sin MODIS ni NOAA-21. |
| Calidad FIRMS | Reglas F0–F8. `type == 0` en SP; `confidence != 'l'`; SP gana sobre NRT. Máscara de fuentes estáticas (celdas de 0,005°): `type == 2` en SP 2022–2025 **∪ celdas persistentes** (≥ 6 meses distintos y ≥ 3 años distintos, ADR-0005), sin dilatación. Marca, no descarta. |
| Clima | Open-Meteo `era5_seamless`, **horario** (5 variables). Spark agrega a diario (ADR-0003). |
| Cruce espacial | GeoPandas (STRtree) dentro de `multiprocessing`, sobre **Colombia completa** (ADR-0002). Nunca en Spark. |
| Paralelo | `Pool` con `spawn`, *initializer*, bloques de 25.000 filas en orden LPT, 4 procesos en producción. Ningún (satélite, fecha) se reparte entre bloques. |
| Spark | `local[*]` en una sola máquina; 8 particiones de shuffle en producción; solo funciones nativas (sin UDF de Python). |
| IRAT | Pesos H 0,30 · P 0,25 · D 0,15 · T 0,15 · R 0,10 · W 0,05. Anomalías = diferencia frente a la media 2022–2025 (**sin z-score**). H = 50 % LOYO + 50 % densidad de los 3 meses anteriores. Rango percentil; categorías por quintiles con cortes exactos (no `ntile`). K5 se reporta en el orden "solo clima → oficial". |
| Modelo BI | Estrella con una sola `dim_mes`; Power BI lee los **Parquet** de `data/gold/` (no los CSV) con el parámetro `RutaGold`. |

## 5. Convenciones de código (obligatorias)

1. **Códigos DIVIPOLA siempre como texto** (`cod_dpto` de 2 caracteres, `cod_mpio` de 5). Leer con `dtype=str`.
2. **Sin valores fijos en el código:** rutas, fechas, filtros, pesos y parámetros de Spark se leen de `config/settings.yaml`.
3. **Compatible con `spawn`:** las funciones que se pasan a `multiprocessing.Pool` viven en módulos `.py` a nivel superior; los scripts usan `if __name__ == "__main__":`. Los benchmarks nunca se corren desde Jupyter.
4. **Etapas idempotentes** con escritura atómica. Si falla un control bloqueante, la etapa no publica nada.
5. **Los benchmarks llaman al mismo código de producción.** Solo cambian `n_procesos` o `master`.
6. **Contratos de datos:** silver y gold coinciden con `docs/diccionario_datos.md`.
7. Estilo: PEP 8, *type hints* en las funciones públicas, docstrings en español, logging en lugar de `print` en `src/`.
8. Commits: `tipo(módulo): mensaje`. Tipos: `feat`, `fix`, `docs`, `bench`, `data`, `net`, `bi`, `chore`. Los PR se fusionan con "Create a merge commit".

## 6. Estructura y dueños

| Ruta | Dueño | Notas |
|---|---|---|
| `network/`, `docs/ieee/`, `docs/poster/` | A (Dafne) | Solo A hace *commit* del `.pkt` |
| `src/firewatch/ingest/`, `src/firewatch/parallel/`, `notebooks/`, `scripts/` | B (Yeison) | |
| `src/firewatch/spark/`, `dashboard/`, `docs/diccionario_datos.md` | C (Sara) | Solo C hace *commit* del `.pbix` |
| `config/`, `benchmarks/`, `docs/adr/` | B + C | Los cambios en `settings.yaml` se avisan en el grupo |

## 7. Comandos

```bash
pip install -r requirements.txt && pip install -e .
python scripts/run_pipeline.py --stage {silver|gold|all}   # gold requiere Java 17
python benchmarks/bench_multiprocessing.py
python benchmarks/bench_spark.py
python -m pytest -q
```

En Windows, si `git pull` falla con `Filename too long`: `git config core.longpaths true`.

## 8. Definición de "terminado"

1. Está en `main` mediante un *pull request* revisado por otro integrante.
2. Corre desde una terminal limpia siguiendo el README.
3. Sus números salen de un informe versionado y coinciden con el IEEE y el dashboard.

## 9. Qué NO debe hacer un asistente de IA en este repositorio

- Añadir dependencias fuera de `requirements.txt` sin un ADR.
- Cambiar los esquemas de `docs/diccionario_datos.md` o los pesos del IRAT.
- Escribir años, rutas o umbrales directamente en el código.
- Subir datos (`data/**`) o binarios ajenos a su dueño.
- Mover o reescribir el *tag* `v1.0-entrega`.
- Presentar las detecciones FIRMS como "incendios confirmados" en textos o en el dashboard.
