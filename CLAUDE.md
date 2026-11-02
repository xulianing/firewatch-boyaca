# CLAUDE.md — Contexto permanente de FireWatch Boyacá

> Este archivo lo leen los asistentes de IA (Claude Code u otros) y los integrantes nuevos antes de tocar el repositorio.
> **Si algo aquí contradice una petición, se pregunta antes de actuar.**

## 1. Qué es el proyecto

Proyecto Integrador de cuarto semestre (Ingeniería de Datos e IA). Integra tres asignaturas en un solo sistema:
- **Redes e Infraestructura:** topología en Packet Tracer, VLSM, OSPF.
- **Computación Paralela y Distribuida:** `multiprocessing` (L2) + PySpark local (L3), con speedup y eficiencia medidos.
- **Analítica y Visualización:** EDA en Matplotlib/Seaborn + dashboard en Power BI.

**Tema:** anomalías térmicas (NASA FIRMS) + clima (ERA5 vía Open-Meteo) → Índice de Riesgo Relativo de Anomalías Térmicas (IRAT) por municipio y mes en Boyacá. ODS 13.

**Fecha límite:** todo entregable el jueves 12 de noviembre de 2026; entrega el 16 (a confirmar, porque ese lunes es festivo).

## 2. Estado: ARQUITECTURA Y ALCANCE CONGELADOS

- No se proponen tecnologías, fuentes de datos ni funcionalidades nuevas, **salvo que exista un riesgo crítico**. En ese caso se escribe un ADR en `docs/adr/` y se requiere la aprobación de los otros dos integrantes.
- Documentos de referencia: `docs/arquitectura.md`, `docs/diccionario_datos.md`, `docs/plan/plan_implementacion.md`, `docs/adr/`.

## 3. Restricciones duras

**Permitido:** Python 3.11, pandas, GeoPandas, multiprocessing, PySpark 3.5 (**solo modo local**), Power BI, Packet Tracer, GitHub, NASA FIRMS, datos meteorológicos abiertos.

**Prohibido:** apps móviles o web, Kubernetes, microservicios, nube, Docker multinodo, deep learning, modelos de IA complejos, Apache Sedona.

## 4. Decisiones que NO se rediscuten

| Tema | Decisión |
|---|---|
| Ventana temporal | `2022-01-01 → 2026-10-31`. Se lee de `settings.yaml`. **Nunca se escriben años en el código.** |
| Satélites | VIIRS S-NPP (`N`) + NOAA-20 (`N20`). Sin MODIS ni NOAA-21. |
| Calidad FIRMS | `type == 0` en SP; máscara de fuentes estáticas (celdas de ~0,005° con `type == 2` en SP 2022–2025) aplicada a todo, incluido NRT 2026; `confidence != 'l'`. |
| Clima | Open-Meteo **horario** (5 variables). Spark agrega a diario (ADR-0003). |
| Cruce espacial | GeoPandas dentro de `multiprocessing`, sobre **Colombia completa**. Nunca en Spark. |
| Spark | Modo `local[N]`; una sola máquina (la de C) para la ejecución oficial y los benchmarks. |
| IRAT | Pesos H 0,30 · P 0,25 · D 0,15 · T 0,15 · R 0,10 · W 0,05. Anomalías = diferencia frente a la media 2022–2025 (**sin z-score**). H = 50 % LOYO + 50 % densidad de los 3 meses anteriores. Normalización por rango percentil. Categorías por quintiles. |
| Modelo BI | Estrella con una sola `dim_mes`; todas las tablas de hechos se relacionan por `fecha_mes`. |

## 5. Convenciones de código (obligatorias)

1. **Códigos DIVIPOLA siempre como texto** (`cod_dpto` de 2 caracteres, `cod_mpio` de 5). Leer con `dtype=str`. Un código numérico pierde el cero inicial y rompe los joins.
2. **Sin valores fijos en el código:** rutas, fechas, filtros, pesos y parámetros de Spark se leen de `config/settings.yaml` a través de `firewatch.config`.
3. **Compatible con `spawn`:** las funciones que se pasan a `multiprocessing.Pool` viven en módulos `.py` a nivel superior; los scripts usan `if __name__ == "__main__":`. Los benchmarks nunca se corren desde Jupyter.
4. **Etapas idempotentes:** volver a ejecutar una etapa sobrescribe su salida sin efectos colaterales.
5. **Los benchmarks llaman al mismo código de producción.** Solo cambian `n_procesos` o `master`.
6. **Contratos de datos:** cualquier tabla silver o gold debe coincidir con `docs/diccionario_datos.md`. Si el esquema cambia, primero se actualiza ese documento, y después del data freeze no se cambia.
7. Estilo: PEP 8, *type hints* en las funciones públicas, docstrings en español, logging en lugar de `print` en `src/`.
8. Commits: `tipo(módulo): mensaje` (por ejemplo, `feat(parallel): runner con Pool e initializer`). Tipos: `feat`, `fix`, `docs`, `bench`, `data`, `net`, `bi`, `chore`.

## 6. Estructura y dueños

| Ruta | Dueño | Notas |
|---|---|---|
| `network/`, `docs/ieee/`, `docs/poster/` | A | Solo A hace *commit* de `.pkt` y de los fuentes del IEEE |
| `src/firewatch/ingest/`, `src/firewatch/parallel/`, `notebooks/`, `scripts/`, `docs/demo/` | B | |
| `src/firewatch/spark/`, `dashboard/`, `docs/diccionario_datos.md` | C | Solo C hace *commit* de `.pbix` |
| `config/`, `src/firewatch/quality/`, `benchmarks/` | B + C | Los cambios en `settings.yaml` se avisan en el grupo |

## 7. Comandos

```bash
pip install -r requirements.txt && pip install -e .
python scripts/download_all.py
python scripts/run_pipeline.py --stage {silver|gold|all}
python benchmarks/bench_multiprocessing.py
python benchmarks/bench_spark.py
pytest -q
```

## 8. Definición de "terminado"

Una tarea está terminada cuando:
1. Está en `main` mediante un *pull request* revisado por otro integrante.
2. Cumple su criterio de aceptación del plan de implementación.
3. Corre desde una terminal limpia siguiendo el README.
4. Sus números coinciden con los que irán al IEEE.

## 9. Hitos

| Fecha | Hito |
|---|---|
| Mié 4, 20:00 | Esqueleto de punta a punta (silver → Spark → gold → Power BI con mapa) |
| Sáb 7, 18:00 | **DATA FREEZE**: *tag* `data-freeze`; el esquema gold no se cambia después |
| Lun 9 | Dashboard funcional |
| Mié 11 | IEEE en inglés v1.0 |
| Jue 12, 22:00 | Todo entregable + *tag* `v1.0` |

## 10. Qué NO debe hacer un asistente de IA en este repositorio

- Añadir dependencias fuera de `requirements.txt` sin un ADR.
- Cambiar los esquemas de `docs/diccionario_datos.md` o los pesos del IRAT.
- Escribir años, rutas o umbrales directamente en el código.
- Subir datos (`data/**`) o binarios ajenos a su dueño.
- Presentar las detecciones FIRMS como "incendios confirmados" en textos o en el dashboard.
