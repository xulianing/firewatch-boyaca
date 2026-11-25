# Entorno de ejecución oficial (entregables C3 y B7)

Datos de la configuración experimental del IEEE. Hay **dos máquinas**: el benchmark de `multiprocessing` (L2)
corrió en la de B y el de Spark (L3) en la de C. **Se comparan las curvas de speedup y eficiencia, no los
segundos**, porque el hardware es distinto.

Fuentes: `benchmarks/results/mp_entorno_20261121_0947.json` (B) y `benchmarks/results/spark_entorno_20261120_1949.json` (C).
"No registrado" = el dato no quedó en ningún archivo del repo.

| Elemento | Máquina de B · `multiprocessing` (L2) | Máquina de C · Spark (L3) |
|---|---|---|
| Fecha del registro | 2026-11-21 09:47 | 2026-11-20 19:49 |
| CPU | 11th Gen Intel Core i5-1135G7 @ 2.40 GHz | Intel64 Family 6 Model 154 (Alder Lake; i5-1235U según C) |
| Núcleos físicos / lógicos | 4 / 8 | No registrado / 12 |
| RAM | 7,8 GB | No registrado en el JSON (15,7 GB según captura de C) |
| Disco | No registrado | No registrado |
| Sistema operativo | Windows 11 (10.0.26200) | Windows 11 (10.0.26200) |
| Python | 3.12.7 (venv) | 3.12.7 (venv) |
| Java | — (no se usa en L2) | 17.0.20.1 |
| PySpark | — | 3.5.5 (`driver_memory` 4g) |
| numpy / pandas | 1.26.4 / 2.2.3 | Según `requirements.txt` |
| GeoPandas / Shapely | 1.0.1 / 2.0.6 | — |
| Método de inicio de procesos | `spawn` | — |
| Power BI Desktop | No registrado | No registrado |
| Packet Tracer (máquina de A) | No registrado | — |

## Protocolo de los benchmarks

| | L2 · `bench_multiprocessing.py` | L3 · `bench_spark.py` |
|---|---|---|
| Operación medida | L2 completa (`runner.run`): 960.830 filas raw, bloques de 25.000 | `build_daily.clima_diario`: 5.210.280 → 217.095 filas |
| Configuraciones | p ∈ {1, 2, 4, 8} | `local[1, 2, 4, *]` × particiones {4, 8, 16, 200} |
| Repeticiones | 1 de calentamiento + 5 medidas; mediana | 1 de calentamiento + 5 medidas; mediana; AQE apagado |
| Control de corrección | `content_sha256` idéntico en las 20 corridas | Huella de contenido independiente del particionado |

## Problemas de entorno resueltos

| Síntoma | Causa | Solución |
|---|---|---|
| `Did not find winutils.exe` (C) | Hadoop en Windows necesita binarios nativos | `winutils.exe` + `hadoop.dll` (Hadoop 3.3.6) en `C:\hadoop\bin`; `HADOOP_HOME=C:\hadoop` |
| `Python worker exited unexpectedly` (C) | Python 3.12 eliminó `distutils` y PySpark 3.5 lo importa | `python -m pip install setuptools` |
| `No module named 'firewatch'` / `'yaml'` (B y C) | Se usaba el Python global (3.14), no el del venv | Activar el venv (`(.venv)` en el prompt) y usar siempre `python -m ...` |
