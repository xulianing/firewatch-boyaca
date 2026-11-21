# benchmarks/ — medición de rendimiento (R3, R4, R5)

Los resultados en `results/` y las figuras en `figures/` **se versionan**: son la evidencia de la sección VI del IEEE y deben coincidir con ella.

## Protocolo (igual para los dos benchmarks)

1. Cada benchmark se mide completo en **una sola máquina**: multiprocessing (L2) en el equipo de B y Spark (L3) en el de C, porque el código y los datos de cada etapa viven allí. El speedup y la eficiencia son relativos a cada máquina, así que lo que se compara entre los dos benchmarks es la forma de las curvas, no los segundos. Cada corrida guarda su entorno (`mp_entorno_*.json`, `spark_entorno_*.json`) y el resumen va a `docs/notas/entorno.md`.
2. Conectada a la corriente, en modo alto rendimiento, sin navegador ni otras aplicaciones abiertas, de noche.
3. 1 corrida de calentamiento descartada y **5 repeticiones** por configuración. Se reportan la media y la desviación estándar.
4. Se mide **el mismo código de producción** (`runner.ejecutar_silver`, etapas de `spark/`); solo cambian `n_procesos` o `master`.

## bench_multiprocessing.py (L2) — dueño B, tarea B7

| Experimento | Configuración | Métricas |
|---|---|---|
| Escalado fuerte | Dataset completo; p = 1, 2, 4, 8 | T(p), speedup S = T1/Tp, eficiencia E = S/p, Karp–Flatt e = (1/S − 1/p)/(1 − 1/p) |
| Escalado débil | Tamaño ∝ p (p bloques de igual tamaño) | Eficiencia débil = T1 / Tp |

Escalado débil: solo la Fase 1 (`runner.ejecutar_fase1`, la misma función que usa `run`); la Fase 0 siempre lee todo el raw y no escala con p.
Control: el `content_sha256` debe ser idéntico en todas las corridas del escalado fuerte; si no, el script aborta.

Uso: `python benchmarks/bench_multiprocessing.py --rapido` (prueba de humo) · `python benchmarks/bench_multiprocessing.py` (oficial).

Salidas: `mp_entorno_<sello>.json`, `mp_bench_crudo_<sello>.csv`, `mp_bench_resumen_<sello>.csv`, `mp_bench_debil_<sello>.csv` · figuras `mp_speedup_eficiencia_<sello>.png`, `mp_karp_flatt_<sello>.png`, `mp_escalado_debil_<sello>.png`.

## bench_spark.py (L3) — dueño C, tarea C7

| Experimento | Configuración | Métricas |
|---|---|---|
| Núcleos | `local[1]`, `local[2]`, `local[4]`, `local[*]` con la etapa horario→diario y el gold completo | T, S, E |
| Particiones | `spark.sql.shuffle.partitions` ∈ {4, 8, 16, 200} | T (efecto del overhead) |
| Referencia | La misma agregación horario→diario en pandas | T pandas frente a T Spark |

Figuras: `spark_speedup.png`, `spark_particiones.png`, `spark_vs_pandas.png`.

## Esquema de los CSV en `results/`

```
fecha_hora, experimento, etapa, configuracion, repeticion, n_filas, tiempo_s, maquina
```
