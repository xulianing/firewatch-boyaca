"""L3 · Demostración de tolerancia a fallos (P1) sin clúster.

Idea: master "local[4,3]" (4 hilos, hasta 3 intentos por tarea). Una transformación
provoca un fallo SOLO en el primer intento de una partición concreta
(TaskContext.get().attemptNumber() == 0). Spark reejecuta la tarea a partir del linaje
del RDD y el job termina bien. Evidencia: captura de la Spark UI (pestaña Stages,
columna de intentos) guardada en docs/ieee/figuras/.

A implementar:
    ejecutar_demo() -> None

Dueño: C · Tarea: C8
"""
