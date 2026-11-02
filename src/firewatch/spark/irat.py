"""L3 · Índice de Riesgo Relativo de Anomalías Térmicas (IRAT) — Arquitectura §7 (CONGELADO).

Componentes (rango percentil en [0,1]; mayor valor = mayor riesgo):
    H  0,30  50 % LOYO del mismo mes + 50 % densidad de los 3 meses anteriores
    P  0,25  −anom_precip
    D  0,15  racha_seca_max
    T  0,15  anom_tmax
    R  0,10  −anom_rh
    W  0,05  wind_max_media

A implementar:
    calcular_componentes(df) -> DataFrame       c_H … c_W con percent_rank sobre todo el panel
    calcular_irat(df, pesos: dict) -> DataFrame  irat = Σ peso·componente
    categorizar(df) -> DataFrame                 quintiles → irat_categoria, irat_orden (1–5)
    variantes_sensibilidad(df) -> DataFrame      pesos iguales y "solo clima" (para B9)

Pesos y categorías: settings.yaml → irat. Nunca escritos en el código.
Dueño: C · Tarea: C6 (la validación la hace B en B9)
"""
