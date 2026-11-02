"""FireWatch Boyacá: analítica geoespacial de anomalías térmicas y riesgo de incendios.

Subpaquetes (uno por capa de la arquitectura):
    ingest    L1  descarga reproducible de las fuentes a data/raw
    parallel  L2  limpieza + cruce espacial con multiprocessing → data/silver
    spark     L3  procesamiento distribuido con PySpark local → data/gold
    quality   transversal  controles de calidad entre capas
    utils     transversal  logging y medición de tiempos
"""

__version__ = "0.1.0"
