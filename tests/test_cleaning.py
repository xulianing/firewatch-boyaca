"""Pruebas de L2 (a implementar por B junto con B4/B6).

Casos mínimos:
    - limpiar_bloque descarta lat/lon fuera de la caja de Colombia y frp <= 0.
    - Deduplicación por (latitude, longitude, acq_date, acq_time, satellite).
    - filtrar_calidad excluye confidence 'l' y marca fuente_estatica según la máscara.
    - Los códigos cod_dpto y cod_mpio quedan como texto con el cero inicial ("05001").
    - Igualdad de salida: ejecutar_silver(1) y ejecutar_silver(4) producen el mismo
      conjunto de id_det (sobre una muestra pequeña de prueba).
"""
