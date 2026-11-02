"""L3 · Agregado municipio-mes y línea base 2022–2025 (int.linea_base).

Decisión ADR-0004: solo medias (sin desviación estándar, porque n = 4); anomalías como
diferencia simple.

A implementar:
    agregar_municipio_mes(municipio_dia) -> DataFrame
        n_det, frp_total, frp_medio, det_100km2, precip_mes, tmax_media, rh_min_media,
        wind_max_media, racha_seca_max.
    construir_linea_base(municipio_mes) -> DataFrame
        Grano municipio × mes calendario (123 × 12). Medias de los años de
        ventana.anios_linea_base; densidad total y por año (para LOYO).
    calcular_anomalias(municipio_mes, linea_base) -> DataFrame
        anom_precip, anom_tmax, anom_rh; H_loyo = (total − año) / (n_años − 1);
        det_3m_previos con lag sobre los meses anteriores.

Dueño: C · Tarea: C6
"""
