# ADR-0003 · Clima horario agregado a diario en Spark

**Contexto:** no está confirmado que Open-Meteo entregue la humedad relativa mínima como variable diaria. Además, R4 exige que Spark procese al menos 100.000 registros o 10 MB.

**Decisión:** descargar las 5 variables (`temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`, `et0_fao_evapotranspiration`) en resolución **horaria**. Spark calcula todos los agregados diarios (máximos, mínimos, sumas y medias).

**Alternativas descartadas:** variables diarias más humedad horaria (dos formatos de descarga); agregar en pandas (le quitaría a Spark su carga principal).

**Consecuencias:** ≈ 5,2 M filas horarias (123 municipios × 42.360 horas) → R4 cubierto sin depender de FIRMS; la agregación horario→diario es la etapa principal del benchmark de Spark. Riesgo: límites de la API → pausas, caché y reanudación.

**Fecha:** 2026-11-02 · Aprobado: A ☐ B ☐ C ☐
