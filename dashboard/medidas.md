# Medidas DAX — definición funcional (C completa la fórmula al implementar, tarea C10)

| Medida | KPI | Definición | Página |
|---|---|---|---|
| `K1 Detecciones` | K1 | Suma de `fact_municipio_mes[n_det]` | 1 |
| `K2 Variación interanual %` | K2 | Detecciones de enero al mes M del año seleccionado frente a enero–mes M del año anterior (año corrido, mismo periodo) | 1 |
| `K3 FRP medio (MW)` | K3 | Promedio de `fact_deteccion[frp]` | 1 |
| `K4 Municipios riesgo alto` | K4 | Conteo distinto de `cod_mpio` con `irat_categoria` ∈ {Alto, Muy alto} en el último mes del filtro | 3 |
| `K5 Tasa de captura %` | K5 | Detecciones en municipio-mes Alto/Muy alto ÷ total de detecciones | 3 |
| `K6 Participación Boyacá %` | K6 | Detecciones de Boyacá ÷ suma de `agg_depto_mes[n_det]` (todos los departamentos) | 1 |

**Control de calidad:** el valor de `K1 Detecciones` sin filtros debe coincidir con el conteo de silver para Boyacá (reconciliación, Arquitectura §3.1).
