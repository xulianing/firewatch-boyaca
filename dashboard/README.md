# dashboard/ — Power BI (R7) · dueño único: C

| Archivo | Contenido | Tarea | Fecha |
|---|---|---|---|
| `firewatch.pbix` | Dashboard (solo C hace *commit*) | C5 → C10 → C11 | Mié 4 (esqueleto) · Lun 9 (funcional) · Mar 10 (final) |
| `boyaca_municipios.topojson` | Plan B del mapa (Shape Map) | C5 | Mié 4, solo si falla el mapa original |
| `medidas.md` | Definición de cada medida DAX (nombre, fórmula en palabras, KPI) | C10 | Lun 9 |
| `theme.json` | Tema de colores y tipografía | C11 | Mar 10 |
| `capturas/` | Una captura por página (para IEEE, póster y video) | C11 | Mar 10 |

## Modelo de datos (estrella — Arquitectura §10)

- **Origen:** los CSV de `data/gold/` (ruta relativa configurada como parámetro de Power Query `RutaGold`, para que funcione en cualquier equipo).
- **Dimensiones:** `dim_mes` (única dimensión temporal), `dim_municipio` (jerarquía provincia → municipio), `dim_departamento`.
- **Hechos:** `fact_municipio_mes`, `fact_deteccion`, `agg_depto_mes`.
- **Relaciones:** 1:N, filtro simple de la dimensión al hecho. Sin relaciones muchos-a-muchos.
- **Tipos:** `cod_mpio` y `cod_dpto` como **Texto** en Power Query (nunca como número entero).

## Páginas (storytelling: qué pasa → dónde pasa → por qué y qué hacer)

| Página | Visuales | Interactividad |
|---|---|---|
| 1. Panorama | Tarjetas K1, K2, K3, K6 · línea mensual · matriz mes × año · barras por provincia | Segmentadores de año y provincia; drill-down año → mes |
| 2. Territorio | Mapa de puntos o coroplético · top 10 de municipios · dispersión altitud vs detecciones | Drill-down provincia → municipio; filtro cruzado |
| 3. Clima y riesgo | Tarjetas K4, K5 · mapa por categoría IRAT · combinado precipitación/detecciones · tabla de municipios en riesgo alto | Segmentador de mes; tooltip H/P/D/T/R/W; marcadores (P1) |

## Convenciones

- Medidas en una tabla vacía `_Medidas`, con prefijo `K1 `, `K2 `… para los KPI.
- Títulos de los visuales en forma de pregunta o hallazgo, no como nombres de campo.
- Texto obligatorio en la página 1: *"Las detecciones FIRMS son anomalías térmicas, no incendios confirmados."*
- Los meses con datos NRT (2026) se marcan con una nota en la página 1.
