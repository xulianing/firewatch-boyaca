# dashboard/ — Power BI (R7) · autora: Sara León (C)

| Archivo | Contenido |
|---|---|
| `firewatch.pbix` | Dashboard final de 3 páginas + página oculta "Control" |
| `medidas.md` | Modelo, medidas DAX K1–K6 con su justificación, colores y valores de control |
| `capturas/` | Capturas de las páginas (`01_panorama.png`, `02_territorio.png`, `03_riesgo_clima.png`) y prototipos |

Las detecciones FIRMS son anomalías térmicas, no incendios confirmados.

## Cómo abrirlo

1. Descargar las tablas gold (OneDrive, enlace en el README principal, §6) en `data/gold/`.
2. Abrir `firewatch.pbix` y, en Transformar datos → Parámetros, apuntar `RutaGold` a la carpeta `data\gold\` local.
3. Actualizar. La página oculta "Control" debe mostrar K1 = 3.908 sin filtros.

## Modelo de datos

- **Origen:** los **Parquet** de `data/gold/` (conector Parquet). No se usan los CSV: con la configuración regional de Colombia alteran los decimales y convierten `"05"` en `5`.
- **Dimensiones:** `dim_mes` (única dimensión temporal), `dim_municipio`, `dim_departamento`.
- **Hechos:** `fact_municipio_mes`, `fact_deteccion`, `agg_depto_mes`, más `irat_sensibilidad` para K5 solo clima.
- **Relaciones:** varios a uno, filtro simple de la dimensión al hecho. Sin relaciones entre hechos ni muchos a muchos. `agg_depto_mes` solo se relaciona con `dim_mes` y `dim_departamento`, por eso K6 compara siempre con el país.
- **Tipos:** `cod_mpio` y `cod_dpto` como texto.

Detalle de cada medida en [`medidas.md`](medidas.md).

## Páginas

| Página | Pregunta | Visuales |
|---|---|---|
| 1. Panorama | ¿Cuánta actividad térmica hay y cómo evoluciona? | K1–K6; detecciones por mes; por año (2026 hasta octubre); estacionalidad por mes del año |
| 2. Territorio | ¿Dónde se concentra? | Mapa (Azure Maps) de las 3.908 detecciones (tamaño = FRP, color día/noche); top 10 de municipios por densidad; matriz por municipio con K1, densidad, IRAT medio y FRP total |
| 3. Riesgo y clima | ¿Dónde y cuándo el riesgo es alto, y qué tan bien ordena el índice? | K5 solo clima, K5, K4, IRAT medio; mapa de calor municipio × mes por IRAT; proporción de categorías por año; lluvia media frente a detecciones |

**Interactividad:** segmentadores de año y municipio sincronizados entre páginas, filtro cruzado entre visuales y drill-down por la jerarquía `año_mes`.

## Valores de control (sin filtros)

| KPI | Valor |
|---|---|
| K1 Detecciones | 3.908 |
| K2 Variación interanual (año corrido) | +39,6 % (ene–oct 2026: 621 frente a 445) |
| K3 FRP medio | 9,41 MW |
| K4 Municipios en riesgo alto (oct 2026) | 25 |
| K5 Tasa de captura solo clima / oficial | 60,7 % / 84,7 % |
| K6 Participación de Boyacá | 0,46 % (3.908 / 855.771) |

Coinciden con `docs/evidencias/gold_validation.json` e `irat_validation.json`.

## Convenciones

- Medidas en la tabla `_Medidas`, con prefijo `K1 `, `K2 `… para los KPI.
- Un color tiene un solo significado en todo el informe: naranja = detecciones diurnas, morado = nocturnas, azul = lluvia, amarillo a rojo oscuro = categorías del IRAT.
- Agosto a octubre de 2026 son datos NRT (provisionales).
