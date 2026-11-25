# Dashboard FireWatch Boyacá · Modelo y medidas DAX

Archivo: `dashboard/firewatch.pbix` · Fuente: tablas gold congeladas (`data/gold/`, etiqueta `data-freeze`) · Autora: Sara León (integrante C)

Las detecciones FIRMS son anomalías térmicas, no incendios confirmados.

## 1. Origen de datos

- Conector Parquet de Power BI. No se usan los CSV: con la configuración regional de Colombia, los CSV alteran los decimales y convierten `"05"` en `5`.
- Parámetro `RutaGold` (texto) con la carpeta de gold, por ejemplo `C:\Users\cris6\firewatch-boyaca\data\gold\`. Ninguna consulta tiene la ruta escrita; para abrir el informe en otro equipo solo se cambia el parámetro.
- Cada tabla se carga con una consulta en blanco de la forma:

```
let
    Origen = Parquet.Document(File.Contents(RutaGold & "fact_municipio_mes.parquet")),
    Tipos = Table.TransformColumnTypes(Origen, {{"cod_mpio", type text}, {"fecha_mes", type date}, ...})
in
    Tipos
```

- `cod_mpio` y `cod_dpto` se fuerzan a texto (códigos DIVIPOLA, decisión C25).
- `provincia` y `cod_provincia` se fuerzan a texto aunque lleguen vacías. Cuando se publique `dim_municipio` con provincias, basta con Actualizar: el archivo conserva el nombre y el modelo no cambia.

| Tabla | Archivo | Filas |
|---|---|---|
| `fact_municipio_mes` | `fact_municipio_mes.parquet` | 7.134 |
| `fact_deteccion` | `fact_deteccion.parquet` | 3.908 |
| `agg_depto_mes` | `agg_depto_mes.parquet` | 1.914 |
| `irat_sensibilidad` | `_analisis/irat_sensibilidad.parquet` | 7.134 |
| `dim_mes` | `dim_mes.parquet` | 58 |
| `dim_municipio` | `dim_municipio.parquet` | 123 |
| `dim_departamento` | `dim_departamento.parquet` | 33 |

## 2. Modelo estrella

Relaciones varios a uno, filtro cruzado único (de la dimensión al hecho), todas activas:

| Dimensión (lado 1) | Hecho (lado N) |
|---|---|
| `dim_mes[fecha_mes]` | `fact_municipio_mes[fecha_mes]`, `fact_deteccion[fecha_mes]`, `agg_depto_mes[fecha_mes]`, `irat_sensibilidad[fecha_mes]` |
| `dim_municipio[cod_mpio]` | `fact_municipio_mes[cod_mpio]`, `fact_deteccion[cod_mpio]`, `irat_sensibilidad[cod_mpio]` |
| `dim_departamento[cod_dpto]` | `agg_depto_mes[cod_dpto]` |

- `dim_mes` es la única dimensión de tiempo. La opción "Fecha y hora automáticas" está desactivada, para que Power BI no cree tablas de fechas ocultas.
- No hay relaciones entre hechos ni entre `dim_municipio` y `dim_departamento`. `agg_depto_mes` es nacional y no debe filtrarse por municipio; por eso K6 compara siempre con el país.
- Las claves de los hechos están ocultas en la vista de informe: todo filtro sale de una dimensión.
- Orden: `nombre_mes` por `mes`; `irat_categoria` por `irat_orden`.
- Jerarquías: `año_mes` (anio → nombre_mes) y `provincia_municipio` (provincia → nombre).
- `lat`, `lon`, `lat_c`, `lon_c` con categoría Latitud/Longitud; columnas no aditivas (año, mes, trimestre, irat_orden, hora, coordenadas, área, elevación, vigencia) en "No resumir".

## 3. Medidas KPI (tabla `_Medidas`)

### K1 Detecciones
```DAX
K1 Detecciones =
SUM ( fact_municipio_mes[n_det] )
```
Sale de la tabla que contiene el IRAT, para que K1 y K5 respondan siempre a los mismos filtros. Coincide con `count(fact_deteccion)` (conciliación G-3).

### K2 Variación interanual % (año corrido)
```DAX
K2 Variación interanual % =
VAR anioSel = MAX ( dim_mes[anio] )
VAR mesSel =
    CALCULATE ( MAX ( dim_mes[mes] ), dim_mes[anio] = anioSel )
VAR actual =
    CALCULATE ( [K1 Detecciones], REMOVEFILTERS ( dim_mes ),
                dim_mes[anio] = anioSel, dim_mes[mes] <= mesSel )
VAR previo =
    CALCULATE ( [K1 Detecciones], REMOVEFILTERS ( dim_mes ),
                dim_mes[anio] = anioSel - 1, dim_mes[mes] <= mesSel )
RETURN
    DIVIDE ( actual - previo, previo )
```
Compara enero hasta el mes M del año seleccionado con el mismo periodo del año anterior, porque 2026 solo llega a octubre. No se usa `SAMEPERIODLASTYEAR` porque exige una tabla de fechas diaria y `dim_mes` es mensual. En 2022 queda en blanco (no hay año anterior).

### K3 FRP medio (MW)
```DAX
K3 FRP medio (MW) =
AVERAGE ( fact_deteccion[frp] )
```
Promedia las detecciones individuales. Promediar `frp_medio` mensual daría un promedio de promedios.

### K4 Municipios riesgo alto
```DAX
K4 Municipios riesgo alto =
VAR ultimo = MAX ( dim_mes[fecha_mes] )
RETURN
    CALCULATE (
        DISTINCTCOUNT ( fact_municipio_mes[cod_mpio] ),
        dim_mes[fecha_mes] = ultimo,
        fact_municipio_mes[irat_orden] >= 4
    )
```
Municipios en Alto (4) o Muy alto (5) en el último mes del filtro. Responde a dónde actuar ahora.

### K5 Tasa de captura %
```DAX
K5 Tasa de captura % =
DIVIDE (
    CALCULATE ( [K1 Detecciones], KEEPFILTERS ( fact_municipio_mes[irat_orden] >= 4 ) ),
    [K1 Detecciones]
)
```
Fracción de las detecciones que ocurrieron en municipio-mes clasificados Alto o Muy alto. `KEEPFILTERS` respeta los segmentadores del usuario.

### K5 Tasa de captura solo clima %
```DAX
K5 Tasa de captura solo clima % =
DIVIDE (
    CALCULATE ( SUM ( irat_sensibilidad[n_det] ), KEEPFILTERS ( irat_sensibilidad[orden_solo_clima] >= 4 ) ),
    SUM ( irat_sensibilidad[n_det] )
)
```
La misma tasa con la variante del IRAT sin el componente histórico H. Se muestra antes que K5 oficial (decisión C42): H se construye con detecciones pasadas, así que la diferencia entre ambas está inflada por circularidad.

### K6 Participación Boyacá %
```DAX
K6 Participación Boyacá % =
VAR dpto =
    CALCULATE ( MIN ( dim_municipio[cod_dpto] ), REMOVEFILTERS ( dim_municipio ) )
VAR boyaca =
    CALCULATE ( SUM ( agg_depto_mes[n_det] ),
                REMOVEFILTERS ( dim_departamento ), dim_departamento[cod_dpto] = dpto )
VAR pais =
    CALCULATE ( SUM ( agg_depto_mes[n_det] ), REMOVEFILTERS ( dim_departamento ) )
RETURN
    DIVIDE ( boyaca, pais )
```
El código del departamento no está escrito en la fórmula: se toma de `dim_municipio` (misma regla que `settings.yaml`, decisión C26). Respeta los filtros de tiempo.

## 4. Medidas auxiliares

```DAX
Densidad media (det/100 km²) = AVERAGE ( fact_municipio_mes[det_100km2] )
IRAT medio = AVERAGE ( fact_municipio_mes[irat] )
Precipitación media (mm) = AVERAGE ( fact_municipio_mes[precip_mes] )
FRP total (MW) = SUM ( fact_deteccion[frp] )
Celdas = COUNTROWS ( fact_municipio_mes )
Orden IRAT = MAX ( fact_municipio_mes[irat_orden] )
```
- La densidad compara municipios de distinto tamaño (un municipio grande acumula más detecciones por superficie).
- La precipitación se promedia porque sumar la lluvia de 123 municipios no tiene sentido físico.

## 5. Medidas de color

```DAX
Color día/noche =
IF ( SELECTEDVALUE ( fact_deteccion[daynight] ) = "N", "#756BB1", "#D95F0E" )
```
```DAX
Color IRAT =
SWITCH ( [Orden IRAT],
    1, "#FFFFB2", 2, "#FECC5C", 3, "#FD8D3C", 4, "#F03B20", 5, "#BD0026" )
```
Se aplican con formato condicional "Valor de campo". Un color tiene un solo significado en todo el informe: naranja = detecciones (día), morado = noche, azul = lluvia, escala amarillo a rojo oscuro = categorías del IRAT (paleta YlOrRd, la misma del EDA).

## 6. Valores de control (sin filtros)

Deben coincidir con `docs/evidencias/gold_validation.json` e `irat_validation.json`. Si no coinciden, el error está en el modelo o en una medida.

| Medida | Valor |
|---|---|
| K1 Detecciones | 3.908 |
| K2 Variación interanual % | 39,6 % (ene–oct 2026: 621 frente a 445) |
| K3 FRP medio (MW) | 9,41 |
| K4 Municipios riesgo alto | 25 (octubre de 2026) |
| K5 Tasa de captura solo clima % | 60,7 % |
| K5 Tasa de captura % | 84,7 % |
| K6 Participación Boyacá % | 0,46 % (3.908 / 855.771) |
| FRP total (MW) | 36.758,2 |

La página oculta "Control" del `.pbix` muestra estas tarjetas sin filtros.

## 7. Páginas

| Página | Pregunta | Visuales |
|---|---|---|
| Panorama | ¿Cuánta actividad térmica hay y cómo evoluciona? | K1–K6; detecciones por mes; por año (2026 hasta octubre); estacionalidad por mes del año; segmentadores de año y municipio |
| Territorio | ¿Dónde se concentra? | Mapa Azure Maps de las 3.908 detecciones (tamaño = FRP, color día/noche); top 10 municipios por densidad; matriz Provincia → Municipio con K1, densidad, IRAT medio y FRP total |
| Riesgo y clima | ¿Dónde y cuándo el riesgo es alto, y qué tan bien acierta el índice? | K5 solo clima, K5, K4, IRAT medio; mapa de calor municipio × mes por IRAT; proporción de categorías por año; lluvia media frente a detecciones |

Interactividad: segmentadores sincronizados entre páginas, filtro cruzado entre visuales y drill-down por las jerarquías `año_mes` y `provincia_municipio`.
