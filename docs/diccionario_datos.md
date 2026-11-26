# Diccionario de datos — contratos entre capas (entregable C2)

**Fuente de verdad** para todos los esquemas. Basado en Arquitectura §9 (v1.1). Si un esquema cambia, primero se actualiza este documento y se avisa en el grupo. **Después del data freeze (21 nov 2026, *tag* `data-freeze`) no se cambia.**

Convenciones: `cod_dpto` = texto de 2 caracteres · `cod_mpio` = texto de 5 caracteres · fechas en ISO (`YYYY-MM-DD`) · `fecha_mes` = primer día del mes · Parquet en silver; en gold, Parquet (lo que lee Power BI) y una copia CSV (`utf-8-sig`) solo para consulta.

---

## Silver (producido por B)

### `silver/detecciones` — Parquet particionado por `anio` · contrato B → C

| Columna | Tipo | Nulos | Descripción |
|---|---|---|---|
| `id_det` | string | no | Hash de (lat, lon, acq_date, acq_time, satellite) — único |
| `lat`, `lon` | double | no | WGS84 |
| `fecha` | date | no | `acq_date` (UTC; desfase despreciable, ver riesgo X5) |
| `hora_utc` | int | no | `acq_time` (HHMM) |
| `anio`, `mes` | int | no | Derivados de `fecha` |
| `daynight` | string | no | `D` / `N` |
| `confianza` | string | no | `n` / `h` (las `l` ya están filtradas) |
| `frp` | double | no | Fire Radiative Power (MW), > 0 |
| `bright_ti4` | double | sí | Temperatura de brillo I4 (K) |
| `satelite` | string | no | `N` (S-NPP) / `N20` (NOAA-20) |
| `proceso` | string | no | `SP` / `NRT` |
| `fuente_estatica` | boolean | no | `true` si cae en la máscara de fuentes estáticas (se excluye de gold) |
| `cod_dpto` | string(2) | sí | DIVIPOLA; nulo si el punto no cae en tierra colombiana |
| `cod_mpio` | string(5) | sí | DIVIPOLA |

### `silver/clima_horario` — Parquet particionado por `anio` · ~5,2 M filas

| Columna | Tipo | Descripción |
|---|---|---|
| `cod_mpio` | string(5) | Municipio (centroide consultado) |
| `ts_local` | timestamp | Hora local America/Bogota |
| `fecha` | date | Fecha local |
| `temp` | double | `temperature_2m` (°C) |
| `rh` | double | `relative_humidity_2m` (%) |
| `precip` | double | `precipitation` (mm) |
| `wind` | double | `wind_speed_10m` (km/h) |
| `et0` | double | `et0_fao_evapotranspiration` (mm) |

Clave única: (`cod_mpio`, `ts_local`).

### `silver/dim_municipio` — 123 filas

`cod_mpio` (PK) · `nombre` · `cod_provincia` · `provincia` · `area_km2` · `lat_c` · `lon_c` · `elevacion_m`

**Provincia (`provincia`, `cod_provincia`).** Origen: `config/provincias_boyaca.csv`, generado por `scripts/build_provincias_boyaca.py` (cruce por nombre con los códigos DIVIPOLA de `config/centroides_boyaca.csv`; se detiene si algún municipio no casa 1 a 1). `cod_provincia` = *slug* ASCII en mayúsculas de `provincia` (D-G4, la misma regla que `dane.slugify_provincia`). 15 unidades territoriales: 13 provincias + Distrito Fronterizo + Zona de Manejo Especial.

Fuentes oficiales (Gobernación de Boyacá):
1. Secretaría de Planeación, Dirección de Seguimiento y Planeación Territorial, *Informe de Resultados de la Nueva Medición del Desempeño Municipal de la Vigencia Fiscal 2021 de los Municipios del Departamento de Boyacá*, Tunja, dic. 2022, cuadros 4–8 (provincia de cada municipio). https://planeacion.boyaca.gov.co/wp-content/uploads/2023/01/Informe-Medicion-Desempeno-Municipal-2021-123-Mpios-Depto-Boyaca.pdf
2. Secretaría de Planeación, *Mapas PDF* (mapas de las 13 provincias, del Distrito Fronterizo y de la Zona de Manejo Especial). https://planeacion.boyaca.gov.co/mapas-boyaca/
3. Secretaría de Cultura, Observatorio Cultural, *Provincias de Boyacá* (13 provincias y 2 distritos especiales: Puerto Boyacá y Cubará). https://observatoriocultural.boyaca.gov.co/provincias-de-boyaca/

Notas: (a) la fuente 1 agrupa a Puerto Boyacá y Cubará como "Especial"; las fuentes 2 y 3 los separan, y así se publican: **Puerto Boyacá (15572) = Zona de Manejo Especial** y **Cubará (15223) = Distrito Fronterizo**. (b) Ventaquemada figura en **Márquez** en la fuente oficial (algunas fuentes no oficiales la ubican en Centro); manda la fuente oficial. (c) Consultadas el 24 nov 2026.

---

## Intermedias de Spark (no salen de L3)

| Tabla | Grano | Columnas clave |
|---|---|---|
| `int.clima_diario` | municipio × día (~217k) | `tmax`, `tmean`, `precip`, `rh_min`, `wind_max`, `et0` |
| `int.municipio_dia` | municipio × día (~217k) | clima diario + `n_det` (0 si no hubo) + `frp_sum` + `precip_30d` + `dia_seco` + `id_racha` |
| `int.linea_base` | municipio × mes calendario (123 × 12) | medias 2022–2025 de precip, tmax, rh_min; densidad total y por año |

---

## Gold (producido por C · consumido por Power BI y por los notebooks)

### `fact_municipio_mes` — 7.134 filas (123 × 58)

| Columna | Tipo | Descripción |
|---|---|---|
| `cod_mpio` | string | FK → `dim_municipio` |
| `fecha_mes` | date | FK → `dim_mes` |
| `n_det` | int | Detecciones de vegetación |
| `frp_total`, `frp_medio` | double | Intensidad (MW) |
| `det_100km2` | double | Densidad |
| `precip_mes`, `tmax_media`, `rh_min_media`, `wind_max_media` | double | Clima agregado |
| `racha_seca_max` | int | Días consecutivos con precipitación < 1 mm |
| `anom_precip`, `anom_tmax`, `anom_rh` | double | Diferencia frente a la línea base 2022–2025 |
| `det_3m_previos` | double | Densidad de los 3 meses anteriores |
| `c_H`, `c_P`, `c_D`, `c_T`, `c_R`, `c_W` | double [0,1] | Componentes en percentil |
| `irat` | double [0,1] | Índice |
| `irat_categoria` | string | Muy bajo · Bajo · Moderado · Alto · Muy alto |
| `irat_orden` | int | 1–5 |

### `fact_deteccion` — solo Boyacá, sin fuentes estáticas

`id_det` (PK) · `cod_mpio` (FK) · `fecha` · `fecha_mes` (FK) · `hora_utc` · `lat` · `lon` · `frp` · `confianza` · `daynight` · `satelite`

### `agg_depto_mes` — 1.914 filas (33 × 58)

`cod_dpto` (FK) · `fecha_mes` (FK) · `n_det` · `frp_total`

### Dimensiones

| Tabla | Filas | Columnas |
|---|---|---|
| `dim_municipio` | 123 | igual que la silver |
| `dim_departamento` | 33 | `cod_dpto` (PK) · `nombre_dpto` (en silver la columna se llama `nombre`; `export_gold` la renombra) |
| `dim_mes` | 58 | `fecha_mes` (PK) · `anio` · `mes` · `nombre_mes` · `trimestre` |

---

Filas observadas en la versión `data-freeze`: ver `docs/evidencias/gold_manifest.json`. Tabla auxiliar `_analisis/irat_sensibilidad` (7.134 filas): `cod_mpio, fecha_mes, n_det, irat, irat_orden, irat_iguales, orden_iguales, irat_solo_clima, orden_solo_clima`; no es contrato gold.
