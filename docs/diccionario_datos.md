# Diccionario de datos — contratos entre capas (entregable C2)

**Fuente de verdad** para todos los esquemas. Basado en Arquitectura §9 (v1.1). Si un esquema cambia, primero se actualiza este documento y se avisa en el grupo. **Después del data freeze (sáb 7) no se cambia.**

Convenciones: `cod_dpto` = texto de 2 caracteres · `cod_mpio` = texto de 5 caracteres · fechas en ISO (`YYYY-MM-DD`) · `fecha_mes` = primer día del mes · Parquet en silver, Parquet + CSV (UTF-8, coma) en gold.

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

### `agg_depto_mes` — ~33 × 58

`cod_dpto` (FK) · `fecha_mes` (FK) · `n_det` · `frp_total`

### Dimensiones

| Tabla | Filas | Columnas |
|---|---|---|
| `dim_municipio` | 123 | igual que la silver |
| `dim_departamento` | ~33 | `cod_dpto` (PK) · `nombre` |
| `dim_mes` | 58 | `fecha_mes` (PK) · `anio` · `mes` · `nombre_mes` · `trimestre` · `fase_enso` (P1) |

---

**Aprobación:** B ☐ · C ☐ · Fecha: ____
