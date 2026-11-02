# 🔥 FireWatch Boyacá

**Sistema distribuido de analítica geoespacial para el monitoreo de anomalías térmicas y la estimación de riesgo de incendios de cobertura vegetal en Boyacá, Colombia.**

![ODS 13](https://img.shields.io/badge/ODS-13%20Acci%C3%B3n%20por%20el%20clima-3F7E44)
![Python](https://img.shields.io/badge/Python-3.11-3776AB)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5%20local-E25A1C)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811)
![Packet Tracer](https://img.shields.io/badge/Cisco-Packet%20Tracer-1BA0D7)

> Proyecto Integrador · Ingeniería de Datos e Inteligencia Artificial · Cuarto semestre · 2026-2
> Asignaturas: **Redes e Infraestructura** · **Computación Paralela y Distribuida** · **Analítica y Visualización de Datos**

---

## 1. Problema

Boyacá enfrenta incendios de cobertura vegetal recurrentes durante las temporadas secas, con impacto en páramos, bosques andinos, fuentes de agua y medios de vida rurales. Las entidades de gestión del riesgo (CDGRD, Corpoboyacá, cuerpos de bomberos) necesitan **priorizar territorialmente** con información oportuna. Los datos satelitales y climáticos son abiertos, pero están dispersos y no se integran en un indicador accionable a escala municipal.

**FireWatch Boyacá** integra las detecciones de anomalías térmicas satelitales (NASA FIRMS) con variables meteorológicas (reanálisis ERA5) para:

1. Caracterizar la actividad térmica en los 123 municipios del departamento.
2. Estimar un **Índice de Riesgo Relativo de Anomalías Térmicas (IRAT)** por municipio y mes.
3. Comunicar los hallazgos en un dashboard interactivo orientado a la toma de decisiones.

**Alineación ODS:** ODS 13 · metas 13.1 (resiliencia frente a riesgos relacionados con el clima) y 13.3 (capacidad institucional de alerta temprana).

---

## 2. Arquitectura

Pipeline por lotes con patrón *medallion* (`raw → silver → gold`), desplegado lógicamente sobre una red diseñada en Cisco Packet Tracer.

```mermaid
flowchart LR
    F["NASA FIRMS<br/>VIIRS S-NPP + NOAA-20"] --> L1
    M["Open-Meteo<br/>ERA5 horario"] --> L1
    D["DANE MGN<br/>municipios"] --> L1
    L1["L1 Ingesta<br/>data/raw"] --> L2["L2 Paralelo<br/>multiprocessing + GeoPandas<br/>data/silver"]
    L2 --> L3["L3 Distribuido<br/>PySpark local<br/>data/gold"]
    L3 --> E["EDA<br/>Matplotlib / Seaborn"]
    L3 --> P["Dashboard<br/>Power BI"]
```

| Capa | Tecnología | Qué hace | Requisito |
|---|---|---|---|
| L0 · Red | Cisco Packet Tracer | Topología de 4 routers, VLSM, OSPF, DHCP y servidores por rol | R2 |
| L1 · Ingesta | Python + `requests` | Descarga reproducible de FIRMS, Open-Meteo y DANE | — |
| L2 · Paralelo | `multiprocessing` + GeoPandas | Limpieza, filtros de calidad y asignación de cada detección a su municipio (Colombia completa) | R3, R5 |
| L3 · Distribuido | PySpark (modo local) | Clima horario→diario, grilla municipio-día, ventanas, línea base e IRAT | R4, R5 |
| L4 · Consumo | Matplotlib, Seaborn, Power BI | EDA y dashboard con KPIs, mapa y drill-down | R6, R7 |

📄 Diseño completo: [`docs/arquitectura.md`](docs/arquitectura.md) · Contratos de datos: [`docs/diccionario_datos.md`](docs/diccionario_datos.md) · Decisiones: [`docs/adr/`](docs/adr/)

---

## 3. Datos

**Ventana temporal:** `2022-01-01 → 2026-10-31` (indicación docente; ver [ADR-0004](docs/adr/0004-ventana-2022-2026.md)).

| Fuente | Producto | Uso |
|---|---|---|
| [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/) | VIIRS 375 m, S-NPP y NOAA-20 (SP 2022–2025; SP + NRT 2026) | Detecciones de anomalías térmicas |
| [Open-Meteo Historical API](https://open-meteo.com/en/docs/historical-weather-api) | Reanálisis ERA5 / ERA5-Land, horario, por centroide municipal | Temperatura, humedad, precipitación, viento, ET0 |
| [DANE – MGN](https://geoportal.dane.gov.co/) | Capa de municipios (DIVIPOLA) | Asignación espacial, áreas, centroides |
| Gobernación de Boyacá | Tabla municipio → provincia (`config/provincias_boyaca.csv`) | Jerarquía del drill-down |

> Los datos **no se versionan**. Se descargan con `scripts/download_all.py`. Ver [`data/README.md`](data/README.md).

---

## 4. Estructura del repositorio

```
firewatch-boyaca/
├── config/          Configuración central (settings.yaml) y semillas
├── data/            raw / silver / gold (no versionado)
├── src/firewatch/   Paquete Python: ingest, parallel, spark, quality, utils
├── scripts/         Puntos de entrada: descarga y pipeline completo
├── benchmarks/      Medición de speedup y eficiencia (resultados versionados)
├── notebooks/       EDA y validación del IRAT
├── tests/           Pruebas unitarias mínimas
├── dashboard/       Power BI (.pbix), TopoJSON, medidas y capturas
├── network/         Packet Tracer (.pkt), plan VLSM, configuraciones y evidencias
└── docs/            Arquitectura, ADR, IEEE (EN/ES), póster, demo y sustentación
```

---

## 5. Instalación

**Requisitos:** Python 3.11 · Java 17 (para Spark) · Power BI Desktop (Windows) · Cisco Packet Tracer 8.x

```bash
git clone https://github.com/<organizacion>/firewatch-boyaca.git
cd firewatch-boyaca
python -m venv .venv
# Windows: .venv\Scripts\activate   |   Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
pip install -e .            # instala el paquete firewatch (necesario para multiprocessing)
```

Verificación de Spark:

```bash
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').getOrCreate(); print(s.range(10**7).count())"
```

---

## 6. Ejecución

```bash
python scripts/download_all.py                 # L1: descarga las fuentes a data/raw
python scripts/run_pipeline.py --stage silver  # L2: limpieza + cruce espacial (paralelo)
python scripts/run_pipeline.py --stage gold    # L3: Spark → modelo estrella
python scripts/run_pipeline.py --stage all     # L2 + L3

python benchmarks/bench_multiprocessing.py     # speedup / eficiencia de L2
python benchmarks/bench_spark.py               # escalado de L3 por núcleos y particiones
```

Todos los parámetros (fechas, filtros, pesos del IRAT, configuración de Spark) viven en [`config/settings.yaml`](config/settings.yaml).

---

## 7. Resultados

> *Esta sección se completa después del data freeze (sábado 7 de noviembre).*

| Métrica | Valor |
|---|---|
| Detecciones procesadas (Colombia, ventana completa) | _pendiente_ |
| Detecciones de vegetación en Boyacá | _pendiente_ |
| Filas horarias de clima procesadas en Spark | ≈ 5,2 M |
| Speedup máximo multiprocessing (p = 8) | _pendiente_ |
| Eficiencia multiprocessing (p = 8) | _pendiente_ |
| ρ de Spearman IRAT(t) vs detecciones(t+1) | _pendiente_ |
| Tasa de captura del IRAT (Alto + Muy alto) | _pendiente_ |

---

## 8. Componentes entregables

| Componente | Ubicación | Asignatura |
|---|---|---|
| Topología de red, VLSM, OSPF | [`network/`](network/) | Redes e Infraestructura |
| Procesamiento paralelo y distribuido + benchmarks | [`src/firewatch/`](src/firewatch/), [`benchmarks/`](benchmarks/) | Computación Paralela y Distribuida |
| EDA + dashboard | [`notebooks/`](notebooks/), [`dashboard/`](dashboard/) | Analítica y Visualización de Datos |
| Artículo IEEE (EN / ES), póster, demo | [`docs/`](docs/) | Las tres |

---

## 9. Equipo

| Rol | Integrante | Responsabilidades |
|---|---|---|
| A · Redes y Documento | _nombre_ | Packet Tracer, plan IP, edición del IEEE, póster |
| B · Data Engineering y Paralelo | _nombre_ | Ingesta, limpieza, multiprocessing, EDA, reproducibilidad |
| C · Spark y Power BI | _nombre_ | Pipeline Spark, IRAT, benchmark Spark, dashboard |

---

## 10. Limitaciones

- Una detección FIRMS es una **anomalía térmica**, no un incendio confirmado.
- La línea base climática cubre 2022–2025 (restricción docente); las anomalías son **relativas a la ventana**.
- El clima se representa por el centroide municipal (resolución ERA5).
- Los datos de 2026 combinan procesamiento estándar (SP) y casi en tiempo real (NRT).

---

## 11. Licencia y citación

Código bajo licencia MIT (ver [`LICENSE`](LICENSE)). Los datos pertenecen a sus proveedores (NASA FIRMS, Copernicus/ECMWF vía Open-Meteo, DANE) y se usan según sus términos.

> FireWatch Boyacá: Distributed Geospatial Analytics for Thermal Anomaly Monitoring and Wildfire Risk Estimation in Boyacá, Colombia. Proyecto Integrador, 2026.
