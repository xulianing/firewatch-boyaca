# FireWatch Boyacá — Plan Director Técnico v2 (3 integrantes)
**Vigencia:** 2 al 16 de noviembre de 2026 · **Reemplaza a la v1 (4 integrantes)** · **v2.1: ventana de datos 2022-01-01 → 2026-10-31 (ADR-0004 en el Plan de Implementación)**

---

## 0. Supuestos y presupuesto de horas

- **3 integrantes**, con **3,5 h efectivas por persona y día** de lunes a sábado. No son 4 h: es tercer corte y hay parciales de otras materias.
- Días de trabajo: del lunes 2 al sábado 14 (13 días). El domingo 15 se reserva para ensayo y margen.
- **Capacidad:** 3 × 13 × 3,5 ≈ **136 h**, más unas 10 h del domingo 15.
- **MVP planificado: ~127 h.** Margen real: **~15 %**, y solo si nadie pierde más de un día completo.

**Conclusión:** con 3 personas no hay espacio para un "alcance ideal" amplio. El plan se organiza en tres niveles:
- **P0:** obligatorio. Sin esto, el proyecto falla.
- **P1:** mejoras baratas y de alto impacto. Solo se hacen si se cumplen los controles indicados en §8.
- **P2:** ideal. Se documenta como trabajo futuro y solo se implementa si sobra tiempo después del 11 de noviembre, lo cual es improbable.

**El 16 de noviembre es festivo** (Independencia de Cartagena, trasladada al lunes). Hay que confirmar con el docente la fecha real. El plan deja **todo entregable el jueves 12** (adelantado un día gracias a la ventana 2022–2026; el viernes 13 queda como margen).

---

## 1. Decisiones técnicas congeladas (no se rediscuten)

| # | Decisión | Justificación corta |
|---|---|---|
| D1 | Clima desde **Open-Meteo (reanálisis ERA5)**, no desde IDEAM DHIME | DHIME es lento y tiene huecos. ERA5 es continuo y citable. Clima horario: 123 municipios × 42.360 h ≈ 5,2 M filas (R4, ADR-0003). |
| D2 | **Spark procesa FIRMS de toda Colombia** (S-NPP + NOAA-20, 2022–2026); el análisis se enfoca en Boyacá | Boyacá sola no llega a 100k registros (R4). |
| D3 | Riesgo = **índice compuesto IRAT** por municipio-mes, validado con Spearman (IRAT del mes *t* frente a detecciones del mes *t*+1) | Se interpreta y se defiende fácilmente. La rúbrica premia el rigor por encima de la complejidad. |
| D4 | El **cruce espacial (punto en polígono) se hace con GeoPandas en multiprocessing**, no en Spark | Es CPU-bound y se reparte bien en bloques, así que da un speedup honesto. Evita Sedona. |
| D5 | Serie con **VIIRS S-NPP + NOAA-20, 2022-01 → 2026-10** (ambos satélites cubren la ventana completa; NOAA-21 queda excluido); filtros `type==0` (o máscara de fuentes estáticas en NRT) y confianza ≠ *low* | Serie homogénea en el tiempo y libre de las fuentes industriales de Nobsa y Sogamoso. |
| D6 | **Spark y los benchmarks se ejecutan en UNA sola máquina** (la de C, con ≥4 núcleos físicos) | Elimina el riesgo de instalar Spark en tres equipos. Además, el benchmark debe medirse siempre en el mismo hardware. |
| D7 | El **plan IP VLSM de Packet Tracer se cita en la arquitectura** y cada componente lógico se asocia a un servidor de PT | Coherencia de la integración (criterio de 10 %) sin el costo de montar un clúster real. |
| D8 | **Desacople por contrato de datos:** B entrega `silver/detecciones` (Colombia, con `cod_mpio`; esquema en Arquitectura §9.1). C programa Spark contra ese esquema desde el día 2 usando datos crudos | Evita que C quede bloqueado esperando a B. |

---

## 2. Alcance mínimo viable obligatorio (P0)

| Componente | Entregable exacto | Criterio de aceptación |
|---|---|---|
| **Problema y ODS** | Contexto de Boyacá (temporadas secas, páramos), usuarios (CDGRD Boyacá, Corpoboyacá, bomberos), metas ODS 13.1 y 13.3 | ≥3 fuentes que evidencien el problema |
| **Red (PT)** | 4 routers (R-TUNJA, R-DUITAMA, R-SOGAMOSO, R-ISP), switches, PCs, servidores con rol (SRV-INGESTA, SRV-PROCESAMIENTO, SRV-ALMACENAMIENTO, SRV-BI + servidores de fuentes simuladas en R-ISP; ver Arquitectura §11) | VLSM sin solapamientos, OSPF área 0 convergido, DHCP, ping y tracert extremo a extremo, tabla de direccionamiento |
| **Paralelo (R3/R5)** | Limpieza y cruce espacial con `multiprocessing.Pool` | p = 1, 2, 4, 8 · 5 repeticiones · speedup, eficiencia y Karp–Flatt en tabla y gráfica |
| **Distribuido (R4/R5)** | PySpark: ingesta de Colombia, join con clima, agregados municipio-mes, ventanas (lags y anomalía de lluvia de 30 días), IRAT | ≥100k registros · benchmark con `local[1/2/4/N]` · comparación con pandas |
| **Conceptos CPD** | Explicación de escalabilidad, procesamiento por lotes y tolerancia a fallos (linaje/DAG) | Sección VI del IEEE + respuesta oral |
| **EDA (R6)** | Notebook con **7** visualizaciones (Matplotlib/Seaborn) interpretadas | Cada gráfico acompañado de un hallazgo escrito |
| **Dashboard (R7)** | Power BI de **3 páginas**: Panorama · Mapa y municipios · Clima y riesgo | ≥5 visuales, ≥3 KPI, slicers + drill-down (provincia→municipio, año→mes) |
| **IEEE (R8)** | Versión en inglés (6–10 páginas) + versión en español (traducción asistida y revisada) | Plantilla oficial, ≥8 referencias IEEE, figuras de arquitectura, red, speedup y dashboard |
| **Repositorio** | `run_pipeline` reproducible + README + scripts de descarga (sin datos crudos) | Se ejecuta en una máquina limpia siguiendo el README |
| **Póster y video** | Póster A1 en inglés + video de 4–6 minutos | Video de respaldo listo antes del ensayo final |

**KPI del dashboard:** K1–K6 definidos en Arquitectura §8.1. Tarjetas principales: K1 detecciones, K2 variación interanual, K4 municipios en riesgo Alto o Muy alto, K5 tasa de captura del IRAT.

---

## 3. Alcance ideal (escalonado)

**P1: mejoras baratas (≤2 h cada una, mucho valor por hora).** Se hacen si se cumple el control correspondiente.

| Mejora | Resp. | h | Por qué vale la pena |
|---|---|---|---|
| **Simulación de tolerancia a fallos en Spark**: master `local[4,3]` (permite reintentos) + fallo inyectado en el primer intento de una tarea (`TaskContext.attemptNumber()`), y mostrar el reintento en la Spark UI | C | 2 | Convierte en demostración un objetivo explícito de los lineamientos, sin necesidad de clúster |
| ACL que restrinja el acceso a la subred de datos + justificación de seguridad | A | 1,5 | Diferencia la red del resto de grupos |
| Marcadores (*bookmarks*) y botones de navegación en Power BI | C | 1,5 | Añade un tercer tipo de interactividad |
| Validación del IRAT contra registros UNGRD (municipio-año) | B | 3 | Refuerza la sección de resultados |

**P2: ideal (casi seguro que no se hace; se documenta como trabajo futuro con su diseño):**
- Clúster Spark standalone en Docker (1 master + 2 workers) con IP del plan VLSM.
- Modelo MLlib comparado con el IRAT.
- OSPF multiárea.
- Publicación en Power BI Service.

---

## 4. Funcionalidades eliminadas

| Se elimina | Motivo | Cómo se defiende en la sustentación |
|---|---|---|
| IDEAM DHIME como fuente principal | Riesgo de cronograma y huecos en los datos | ERA5 tiene continuidad espacial y temporal y es estándar en la literatura |
| Streaming / tiempo real | Fuera del tiempo disponible | FIRMS NRT tiene latencia de horas; el lote diario basta para gestionar el riesgo (trabajo futuro) |
| Apache Sedona | Configuración frágil (1–2 días) | El cruce espacial se resuelve en multiprocessing |
| NDVI e imágenes ráster | Complejidad desproporcionada | Trabajo futuro |
| Machine learning (MLlib) en el MVP | No tiene peso propio en la rúbrica | El índice validado es interpretable para el usuario final |
| Despliegue en la nube y app web | No puntúan directamente | Arquitectura cloud propuesta como trabajo futuro |
| Clúster físico entre portátiles | Firewalls, versiones y red Wi-Fi universitaria | Riesgo inaceptable en una demo en vivo |
| Spark instalado en los 3 equipos | Riesgo de entorno | Una sola máquina de ejecución (D6) |
| MODIS y NOAA-21 | Resolución distinta (MODIS) y entrada a mitad de la ventana (NOAA-21) | Mantener la serie homogénea |
| Datos anteriores a 2022 | Indicación de la docente (carga de los equipos) | Línea base relativa a la ventana, declarada como limitación |
| 4.ª página del dashboard y más de 7 gráficos de EDA | Rendimiento decreciente | Superan igual los mínimos (R6 = 6, R7 = 5) |
| Encuesta formal a usuarios | Tiempo | Se reemplaza por la retroalimentación de 1–2 personas (docente o compañero), citada en el documento |

---

## 5. División de responsabilidades

| | **A · Redes y Documento** | **B · Data Engineering y Paralelo** | **C · Spark y Power BI** |
|---|---|---|---|
| Responsable de | Packet Tracer, plan IP, edición del IEEE (EN y ES), póster, referencias | Ingesta (FIRMS, clima, DANE), limpieza, cruce espacial, multiprocessing y su benchmark, EDA, reproducibilidad, video | Entorno y pipeline Spark, IRAT, benchmark Spark, tolerancia a fallos, modelo de datos y dashboard |
| Archivos de los que es dueño único | `.pkt`, documento IEEE, póster | `src/ingest`, `src/parallel`, `notebooks/eda`, README | `src/spark`, `.pbix` |
| Secciones del IEEE | I–V, VIII–IX, integración final | VI-A (paralelo), parte EDA de VII | VI-B (Spark), VII (dashboard) |
| Requisito de equipo | Packet Tracer instalado | Python + GeoPandas | **Windows (Power BI) y el equipo con más núcleos** |
| Respaldo de | B (EDA) | C (Spark) | A (revisión técnica del IEEE) |

**Regla de oro:** solo el dueño hace *commit* de los binarios (`.pkt`, `.pbix`, `.docx`). Si C no tiene Windows, se intercambia Power BI con B **hoy mismo**.

---

## 6. Inventario de tareas y horas

★ = ruta crítica · P0 salvo que se indique

| ID | Tarea | Resp. | h | Depende de |
|---|---|---|---|---|
| T01 | Arranque: alcance, contrato de datos, repo, preguntas al docente | Todos | 1,5 × 3 | — |
| T02★ | Descarga FIRMS Colombia VIIRS S-NPP + NOAA-20, 2022-01 → 2026-10 + **gate de volumen** | B | 1,5 | T01 |
| T03 | Municipios DANE MGN Boyacá + tabla municipio→provincia | B | 1,5 | T01 |
| T04 | Clima Open-Meteo (123 centroides, horario, 2022-01 → 2026-10) | B | 2,5 | T03 |
| T05★ | Pipeline secuencial: limpieza + filtros + cruce espacial (Colombia completa, ver Arquitectura §1.4) → `silver/detecciones` | B | 4 | T02, T03 |
| T06 | Paralelización multiprocessing (módulo `.py`, método *spawn*) | B | 3 | T05 |
| T07 | Benchmark multiprocessing + gráficas (S, E, Karp–Flatt) | B | 3 | T06 |
| T08★ | Entorno Spark en la máquina de C (Java 17, PySpark fijado) | C | 2 | T01 |
| T09 | Ingesta Spark de FIRMS Colombia (R4) + limpieza distribuida | C | 2 | T02, T08 |
| T10★ | Spark: join con clima, agregados municipio-mes, ventanas, IRAT | C | 8 | T05, T04, T09 |
| T11 | Benchmark Spark (núcleos, particiones, pandas frente a Spark) | C | 4 | T10 |
| T12★ | **Data freeze**: exportar modelo estrella (CSV/Parquet) | C | 1 | T10 |
| T13 | Plan IP VLSM + diagrama lógico | A | 2 | T01 |
| T14 | Topología PT + configuración básica | A | 5 | T13 |
| T15 | OSPF, DHCP, servicios en servidores, verificación | A | 4 | T14 |
| T16 | Documentación de red (tabla, capturas de `show`) | A | 2 | T15 |
| T17 | Evidencia del problema/ODS + búsqueda de ≥12 referencias | A | 3 | T01 |
| T18 | EDA con 7 visualizaciones interpretadas | B | 6 | T05 |
| T19★ | Power BI: modelo, DAX, KPI, **prueba del mapa** (plan B: Shape Map con TopoJSON) | C | 5 | T12 |
| T20★ | Power BI: 3 páginas, drill-down, slicers, storytelling | C | 7 | T19 |
| T21 | Validación del IRAT (Spearman + revisión del top 10 frente a industria) | B | 1,5 | T10 |
| T22 | `run_pipeline` + README + prueba en máquina limpia | B | 3 | T07, T11 |
| T23 | IEEE EN: Abstract, I–V | A | 6 | T16, T17 |
| T24★ | IEEE EN: VI–IX + integración + referencias + formato | A (+B, C con 2–3 h cada uno) | 8 + 5 | T07, T11, T18, T20 |
| T25 | Versión en español (traducción asistida + revisión técnica) | A | 3 | T24 |
| T26 | Póster A1 | A (C aporta figuras) | 4 | T24 |
| T27 | Video de la demo | B | 2 | T20, T15, T22 |
| T28 | Puntos de control (8 y 10 de nov) + 2 sesiones de cross-training + banco de preguntas Saber Pro | Todos | 4 × 3 | — |
| T29 | Ensayos 1 y 2 + empaquetado final | Todos | 3 × 3 | Todo |

**Carga por persona (P0):**

| | Trabajo propio | Tareas comunes (T01, T28, T29) | Total | Disponible (13 d × 3,5 h) |
|---|---|---|---|---|
| A | 37 h | 8,5 h | **45,5 h** | 45,5 h ← **sin margen: el domingo 15 es su colchón** |
| B | 37 h | 8,5 h | **45,5 h** | 45,5 h ← igual |
| C | 31,5 h | 8,5 h | **40 h** | 45,5 h → ~5 h para P1 |

**Lectura honesta:** A y B quedan al 100 %. Por eso:
1. La traducción al español debe ser asistida (no se reescribe).
2. El póster se hace sobre una plantilla.
3. C absorbe las P1 y apoya en la revisión del IEEE.

Si cualquiera pierde 2 días seguidos, se activa el recorte de emergencia (§9).

---

## 7. Ruta crítica

```
(v2.1, fechas aceleradas por ADR-0004; el detalle está en el Plan de Implementación §7–§8)
T02 FIRMS (lun 2) → T05 cruce espacial secuencial (mar 3) → T10 Spark + IRAT (mié 4 – vie 6)
→ T12 DATA FREEZE (sáb 7) → T19–T20 Power BI (dom 8 – mar 10) → T24 IEEE EN (mié 11)
→ T25/T26 ES + póster (jue 12) → margen (vie 13) → T29 ensayos (vie 13 – dom 15)
```

**Holguras (float):**

| Bloque | Holgura | Comentario |
|---|---|---|
| Red PT (T13–T16) | ~4 días | No retrasa nada, pero debe cerrarse el viernes 6 para liberar a A hacia el documento |
| Multiprocessing (T06–T07) | ~3 días | **No bloquea a Spark**: Spark consume la salida del pipeline secuencial, que es idéntica |
| EDA (T18) | ~3 días | |

**Los dos puntos de mayor riesgo de toda la ruta:**
- **El cruce espacial del miércoles 4.** Si se atrasa, se atrasa todo.
- **El mapa de Power BI del lunes 9.** Si falla tarde, no hay tiempo de cambiar de visual.

---

## 8. Cronograma diario — ⚠ REEMPLAZADO por el Plan de Implementación §8 (v1.1). Se conserva solo como histórico.

| Fecha | A · Redes y Documento | B · Data y Paralelo | C · Spark y BI | Control |
|---|---|---|---|---|
| **Lun 2** | T01, T13 plan VLSM, enviar preguntas al docente | T01, T02 + **gate de volumen**, T03 | T01, T08 entorno, esqueleto del repo | ¿FIRMS supera los 100k? ¿C tiene Windows? |
| **Mar 3** | T14 topología | T04 clima, inicio de T05 | T09 ingesta Colombia + limpieza en Spark | Spark corre en la máquina de C |
| **Mié 4** | T14 configuración, inicio de T15 | **T05 terminado → entrega del parquet a C** | T10 join con clima (usa el parquet) | 🚩 Sin parquet a las 20:00 → fallback: C agrega por celda de 0,1° y municipaliza después |
| **Jue 5** | T15 OSPF, DHCP, servicios | T06 multiprocessing | T10 agregados + ventanas | — |
| **Vie 6** | T15 verificación, T16 documentación, ACL (P1) | T07 benchmark | T10 IRAT + T21 revisión con B | **Red cerrada** |
| **Sáb 7** | T17 evidencia y referencias, inicio de T23 | T18 EDA | T11 benchmark Spark, tolerancia a fallos (P1) | — |
| **Dom 8** | T23 I–IV (trabajo ligero) | T18 EDA (≥5 gráficos) | **T12 DATA FREEZE** | **Punto de control 1** (videollamada de 90 min) + cross-training 1 |
| **Lun 9** | T23 V + diagrama de arquitectura | T18 cierre (7 gráficos), T21 Spearman | T19 modelo, DAX, **mapa** | 🚩 El mapa no funciona a las 18:00 → Shape Map con TopoJSON |
| **Mar 10** | Inicio de T24 (VIII–IX) | T22 `run_pipeline` + README, texto VI-A | T20 páginas + drill-down | **Punto de control 2** — dashboard funcional |
| **Mié 11** | T24 integra VI–VII | T22 prueba en máquina limpia, UNGRD (P1) | T20 storytelling, marcadores (P1), texto VI-B y VII | — |
| **Jue 12** | **T24 IEEE EN cerrado** | Revisión técnica del IEEE, guion del video | Revisión técnica del IEEE, figuras para el póster | Cross-training 2 |
| **Vie 13** | T25 ES + T26 póster | T27 video, release v1.0 | Banco de preguntas Saber Pro, apoyo con el póster | **TODO ENTREGABLE** |
| **Sáb 14** | Ensayo 1 + correcciones | Ensayo 1 + correcciones | Ensayo 1 + correcciones | — |
| **Dom 15** | Ensayo 2, PDFs finales | Respaldo offline (USB) | Verificación del `.pbix` final | Paquete listo |
| Lun 16 | — | — | — | Entrega (festivo: confirmar fecha) |

---

## 9. Hitos de corte (lo que DEBE existir en cada fecha)

### Fin de esta semana — domingo 8 de noviembre
- [ ] Respuestas del docente (o supuestos documentados si no respondió).
- [ ] **Red PT 100 % terminada y verificada:** VLSM, OSPF, DHCP, servicios, tabla de direccionamiento y capturas.
- [ ] `silver/detecciones` limpio, filtrado (`type==0`, confianza) y municipalizado.
- [ ] Benchmark de multiprocessing terminado: tabla y gráficas de S, E y Karp–Flatt.
- [ ] Pipeline Spark completo con IRAT. Volumen procesado ≥100k registros, documentado.
- [ ] **Data freeze (sábado 7, 18:00):** modelo estrella exportado. A partir de aquí no se cambia el esquema.
- [ ] Benchmark Spark terminado (si se corre el lunes 9 no pasa nada grave, pero no más tarde).
- [ ] EDA con ≥5 gráficos.
- [ ] IEEE: secciones I–IV en borrador, con ≥10 referencias identificadas.

### Martes 10 de noviembre
- [ ] **Dashboard funcional:** 3 páginas, ≥5 visuales, ≥3 KPI, slicers y drill-down operativos, mapa funcionando con la visual original o con el plan B.
- [ ] EDA con 7 gráficos interpretados.
- [ ] Validación del IRAT (Spearman + revisión del top 10).
- [ ] `run_pipeline` + README listos.
- [ ] IEEE: I–V completas; texto técnico de VI-A entregado por B.
- [ ] P1 de tolerancia a fallos hecha o descartada formalmente.

### Jueves 12 de noviembre — CONGELAMIENTO TOTAL (el viernes 13 queda como margen)
- [ ] IEEE en inglés final (6–10 páginas, ≥8 referencias) + versión en español en PDF.
- [ ] Póster A1 en PDF.
- [ ] Video de la demo (4–6 min).
- [ ] Repositorio con *tag* `v1.0`, probado en una máquina limpia.
- [ ] `.pbix` final, `.pkt` final.
- [ ] Banco de preguntas Saber Pro (al menos 5 por materia, con respuesta razonada).

### Domingo 15 de noviembre
- [ ] 2 ensayos cronometrados. **Cada integrante explica las tres materias** (la sustentación es individual).
- [ ] Simulacro de preguntas difíciles: ¿por qué ERA5 y no IDEAM?, ¿por qué Spark es más lento que pandas?, ¿por qué OSPF y no RIP?, ¿por qué un índice y no ML?, ¿qué mide realmente FIRMS?
- [ ] Paquete final verificado con checklist y respaldo offline: USB con `.pkt`, `.pbix`, video, PDFs y salidas precalculadas.

---

## 10. Recorte de emergencia (se activa si alguien pierde ≥2 días o falla un control 🚩)

Orden de sacrificio, de lo que menos duele a lo que más:
1. Todas las P1.
2. EDA reducido a 6 gráficos (el mínimo de R6).
3. Póster sobre una plantilla mínima.
4. Benchmark con 3 repeticiones en lugar de 5.
5. Dashboard de 2 páginas con 5 visuales (el mínimo de R7).

**Nunca se recorta:** la verificación de la red, el speedup y la eficiencia, R4, el IEEE en inglés ni los ensayos.

---

## 11. Preguntas al docente (enviar hoy)

1. ¿La entrega y la sustentación son el lunes 16 (festivo)?
2. ¿El documento IEEE va solo en inglés, o en inglés y español?
3. ¿La propuesta de 4 páginas se entrega por separado?
4. ¿ML y nube son obligatorios en el documento?
5. ¿Se acepta ERA5 en lugar de las estaciones IDEAM?

(Spark en modo local ya está confirmado como permitido en *Restricciones del proyecto*.)
