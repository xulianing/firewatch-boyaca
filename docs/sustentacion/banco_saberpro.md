# Banco de preguntas tipo Saber Pro y de sustentación

Formato: enunciado con contexto del proyecto, 4 opciones, respuesta correcta y justificación. Todas las cifras salen de `README.md` §7, `docs/evidencias/` y `benchmarks/results/`.

## Redes e Infraestructura

**R1.** El segmento CDGRD tiene 30 equipos y se dimensiona con un 50 % de crecimiento más el gateway (46 direcciones). ¿Qué prefijo es el menor que lo cubre?
A) /27 · B) /26 · C) /25 · D) /28
**Respuesta: B.** Un /27 ofrece 30 direcciones útiles (insuficiente); un /26 ofrece 62.

**R2.** Un PC recibe por DHCP la dirección 172.16.15.166/27. ¿A qué subred pertenece?
A) 172.16.15.128/27 (Duitama) · B) 172.16.15.160/27 (Sogamoso) · C) 172.16.15.96/27 (Analistas) · D) 172.16.15.192/29 (Internet)
**Respuesta: B.** Con /27 los bloques avanzan de 32 en 32: .160 a .191. La .166 queda en la LAN de Sogamoso.

**R3.** En R-TUNJA, la ruta OSPF hacia la LAN de Duitama tiene costo 65. ¿De dónde sale?
A) 65 saltos · B) Serial de 1,544 Mbps (64) + red Gigabit de destino (1) · C) Distancia administrativa · D) Retardo en milisegundos
**Respuesta: B.** Costo = 10^8 / ancho de banda; 10^8 / 1,544·10^6 ≈ 64.

**R4.** ¿Por qué las LAN de usuarios se configuran como interfaces pasivas en OSPF?
A) Para que no se anuncien · B) Para que se anuncien sin enviar Hello, pues no hay routers vecinos · C) Para bajar su costo · D) Para activar DHCP
**Respuesta: B.** La red se anuncia, pero no se forman adyacencias por esa interfaz: menos tráfico y nadie puede inyectar rutas desde una LAN.

**R5.** Las tres VLAN de Tunja salen por un solo puerto físico de R-TUNJA. ¿Qué permite enrutar entre ellas?
A) Un switch de capa 2 · B) Subinterfaces con encapsulación 802.1Q y un enlace troncal (router-on-a-stick) · C) Tres procesos OSPF · D) NAT
**Respuesta: B.**

**R6.** Si cae el enlace serial Tunja–Duitama, ¿qué ocurre?
A) Duitama queda aislada · B) OSPF recalcula y enruta por Sogamoso con mayor costo · C) Hay que crear una ruta estática · D) Se reinicia DHCP
**Respuesta: B.** El triángulo da un camino alterno; OSPF reconverge solo.

## Computación Paralela y Distribuida

**C1.** L2 tarda 80,9 s con 1 proceso y 41,6 s con 4. ¿Cuál es la eficiencia?
A) 1,94 · B) 0,49 · C) 0,77 · D) 0,25
**Respuesta: B.** S = 80,9 / 41,6 = 1,94; E = S / p = 1,94 / 4 ≈ 0,49.

**C2.** La fracción serial medida de L2 es f = 0,198. Según Amdahl, ¿cuál es el speedup máximo con 4 procesos?
A) 4 · B) 2,51 · C) 5,05 · D) 1,94
**Respuesta: B.** 1 / (0,198 + 0,802/4) ≈ 2,51. El valor 5,05 es el límite con infinitos procesos (1/f).

**C3.** ¿Por qué el cruce espacial usa `multiprocessing` y no hilos?
A) Los hilos no comparten memoria · B) El GIL de CPython impide que varios hilos ejecuten bytecode a la vez en trabajo CPU-bound · C) `multiprocessing` usa la GPU · D) Los hilos no funcionan en Windows
**Respuesta: B.**

**C4.** En Spark, `groupBy(cod_mpio, fecha)` sobre 5,2 millones de horas:
A) Es una transformación estrecha sin shuffle · B) Es una transformación ancha que provoca un shuffle y un límite de etapa · C) Se ejecuta en cuanto se declara · D) Solo funciona con UDF
**Respuesta: B.** Además, por evaluación perezosa, nada corre hasta una acción.

**C5.** Con 200 particiones de shuffle `clima_diario` tardó 1,74 s y con 8, 0,43 s. ¿Por qué?
A) Más particiones siempre aceleran · B) Con pocas filas por partición, el costo de planificar y lanzar tareas supera el cómputo · C) Spark limita a 8 particiones · D) Falta memoria
**Respuesta: B.**

**C6.** La salida paralela de L2 tiene la misma huella SHA-256 con p = 1, 2, 4 y 8. ¿Qué demuestra?
A) Que los datos son correctos frente a la realidad · B) Que el resultado no depende del número de procesos ni del reparto (equivalencia y determinismo) · C) Que no hubo duplicados en la fuente · D) Que el speedup es lineal
**Respuesta: B.**

## Analítica y Visualización de Datos

**A1.** Alto y Muy alto agrupan el 40 % de los municipio-mes y concentran el 84,7 % de las detecciones. ¿Qué porcentaje capturaría una clasificación al azar?
A) 20 % · B) 40 % · C) 50 % · D) 84,7 %
**Respuesta: B.**

**A2.** ¿Por qué la grilla municipio-día se construye a partir del clima y no de las detecciones?
A) Porque el clima tiene más columnas · B) Para conservar los días sin detecciones (n_det = 0); de lo contrario las medias se sesgan hacia los días con fuego · C) Por rendimiento · D) Porque Power BI lo exige
**Respuesta: B.**

**A3.** K3 (FRP medio) se calcula como `AVERAGE(fact_deteccion[frp])` y no como el promedio de `frp_medio` mensual porque:
A) Es más rápido · B) Promediar promedios mensuales da el mismo peso a un mes con 1 detección y a uno con 500 · C) `frp_medio` no existe · D) DAX no permite promediar columnas de hechos
**Respuesta: B.**

**A4.** La correlación entre la lluvia del mes anterior y las detecciones pasa de −0,64 a −0,27 al usar anomalías. ¿Qué indica?
A) Que la lluvia no influye · B) Que la mayor parte de la correlación cruda era efecto del calendario (estacionalidad compartida) · C) Un error de cálculo · D) Que las anomalías están mal definidas
**Respuesta: B.**

**A5.** ¿Por qué Power BI lee los Parquet de gold y no los CSV?
A) Los CSV no existen · B) Con la configuración regional de Colombia, los CSV alteran los decimales y convierten "05" en 5, rompiendo los códigos DIVIPOLA · C) Parquet es obligatorio en Power BI · D) Los CSV pesan menos
**Respuesta: B.**

**A6.** El IRAT de un municipio-mes es 0,88. ¿Qué significa?
A) 88 % de probabilidad de incendio · B) Que ese municipio-mes está entre los de mayor riesgo relativo dentro de la ventana 2022–2026 · C) Que hubo 88 detecciones · D) Que el 88 % del municipio está seco
**Respuesta: B.** El IRAT es un índice relativo de priorización, no una probabilidad.

## Preguntas difíciles del jurado (respuesta preparada, ≤ 1 minuto)

1. **¿Por qué ERA5 y no IDEAM?** ADR-0001: ERA5 da series continuas sin vacíos y descarga programática; las estaciones tienen cobertura desigual y descarga manual. Limitación: ~31 km de resolución, por eso se trabaja con anomalías respecto al propio municipio.
2. **¿Por qué Spark puede ser más lento que pandas con estos datos?** Con 1 hilo empatan (1,18 s frente a 1,20 s): Spark paga planificación y shuffle. Con 12 hilos es ~2,8 veces más rápido. La fracción serial (~0,3) limita el speedup a ~3,3.
3. **¿Spark en modo local es realmente "distribuido"?** Usa el mismo modelo que un clúster (DAG, etapas, tareas, shuffle, linaje) en una sola JVM. No es distribuido entre máquinas; pasar a un clúster cambia el master, y la red reserva .70–.94 para workers.
4. **¿Por qué OSPF y no RIP o EIGRP?** Estado de enlace, convergencia rápida, costo por ancho de banda y sin límite de 15 saltos (RIP). EIGRP es de origen propietario de Cisco; OSPF es estándar abierto.
5. **¿Por qué un índice y no machine learning?** Restricción del curso y razón técnica: con 4 años y el 88,7 % de los municipio-mes en cero, un modelo supervisado tendría pocos eventos por municipio; un índice con pesos explícitos se audita y se explica.
6. **¿Qué mide realmente FIRMS? ¿Una detección es un incendio?** Mide anomalías térmicas en píxeles de 375 m en cada paso del satélite; pueden ser incendios, quemas o fuentes industriales. Por eso los filtros de tipo, confianza y la máscara (ADR-0005).
7. **¿Bastan 4 años de datos para una línea base?** Es indicación docente (ADR-0004) y se declara. Las anomalías no se estandarizan (n = 4) y el índice es relativo a la ventana, que incluye La Niña 2022 y El Niño 2023–24.
8. **¿Cómo evitaron la fuga de información en el índice?** La propensión histórica se calcula sin el año evaluado (LOYO), los 3 meses previos son pasado estricto y los pesos se fijaron antes del EDA. Además, K5 se reporta primero sin H (60,7 %).
9. **¿Cómo se integra la red de Packet Tracer con el procesamiento real?** Cada capa lógica tiene su servidor en el centro de datos (ingesta, procesamiento, almacenamiento, BI) y las fuentes externas están detrás de R-ISP; desde las sedes se llega al tablero por nombre (DNS + HTTP). La ejecución real corre en una máquina; la red es el diseño de despliegue.
10. **¿Qué cambiaría para escalar a un clúster real?** Master de Spark apuntando al clúster, silver en almacenamiento compartido y workers en el rango reservado; el cruce espacial tendría que repartirse por nodo o pasar a Spark con un índice espacial.
