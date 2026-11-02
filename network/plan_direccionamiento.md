# Plan de direccionamiento VLSM — FireWatch Boyacá (entregable A1)

**Bloque asignado:** _por definir por A_ · **Método:** VLSM, subredes ordenadas de mayor a menor necesidad de hosts, con un margen de crecimiento del 30–50 %.

## 1. Requerimientos por segmento

| # | Segmento | Router | Hosts actuales | Hosts con crecimiento | Prefijo | Justificación |
|---|---|---|---|---|---|---|
| 1 | Centro de datos | R-TUNJA | | | | Incluye margen para nodos worker de Spark |
| 2 | Analistas | R-TUNJA | | | | |
| 3 | Gestión del riesgo (CDGRD) | R-TUNJA | | | | |
| 4 | Sede Duitama | R-DUITAMA | | | | |
| 5 | Sede Sogamoso | R-SOGAMOSO | | | | |
| 6 | Internet / fuentes simuladas | R-ISP | | | | |
| 7–n | Enlaces WAN | — | 2 | 2 | /30 | Punto a punto |

## 2. Tabla de subredes

| Segmento | Red | Máscara | Prefijo | Primera utilizable | Última utilizable | Broadcast |
|---|---|---|---|---|---|---|
| | | | | | | |

## 3. Tabla de direccionamiento por dispositivo

| Dispositivo | Interfaz | IP | Máscara | Gateway | Descripción |
|---|---|---|---|---|---|
| | | | | | |

## 4. Verificación

- [ ] Sin solapamientos entre subredes
- [ ] OSPF anuncia todas las redes (`show ip route` en cada router)
- [ ] Ping y tracert de extremo a extremo (Sede Sogamoso → SRV-BI; SRV-PROCESAMIENTO → SRV-FIRMS-SIM)
