
# Plan de direccionamiento VLSM — FireWatch Boyacá

**Bloque:** 172.16.15.0/24 (privado, RFC 1918). En un /24 el VLSM es necesario: con subredes de tamaño fijo solo cabrían 4 de las 10 que se requieren.
**Regla de dimensionamiento:** hosts requeridos = hosts actuales × 1,5 (crecimiento del 50 %) + 1 (gateway). Prefijo = el menor con 2^bits_host − 2 ≥ hosts requeridos. Asignación de mayor a menor para mantener la alineación y evitar solapamientos. Gateway = primera IP utilizable de cada subred.

## 1. Requerimientos

| Segmento | Hosts actuales | ×1,5 + gateway | Prefijo | Útiles |
|---|---|---|---|---|
| Gestión del riesgo (CDGRD) | 30 | 46 | /26 | 62 |
| Centro de datos (4 servidores + 8 workers Spark reservados) | 12 | 19 | /27 | 30 |
| Analistas | 12 | 19 | /27 | 30 |
| Sede Duitama | 15 | 24 | /27 | 30 |
| Sede Sogamoso | 15 | 24 | /27 | 30 |
| Internet / fuentes externas | 2 | 4 | /29 | 6 |
| Enlaces WAN (×4) | 2 | 2 | /30 | 2 |

## 2. Tabla de subredes

| # | Segmento | Red | Máscara | Rango útil | Broadcast | Gateway |
|---|---|---|---|---|---|---|
| 1 | CDGRD (VLAN 30) | 172.16.15.0/26 | 255.255.255.192 | .1 – .62 | .63 | .1 |
| 2 | Centro de datos (VLAN 10) | 172.16.15.64/27 | 255.255.255.224 | .65 – .94 | .95 | .65 |
| 3 | Analistas (VLAN 20) | 172.16.15.96/27 | 255.255.255.224 | .97 – .126 | .127 | .97 |
| 4 | Sede Duitama | 172.16.15.128/27 | 255.255.255.224 | .129 – .158 | .159 | .129 |
| 5 | Sede Sogamoso | 172.16.15.160/27 | 255.255.255.224 | .161 – .190 | .191 | .161 |
| 6 | Internet / fuentes | 172.16.15.192/29 | 255.255.255.248 | .193 – .198 | .199 | .193 |
| 7 | WAN Tunja–Duitama | 172.16.15.200/30 | 255.255.255.252 | .201 – .202 | .203 | — |
| 8 | WAN Tunja–Sogamoso | 172.16.15.204/30 | 255.255.255.252 | .205 – .206 | .207 | — |
| 9 | WAN Duitama–Sogamoso | 172.16.15.208/30 | 255.255.255.252 | .209 – .210 | .211 | — |
| 10 | WAN Tunja–ISP | 172.16.15.212/30 | 255.255.255.252 | .213 – .214 | .215 | — |
| — | Libre (crecimiento) | 172.16.15.216 – .255 | — | — | — | — |

Uso del bloque: 216 de 256 direcciones (84 %).

## 3. Tabla por dispositivo

| Dispositivo | Interfaz | IP / prefijo | Gateway | Notas |
|---|---|---|---|---|
| R-TUNJA | Gi0/0.10 | 172.16.15.65/27 | — | VLAN 10 Centro de datos (router-on-a-stick) |
| R-TUNJA | Gi0/0.20 | 172.16.15.97/27 | — | VLAN 20 Analistas |
| R-TUNJA | Gi0/0.30 | 172.16.15.1/26 | — | VLAN 30 CDGRD |
| R-TUNJA | S0/2/1 (DCE) | 172.16.15.201/30 | — | WAN a R-DUITAMA |
| R-TUNJA | S0/2/0 | 172.16.15.205/30 | — | WAN a R-SOGAMOSO |
| R-TUNJA | S0/1/0 (DCE) | 172.16.15.213/30 | — | WAN a R-ISP |
| R-DUITAMA | Gi0/0 | 172.16.15.129/27 | — | LAN Sede Duitama |
| R-DUITAMA | S0/2/1 | 172.16.15.202/30 | — | WAN a R-TUNJA |
| R-DUITAMA | S0/2/0 | 172.16.15.209/30 | — | WAN a R-SOGAMOSO |
| R-SOGAMOSO | Gi0/0 | 172.16.15.161/27 | — | LAN Sede Sogamoso |
| R-SOGAMOSO | S0/2/0 (DCE) | 172.16.15.206/30 | — | WAN a R-TUNJA |
| R-SOGAMOSO | S0/2/1 | 172.16.15.210/30 | — | WAN a R-DUITAMA |
| R-ISP | Gi0/0 | 172.16.15.193/29 | — | LAN fuentes externas |
| R-ISP | S0/2/0 | 172.16.15.214/30 | — | WAN a R-TUNJA |
| SRV-INGESTA | Fa0 | 172.16.15.66/27 | .65 | Estática |
| SRV-PROCESAMIENTO | Fa0 | 172.16.15.67/27 | .65 | Estática |
| SRV-ALMACENAMIENTO | Fa0 | 172.16.15.68/27 | .65 | Estática |
| SRV-BI | Fa0 | 172.16.15.69/27 | .65 | Estática · HTTP |
| Reserva workers Spark | — | .70 – .94 | .65 | Escalabilidad del clúster |
| SRV-FIRMS-SIM | Fa0 | 172.16.15.194/29 | .193 | Estática · HTTP |
| SRV-METEO-SIM | Fa0 | 172.16.15.195/29 | .193 | Estática · HTTP |
| PC de CDGRD | Fa0 | DHCP (.11 – .62) | .1 | Pool CDGRD |
| PC-ANALISTA-1/2 | Fa0 | DHCP (.107 – .126) | .97 | Pool ANALISTAS |
| PC-DUI-1/2 | Fa0 | DHCP (.134 – .158) | .129 | Pool DUITAMA |
| PC-SOG-1/2 | Fa0 | DHCP (.166 – .190) | .161 | Pool SOGAMOSO |

## 4. VLAN en SW-TUNJA

| VLAN | Nombre | Puertos |
|---|---|---|
| 10 | CENTRO_DATOS | Fa0/2 – Fa0/5 |
| 20 | ANALISTAS | Fa0/6 – Fa0/10 |
| 30 | CDGRD | Fa0/11 – Fa0/20 |
| Troncal 802.1Q | — | Fa0/1 → R-TUNJA Gi0/0 |

## 5. Enrutamiento
OSPF proceso 1, área 0 en los 4 routers (router-id 1.1.1.1 Tunja, 2.2.2.2 Duitama, 3.3.3.3 Sogamoso, 4.4.4.4 ISP). Las LAN son interfaces pasivas. Costos: serial = 64, Gigabit = 1.