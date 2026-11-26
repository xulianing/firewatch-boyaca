# network/ — Infraestructura de red en Packet Tracer (R2) · autora: Dafne Suspes (A)

| Archivo / carpeta | Contenido |
|---|---|
| `firewatch.pkt` | **Topología final** (configuraciones de routers, switches, servidores y PC incluidas en el archivo) |
| `plan_direccionamiento.md` | VLSM, tabla de subredes, tabla por dispositivo, VLAN y OSPF |
| `evidencias/` | Capturas de verificación (topología, tablas de rutas, vecinos OSPF, ping/tracert, DHCP, VLAN/troncal, DNS/HTTP) |
| `evidencias/Prototipo1.pkt`, `evidencias/Prototipo2.pkt` | Versiones anteriores, solo como historial del diseño. **El archivo que se evalúa es `firewatch.pkt`** |

`configs/` quedó sin archivos: las configuraciones viven dentro de `firewatch.pkt` y se pueden ver con `show running-config` en cada equipo.

## Diseño

- **Bloque:** 172.16.15.0/24 (RFC 1918) con VLSM en 10 subredes; se usan 216 de 256 direcciones. Regla de tamaño: hosts × 1,5 + gateway.
- **Routers (2911):** R-TUNJA (núcleo y centro de datos), R-DUITAMA, R-SOGAMOSO y R-ISP (simula Internet y las fuentes externas).
- **Enlaces WAN:** seriales /30. Tunja, Duitama y Sogamoso forman un triángulo (redundancia); Tunja se conecta además con R-ISP.
- **Tunja:** SW-TUNJA con VLAN 10 (centro de datos), 20 (analistas) y 30 (CDGRD), troncal 802.1Q hacia R-TUNJA (router-on-a-stick, subinterfaces Gi0/0.10/.20/.30).
- **Enrutamiento:** OSPF proceso 1, área 0; router-id 1.1.1.1 a 4.4.4.4; LAN como interfaces pasivas; costo serial 64, Gigabit 1.
- **Servicios:** DHCP en las LAN de usuarios; DNS en SRV-INGESTA; HTTP en SRV-BI y en los servidores simulados.
- **Servidores:** `SRV-INGESTA`, `SRV-PROCESAMIENTO`, `SRV-ALMACENAMIENTO`, `SRV-BI` (centro de datos, .66–.69) y `SRV-FIRMS-SIM`, `SRV-METEO-SIM` (detrás de R-ISP). El rango .70–.94 del centro de datos queda reservado para futuros workers de Spark.

Detalle completo en [`plan_direccionamiento.md`](plan_direccionamiento.md).

## Verificación (capturas en `evidencias/`)

- Adyacencias OSPF en estado FULL; cada router aprende por OSPF las subredes remotas. R-TUNJA llega a las LAN de Duitama y Sogamoso con costo 65 (serial 64 + red de destino 1); R-DUITAMA y R-SOGAMOSO tienen dos rutas de igual costo (128) hacia el enlace opuesto del triángulo.
- PC-SOG-1 recibe por DHCP 172.16.15.166/27, gateway .161 y DNS .66, y alcanza SRV-BI (4/4).
- SRV-PROCESAMIENTO alcanza SRV-FIRMS-SIM en 3 saltos.
- VLAN y troncal en SW-TUNJA (`vlan_trunk_SW-TUNJA.png`).
- Acceso por nombre al servicio HTTP de SRV-BI desde PC-DUI-1 (`http_dns_PC-DUI-1.png`).

## Simplificación declarada

R-ISP participa en OSPF para que las fuentes simuladas sean alcanzables. Un proveedor real no comparte su IGP: lo normal sería una ruta por defecto hacia el ISP.
