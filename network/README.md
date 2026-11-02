# network/ — Infraestructura de red en Packet Tracer (R2) · dueño único: A

| Archivo / carpeta | Contenido | Tarea | Fecha |
|---|---|---|---|
| `plan_direccionamiento.md` | Tabla VLSM y tabla de direccionamiento por dispositivo | A1 | Lun 2 |
| `firewatch.pkt` | Topología completa (solo A hace *commit*) | A3 → A4 | Mié 4 → Vie 6 |
| `configs/` | `show running-config` de cada router y switch, como `.txt` (`R-TUNJA.txt`, …) | A4 | Vie 6 |
| `evidencias/` | Capturas: `show ip route`, `show ip ospf neighbor`, `show ip interface brief`, ping y tracert de extremo a extremo, DHCP asignando direcciones | A5 | Vie 6 |

## Diseño (Arquitectura §11)

- **Routers:** R-TUNJA (núcleo y centro de datos), R-DUITAMA, R-SOGAMOSO, R-ISP (simula Internet y las fuentes externas).
- **Enrutamiento:** OSPF área 0 en todos los routers. ACL hacia la subred del centro de datos como P1.
- **Segmentos:** Centro de datos · Analistas · Gestión del riesgo (CDGRD) · Sede Duitama · Sede Sogamoso · Internet · enlaces WAN /30.
- **Servidores** (nombres exactos): `SRV-INGESTA`, `SRV-PROCESAMIENTO`, `SRV-ALMACENAMIENTO`, `SRV-BI`, `SRV-FIRMS-SIM`, `SRV-METEO-SIM`.
- **Servicios:** DHCP en las LAN de usuarios; HTTP en `SRV-BI` y en los servidores simulados; DNS opcional.
- **Escalabilidad:** la subred del centro de datos se dimensiona con hosts libres para futuros nodos worker de Spark (argumento compartido con CPD).

## Convenciones

- *Hostnames* en mayúsculas iguales a los del diagrama; `description` en todas las interfaces WAN.
- Direcciones de gateway: la **primera** IP utilizable de cada subred.
- Cada cambio en el `.pkt` va acompañado de la actualización de `plan_direccionamiento.md` y de `configs/`.
