# ADR-0005 · Máscara de fuentes estáticas ampliada por persistencia

**Contexto:** la máscara de fuentes estáticas (política congelada) solo incluía celdas de 0,005° con al menos una detección SP `type = 2`. El EDA del 21 nov 2026 (`notebooks/01_eda.ipynb`, diagnóstico 2) encontró que Samacá, Ráquira y Socha encabezaban la densidad de detecciones de Boyacá con una firma incompatible con incendios de vegetación:

| Municipio | Detecciones en celdas persistentes | Nocturnas | En meses de lluvia | FRP mediana |
|---|---|---|---|---|
| Samacá | 86 % | 97 % | 31 % | 0,83 MW |
| Ráquira | 81 % | 83 % | 35 % | 0,81 MW |
| Socha | 53 % | 90 % | 27 % | 0,57 MW |
| Boyacá (referencia) | 10 % | 25 % | 12 % | 5,0 MW |

Son hornos de coquización y ladrilleras: fuentes pequeñas (~0,8 MW) que FIRMS clasificó como `type = 0` (vegetación). En total, 19 celdas y 438 detecciones (10 % de Boyacá) quedaban fuera de la máscara. Sin corregirlo, el IRAT habría puesto a Samacá como el municipio de mayor riesgo de incendio forestal del departamento.

**Decisión:** se añade a la máscara toda celda con detecciones SP `type = 0` (confianza n/h) en **≥ 6 meses distintos y ≥ 3 años distintos** de `anios_fuente` (2022–2025). Se probó además dilatar cada celda persistente a sus 8 vecinas; se descartó (ver más abajo) y queda desactivada (`dilatacion: 0`). Los umbrales están en `firms.mascara_estatica.persistencia` de `config/settings.yaml`. La regla se aplica a toda Colombia y se resuelve en silver, no en gold.

**Por qué esos umbrales:** una quema agrícola recurrente repite el mismo lote una vez al año, en la misma temporada (1 o 2 meses por año). Una fuente industrial se repite en cualquier época. Exigir 6 meses distintos repartidos en al menos 3 años separa los dos patrones sin depender de un municipio concreto.

**Alternativas descartadas:**
- Filtrar en Spark/gold: deja la lógica de calidad de FIRMS en la capa equivocada y silver queda contaminado.
- Solo documentar la limitación: deja un resultado principal del dashboard que es falso.
- Usar FRP bajo o la hora como regla: excluiría también incendios reales pequeños o nocturnos.

**Consecuencias:**
- Cambia el `content_sha256` de `silver.detecciones` → nueva entrega a C (zip + SHA-256) y nueva corrida de `build_daily` y gold **antes del data freeze**.
- La tabla de auditoría `mascara_fuentes_estaticas.parquet` añade las columnas `n_meses` y `criterio` (`type2` | `persistencia`).
- Nuevo check informativo V-C13p (celdas y detecciones añadidas por persistencia). V-C13c sigue alertando si el sobre-enmascaramiento total supera el 2 %.
- El benchmark de multiprocessing (`f1dc4f0`) sigue siendo válido: la regla agrega un `groupby` a la Fase 0.
- Limitación declarada: la regla usa SP 2022–2025; una fuente industrial que aparezca solo en 2026 no se enmascara.
- Limitación declarada: queda un residuo industrial de ≈ 3 % en Boyacá (celdas contiguas a hornos de Samacá, Ráquira y Socha) por efecto de borde de la grilla de 0,005°.

**Verificación:** tras volver a correr L2, el diagnóstico 2 del EDA debe dar ~0 % de detecciones persistentes en Samacá, Ráquira y Socha, y las celdas de mayor persistencia deben corresponder a hornos visibles en imagen satelital (capturas en `docs/evidencias/`).

**Primera corrida, sin dilatación (21 nov 2026, Colombia completa):**
- V-C13p: **413 celdas** añadidas por persistencia, con **5.081 detecciones SP type 0** (0,67 % de las 785.955 SP de vegetación 2022–2025).
- Máscara total: 731 celdas; sobre-enmascaramiento total 1,22 % (V-C13c, umbral 2 %: OK). Cobertura de las SP type 2: 100 % (V-C13: OK).
- Nuevo `content_sha256` de `silver.detecciones`: `a41f8fb81305565589fa19137a8eb1be2dc35d086a435a5889b7a6bc61743db4` (antes `4ab11cd5…`).
- Efecto en Boyacá: detecciones en celdas persistentes 10,2 % → 1,3 %; de las 386 detecciones reclasificadas, 382 eran nocturnas (99 %). La hora no forma parte de la regla, así que es una validación independiente.
- Residuo: 4 celdas persistentes (52 detecciones) en Samacá, Ráquira y Socha, que conservaban la firma industrial (58–86 % nocturnas, FRP mediana 0,6–1,1 MW).

**Ajuste: dilatación de 1 celda.** Las 4 celdas residuales eran vecinas directas de celdas ya enmascaradas (efecto de borde: una batería de hornos de varios cientos de metros reparte sus detecciones entre celdas contiguas de 0,005° ≈ 550 m, con píxeles VIIRS de 375 m). Se añade `persistencia.dilatacion: 1`: las 8 vecinas de cada celda persistente entran a la máscara con `criterio = persistencia_vecina`. La dilatación **no** se aplica a las celdas `type 2`, que siguen la política congelada. Criterio de reversión: si V-C13c supera el 2 % de sobre-enmascaramiento nacional, se retira la dilatación y el residuo se declara como limitación.

**Corrida con dilatación (21 nov 2026): revertida.** La dilatación añadió 3.121 celdas vecinas a las 413 núcleo; el criterio de persistencia pasó a cubrir 12.879 detecciones SP de vegetación (1,74 %) y el sobre-enmascaramiento total subió a **2,28 %**, por encima del umbral de 2 % (V-C13c en advertencia). Aplicando el criterio de reversión fijado antes de la corrida, se vuelve a `dilatacion: 0`. El código queda disponible y probado, pero desactivado.

**Decisión final:** persistencia sin dilatación (`content_sha256` `a41f8fb8…`). El residuo industrial en Boyacá (~125 detecciones nocturnas en celdas contiguas a hornos de Samacá, Ráquira y Socha, ≈ 3 % del departamento) se declara como limitación en el IEEE y en el dashboard. Trabajo futuro: umbral por histéresis (umbral estricto para el núcleo y uno más laxo para las vecinas), que enmascararía solo las vecinas con actividad recurrente propia.

**Fecha:** 2026-11-21 · Propuesto por: B · Aprobado: A ☐ B ☐ C ☐
