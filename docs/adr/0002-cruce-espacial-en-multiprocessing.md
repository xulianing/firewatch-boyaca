# ADR-0002 · Cruce espacial con GeoPandas en multiprocessing (no en Spark)

**Contexto:** cada detección FIRMS debe asignarse a su municipio. Las restricciones prohíben Apache Sedona.

**Decisión:** el cruce punto-en-polígono se hace con GeoPandas (`sjoin` con STRtree) dentro de `multiprocessing.Pool`, sobre **todas las detecciones de Colombia** contra los ~1.120 municipios, en bloques de tamaño fijo. Las geometrías se cargan una vez por proceso con el `initializer`.

**Alternativas descartadas:** UDF de Shapely en Spark (costo de serialización Python↔JVM y difícil de defender); cruzar solo Boyacá (carga tan pequeña que el overhead dominaría y el benchmark no valdría).

**Consecuencias:** un speedup medible y honesto para R3/R5; cada tecnología hace aquello en lo que es fuerte (GeoPandas → geometría; Spark → agregaciones y ventanas).

**Fecha:** 2026-11-02 · Aprobado: A ☐ B ☐ C ☐
