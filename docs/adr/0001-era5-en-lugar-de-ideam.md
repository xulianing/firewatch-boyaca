# ADR-0001 · Clima desde ERA5 (Open-Meteo) en lugar de IDEAM DHIME

**Contexto:** el tema original mencionaba datos del IDEAM. La plataforma DHIME requiere descargas manuales y lentas, y sus estaciones tienen series incompletas y una cobertura desigual en Boyacá.

**Decisión:** usar el reanálisis ERA5 / ERA5-Land a través de la Open-Meteo Historical Weather API, consultado en el centroide de cada municipio.

**Alternativas descartadas:** IDEAM DHIME (riesgo de cronograma y huecos); NASA POWER (válida, pero se prefiere una sola API con resolución horaria).

**Consecuencias:** continuidad espacio-temporal completa y una fuente citable. Limitaciones: ERA5 suaviza los extremos locales y un centroide representa mal a los municipios con mucho relieve. Ambas se declaran en el IEEE.

**Fecha:** 2026-11-02 · Aprobado: A ☐ B ☐ C ☐
