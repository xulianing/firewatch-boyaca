"""
Genera config/provincias_boyaca.csv (A2): municipio → provincia para los 123 municipios de Boyacá.

Fuente oficial (Gobernación de Boyacá):
  [F1] Gobernación de Boyacá, Secretaría de Planeación, Dirección de Seguimiento y Planeación Territorial,
       "Informe de Resultados de la Nueva Medición del Desempeño Municipal de la Vigencia Fiscal 2021
       de los Municipios del Departamento de Boyacá", Tunja, dic. 2022, cuadros 4 a 8 (provincia de cada
       municipio). https://planeacion.boyaca.gov.co/wp-content/uploads/2023/01/Informe-Medicion-Desempeno-Municipal-2021-123-Mpios-Depto-Boyaca.pdf
  [F2] Secretaría de Planeación de Boyacá, "Mapas PDF": mapas provinciales de 13 provincias + "Distrito
       Fronterizo" + "Zona de Manejo Especial". https://planeacion.boyaca.gov.co/mapas-boyaca/
  [F3] Secretaría de Cultura de Boyacá, Observatorio Cultural, "Provincias de Boyacá" (13 provincias,
       2 distritos especiales: Puerto Boyacá y Cubará). https://observatoriocultural.boyaca.gov.co/provincias-de-boyaca/

[F1] agrupa a Puerto Boyacá y Cubará como "Especial". [F2] y [F3] los separan en dos unidades: Puerto Boyacá
= Zona de Manejo Especial y Cubará = Distrito Fronterizo. Se usa esa separación (más específica, misma fuente
institucional); no se asigna ninguno de los dos a una de las 13 provincias.

El script NO inventa códigos: cruza por nombre normalizado con config/centroides_boyaca.csv (DIVIPOLA del
MGN 2025) y se detiene si algún municipio no casa exactamente 1 a 1.

Uso (desde la raíz del repo):
    python scripts/build_provincias_boyaca.py
Dueño: B (asumido por A2 el 24 nov) · Tarea: A2
"""

from __future__ import annotations

import csv
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CENTROIDES = RAIZ / "config" / "centroides_boyaca.csv"
SALIDA = RAIZ / "config" / "provincias_boyaca.csv"
N_MUNICIPIOS = 123

# Nombre canónico de cada unidad (grafía de [F2]/[F3]).
ZME = "Zona de Manejo Especial"
DF = "Distrito Fronterizo"

# [F1], cuadros 4–8, transcrito tal cual (provincia; municipio). "Especial" se resuelve con [F2]/[F3].
FUENTE_F1 = """
Centro;Tuta
Centro;Tunja
Centro;Samacá
Centro;Oicatá
Sugamuxi;Tibasosa
Sugamuxi;Nobsa
Sugamuxi;Sogamoso
Sugamuxi;Iza
Tundama;Paipa
Tundama;Corrales
Tundama;Duitama
Tundama;Busbanzá
Lengupá;Campohermoso
Lengupá;Páez
Lengupá;Zetaquira
Lengupá;Miraflores
Ricaurte;Villa de Leyva
Ricaurte;Santa Sofía
Ricaurte;Sáchica
Occidente;Chiquinquirá
Occidente;Tununguá
Occidente;Otanche
Márquez;Jenesano
Márquez;Ramiriquí
Especial;Puerto Boyacá
Neira;Santa María
Centro;Chivatá
Centro;Soracá
Centro;Cucaita
Centro;Toca
Centro;Sotaquirá
Oriente;Guateque
Oriente;La Capilla
Oriente;Sutatenza
Oriente;Guayatá
Oriente;Tenza
Neira;Garagoa
Neira;Macanal
Neira;Pachavita
Neira;San Luis de Gaceno
Occidente;San Miguel de Sema
Occidente;Muzo
Occidente;Briceño
Occidente;La Victoria
Ricaurte;Sutamarchán
Ricaurte;Moniquirá
Ricaurte;Arcabuco
Sugamuxi;Tópaga
Sugamuxi;Aquitania
Sugamuxi;Firavitoba
Márquez;Boyacá
Márquez;Nuevo Colón
Márquez;Ventaquemada
Lengupá;San Eduardo
Lengupá;Berbeo
Tundama;Belén
Tundama;Tutazá
La Libertad;Pajarito
La Libertad;Pisba
Valderrama;Socha
Valderrama;Paz de Río
Norte;Sativasur
Norte;Soatá
Norte;Covarachía
Norte;Sativanorte
Norte;La Uvita
Norte;San Mateo
Norte;Boavita
Ricaurte;Gachantivá
Ricaurte;Ráquira
Ricaurte;Santana
Ricaurte;Tinjacá
Ricaurte;San José de Pare
Centro;Siachoque
Centro;Motavita
Centro;Cómbita
Centro;Chíquiza
Occidente;Saboyá
Occidente;Páuna
Occidente;Caldas
Occidente;Quípama
Márquez;Tibaná
Márquez;Ciénega
Márquez;Turmequé
Gutiérrez;Panqueba
Gutiérrez;El Espino
Gutiérrez;El Cocuy
Valderrama;Jericó
Valderrama;Socotá
Sugamuxi;Cuítiva
Sugamuxi;Monguí
Oriente;Somondoco
Oriente;Chivor
Tundama;Cerinza
La Libertad;Labranzagrande
Neira;Chinavita
Sugamuxi;Tota
Sugamuxi;Pesca
Sugamuxi;Mongua
Sugamuxi;Gámeza
Occidente;Buenavista
Occidente;San Pablo de Borbur
Occidente;Maripí
Occidente;Coper
Gutiérrez;Chiscas
Gutiérrez;Guacamayas
Gutiérrez;Güicán de la Sierra
Valderrama;Chita
Valderrama;Tasco
Valderrama;Betéitiva
Márquez;Viracachá
Márquez;Úmbita
Ricaurte;Chitaraque
Ricaurte;Toguí
Tundama;Floresta
Tundama;Santa Rosa de Viterbo
Lengupá;Rondón
Norte;Tipacoque
Oriente;Almeida
Centro;Sora
Especial;Cubará
La Libertad;Paya
Norte;Susacón
"""

ESPECIALES = {"PUERTO BOYACA": ZME, "CUBARA": DF}   # [F2]/[F3]


def norm(s: str) -> str:
    """Clave de cruce: sin tildes ni diéresis, mayúsculas, espacios simples."""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s).strip().upper()


def slug(nombre: str) -> str:
    """Igual que dane.slugify_provincia (D-G4)."""
    s = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").upper()


def main() -> int:
    fuente = [tuple(x.split(";")) for x in FUENTE_F1.strip().splitlines()]
    claves = Counter(norm(m) for _, m in fuente)
    dup = [k for k, n in claves.items() if n > 1]
    with CENTROIDES.open(encoding="utf-8-sig", newline="") as f:
        mgn = {norm(r["nombre"]): (r["cod_mpio"], r["nombre"]) for r in csv.DictReader(f)}

    errores = []
    if len(fuente) != N_MUNICIPIOS:
        errores.append(f"la fuente tiene {len(fuente)} municipios (esperado {N_MUNICIPIOS})")
    if dup:
        errores.append(f"municipios repetidos en la fuente: {dup}")
    sin_codigo = sorted(set(claves) - set(mgn))
    sin_provincia = sorted(set(mgn) - set(claves))
    if sin_codigo:
        errores.append(f"en la fuente pero no en centroides_boyaca.csv: {sin_codigo}")
    if sin_provincia:
        errores.append(f"en centroides_boyaca.csv pero no en la fuente: {sin_provincia}")
    if errores:
        print("ERROR:\n- " + "\n- ".join(errores))
        return 1

    filas = []
    for prov, mpio in fuente:
        k = norm(mpio)
        cod, nombre_mgn = mgn[k]
        provincia = ESPECIALES.get(k, prov)
        if prov == "Especial" and k not in ESPECIALES:
            print(f"ERROR: '{mpio}' marcado como Especial sin unidad definida")
            return 1
        filas.append({"cod_mpio": cod, "municipio": nombre_mgn,
                      "cod_provincia": slug(provincia), "provincia": provincia})
    filas.sort(key=lambda r: r["cod_mpio"])

    with SALIDA.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["cod_mpio", "municipio", "cod_provincia", "provincia"],
                           lineterminator="\n")
        w.writeheader()
        w.writerows(filas)

    por_prov = Counter(r["provincia"] for r in filas)
    print(f"OK: {len(filas)} municipios, {len(por_prov)} unidades → {SALIDA.relative_to(RAIZ)}")
    for p, n in sorted(por_prov.items()):
        print(f"  {p:<25} {n:>3}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
