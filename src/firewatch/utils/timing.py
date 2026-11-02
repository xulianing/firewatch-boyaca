"""Medición de tiempos para los benchmarks y los logs.

A implementar:
    cronometro(nombre: str)  -> context manager con time.perf_counter()
    registrar_resultado(ruta_csv: Path, fila: dict) -> None
        Añade una fila a benchmarks/results/*.csv con el esquema de benchmarks/README.md.
"""
