"""Carga de la configuración central (config/settings.yaml).

Responsabilidad:
    Único punto de acceso a los parámetros del proyecto. Ningún otro módulo lee el YAML
    directamente ni define fechas, rutas, umbrales o pesos propios.

A implementar:
    cargar_config(ruta: str | None = None) -> dict
        Lee settings.yaml una sola vez (caché) y lo devuelve como dict.
    ruta_capa(capa: str) -> pathlib.Path
        Ruta absoluta de raw/silver/gold resuelta desde la raíz del repositorio.
    validar_config(cfg: dict) -> None
        Falla pronto si: los pesos del IRAT no suman 1; fecha_inicio >= fecha_fin;
        cod_dpto_foco no es texto de 2 caracteres.

Dueño: C (con B) · Tarea: C1
"""
