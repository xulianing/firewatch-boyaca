"""Utilidades de E/S compartidas por las etapas del pipeline (escritura atómica y hashes).

Nota de refactor: meteo.py conserva su propia write_atomic/sha256_bytes (v0.1 congelada);
en la próxima iteración de meteo se reemplazan por estas para no duplicar lógica.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path


def write_atomic(path: Path, data: bytes) -> None:
    """Escribe en <path>.part, fsync y os.replace: nunca queda un archivo a medias con el nombre final."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def replace_atomic(tmp: Path, final: Path) -> None:
    """Promueve un archivo ya escrito (p. ej. por pyarrow) a su nombre final de forma atómica."""
    Path(final).parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp, final)


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()
