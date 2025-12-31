from __future__ import annotations

from typing import Literal
from PyPDF2 import PdfReader

Bureau = Literal["TRANSUNION", "EXPERIAN", "EQUIFAX", "UNKNOWN"]


def _safe_lower(s: str) -> str:
    return (s or "").lower()


def detect_bureau(pdf_path: str, pages_to_scan: int = 2) -> Bureau:
    """
    Detecta buró leyendo texto de las primeras páginas del PDF.
    Fail-closed: si no puede leer, devuelve UNKNOWN.
    """
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for i in range(min(pages_to_scan, len(reader.pages))):
            page_text = reader.pages[i].extract_text() or ""
            text += "\n" + page_text

        t = _safe_lower(text)

        # Señales típicas
        if "transunion" in t:
            return "TRANSUNION"
        if "experian" in t:
            return "EXPERIAN"
        if "equifax" in t:
            return "EQUIFAX"

        # Algunas variantes comunes en PDFs
        if "trans union" in t:
            return "TRANSUNION"

        return "UNKNOWN"
    except Exception:
        return "UNKNOWN"
