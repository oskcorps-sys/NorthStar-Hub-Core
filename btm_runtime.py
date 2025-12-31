from __future__ import annotations

import json
import os
from typing import Any, Dict


def load_btm(soul_dir: str, bureau: str) -> Dict[str, Any]:
    """
    Carga el JSON BTM del buró, si existe.
    Convención:
      00_NORTHSTAR_SOUL_IMPUT/BTM/BTM_TRANSUNION_v1_0.json
      00_NORTHSTAR_SOUL_IMPUT/BTM/BTM_EXPERIAN_v1_1.json
      00_NORTHSTAR_SOUL_IMPUT/BTM/BTM_EQUIFAX_v1_1.json
    Fail-closed: si no existe o falla, retorna {}.
    """
    try:
        btm_dir = os.path.join(soul_dir, "BTM")
        if not os.path.isdir(btm_dir):
            return {}

        candidates = [
            f"BTM_{bureau}_v1_1.json",
            f"BTM_{bureau}_v1_0.json",
            f"BTM_{bureau}.json",
        ]

        for name in candidates:
            path = os.path.join(btm_dir, name)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)

        return {}
    except Exception:
        return {}


def btm_to_instruction(btm: Dict[str, Any]) -> str:
    """
    Convierte BTM JSON a una instrucción corta y CLARA para Gemini.
    No mete “pasos”, solo reglas de traducción + política de hallazgos.
    """
    if not btm:
        return (
            "BTM: NONE. If bureau-native codes appear, do not flag format differences "
            "unless the discrepancy is evidence-bound and impacts interpretation."
        )

    bureau = btm.get("bureau", "UNKNOWN")
    ver = btm.get("version", "NA")
    mappings = btm.get("mappings", {}) or {}

    # Compactar mappings (solo un resumen)
    lines = [f"BTM ACTIVE: {bureau} v{ver}. Use these translations before judging inconsistencies."]

    # MOP mappings (si existen)
    mop = mappings.get("MOP") or {}
    if isinstance(mop, dict) and mop:
        pairs = ", ".join([f"{k}->{v}" for k, v in mop.items()])
        lines.append(f"MOP map: {pairs}.")

    # Payment grid mappings (si existen)
    grid = mappings.get("PAYMENT_HISTORY_GRID") or {}
    if isinstance(grid, dict) and grid:
        # no explotar tokens: muestra subset ordenado
        keys = list(grid.keys())[:18]
        sample = ", ".join([f"{k}->{grid[k]}" for k in keys])
        lines.append(f"Payment grid sample map: {sample}.")

    # ECOA mappings (si existen)
    ecoa = mappings.get("ECOA_RESPONSIBILITY") or mappings.get("ECOA_WHOSE") or {}
    if isinstance(ecoa, dict) and ecoa:
        keys = list(ecoa.keys())[:18]
        sample = ", ".join([f"{k}->{ecoa[k]}" for k in keys])
        lines.append(f"ECOA map sample: {sample}.")

    # Política clave anti-ruido
    lines.append(
        "RULE: Do NOT flag bureau-native formatting/codes as inconsistencies if they are translated by BTM. "
        "Only flag CODE_MAPPING_INCONSISTENCY when a code appears that has NO translation AND the field is materially used."
    )

    return " ".join(lines)
