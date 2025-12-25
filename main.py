"""
NorthStar Hub — Backend Core
Kernel: NS-DK-1.0 (Alpha)
Scope: Technical data consistency check only
"""

from __future__ import annotations

import os
import datetime as dt
from typing import Any, Dict, List


# -----------------------------
# CONSTANTS (CANONICAL)
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}


# -----------------------------
# UTILITIES
# -----------------------------
def _utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _empty_payload(status: str = "INCOMPLETE", risk_level: str = "NONE", confidence: float = 0.0) -> Dict[str, Any]:
    return {
        "version": KERNEL_VERSION,
        "timestamp": _utc_iso(),
        "status": status,
        "risk_level": risk_level,
        "findings": [],
        "confidence": float(confidence),
        "notes": NOTES_IMMUTABLE,
    }


def _validate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hard validation to prevent UI breakage and enforce canonical boundaries.
    If invalid, degrade to UNKNOWN safely.
    """
    try:
        if payload.get("version") != KERNEL_VERSION:
            payload["version"] = KERNEL_VERSION

        payload["timestamp"] = payload.get("timestamp") or _utc_iso()

        status = payload.get("status")
        risk = payload.get("risk_level")
        findings = payload.get("findings")
        confidence = payload.get("confidence")

        # Enforce enums
        if status not in ALLOWED_STATUS:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0)

        if risk not in ALLOWED_RISK:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0)

        # Enforce findings list
        if not isinstance(findings, list):
            payload["findings"] = []

        # Enforce confidence
        if not isinstance(confidence, (int, float)):
            payload["confidence"] = 0.0
        else:
            payload["confidence"] = float(confidence)

        # Confidence hard gate (canonical)
        if payload["confidence"] < 0.70:
            # override everything
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=payload["confidence"])

        # Notes immutable
        payload["notes"] = NOTES_IMMUTABLE

        return payload

    except Exception:
        # Fail-closed
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0)


# -----------------------------
# KERNEL HOOK (REPLACE LATER)
# -----------------------------
def _run_kernel_on_pdf(file_path: str) -> Dict[str, Any]:
    """
    This is the only place you should later plug in the real Gemini/kernel logic.
    For Alpha wiring, it returns a deterministic placeholder payload.
    """

    # TODO (Gemini integration): parse + extract + run detection rules
    # MUST remain non-prescriptive and evidence-bound.

    return {
        "version": KERNEL_VERSION,
        "timestamp": _utc_iso(),
        "status": "RISK_DETECTED",
        "risk_level": "HIGH",
        "findings": [
            {
                "type": "METRO_2_STATUS_MISMATCH",
                "description": "Post-discharge account shows status '05' instead of '13'",
                "evidence": {
                    "document": os.path.basename(file_path),
                    "page": 4,
                    "field": "Account Status",
                },
            }
        ],
        "confidence": 0.95,
        "notes": NOTES_IMMUTABLE,
    }


# -----------------------------
# PUBLIC API (CALLED BY STREAMLIT)
# -----------------------------
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    """
    Canonical entry point for NorthStar Hub.
    Streamlit calls this function with a local temp PDF path.
    Output MUST match NS-DK-1.0.
    """

    # Basic file checks (no OCR here; just sanity)
    if not file_path or not isinstance(file_path, str):
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0)

    if not os.path.exists(file_path):
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0)

    if not file_path.lower().endswith(".pdf"):
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0)

    # Run kernel
    raw = _run_kernel_on_pdf(file_path)

    # Validate + enforce hard gates
    return _validate_payload(raw)


# -----------------------------
# LOCAL SMOKE TEST
# -----------------------------
if __name__ == "__main__":
    print("NorthStar Hub Backend Initialized...")
    # Optional: quick sanity check (won't run kernel unless you pass a PDF path)
    sample = _empty_payload()
    print(sample)
