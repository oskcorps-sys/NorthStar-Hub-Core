"""
NorthStar Hub — Backend Core
Kernel: NS-DK-1.0 (Alpha)
Scope: Technical data consistency check only
"""

from __future__ import annotations

import os
import datetime as dt
from typing import Any, Dict

# -----------------------------
# CANON
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}


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
    # Fail-closed: if anything is off, degrade safely
    try:
        payload["version"] = KERNEL_VERSION
        payload["timestamp"] = payload.get("timestamp") or _utc_iso()

        if payload.get("status") not in ALLOWED_STATUS:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0)

        if payload.get("risk_level") not in ALLOWED_RISK:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0)

        if not isinstance(payload.get("findings"), list):
            payload["findings"] = []

        conf = payload.get("confidence")
        payload["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0

        # Hard gate for Alpha integrity
        if payload["confidence"] < 0.70:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=payload["confidence"])

        payload["notes"] = NOTES_IMMUTABLE
        return payload

    except Exception:
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0)


def _run_kernel_stub(file_path: str) -> Dict[str, Any]:
    """
    TEMP stub. Replace this function body with your real Gemini logic later.
    MUST remain non-prescriptive and evidence-bound.
    """
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


def audit_credit_report(file_path: str) -> Dict[str, Any]:
    """
    Canonical entry point for the Front-End.
    Input: local PDF path
    Output: NS-DK-1.0 dict
    """
    if not file_path or not isinstance(file_path, str):
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0)

    if not os.path.exists(file_path):
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0)

    if not file_path.lower().endswith(".pdf"):
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0)

    raw = _run_kernel_stub(file_path)
    return _validate_payload(raw)


if __name__ == "__main__":
    print("NorthStar Hub Backend Initialized...")
    print(_empty_payload())
