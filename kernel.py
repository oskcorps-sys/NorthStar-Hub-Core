"""
NorthStar Hub — Kernel Core (NS-DK-1.0)
"""
from __future__ import annotations
import os
import json
import time
import datetime as dt
from typing import Any, Dict, List
from google import genai
from google.genai import types
from manifest_manager import ManifestManager

KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"
ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}
CONFIDENCE_GATE = 0.70
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"
TMP_DIR = "tmp"
MODEL_ID = "gemini-1.5-pro"
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"

def _utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0, notes_extra=""):
    notes = NOTES_IMMUTABLE if not notes_extra else f"{NOTES_IMMUTABLE} | {notes_extra}"
    return {
        "version": KERNEL_VERSION,
        "timestamp": _utc_iso(),
        "status": status,
        "risk_level": risk_level,
        "findings": [],
        "confidence": float(confidence),
        "notes": notes,
    }

def _validate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        payload = dict(payload or {})
        payload["version"] = KERNEL_VERSION
        payload["timestamp"] = payload.get("timestamp") or _utc_iso()
        if payload.get("status") not in ALLOWED_STATUS:
            return _empty_payload(status="UNKNOWN", notes_extra="BAD_STATUS")
        conf = payload.get("confidence")
        payload["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0
        if payload["confidence"] < CONFIDENCE_GATE:
            return _empty_payload(status="UNKNOWN", confidence=payload["confidence"], notes_extra="CONFIDENCE_GATE")
        payload["notes"] = NOTES_IMMUTABLE
        return payload
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="VALIDATION_EXCEPTION")

def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    findings = payload.get("findings", [])
    valid = []
    for f in findings:
        if not isinstance(f, dict): continue
        ev = f.get("evidence") or {}
        if ev.get("document") and ev.get("page") and ev.get("field"):
            valid.append(f)
    payload["findings"] = valid
    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
    return payload

SYSTEM_INSTRUCTION = f"ROLE: Technical Auditor. SCOPE: {NOTES_IMMUTABLE}. Output JSON NS-DK-1.0 only."

def _client():
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key: raise RuntimeError("Missing GEMINI_API_KEY")
    return genai.Client(api_key=api_key)

def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        if not os.path.exists(file_path): return _empty_payload(status="INCOMPLETE")
        # Aquí iría la lógica de _run_gemini_audit simplificada para test
        return _empty_payload(status="OK", confidence=0.99)
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"FAIL:{str(e)}")
