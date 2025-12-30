"""
NorthStar Hub — Kernel Core (NS-DK-1.0)
Forensic Technical Data Consistency Engine
FAIL-CLOSED • Evidence-Bound • Bureau-Aware
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

# ======================================================
# CANON (DO NOT DRIFT)
# ======================================================

KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {
    "OK",
    "RISK_DETECTED",
    "INCOMPLETE",
    "UNKNOWN",
    "SCOPE_LIMITATION",
}

ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}

CONFIDENCE_GATE = 0.70

# ======================================================
# PATHS (Repo-Local)
# ======================================================

SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"
TMP_DIR = "tmp"

# ======================================================
# GEMINI CONFIG
# ======================================================

MODEL_ID = "gemini-2.5-flash"
API_KEY_ENV = "GEMINI_API_KEY"


# ======================================================
# UTILITIES
# ======================================================

def _utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _empty_payload(
    status: str = "INCOMPLETE",
    risk_level: str = "NONE",
    confidence: float = 0.0,
    notes_extra: str = "",
) -> Dict[str, Any]:
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


def _validate_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enforce NS-DK-1.0 schema + fail-closed behavior
    """
    try:
        payload = dict(payload or {})
        payload["version"] = KERNEL_VERSION
        payload["timestamp"] = payload.get("timestamp") or _utc_iso()

        if payload.get("status") not in ALLOWED_STATUS:
            return _empty_payload(status="UNKNOWN", notes_extra="BAD_STATUS")

        if payload.get("risk_level") not in ALLOWED_RISK:
            return _empty_payload(status="UNKNOWN", notes_extra="BAD_RISK_LEVEL")

        if not isinstance(payload.get("findings"), list):
            payload["findings"] = []

        conf = payload.get("confidence")
        payload["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0

        payload["notes"] = NOTES_IMMUTABLE
        return payload

    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="CONTRACT_VALIDATION_FAIL")


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove findings without concrete evidence.
    If RISK_DETECTED but no valid findings remain → UNKNOWN.
    """
    findings = payload.get("findings", [])
    valid: List[Dict[str, Any]] = []

    for f in findings:
        if not isinstance(f, dict):
            continue
        ev = f.get("evidence", {})
        if not isinstance(ev, dict):
            continue

        if ev.get("document") and ev.get("page") and ev.get("field"):
            if str(ev["page"]).upper() != "UNKNOWN" and str(ev["field"]).upper() != "UNKNOWN":
                valid.append(f)

    payload["findings"] = valid

    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(payload.get("confidence", 0.0), 0.5)

    return payload


# ======================================================
# SYSTEM INSTRUCTION (FINAL)
# ======================================================

SYSTEM_INSTRUCTION = f"""
ROLE: Principal Forensic Data Consistency Auditor (NorthStar Hub)

SCOPE:
{NOTES_IMMUTABLE}.
This system is NOT legal advice, NOT financial advice, and NOT credit repair.

MISSION:
Audit a credit report PDF against authoritative SOUL standards to detect
evidence-bound technical inconsistencies only.

OPERATING MODE:
FAIL-CLOSED. Prefer silence over speculation.

BUREAU-AWARE LOGIC:
1) Identify bureau implicitly (Experian, Equifax, TransUnion).
2) Apply Metro 2 as universal reference.
3) Respect bureau-specific conventions when supported by SOUL materials.
4) Flag CODE_MAPPING_INCONSISTENCY only if no valid mapping exists.

HARD RULES:
- OUTPUT ONLY valid NS-DK-1.0 JSON.
- NO recommendations, NO advice, NO action steps.
- EVERY finding must include document, page, and field.
- If evidence is ambiguous or missing → DO NOT report.
- If scanned/unreadable → INCOMPLETE.
- If uncertain → UNKNOWN.

CONFIDENCE:
If confidence < {CONFIDENCE_GATE}, output UNKNOWN and remove findings.

JSON CONTRACT:
{{
  "version": "{KERNEL_VERSION}",
  "timestamp": "ISO-UTC",
  "status": "OK | RISK_DETECTED | INCOMPLETE | UNKNOWN | SCOPE_LIMITATION",
  "risk_level": "NONE | LOW | MEDIUM | HIGH",
  "findings": [
    {{
      "type": "STRING_ENUM",
      "description": "technical, concise",
      "evidence": {{
        "document": "PDF_NAME",
        "page": NUMBER,
        "field": "FIELD_NAME"
      }}
    }}
  ],
  "confidence": NUMBER,
  "notes": "{NOTES_IMMUTABLE}"
}}
""".strip()


# ======================================================
# GEMINI CORE
# ======================================================

def _client() -> genai.Client:
    api_key = os.getenv(API_KEY_ENV)
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY missing")
    return genai.Client(api_key=api_key)


def _upload_and_wait(client: genai.Client, file_path: str):
    f = client.files.upload(file=file_path)
    while getattr(f.state, "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


def _run_gemini_audit(file_path: str) -> Dict[str, Any]:
    client = _client()

    # Load SOUL
    if not os.path.isdir(SOUL_DIR):
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

    mm = ManifestManager(MANIFEST_PATH, client)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_EMPTY")

    # Upload report
    report = _upload_and_wait(client, file_path)

    parts: List[types.Part] = []

    for ref in soul_refs:
        parts.append(types.Part.from_uri(
            file_uri=ref["uri"],
            mime_type="application/pdf"
        ))

    parts.append(types.Part.from_uri(
        file_uri=report.uri,
        mime_type="application/pdf"
    ))

    parts.append(types.Part.from_text(
        "Perform forensic technical consistency audit. Return ONLY NS-DK-1.0 JSON."
    ))

    response = client.models.generate_content(
        model=MODEL_ID,
        contents=[types.Content(role="user", parts=parts)],
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )

    raw = json.loads(response.text)
    raw = _validate_contract(raw)
    raw = _evidence_gate(raw)

    if raw["confidence"] < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            confidence=raw["confidence"],
            notes_extra="CONFIDENCE_GATE_ACTIVE",
        )

    return raw


# ======================================================
# PUBLIC API (CALLED BY main.py)
# ======================================================

def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        if not file_path or not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        os.makedirs(TMP_DIR, exist_ok=True)
        return _run_gemini_audit(file_path)

    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON")

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
