from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any

from google import genai
from google.genai import types

from manifest_manager import ManifestManager, upload_any

# =============================
# CANON
# =============================
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}

CONFIDENCE_GATE = 0.70

SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"

MODEL_ID = "models/gemini-2.5-flash"  # el que CONFIRMASTE
API_ENV = "GEMINI_API_KEY"


# =============================
# HELPERS
# =============================
def _utc() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _empty(status="UNKNOWN", risk="NONE", confidence=0.0, note="") -> Dict[str, Any]:
    return {
        "version": KERNEL_VERSION,
        "timestamp": _utc(),
        "status": status,
        "risk_level": risk,
        "findings": [],
        "confidence": float(confidence),
        "notes": f"{NOTES_IMMUTABLE}{' | ' + note if note else ''}",
    }


def _client():
    key = os.getenv(API_ENV)
    if not key:
        raise RuntimeError("Missing GEMINI_API_KEY")
    return genai.Client(api_key=key)


def _validate(payload: Dict[str, Any]) -> Dict[str, Any]:
    if payload.get("status") not in ALLOWED_STATUS:
        return _empty(note="BAD_STATUS")

    if payload.get("risk_level") not in ALLOWED_RISK:
        payload["risk_level"] = "NONE"

    conf = float(payload.get("confidence", 0.0) or 0.0)
    payload["confidence"] = conf

    if conf < CONFIDENCE_GATE:
        return _empty(
            status="UNKNOWN",
            confidence=conf,
            note="CONFIDENCE_GATE_ACTIVE",
        )

    payload["version"] = KERNEL_VERSION
    payload["timestamp"] = _utc()
    payload["notes"] = NOTES_IMMUTABLE
    return payload


# =============================
# CORE ENTRY
# =============================
def audit_credit_report(file_path: str, bureau_id: str = "UNKNOWN") -> Dict[str, Any]:
    try:
        # -------- Input guard --------
        if not file_path or not os.path.exists(file_path):
            return _empty(status="INCOMPLETE", note="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty(status="INCOMPLETE", note="NOT_PDF")

        client = _client()

        # -------- Load SOUL --------
        if not os.path.isdir(SOUL_DIR):
            return _empty(status="INCOMPLETE", note="SOUL_DIR_MISSING")

        mm = ManifestManager(MANIFEST_PATH, client)
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

        if not soul_refs:
            return _empty(status="INCOMPLETE", note="SOUL_EMPTY")

        # -------- Upload report --------
        report_file = upload_any(client, file_path)

        # -------- Build context --------
        parts = []

        for ref in soul_refs:
            parts.append(
                types.Part.from_uri(
                    file_uri=ref["uri"],
                    mime_type="application/pdf"
                )
            )

        parts.append(
            types.Part.from_uri(
                file_uri=report_file.uri,
                mime_type="application/pdf"
            )
        )

        parts.append(
            types.Part.from_text(
                f"""
ROLE: Principal Technical Data Consistency Auditor.
SCOPE: {NOTES_IMMUTABLE}.
BUREAU: {bureau_id}.

RULES:
- Evidence-bound findings only.
- No advice, no repair, no recommendations.
- Each finding MUST include document + page + field.
- If unsure, return UNKNOWN.

Return ONLY NS-DK-1.0 JSON.
""".strip()
            )
        )

        # -------- Gemini call --------
        resp = client.models.generate_content(
            model=MODEL_ID,
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )

        raw = json.loads(resp.text)
        return _validate(raw)

    except json.JSONDecodeError:
        return _empty(note="BAD_JSON_OUTPUT")

    except Exception as e:
        return _empty(note=f"KERNEL_FAIL:{type(e).__name__}")
