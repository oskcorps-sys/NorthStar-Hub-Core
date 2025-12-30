"""
NorthStar Hub — Kernel Core (NS-DK-1.1)
Scope: Technical Data Consistency Check Only
Fail-Closed | Evidence-Bound | Bureau-Adaptive
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from pathlib import Path
from typing import Dict, Any, List, Optional

from google import genai
from google.genai import types

from manifest_manager import ManifestManager


# ==========================================================
# CANON
# ==========================================================
KERNEL_VERSION = "NS-DK-1.1"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

CONFIDENCE_GATE = 0.70

SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"

MODEL_ID = "models/gemini-2.5-flash"   # CONFIRMADO CLOUD
API_ENV = "GEMINI_API_KEY"


# ==========================================================
# UTILITIES
# ==========================================================
def _utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _empty_payload(
    status: str = "UNKNOWN",
    risk_level: str = "NONE",
    confidence: float = 0.0,
    notes_extra: str = ""
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


# ==========================================================
# CLIENT
# ==========================================================
def _client() -> genai.Client:
    api_key = os.getenv(API_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY")
    return genai.Client(api_key=api_key)


def _upload_and_wait(client: genai.Client, path: str):
    f = client.files.upload(file=path)
    while getattr(f.state, "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


# ==========================================================
# SYSTEM INSTRUCTION (FINAL)
# ==========================================================
SYSTEM_INSTRUCTION = """
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY. Not legal advice. Not financial advice. Not credit repair.

MISSION:
Detect technical inconsistencies in a credit report PDF using evidence-only validation
against provided SOUL standards (Metro 2 + related references).

HARD RULES (FAIL-CLOSED):
1) OUTPUT ONLY valid JSON following the NS-DK-1.0 contract.
2) NO recommendations, NO repair language, NO advice.
3) Every finding MUST include: document, page (integer), field.
4) If evidence is missing or ambiguous → DO NOT output the finding.
5) If unsure → status UNKNOWN.
6) If unreadable/missing sections → status INCOMPLETE.

BUREAU ADAPTATION:
If a Bureau Translation Manifest (BTM) JSON is provided, it is authoritative.
Translate bureau codes BEFORE flagging mismatches.
Only flag CODE_MAPPING_INCONSISTENCY if no valid translation exists.

CONFIDENCE:
If confidence < 0.70 → status UNKNOWN and findings must be empty.

RETURN EXACTLY THIS JSON:
{
  "version": "NS-DK-1.0",
  "timestamp": "ISO-UTC",
  "status": "OK|RISK_DETECTED|INCOMPLETE|UNKNOWN|SCOPE_LIMITATION",
  "risk_level": "NONE|LOW|MEDIUM|HIGH",
  "findings": [
    {
      "type": "STRING",
      "description": "short, technical",
      "evidence": {
        "document": "PDF_NAME",
        "page": 1,
        "field": "FIELD_NAME"
      }
    }
  ],
  "confidence": 0.0,
  "notes": "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"
}
""".strip()


# ==========================================================
# KERNEL
# ==========================================================
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        # -------- Input Guard --------
        if not file_path or not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        if not os.path.isdir(SOUL_DIR):
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

        client = _client()

        # -------- Load SOUL --------
        mm = ManifestManager(MANIFEST_PATH, client)
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

        if not soul_refs:
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_EMPTY")

        # -------- Upload Target Report --------
        report_file = _upload_and_wait(client, file_path)

        # -------- Build Context --------
        parts: List[types.Part] = []

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
                text="Perform a technical consistency audit. Return ONLY NS-DK-1.0 JSON."
            )
        )

        # -------- Model Call --------
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

        # -------- Confidence Gate --------
        conf = float(raw.get("confidence", 0.0))
        if conf < CONFIDENCE_GATE:
            return _empty_payload(
                status="UNKNOWN",
                confidence=conf,
                notes_extra="CONFIDENCE_GATE_ACTIVE"
            )

        # -------- Normalize --------
        raw["version"] = KERNEL_VERSION
        raw["timestamp"] = _utc_iso()
        raw["notes"] = NOTES_IMMUTABLE

        return raw

    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON")

    except Exception as e:
        return _empty_payload(
            status="UNKNOWN",
            notes_extra=f"KERNEL_FAIL:{type(e).__name__}"
        )
