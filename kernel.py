"""
NorthStar Hub — Kernel Core (NS-DK-1.0)
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict

from google.genai import Client, types
from manifest_manager import ManifestManager

# -----------------------------
# CANON (DO NOT DRIFT)
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"
CONFIDENCE_GATE = 0.70

SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"
MODEL_ID = "gemini-1.5-pro"


# -----------------------------
# UTILITIES
# -----------------------------
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


def _client() -> Client:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY")
    return Client(api_key=api_key)


# -----------------------------
# KERNEL
# -----------------------------
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        # 1) Input validation
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")

        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        client = _client()

        # 2) Load SOUL (repo-local)
        if not os.path.isdir(SOUL_DIR):
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

        mm = ManifestManager(MANIFEST_PATH, client)
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

        if not soul_refs:
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_EMPTY")

        # 3) Upload report
        report_file = client.files.upload(path=file_path)
        while getattr(report_file.state, "name", "") == "PROCESSING":
            time.sleep(2)
            report_file = client.files.get(name=report_file.name)

        # 4) Build Gemini context
        parts = []

        # SOUL first (standards)
        for ref in soul_refs:
            parts.append(
                types.Part.from_uri(
                    file_uri=ref["uri"],
                    mime_type="application/pdf",
                )
            )

        # Report second (object)
        parts.append(
            types.Part.from_uri(
                file_uri=report_file.uri,
                mime_type="application/pdf",
            )
        )

        # Instruction last
        parts.append(
            types.Part.from_text(
                "Perform a technical Metro 2 data consistency audit. "
                "Detect discrepancies only. "
                "Every finding MUST include document, page, and field. "
                "Return ONLY NS-DK-1.0 JSON."
            )
        )

        # 5) Gemini call
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )

        raw = json.loads(response.text)

        # 6) Confidence gate
        conf = float(raw.get("confidence", 0.0))
        if conf < CONFIDENCE_GATE:
            return _empty_payload(
                status="UNKNOWN",
                confidence=conf,
                notes_extra="CONFIDENCE_GATE_ACTIVE",
            )

        # 7) Normalize
        raw["version"] = KERNEL_VERSION
        raw["timestamp"] = _utc_iso()
        raw["notes"] = NOTES_IMMUTABLE

        return raw

    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_FROM_MODEL")

    except Exception as e:
        return _empty_payload(
            status="UNKNOWN",
            notes_extra=f"KERNEL_FAIL:{type(e).__name__}",
        )
