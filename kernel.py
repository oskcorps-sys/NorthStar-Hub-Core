from __future__ import annotations

import os
import json
import datetime as dt
from typing import Any, Dict, List

from google import genai
from google.genai import types

from manifest_manager import ManifestManager, upload_any

# -----------------------------
# CANON (DO NOT DRIFT)
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}
CONFIDENCE_GATE = 0.70

SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"

MODEL_ID = "gemini-1.5-pro"
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"


def _utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0, notes_extra="") -> Dict[str, Any]:
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


def _normalize_contract(p: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(p or {})
    p["version"] = KERNEL_VERSION
    p["timestamp"] = p.get("timestamp") or _utc_iso()
    p["notes"] = NOTES_IMMUTABLE

    if p.get("status") not in ALLOWED_STATUS:
        p["status"] = "UNKNOWN"
    if p.get("risk_level") not in ALLOWED_RISK:
        p["risk_level"] = "NONE"
    if not isinstance(p.get("findings"), list):
        p["findings"] = []

    conf = p.get("confidence")
    p["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0
    return p


def _evidence_gate(p: Dict[str, Any]) -> Dict[str, Any]:
    findings = p.get("findings", [])
    valid = []

    for f in findings:
        if not isinstance(f, dict):
            continue
        ev = f.get("evidence")
        if not isinstance(ev, dict):
            continue

        doc = ev.get("document")
        page = ev.get("page")
        field = ev.get("field")

        if doc and page and field:
            if str(page).strip().upper() != "UNKNOWN" and str(field).strip().upper() != "UNKNOWN":
                valid.append(f)

    p["findings"] = valid

    if p.get("status") == "RISK_DETECTED" and not valid:
        p["status"] = "UNKNOWN"
        p["risk_level"] = "NONE"
        p["confidence"] = min(float(p.get("confidence", 0.0)), 0.5)

    return p


SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies between the credit report and SOUL standards.

HARD RULES:
1) OUTPUT ONLY valid JSON (NS-DK-1.0). No extra text.
2) NO recommendations, NO action steps, NO dispute/repair language.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous => do NOT output the finding.
5) If unreadable/scan/OCR weak => status INCOMPLETE.
6) Only PDFs count as evidence.

JSON CONTRACT:
{{
  "version": "{KERNEL_VERSION}",
  "timestamp": "ISO-UTC",
  "status": "OK|RISK_DETECTED|INCOMPLETE|UNKNOWN|SCOPE_LIMITATION",
  "risk_level": "NONE|LOW|MEDIUM|HIGH",
  "findings": [
    {{
      "type": "STRING_ENUM",
      "description": "short, technical",
      "evidence": {{"document": "PDF_NAME", "page": 1, "field": "FIELD_NAME"}}
    }}
  ],
  "confidence": 0.0,
  "notes": "{NOTES_IMMUTABLE}"
}}
""".strip()


def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("MISSING_API_KEY")
    return genai.Client(api_key=api_key)


def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        # STEP 0: input
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")
        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")
        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        client = _client()

        # STEP 1: SOUL load + upload
        if not os.path.isdir(SOUL_DIR):
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

        try:
            mm = ManifestManager(MANIFEST_PATH, client)
            soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
        except Exception as e:
            return _empty_payload(status="INCOMPLETE", notes_extra=f"SOUL_MANIFEST_FAIL:{type(e).__name__}:{str(e)[:120]}")

        if not soul_refs:
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

        # STEP 2: upload report (robust)
        try:
            report_file = upload_any(client, file_path)
        except Exception as e:
            return _empty_payload(status="UNKNOWN", notes_extra=f"UPLOAD_REPORT_FAIL:{type(e).__name__}:{str(e)[:120]}")

        # STEP 3: build parts (SOUL first, then report)
        parts: List[types.Part] = []
        for ref in soul_refs:
            parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

        parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

        # ✅ IMPORTANT: keyword arg for your SDK version
        parts.append(
            types.Part.from_text(
                text=(
                    "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
                    "Return ONLY NS-DK-1.0 JSON."
                )
            )
        )

        # STEP 4: model call
        try:
            resp = client.models.generate_content(
                model=MODEL_ID,
                contents=[types.Content(role="user", parts=parts)],
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
        except Exception as e:
            return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{type(e).__name__}:{str(e)[:120]}")

        # STEP 5: parse JSON
        try:
            raw = json.loads(resp.text)
        except Exception as e:
            return _empty_payload(status="UNKNOWN", notes_extra=f"BAD_JSON_OUTPUT:{type(e).__name__}:{str(e)[:120]}")

        # STEP 6: gates + normalize
        payload = _normalize_contract(raw)
        payload = _evidence_gate(payload)

        if float(payload.get("confidence", 0.0)) < CONFIDENCE_GATE:
            return _empty_payload(
                status="UNKNOWN",
                confidence=float(payload.get("confidence", 0.0)),
                notes_extra="CONFIDENCE_GATE",
            )

        return payload

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}:{str(e)[:160]}")
