"""
NorthStar Hub — Kernel Core (NS-DK-1.0)
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
- Gemini Native (google-genai) + Files API
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

# -----------------------------
# CANON (DO NOT DRIFT)
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}

CONFIDENCE_GATE = 0.70

# -----------------------------
# PATHS (Repo-local for Alpha)
# -----------------------------
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"          # Must exist in repo
MANIFEST_PATH = "manifests/soul_manifest.json"
TMP_DIR = "tmp"

# -----------------------------
# GEMINI
# -----------------------------
MODEL_ID = "gemini-1.5-pro"
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"


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


def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY environment variable.")
    return genai.Client(api_key=api_key)


def _upload_and_wait(client: genai.Client, path: str) -> types.File:
    f = client.files.upload(path=path)
    while getattr(f.state, "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Strips findings missing required evidence fields.
    If status=RISK_DETECTED but no valid findings => UNKNOWN.
    """
    findings = payload.get("findings", [])
    if not isinstance(findings, list):
        payload["findings"] = []
        return payload

    valid: List[Dict[str, Any]] = []
    for f in findings:
        if not isinstance(f, dict):
            continue
        ev = f.get("evidence") or {}
        if not isinstance(ev, dict):
            continue

        doc = ev.get("document")
        page = ev.get("page")
        field = ev.get("field")

        if doc and page and field:
            if str(page).strip().upper() != "UNKNOWN" and str(field).strip().upper() != "UNKNOWN":
                valid.append(f)

    payload["findings"] = valid

    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

    return payload


def _normalize_contract(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes model output to NS-DK-1.0. Fail-closed defaults.
    """
    if not isinstance(raw, dict):
        return _empty_payload(status="UNKNOWN", notes_extra="NON_DICT_OUTPUT")

    payload = {
        "version": KERNEL_VERSION,
        "timestamp": _utc_iso(),
        "status": raw.get("status", "UNKNOWN"),
        "risk_level": raw.get("risk_level", "NONE"),
        "findings": raw.get("findings", []),
        "confidence": raw.get("confidence", 0.0),
        "notes": NOTES_IMMUTABLE,
    }

    if payload["status"] not in ALLOWED_STATUS:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_STATUS")

    if payload["risk_level"] not in ALLOWED_RISK:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_RISK_LEVEL")

    if not isinstance(payload.get("findings"), list):
        payload["findings"] = []

    try:
        payload["confidence"] = float(payload.get("confidence", 0.0))
    except Exception:
        payload["confidence"] = 0.0

    # Hard integrity gate
    if payload["confidence"] < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            confidence=payload["confidence"],
            notes_extra="CONFIDENCE_GATE"
        )

    payload["notes"] = NOTES_IMMUTABLE
    return payload


SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies and mismatches between a credit report and provided standards (SOUL docs).

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations, NO action steps, NO dispute-letter writing, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report text is unreadable/scan/OCR weak => status INCOMPLETE.
6) User narrative is NOT evidence. Only the PDFs.

NS-DK-1.0 JSON CONTRACT:
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

OUTPUT REQUIREMENTS:
- If no inconsistencies found => status OK, risk_level NONE.
- If file unreadable or missing key pages => status INCOMPLETE.
- If unsure => status UNKNOWN (fail-closed).
""".strip()


def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # 1) Ensure SOUL files are active in Gemini Files API
    mm = ManifestManager(MANIFEST_PATH, client)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # 2) Upload report (STREET)
    report_file = _upload_and_wait(client, report_path)

    # 3) Build contents: SOUL PDFs + report PDF + instruction
    parts: List[types.Part] = []

    # Attach SOUL first (context)
    for ref in soul_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    # Attach report second (target)
    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

    # Final task instruction
    parts.append(types.Part.from_text(
        "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
        "Identify technical discrepancies only. Return ONLY NS-DK-1.0 JSON."
    ))

    resp = client.models.generate_content(
        model=MODEL_ID,
        contents=[types.Content(role="user", parts=parts)],
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )

    if not getattr(resp, "text", None):
        return _empty_payload(status="UNKNOWN", notes_extra="EMPTY_MODEL_RESPONSE")

    raw = json.loads(resp.text)

    # Evidence gate first
    raw = _evidence_gate(raw)

    # Normalize to contract (+ confidence gate)
    payload = _normalize_contract(raw)

    # Extra hard rule: RISK_DETECTED must have findings
    if payload.get("status") == "RISK_DETECTED" and not payload.get("findings"):
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

    return payload


def audit_credit_report(file_path: str) -> Dict[str, Any]:
    """
    Canonical entry point called by Streamlit (main.py)
    """
    try:
        # Input checks
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")

        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        # SOUL must exist in repo
        if not os.path.isdir(SOUL_DIR):
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

        os.makedirs(TMP_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)

        return _run_gemini_audit(file_path)

    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")


if __name__ == "__main__":
    print("NorthStar Hub Kernel Initialized...")
    print(_empty_payload())
