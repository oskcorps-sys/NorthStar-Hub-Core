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
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
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


def _normalize_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize keys + enforce required fields presence.
    Fail-closed: anything weird => UNKNOWN.
    """
    try:
        p = dict(payload or {})

        # Required top-level fields (default safe)
        status = p.get("status", "UNKNOWN")
        risk = p.get("risk_level", "NONE")
        findings = p.get("findings", [])
        confidence = p.get("confidence", 0.0)

        # Type normalization
        if status not in ALLOWED_STATUS:
            status = "UNKNOWN"
        if risk not in ALLOWED_RISK:
            risk = "NONE"
        if not isinstance(findings, list):
            findings = []
        if not isinstance(confidence, (int, float)):
            confidence = 0.0

        # Canonical fields overwrite
        p["version"] = KERNEL_VERSION
        p["timestamp"] = _utc_iso()
        p["status"] = status
        p["risk_level"] = risk
        p["findings"] = findings
        p["confidence"] = float(confidence)
        p["notes"] = NOTES_IMMUTABLE

        return p
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="NORMALIZE_FAIL")


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove findings missing required evidence fields.
    If status=RISK_DETECTED but no valid findings => UNKNOWN.
    """
    try:
        findings = payload.get("findings", [])
        if not isinstance(findings, list):
            payload["findings"] = []
            return payload

        valid = []
        for f in findings:
            if not isinstance(f, dict):
                continue

            ev = f.get("evidence") or {}
            if not isinstance(ev, dict):
                continue

            doc = ev.get("document")
            page = ev.get("page")
            field = ev.get("field")

            # must exist & not be placeholders
            if doc and page and field:
                if str(page).strip().upper() != "UNKNOWN" and str(field).strip().upper() != "UNKNOWN":
                    valid.append(f)

        payload["findings"] = valid

        if payload.get("status") == "RISK_DETECTED" and not valid:
            payload["status"] = "UNKNOWN"
            payload["risk_level"] = "NONE"
            payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

        return payload
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="EVIDENCE_GATE_FAIL")


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


def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY environment variable.")
    return genai.Client(api_key=api_key)


def _upload_and_wait(client: genai.Client, path: str) -> Any:
    f = client.files.upload(path=path)
    while getattr(f.state, "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # 1) Ensure SOUL files are active
    mm = ManifestManager(MANIFEST_PATH, client)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # 2) Upload report (STREET)
    report_file = _upload_and_wait(client, report_path)

    # 3) Build contents (CLOUD-SAFE) — avoids TypeError from Part.from_uri signature mismatch
    contents = []

    # SOUL manuals first
    for ref in soul_refs:
        contents.append(
            types.Content(
                role="user",
                parts=[types.Part.from_uri(ref["uri"])]
            )
        )

    # Report next
    contents.append(
        types.Content(
            role="user",
            parts=[types.Part.from_uri(report_file.uri)]
        )
    )

    # Instruction last
    contents.append(
        types.Content(
            role="user",
            parts=[types.Part.from_text(
                "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
                "Identify technical discrepancies only. Return ONLY NS-DK-1.0 JSON."
            )]
        )
    )

    resp = client.models.generate_content(
        model=MODEL_ID,
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )

    raw = json.loads(resp.text)
    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)

    # Confidence gate (final)
    if float(payload.get("confidence", 0.0)) < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            risk_level="NONE",
            confidence=payload.get("confidence", 0.0),
            notes_extra="CONFIDENCE_GATE_ACTIVE",
        )

    return payload


def audit_credit_report(file_path: str) -> Dict[str, Any]:
    """
    Canonical entry point called by Streamlit (main.py)
    """
    try:
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")

        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        os.makedirs(TMP_DIR, exist_ok=True)

        return _run_gemini_audit(file_path)

    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
