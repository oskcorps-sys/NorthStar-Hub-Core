"""
NorthStar Hub — Kernel Core (NS-DK-1.0)
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
- Gemini Native Files API (google-genai)
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types

# Local helper (must exist in repo): manifest_manager.py
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
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"          # folder in repo (PDF standards)
MANIFEST_PATH = "manifests/soul_manifest.json"
TMP_DIR = "tmp"

# -----------------------------
# GEMINI
# -----------------------------
MODEL_ID = "gemini-1.5-pro"
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"


# -----------------------------
# SYSTEM INSTRUCTION (HARD-LOCK)
# -----------------------------
SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect ONLY technical inconsistencies/mismatches between the credit report PDF and provided SOUL standards PDFs.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations, NO action steps, NO dispute letters, NO lender suggestions, NO "what to do".
3) EVERY finding MUST include evidence: document + page + field (all required).
4) If evidence is missing/ambiguous => DO NOT output the finding.
5) If report is unreadable/scan/OCR weak OR key pages missing => status INCOMPLETE.
6) If unsure => status UNKNOWN (fail-closed).
7) User narrative is NOT evidence. Only the PDFs.

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
- If no inconsistencies found => status OK, risk_level NONE, findings [].
- If file unreadable or missing key pages => status INCOMPLETE.
- If unsure => status UNKNOWN (fail-closed).
""".strip()


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


def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY environment variable.")
    return genai.Client(api_key=api_key)


def _files_upload_compat(client: genai.Client, file_path: str):
    """
    google-genai SDK has had signature differences across versions.
    This helper tries the known variants to avoid 'unexpected keyword argument' errors.
    """
    # 1) Named arg: file=
    try:
        return client.files.upload(file=file_path)
    except TypeError:
        pass

    # 2) Positional arg
    try:
        return client.files.upload(file_path)
    except TypeError:
        pass

    # 3) Older/other variant: path=
    # (kept as last attempt; your earlier error suggests this one may fail)
    return client.files.upload(path=file_path)


def _wait_until_active(client: genai.Client, f) -> Any:
    while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


def _validate_payload(payload: Any) -> Dict[str, Any]:
    """
    Enforces NS-DK-1.0 contract and fail-closed behavior.
    Hard gate: confidence < CONFIDENCE_GATE => UNKNOWN (no conclusions).
    """
    try:
        # ✅ HARD TYPE GATE
        if not isinstance(payload, dict):
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="BAD_JSON_SCHEMA")

        payload = dict(payload)
        payload["version"] = KERNEL_VERSION
        payload["timestamp"] = payload.get("timestamp") or _utc_iso()

        if payload.get("status") not in ALLOWED_STATUS:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="BAD_STATUS")

        if payload.get("risk_level") not in ALLOWED_RISK:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="BAD_RISK_LEVEL")

        if not isinstance(payload.get("findings"), list):
            payload["findings"] = []

        conf = payload.get("confidence")
        try:
            payload["confidence"] = float(conf)
        except Exception:
            payload["confidence"] = 0.0

        # Integrity gate (Alpha)
        if payload["confidence"] < CONFIDENCE_GATE:
            return _empty_payload(
                status="UNKNOWN",
                risk_level="NONE",
                confidence=payload["confidence"],
                notes_extra="CONFIDENCE_GATE",
            )

        payload["notes"] = NOTES_IMMUTABLE
        return payload

    except Exception:
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="VALIDATION_EXCEPTION")


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

        # Must be present + not placeholder
        if doc and page and field:
            if str(page).strip().upper() != "UNKNOWN" and str(field).strip().upper() != "UNKNOWN":
                valid.append(f)

    payload["findings"] = valid

    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

    return payload


# -----------------------------
# CORE RUNNER
# -----------------------------
def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # 1) Ensure SOUL folder exists
    if not os.path.isdir(SOUL_DIR):
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0, notes_extra="SOUL_DIR_MISSING")

    # 2) Ensure SOUL PDFs are ACTIVE via manifest (re-upload if needed)
    mm = ManifestManager(MANIFEST_PATH, client)
    try:
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    except Exception:
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0, notes_extra="SOUL_MANIFEST_FAIL")

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0, notes_extra="SOUL_NO_PDFS_FOUND")

    # 3) Upload report (STREET) + wait
    report_file = _files_upload_compat(client, report_path)
    report_file = _wait_until_active(client, report_file)

    # 4) Build Parts: SOUL then REPORT then task instruction
    parts: List[types.Part] = []

    for ref in soul_refs:
        # ref expected: {"name": "...", "uri": "...", "local": "..."}  (uri required)
        uri = ref.get("uri")
        if uri:
            parts.append(types.Part.from_uri(file_uri=uri, mime_type="application/pdf"))

    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

    parts.append(
        types.Part.from_text(
            "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
            "Identify ONLY technical discrepancies. Return ONLY NS-DK-1.0 JSON."
        )
    )

    # 5) Execute
    resp = client.models.generate_content(
        model=MODEL_ID,
        contents=[types.Content(role="user", parts=parts)],
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )

    # 6) Parse (fail-closed)
    try:
        raw = json.loads(resp.text)
    except Exception:
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="BAD_JSON_OUTPUT")

    if not isinstance(raw, dict):
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="BAD_JSON_SCHEMA")

    raw = _evidence_gate(raw)
    return _validate_payload(raw)


# -----------------------------
# PUBLIC API (CALLED BY STREAMLIT main.py)
# -----------------------------
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    """
    Canonical entry point called by Streamlit (main.py).
    Input: local PDF path
    Output: NS-DK-1.0 dict
    """
    try:
        # Input hard gates (fail-closed)
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0, notes_extra="BAD_INPUT")

        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0, notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", risk_level="NONE", confidence=0.0, notes_extra="NOT_PDF")

        os.makedirs(TMP_DIR, exist_ok=True)

        return _run_gemini_audit(file_path)

    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="BAD_JSON_OUTPUT")
    except Exception as e:
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
