"""
NorthStar Hub — Kernel Core (NS-DK-1.0)
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
"""

from __future__ import annotations

import json
import os
import time
import datetime as dt
from typing import Any, Dict, List

from google import genai

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

# -----------------------------
# GEMINI
# -----------------------------
# Use the model that actually exists in Gemini API now
MODEL_ID = "gemini-2.5-flash"
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"

# -----------------------------
# PROMPT (HARD-LOCKED)
# -----------------------------
SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.

HARD RULES:
1) Output ONLY valid JSON. No extra text.
2) NO recommendations, NO action steps, NO dispute-letter writing, NO lender suggestions.
3) Every finding MUST include evidence with: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report is unreadable / scan quality weak / missing sections => status INCOMPLETE.
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
        raise RuntimeError("MISSING_GEMINI_API_KEY_ENV")
    return genai.Client(api_key=api_key)


def _wait_file_active(client: genai.Client, file_obj) -> Any:
    # Wait until the file is ACTIVE (or not processing)
    while getattr(getattr(file_obj, "state", None), "name", "") == "PROCESSING":
        time.sleep(2)
        file_obj = client.files.get(name=file_obj.name)
    return file_obj


def _normalize_contract(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw = dict(raw or {})
    raw["version"] = KERNEL_VERSION
    raw["timestamp"] = raw.get("timestamp") or _utc_iso()
    raw["notes"] = NOTES_IMMUTABLE

    if raw.get("status") not in ALLOWED_STATUS:
        raw["status"] = "UNKNOWN"

    if raw.get("risk_level") not in ALLOWED_RISK:
        raw["risk_level"] = "NONE"

    if not isinstance(raw.get("findings"), list):
        raw["findings"] = []

    conf = raw.get("confidence", 0.0)
    raw["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0

    return raw


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    findings = payload.get("findings", [])
    if not isinstance(findings, list):
        payload["findings"] = []
        return payload

    valid: List[Dict[str, Any]] = []
    for f in findings:
        if not isinstance(f, dict):
            continue
        ev = f.get("evidence")
        if not isinstance(ev, dict):
            continue

        doc = ev.get("document")
        page = ev.get("page")
        field = ev.get("field")

        if not doc or page is None or not field:
            continue

        if str(page).strip().upper() == "UNKNOWN":
            continue
        if str(field).strip().upper() == "UNKNOWN":
            continue

        valid.append(f)

    payload["findings"] = valid

    # If it claims risk but can't prove it -> fail closed
    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

    return payload


def _confidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    conf = float(payload.get("confidence", 0.0) or 0.0)
    if conf < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            risk_level="NONE",
            confidence=conf,
            notes_extra="CONFIDENCE_GATE_ACTIVE",
        )
    return payload


def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # 1) SOUL manifests (re-upload if missing/expired)
    try:
        mm = ManifestManager(MANIFEST_PATH, client)
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    except Exception:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_MANIFEST_FAIL")

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_EMPTY")

    # 2) Upload report using the correct SDK signature: file=...
    try:
        report_file = client.files.upload(file=report_path)
        report_file = _wait_file_active(client, report_file)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="REPORT_UPLOAD_FAIL")

    # 3) Compose request (NO Part.from_text to avoid signature drift)
    # Contents accepts file objects + strings in the Python SDK examples. :contentReference[oaicite:2]{index=2}
    contents = []

    # Instruction first (deterministic)
    contents.append(
        "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
        "Identify technical discrepancies only. Return ONLY NS-DK-1.0 JSON."
    )

    # Attach SOUL PDFs (as uploaded file references)
    # We re-get remote file objects to pass directly (stable).
    for ref in soul_refs:
        try:
            remote = client.files.get(name=ref["name"])
            contents.append(remote)
        except Exception:
            # If one SOUL doc is missing remotely, keep going (fail-closed later via evidence gate)
            continue

    # Attach report last
    contents.append(report_file)

    # 4) Model call
    try:
        resp = client.models.generate_content(
            model=MODEL_ID,
            contents=contents,
            config={
                "system_instruction": SYSTEM_INSTRUCTION,
                "temperature": 0.0,
                "response_mime_type": "application/json",
            },
        )
        raw = json.loads(resp.text)
    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")
    except Exception as e:
        # Catch rate limit + model name + general client errors
        msg = str(e)
        if "429" in msg:
            return _empty_payload(status="UNKNOWN", notes_extra="MODEL_CALL_FAIL:429")
        if "404" in msg:
            return _empty_payload(status="UNKNOWN", notes_extra="MODEL_CALL_FAIL:404")
        return _empty_payload(status="UNKNOWN", notes_extra="MODEL_CALL_FAIL")

    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)
    payload = _confidence_gate(payload)
    return payload


# -----------------------------
# PUBLIC API (CALLED BY UI)
# -----------------------------
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")

        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        if not os.path.isdir(SOUL_DIR):
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

        return _run_gemini_audit(file_path)

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
