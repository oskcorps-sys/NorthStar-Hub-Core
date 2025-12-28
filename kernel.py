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
from typing import Any, Dict, List

from google import genai

from manifest_manager import ManifestManager, upload_any

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
MODEL_ID = "gemini-2.5-flash"
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"

SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.

HARD RULES:
1) Output ONLY valid JSON. No extra text.
2) NO recommendations, NO action steps, NO letter generation, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
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
        raise RuntimeError("MISSING_GEMINI_API_KEY")
    return genai.Client(api_key=api_key)


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

    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

    return payload


def _confidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    conf = float(payload.get("confidence", 0.0) or 0.0)
    if conf < CONFIDENCE_GATE:
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=conf, notes_extra="CONFIDENCE_GATE_ACTIVE")
    return payload


def _call_model_with_backoff(client: genai.Client, contents: List[Any]) -> str:
    delays = [1, 2, 4, 8]
    last_err: Exception | None = None

    for d in delays:
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
            return resp.text
        except Exception as e:
            last_err = e
            msg = str(e)
            if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                time.sleep(d)
                continue
            raise

    raise last_err if last_err else RuntimeError("MODEL_CALL_FAIL")


def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # SOUL sync
    try:
        mm = ManifestManager(MANIFEST_PATH, client)
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    except Exception:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_MANIFEST_FAIL")

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_EMPTY")

    # Upload report (SDK-proof)
    try:
        report_file = upload_any(client, report_path)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="REPORT_UPLOAD_FAIL")

    # Contents (NO types.Part, NO from_text, NO SDK drift)
    contents: List[Any] = []
    contents.append(
        "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
        "Identify technical discrepancies only. Return ONLY NS-DK-1.0 JSON."
    )

    for ref in soul_refs:
        try:
            contents.append(client.files.get(name=ref["name"]))
        except Exception:
            continue

    contents.append(report_file)

    # Model call
    try:
        text = _call_model_with_backoff(client, contents)
        raw = json.loads(text)
    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")
    except Exception as e:
        msg = str(e)
        if "404" in msg or "NOT_FOUND" in msg:
            return _empty_payload(status="UNKNOWN", notes_extra="MODEL_CALL_FAIL:404")
        if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
            return _empty_payload(status="UNKNOWN", notes_extra="MODEL_CALL_FAIL:429")
        return _empty_payload(status="UNKNOWN", notes_extra="MODEL_CALL_FAIL")

    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)
    payload = _confidence_gate(payload)
    return payload


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
