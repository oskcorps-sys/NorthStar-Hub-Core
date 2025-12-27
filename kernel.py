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
# PATHS (Repo-local)
# -----------------------------
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"
MODEL_ID = "gemini-1.5-pro"

GEMINI_API_KEY_ENV = "GEMINI_API_KEY"


def _utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _empty_payload(
    status: str = "INCOMPLETE",
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


def _normalize_contract(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Force NS-DK-1.0 envelope. Fail-closed on weird types.
    """
    if not isinstance(raw, dict):
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_RESPONSE_TYPE")

    payload = dict(raw)

    # version/timestamp/notes are authoritative from kernel
    payload["version"] = KERNEL_VERSION
    payload["timestamp"] = payload.get("timestamp") or _utc_iso()
    payload["notes"] = NOTES_IMMUTABLE

    # status/risk validation
    if payload.get("status") not in ALLOWED_STATUS:
        payload["status"] = "UNKNOWN"
    if payload.get("risk_level") not in ALLOWED_RISK:
        payload["risk_level"] = "NONE"

    # findings normalization
    if not isinstance(payload.get("findings"), list):
        payload["findings"] = []

    # confidence normalization
    conf = payload.get("confidence", 0.0)
    try:
        payload["confidence"] = float(conf)
    except Exception:
        payload["confidence"] = 0.0

    return payload


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only findings that have: evidence.document + evidence.page + evidence.field
    If status says RISK_DETECTED but nothing survives => UNKNOWN.
    """
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

        if doc and page and field:
            # avoid placeholders
            if str(page).strip().upper() != "UNKNOWN" and str(field).strip().upper() != "UNKNOWN":
                valid.append(f)

    payload["findings"] = valid

    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

    return payload


SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies between a credit report PDF and the provided SOUL standards PDFs.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations, NO action steps, NO dispute letters, NO lender guidance.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report is unreadable/scan/OCR weak => status INCOMPLETE.
6) User narrative is NOT evidence. Only PDFs.

OUTPUT RULES:
- If no inconsistencies => status OK, risk_level NONE.
- If missing key pages / unreadable => status INCOMPLETE.
- If unsure => status UNKNOWN (fail-closed).
""".strip()


def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("MISSING_GEMINI_API_KEY")
    return genai.Client(api_key=api_key)


def _upload_and_wait(client: genai.Client, path: str) -> Any:
    f = client.files.upload(path=path)
    while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # SOUL must exist in repo
    if not os.path.isdir(SOUL_DIR):
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

    mm = ManifestManager(MANIFEST_PATH, client)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # Upload report
    report_file = _upload_and_wait(client, report_path)

    # Build parts (NO KEYWORDS to avoid SDK signature mismatch)
    parts: List[Any] = []

    # SOUL first (context)
    for ref in soul_refs:
        uri = ref.get("uri")
        if uri:
            parts.append(types.Part.from_uri(uri, "application/pdf"))

    # Report (target)
    parts.append(types.Part.from_uri(report_file.uri, "application/pdf"))

    # Task
    parts.append(types.Part.from_text(
        "Perform a technical Metro 2 consistency audit of the attached credit report against the SOUL standards. "
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

    # Guard: if text is missing, fail-closed
    txt = getattr(resp, "text", None)
    if not txt or not isinstance(txt, str):
        return _empty_payload(status="UNKNOWN", notes_extra="EMPTY_RESPONSE_TEXT")

    raw = json.loads(txt)
    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)

    # Confidence gate
    if float(payload.get("confidence", 0.0) or 0.0) < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            risk_level="NONE",
            confidence=float(payload.get("confidence", 0.0) or 0.0),
            notes_extra="CONFIDENCE_GATE_ACTIVE"
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

        return _run_gemini_audit(file_path)

    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")
    except Exception as e:
        # IMPORTANT: include message so you can actually debug next time
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}:{str(e)}")
