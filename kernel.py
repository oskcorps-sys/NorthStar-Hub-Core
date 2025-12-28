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
# PATHS (Repo-local for Alpha)
# -----------------------------
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"          # (tu folder en repo)
MANIFEST_PATH = "manifests/soul_manifest.json"
TMP_DIR = "tmp"

# -----------------------------
# GEMINI
# -----------------------------
# Recomendado por docs actuales del SDK:
DEFAULT_MODEL_ID = "gemini-2.5-flash"  # rápido y estable en el SDK actual
MODEL_ID = os.getenv("NS_MODEL_ID", DEFAULT_MODEL_ID)

GEMINI_API_KEY_ENV = "GEMINI_API_KEY"


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


def _normalize_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza llaves mínimas del contrato, sin “inventar” findings.
    """
    payload = dict(payload or {})
    payload["version"] = KERNEL_VERSION
    payload["timestamp"] = payload.get("timestamp") or _utc_iso()

    if payload.get("status") not in ALLOWED_STATUS:
        payload["status"] = "UNKNOWN"
    if payload.get("risk_level") not in ALLOWED_RISK:
        payload["risk_level"] = "NONE"

    if not isinstance(payload.get("findings"), list):
        payload["findings"] = []

    conf = payload.get("confidence")
    payload["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0

    payload["notes"] = NOTES_IMMUTABLE
    return payload


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


def _validate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enforces NS-DK-1.0 contract and fail-closed behavior.
    Hard gate: confidence < CONFIDENCE_GATE => UNKNOWN (no conclusions).
    """
    try:
        payload = _normalize_contract(payload)

        # Integrity gate
        if payload["confidence"] < CONFIDENCE_GATE:
            return _empty_payload(
                status="UNKNOWN",
                risk_level="NONE",
                confidence=payload["confidence"],
                notes_extra="CONFIDENCE_GATE",
            )

        return payload

    except Exception:
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="VALIDATION_EXCEPTION")


# -----------------------------
# PROMPT (BLINDAJE)
# -----------------------------
SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.

MISSION:
Detect technical inconsistencies and mismatches between a credit report PDF and the SOUL standards PDFs.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations. NO action steps. NO dispute-letter writing. NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report is unreadable / scanned / OCR weak => status INCOMPLETE.
6) User narrative is NOT evidence. Only the PDFs provided.

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
# GEMINI CLIENT + FILE HELPERS
# -----------------------------
def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY environment variable.")
    return genai.Client(api_key=api_key)


def _upload_and_wait(client: genai.Client, local_path: str) -> Any:
    """
    IMPORTANT: google-genai expects upload(file=...) in the current SDK.
    """
    f = client.files.upload(file=local_path)
    while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


# -----------------------------
# CORE RUNNER
# -----------------------------
def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # Ensure folders exist
    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)

    # 1) Ensure SOUL files are active (manifest handles re-upload)
    mm = ManifestManager(MANIFEST_PATH, client)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # 2) Upload report (STREET)
    report_file = _upload_and_wait(client, report_path)

    # 3) Build contents: SOUL PDFs + report PDF + task text
    parts: List[types.Part] = []

    # SOUL first (context)
    for ref in soul_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    # Report second (target)
    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

    # Task instruction (MUST be keyword arg "text=")
    parts.append(
        types.Part.from_text(
            text="Perform a technical data consistency audit of the attached credit report against SOUL standards. "
                 "Identify technical discrepancies only. Return ONLY NS-DK-1.0 JSON."
        )
    )

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
        return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{type(e).__name__}")

    # Parse JSON safely
    try:
        raw = json.loads(resp.text)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")

    # Gates
    raw = _normalize_contract(raw)
    raw = _evidence_gate(raw)
    return _validate_payload(raw)


# -----------------------------
# PUBLIC API (CALLED BY Streamlit)
# -----------------------------
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

        if not os.path.isdir(SOUL_DIR):
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

        return _run_gemini_audit(file_path)

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
