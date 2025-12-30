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
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types

from manifest_manager import ManifestManager

try:
    from PyPDF2 import PdfReader
except Exception:
    PdfReader = None


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
# Tú dijiste: "el API es 2.5 flash"
MODEL_ID = "gemini-2.5-flash"
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


def _detect_bureau(pdf_path: str) -> str:
    """
    Lightweight bureau detector (no LLM).
    Returns: 'EXPERIAN' | 'EQUIFAX' | 'TRANSUNION' | 'UNKNOWN'
    """
    if PdfReader is None:
        return "UNKNOWN"
    try:
        reader = PdfReader(pdf_path)
        if not reader.pages:
            return "UNKNOWN"
        text = (reader.pages[0].extract_text() or "").upper()
        if "EXPERIAN" in text:
            return "EXPERIAN"
        if "EQUIFAX" in text:
            return "EQUIFAX"
        if "TRANSUNION" in text or "TRANS UNION" in text:
            return "TRANSUNION"
        return "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _coerce_int(x, default=None):
    try:
        return int(str(x).strip())
    except Exception:
        return default


def _normalize_contract(payload: Dict[str, Any], report_path: str) -> Dict[str, Any]:
    """
    Kernel OWNS:
      - version, timestamp, notes
      - document name normalization
      - contract shape + enum enforcement
      - evidence gate (page+field required)
      - fail-closed if RISK_DETECTED but no valid findings
    """
    out = dict(payload or {})
    out["version"] = KERNEL_VERSION
    out["timestamp"] = _utc_iso()
    out["notes"] = NOTES_IMMUTABLE

    # enums
    if out.get("status") not in ALLOWED_STATUS:
        out["status"] = "UNKNOWN"
    if out.get("risk_level") not in ALLOWED_RISK:
        out["risk_level"] = "NONE"

    # confidence
    try:
        out["confidence"] = float(out.get("confidence", 0.0))
    except Exception:
        out["confidence"] = 0.0

    # findings must be list
    findings = out.get("findings", [])
    if not isinstance(findings, list):
        findings = []
    doc_name = os.path.basename(report_path)

    normalized_findings: List[Dict[str, Any]] = []
    for f in findings:
        if not isinstance(f, dict):
            continue
        ev = f.get("evidence") or {}
        if not isinstance(ev, dict):
            continue

        # force document
        ev["document"] = doc_name

        page = _coerce_int(ev.get("page"), default=None)
        field = ev.get("field")

        # require page + field
        if page is None:
            continue
        if not field or str(field).strip().upper() == "UNKNOWN":
            continue

        ev["page"] = page
        f["evidence"] = ev

        # minimal finding fields
        if "type" not in f or not f["type"]:
            f["type"] = "UNKNOWN_FINDING"
        if "description" not in f or not f["description"]:
            f["description"] = ""

        normalized_findings.append(f)

    out["findings"] = normalized_findings

    # fail-closed if claims risk but has no evidence
    if out.get("status") == "RISK_DETECTED" and not out["findings"]:
        out["status"] = "UNKNOWN"
        out["risk_level"] = "NONE"
        out["confidence"] = min(out["confidence"], 0.5)

    return out


SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies and mismatches between a credit report and SOUL standards.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations, NO action steps, NO dispute letters, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report is unreadable/scan/OCR weak => status INCOMPLETE.
6) User narrative is NOT evidence. Only the PDFs.
7) If bureau uses proprietary phrasing/codes, treat as "NEEDS_MAPPING" unless SOUL standards explicitly define it.

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


def _upload_and_wait(client: genai.Client, local_path: str) -> Any:
    """
    Upload report to Gemini Files API.
    Uses the robust uploader from ManifestManager module is separate,
    but for the report we can do simple upload (SDK usually stable).
    """
    # We still need to survive signature differences:
    # We'll try common patterns.
    try:
        f = client.files.upload(path=local_path)
    except TypeError:
        try:
            f = client.files.upload(file=local_path)
        except TypeError:
            with open(local_path, "rb") as fh:
                f = client.files.upload(file=fh)

    while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


def _call_model_with_retries(client: genai.Client, content: types.Content, max_tries: int = 4) -> str:
    """
    Handles transient 429 / 5xx with exponential backoff.
    Returns response.text (JSON string).
    """
    last_err = None
    for i in range(max_tries):
        try:
            resp = client.models.generate_content(
                model=MODEL_ID,
                contents=[content],
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
            return resp.text
        except Exception as e:
            last_err = e
            # backoff
            sleep_s = min(2 ** i, 10)
            time.sleep(sleep_s)
    raise last_err  # type: ignore


def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # Pre-read bureau for context (no separate function signature needed)
    bureau = _detect_bureau(report_path)

    # Ensure SOUL files are active (uploaded + ACTIVE)
    mm = ManifestManager(MANIFEST_PATH, client, max_pages=1000)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # Upload report
    report_file = _upload_and_wait(client, report_path)

    # Build parts (SOUL first, then report, then instruction)
    parts: List[types.Part] = []

    for ref in soul_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

    parts.append(
        types.Part.from_text(
            text=(
                f"BUREAU_ID={bureau}. Perform a technical consistency audit of the attached credit report against SOUL standards. "
                f"Return ONLY NS-DK-1.0 JSON. Evidence MUST include page+field."
            )
        )
    )

    content = types.Content(role="user", parts=parts)

    raw_text = _call_model_with_retries(client, content)
    raw = json.loads(raw_text)

    payload = _normalize_contract(raw, report_path=report_path)

    # Confidence gate (hard)
    if float(payload.get("confidence", 0.0)) < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            risk_level="NONE",
            confidence=float(payload.get("confidence", 0.0)),
            notes_extra="CONFIDENCE_GATE_ACTIVE",
        )

    return payload


def audit_credit_report(file_path: str) -> Dict[str, Any]:
    """
    Canonical entry point called by Streamlit (main.py).
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
        # Keep error type only (avoid leaking details on Streamlit)
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
