"""
NorthStar Hub — Kernel Core (NS-DK-1.0) — V2.1
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
- Bureau Detector + BTM (Bureau Translation Manifest) normalization bridge
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict, Optional, Tuple

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
BTM_DIR = os.path.join(SOUL_DIR, "btm")
MANIFEST_PATH = "manifests/soul_manifest.json"
TMP_DIR = "tmp"

# -----------------------------
# GEMINI
# -----------------------------
# You said: gemini-2.5-flash
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


def _validate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Enforces NS-DK-1.0 contract + fail-closed."""
    try:
        payload = dict(payload or {})
        payload["version"] = KERNEL_VERSION
        payload["timestamp"] = payload.get("timestamp") or _utc_iso()

        if payload.get("status") not in ALLOWED_STATUS:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="BAD_STATUS")

        if payload.get("risk_level") not in ALLOWED_RISK:
            return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="BAD_RISK_LEVEL")

        if not isinstance(payload.get("findings"), list):
            payload["findings"] = []

        conf = payload.get("confidence")
        payload["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0

        # Integrity gate
        if payload["confidence"] < CONFIDENCE_GATE:
            return _empty_payload(
                status="UNKNOWN",
                risk_level="NONE",
                confidence=payload["confidence"],
                notes_extra="CONFIDENCE_GATE_ACTIVE",
            )

        payload["notes"] = NOTES_IMMUTABLE
        return payload

    except Exception:
        return _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="VALIDATION_EXCEPTION")


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only findings that have evidence: document + page + field.
    If status=RISK_DETECTED but no valid findings => UNKNOWN.
    """
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


# -----------------------------
# API KEY (Streamlit Cloud safe)
# -----------------------------
def _get_api_key() -> Optional[str]:
    # 1) env
    k = os.getenv(GEMINI_API_KEY_ENV)
    if k:
        return k

    # 2) streamlit secrets if available
    try:
        import streamlit as st  # noqa
        if "GEMINI_API_KEY" in st.secrets:
            return st.secrets["GEMINI_API_KEY"]
    except Exception:
        pass

    return None


def _client() -> genai.Client:
    api_key = _get_api_key()
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (env or Streamlit secrets).")
    return genai.Client(api_key=api_key)


# -----------------------------
# BUREAU DETECTOR (simple + reliable)
# -----------------------------
def _detect_bureau_from_filename(file_path: str) -> str:
    """Fallback detector if PDF text extraction is not available."""
    name = (os.path.basename(file_path) or "").lower()
    if "experian" in name:
        return "EXPERIAN"
    if "equifax" in name:
        return "EQUIFAX"
    if "transunion" in name or "tu" in name:
        return "TRANSUNION"
    return "UNKNOWN"


def _load_btm(bureau: str) -> Optional[Dict[str, Any]]:
    """
    Loads BTM JSON from SOUL/btm.
    Expected file names:
      - BTM_TRANSUNION.json
      - BTM_EXPERIAN.json
      - BTM_EQUIFAX.json
    """
    fname = f"BTM_{bureau.upper()}.json"
    path = os.path.join(BTM_DIR, fname)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _btm_summary_for_prompt(btm: Dict[str, Any]) -> str:
    """
    Convert BTM to compact prompt text so Gemini uses it as a translation dictionary,
    not as an inconsistency trigger.
    """
    bureau = btm.get("bureau", "UNKNOWN")
    version = btm.get("version", "NA")

    # Guards
    guards = btm.get("guards", {})
    do_not_flag = guards.get("do_not_flag_as_inconsistency", [])
    flag_only_if = guards.get("flag_as_inconsistency_only_if", [])

    # Key mappings (keep short; Gemini has the full JSON in context)
    mappings = btm.get("mappings", {})

    return (
        f"BTM_LOADED: {bureau} v{version}\n"
        f"BTM_GUARDS_DO_NOT_FLAG: {do_not_flag}\n"
        f"BTM_FLAG_ONLY_IF: {flag_only_if}\n"
        f"BTM_MAPPINGS_KEYS: {list(mappings.keys())}\n"
        f"BTM_NOTE: The BTM is a translation dictionary. Bureau-native codes/formatting that are mapped/guarded MUST NOT be treated as inconsistencies.\n"
    )


# -----------------------------
# SYSTEM INSTRUCTION (V2.1)
# -----------------------------
SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies and mismatches between a credit report and SOUL standards.

BTM (Bureau Translation Manifest) RULE:
- If a BTM is provided for the bureau, you MUST use it as a translation dictionary BEFORE judging.
- Bureau-native conventions covered by the BTM or explicitly guarded must NOT be flagged as inconsistencies.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations, NO action steps, NO dispute letters, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report is unreadable/scan/OCR weak => status INCOMPLETE.
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
- If missing key sections / unreadable => status INCOMPLETE.
- If unsure => status UNKNOWN (fail-closed).
""".strip()


# -----------------------------
# UPLOAD HELPERS (uses ManifestManager for SOUL)
# -----------------------------
def _upload_and_wait(client: genai.Client, file_path: str):
    # IMPORTANT: ManifestManager already uses upload_any for SOUL.
    # For the report, we do a simple upload but must be SDK-safe.
    # We'll reuse ManifestManager.upload_any logic by importing it indirectly is messy.
    # So we do a minimal compatible approach:
    from manifest_manager import upload_any  # local import to avoid cycles
    return upload_any(client, file_path)


def _build_parts_with_soul_and_btm(
    soul_refs,
    report_uri: str,
    bureau: str,
    btm_json: Optional[Dict[str, Any]],
) -> list:
    parts = []

    # 1) SOUL PDFs first
    for ref in soul_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    # 2) BTM JSON as text (compact + full JSON)
    if btm_json:
        parts.append(types.Part.from_text(text=_btm_summary_for_prompt(btm_json)))
        parts.append(types.Part.from_text(text="BTM_JSON_BEGIN"))
        parts.append(types.Part.from_text(text=json.dumps(btm_json, ensure_ascii=False)))
        parts.append(types.Part.from_text(text="BTM_JSON_END"))
    else:
        parts.append(types.Part.from_text(text=f"BTM_NOT_FOUND for bureau={bureau}. Proceed fail-closed."))

    # 3) Report PDF last
    parts.append(types.Part.from_uri(file_uri=report_uri, mime_type="application/pdf"))

    # 4) Task
    parts.append(
        types.Part.from_text(
            text=(
                "Perform a technical consistency audit of the attached credit report against SOUL standards. "
                "Use BTM as a translation dictionary first if provided. "
                "Return ONLY NS-DK-1.0 JSON."
            )
        )
    )

    return parts


def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # 1) SOUL PDFs (excluding btm folder; only PDFs at SOUL root)
    if not os.path.isdir(SOUL_DIR):
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

    mm = ManifestManager(MANIFEST_PATH, client)
    try:
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    except Exception:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_MANIFEST_FAIL")

    # Safety: if SOUL has no PDFs at root, fail-closed
    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # 2) Detect bureau + load BTM
    bureau = _detect_bureau_from_filename(report_path)
    btm = _load_btm(bureau) if bureau != "UNKNOWN" else None

    # If bureau unknown, still proceed, but with stricter fail-closed
    if bureau == "UNKNOWN":
        btm = None

    # 3) Upload report
    report_file = _upload_and_wait(client, report_path)

    # 4) Build content parts (SOUL + BTM + report + instruction)
    parts = _build_parts_with_soul_and_btm(
        soul_refs=soul_refs,
        report_uri=report_file.uri,
        bureau=bureau,
        btm_json=btm,
    )

    # 5) Model call
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

    # 6) Parse + gates
    try:
        raw = json.loads(resp.text)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")

    raw = _evidence_gate(raw)
    raw = _validate_payload(raw)

    # Add bureau context into notes (non-drifting, still technical)
    if raw.get("status") != "INCOMPLETE":
        raw["notes"] = f"{NOTES_IMMUTABLE} | BUREAU={bureau}"

    return raw


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

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
