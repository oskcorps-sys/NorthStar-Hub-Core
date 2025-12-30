"""
NorthStar Hub — Kernel Core (NS-DK-1.0) — Kernel V2.2 (Cloud Hardened)
Scope: Technical data consistency check only (fail-closed)
- Evidence-bound JSON output
- Confidence gate
- Bureau Detector + BTM (Bureau Translation Manifest) auto-load
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
import inspect
from typing import Any, Dict, List, Optional

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

# -----------------------------
# GEMINI
# -----------------------------
# Usa un modelo que exista en tu cuenta (ya dijiste 2.5 flash)
MODEL_ID = os.getenv("GEMINI_MODEL_ID", "gemini-2.5-flash")
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"


# -----------------------------
# UTIL
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
    Normaliza output al contrato NS-DK-1.0.
    Fail-closed: si status/risk inválidos => UNKNOWN.
    """
    try:
        p = dict(payload or {})
        p["version"] = KERNEL_VERSION
        p["timestamp"] = p.get("timestamp") or _utc_iso()

        if p.get("status") not in ALLOWED_STATUS:
            return _empty_payload(status="UNKNOWN", notes_extra="BAD_STATUS")

        if p.get("risk_level") not in ALLOWED_RISK:
            return _empty_payload(status="UNKNOWN", notes_extra="BAD_RISK_LEVEL")

        if not isinstance(p.get("findings"), list):
            p["findings"] = []

        conf = p.get("confidence")
        p["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0

        # notes immutable
        p["notes"] = NOTES_IMMUTABLE
        return p
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="NORMALIZE_FAIL")


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Strips findings missing required evidence fields.
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
            if str(page).strip().upper() != "UNKNOWN" and str(field).strip().upper() != "UNKNOWN":
                valid.append(f)

    payload["findings"] = valid

    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

    return payload


def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY env var (Streamlit Secrets).")
    return genai.Client(api_key=api_key)


def upload_any(client: genai.Client, file_path: str):
    """
    Upload robusto (SDK signature drift-proof).
    Maneja: upload(path=...), upload(file=...), upload(positional)
    """
    fn = client.files.upload

    def _wait(f, sleep_s=2):
        while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
            time.sleep(sleep_s)
            f = client.files.get(name=f.name)
        return f

    # introspect signature
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())
    except Exception:
        params = []

    # keyword: path
    if "path" in params:
        f = fn(path=file_path)
        return _wait(f)

    # keyword: file
    if "file" in params:
        try:
            f = fn(file=file_path)
            return _wait(f)
        except TypeError:
            pass
        with open(file_path, "rb") as fh:
            f = fn(file=fh)
            return _wait(f)

    # fallback positional
    try:
        f = fn(file_path)
        return _wait(f)
    except TypeError:
        with open(file_path, "rb") as fh:
            f = fn(fh)
            return _wait(f)


# -----------------------------
# BUREAU DETECTOR + BTM
# -----------------------------
BUREAU_KEYWORDS = {
    "EXPERIAN": ["EXPERIAN"],
    "EQUIFAX": ["EQUIFAX"],
    "TRANSUNION": ["TRANSUNION", "TRANS UNION"],
}

BTM_FILE_BY_BUREAU = {
    "EXPERIAN": "BTM_EXPERIAN.json",
    "EQUIFAX": "BTM_EQUIFAX.json",
    "TRANSUNION": "BTM_TRANSUNION.json",
}


def _detect_bureau_from_pdf_text(file_path: str) -> Optional[str]:
    """
    Ligero: intenta leer texto con PyPDF2 (si está disponible).
    Si no hay texto (scan), devuelve None y el kernel sigue sin BTM.
    """
    try:
        from PyPDF2 import PdfReader  # optional

        reader = PdfReader(file_path)
        # mira primeras 2 páginas
        max_pages = min(2, len(reader.pages))
        text = ""
        for i in range(max_pages):
            t = reader.pages[i].extract_text() or ""
            text += " " + t

        up = text.upper()
        for bureau, keys in BUREAU_KEYWORDS.items():
            for k in keys:
                if k in up:
                    return bureau
        return None
    except Exception:
        return None


def _load_btm_text(bureau: Optional[str]) -> Optional[str]:
    if not bureau:
        return None
    fname = BTM_FILE_BY_BUREAU.get(bureau)
    if not fname:
        return None
    path = os.path.join(SOUL_DIR, fname)
    if not os.path.exists(path):
        return None
    try:
        raw = json.loads(open(path, "r", encoding="utf-8").read())
        # Lo metemos como texto estructurado para que Gemini lo use como diccionario
        return json.dumps(
            {"bureau_id": bureau, "btm": raw},
            ensure_ascii=False,
            indent=2,
        )
    except Exception:
        return None


# -----------------------------
# SYSTEM INSTRUCTION (STRICT)
# -----------------------------
SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies between a credit report and SOUL standards (Metro 2 + applicable references).
BTM: If a Bureau Translation Manifest is provided, treat it as the canonical code-mapping layer for that bureau before comparing to Metro 2.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations, NO action steps, NO dispute letters, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous => DO NOT output the finding.
5) If PDF is unreadable/scan/OCR weak => status INCOMPLETE.
6) If unsure => status UNKNOWN (fail-closed).

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
""".strip()


# -----------------------------
# CORE
# -----------------------------
def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # 1) Ensure SOUL PDFs active (only PDFs go to Files API)
    mm = ManifestManager(MANIFEST_PATH, client)
    try:
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    except Exception as e:
        return _empty_payload(status="INCOMPLETE", notes_extra=f"SOUL_MANIFEST_FAIL:{type(e).__name__}:{str(e)[:120]}")

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # 2) Upload report
    report_file = upload_any(client, report_path)

    # 3) Bureau + BTM (text)
    bureau = _detect_bureau_from_pdf_text(report_path)
    btm_text = _load_btm_text(bureau)

    # 4) Build “evidence briefcase” parts
    parts: List[types.Part] = []

    # SOUL PDFs first (context)
    for ref in soul_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    # Report PDF (target)
    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

    # BTM as text (NOT upload)
    if btm_text:
        parts.append(types.Part.from_text(text=f"BTM (Bureau Translation Manifest):\n{btm_text}"))

    # Task instruction (keyword arg to avoid Part.from_text TypeError)
    parts.append(
        types.Part.from_text(
            text=(
                "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
                "If BTM is provided, use it to translate bureau-specific codes before judging inconsistencies. "
                "Identify technical discrepancies only. Return ONLY NS-DK-1.0 JSON."
            )
        )
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
        return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{type(e).__name__}:{str(e)[:140]}")

    # 6) Parse + gates
    try:
        raw = json.loads(resp.text)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")

    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)

    # Confidence hard gate
    if float(payload.get("confidence", 0.0) or 0.0) < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            confidence=float(payload.get("confidence", 0.0) or 0.0),
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

        return _run_gemini_audit(file_path)

    except TypeError as e:
        # IMPORTANT: include the message so you stop seeing “TypeError” only.
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:TypeError:{str(e)[:160]}")
    except ValueError as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:ValueError:{str(e)[:160]}")
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}:{str(e)[:160]}")
