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
from google.genai import types, errors  # google-genai SDK

# -----------------------------
# CANON (DO NOT DRIFT)
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}

CONFIDENCE_GATE = 0.70

# Repo-local (Streamlit Cloud)
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"  # (tu folder en repo)
MODEL_PREFERRED = [
    # Orden: primero lo ideal, luego fallback seguro
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    "gemini-2.0-pro",
    "gemini-2.0-flash",
    "gemini-1.5-pro",
    "gemini-1.5-flash",
]

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
    Normaliza a NS-DK-1.0 SIEMPRE (aunque Gemini devuelva algo raro).
    """
    p = dict(payload or {})
    p["version"] = KERNEL_VERSION
    p["timestamp"] = p.get("timestamp") or _utc_iso()
    p["notes"] = NOTES_IMMUTABLE

    status = p.get("status")
    risk = p.get("risk_level")

    if status not in ALLOWED_STATUS:
        status = "UNKNOWN"
    if risk not in ALLOWED_RISK:
        risk = "NONE"

    p["status"] = status
    p["risk_level"] = risk

    findings = p.get("findings")
    if not isinstance(findings, list):
        findings = []
    p["findings"] = findings

    conf = p.get("confidence")
    try:
        p["confidence"] = float(conf)
    except Exception:
        p["confidence"] = 0.0

    return p


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filtra findings sin evidencia mínima: document + page + field.
    Si status=RISK_DETECTED pero no hay findings válidos => UNKNOWN (fail-closed).
    """
    p = dict(payload or {})
    findings = p.get("findings", [])
    valid: List[Dict[str, Any]] = []

    if isinstance(findings, list):
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

    p["findings"] = valid

    if p.get("status") == "RISK_DETECTED" and not valid:
        p["status"] = "UNKNOWN"
        p["risk_level"] = "NONE"
        p["confidence"] = min(float(p.get("confidence", 0.0) or 0.0), 0.5)

    return p


SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.

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
- If missing key pages or unreadable => status INCOMPLETE.
- If unsure => status UNKNOWN (fail-closed).
""".strip()


# -----------------------------
# GEMINI CLIENT + MODEL PICKER
# -----------------------------
def _client() -> genai.Client:
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY). Set it in Streamlit Secrets.")
    # Nota: el SDK usa endpoints beta por defecto; esto es OK.
    return genai.Client(api_key=api_key)


def _list_model_names(client: genai.Client) -> List[str]:
    names: List[str] = []
    try:
        for m in client.models.list():
            n = getattr(m, "name", None) or getattr(m, "model", None)
            if isinstance(n, str) and n:
                # En list() puede venir con prefijo "models/..."
                names.append(n.replace("models/", ""))
    except Exception:
        pass
    return sorted(set(names))


def _pick_model(client: genai.Client) -> str:
    available = _list_model_names(client)

    # 1) Preferidos (si existen)
    for m in MODEL_PREFERRED:
        if m in available:
            return m

    # 2) Si list() no devolvió nada, igual intentamos el primero preferido
    if not available:
        return MODEL_PREFERRED[0]

    # 3) Fallback: primer "gemini-" disponible
    for n in available:
        if n.startswith("gemini-"):
            return n

    return available[0]


def _upload_and_wait(client: genai.Client, local_path: str):
    # IMPORTANT: SDK usa file= (no path=). :contentReference[oaicite:2]{index=2}
    f = client.files.upload(file=local_path)
    while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


def _load_soul_pdfs() -> List[str]:
    if not os.path.isdir(SOUL_DIR):
        return []
    pdfs = []
    for name in sorted(os.listdir(SOUL_DIR)):
        if name.lower().endswith(".pdf"):
            pdfs.append(os.path.join(SOUL_DIR, name))
    return pdfs


def _run_gemini_audit(client: genai.Client, model_id: str, report_path: str) -> Dict[str, Any]:
    soul_paths = _load_soul_pdfs()
    if not soul_paths:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # Subimos SOUL + Report
    soul_files = []
    for p in soul_paths:
        soul_files.append(_upload_and_wait(client, p))

    report_file = _upload_and_wait(client, report_path)

    # Parts: SOUL primero, luego Report, luego instrucción
    parts: List[types.Part] = []
    for sf in soul_files:
        parts.append(types.Part.from_uri(file_uri=sf.uri, mime_type="application/pdf"))
    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))
    parts.append(
        types.Part.from_text(
            text=(
                "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
                "Identify technical discrepancies only. Return ONLY NS-DK-1.0 JSON."
            )
        )
    )

    try:
        resp = client.models.generate_content(
            model=model_id,
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )
    except errors.APIError as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{e.code}")
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{type(e).__name__}")

    try:
        raw = json.loads(resp.text)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")

    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)

    # Confidence gate final
    if float(payload.get("confidence", 0.0) or 0.0) < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            confidence=float(payload.get("confidence", 0.0) or 0.0),
            notes_extra="CONFIDENCE_GATE_ACTIVE",
        )

    return payload


# -----------------------------
# PUBLIC API (CALLED BY main.py)
# -----------------------------
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        # Input defense
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")
        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")
        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        client = _client()
        model_id = _pick_model(client)

        return _run_gemini_audit(client, model_id, file_path)

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
