"""
NorthStar Hub — Kernel Core (NS-DK-1.0)
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
- SOUL manifest caching (avoid re-upload storm)
- 429 backoff (rate-limit resilience)
"""

from __future__ import annotations

import os
import json
import time
import hashlib
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types, errors


# -----------------------------
# CANON (DO NOT DRIFT)
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}

CONFIDENCE_GATE = 0.70

# -----------------------------
# PATHS
# -----------------------------
SOUL_DIR = Path("00_NORTHSTAR_SOUL_IMPUT")
MANIFEST_PATH = Path("manifests/soul_manifest.json")

# -----------------------------
# MODELS (auto-pick)
# -----------------------------
MODEL_PREFERRED = [
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    "gemini-2.0-pro",
    "gemini-2.0-flash",
    "gemini-1.5-pro",
    "gemini-1.5-flash",
]

# Rate-limit handling
MAX_RETRIES = 6
BASE_SLEEP_S = 2  # exponential backoff base


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
    p = dict(payload or {})
    p["version"] = KERNEL_VERSION
    p["timestamp"] = p.get("timestamp") or _utc_iso()
    p["notes"] = NOTES_IMMUTABLE

    if p.get("status") not in ALLOWED_STATUS:
        p["status"] = "UNKNOWN"
    if p.get("risk_level") not in ALLOWED_RISK:
        p["risk_level"] = "NONE"

    if not isinstance(p.get("findings"), list):
        p["findings"] = []

    try:
        p["confidence"] = float(p.get("confidence", 0.0))
    except Exception:
        p["confidence"] = 0.0

    return p


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
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
    return genai.Client(api_key=api_key)


def _list_model_names(client: genai.Client) -> List[str]:
    names: List[str] = []
    try:
        for m in client.models.list():
            n = getattr(m, "name", None) or getattr(m, "model", None)
            if isinstance(n, str) and n:
                names.append(n.replace("models/", ""))
    except Exception:
        pass
    return sorted(set(names))


def _pick_model(client: genai.Client) -> str:
    available = _list_model_names(client)

    for m in MODEL_PREFERRED:
        if m in available:
            return m

    if not available:
        return MODEL_PREFERRED[-1]

    for n in available:
        if n.startswith("gemini-"):
            return n
    return available[0]


# -----------------------------
# MANIFEST (SOUL CACHE)
# -----------------------------
def _fingerprint(path: Path) -> str:
    st = path.stat()
    # includes size + mtime; stable enough
    raw = f"{path.name}::{st.st_size}::{int(st.st_mtime)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _load_manifest() -> Dict[str, Dict[str, str]]:
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_manifest(data: Dict[str, Dict[str, str]]) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _remote_active(client: genai.Client, remote_name: str) -> bool:
    try:
        f = client.files.get(name=remote_name)
        return getattr(getattr(f, "state", None), "name", "") == "ACTIVE"
    except Exception:
        return False


def _upload_and_wait(client: genai.Client, local_path: str):
    # IMPORTANT: use file=, not path=
    f = client.files.upload(file=local_path)
    while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
        time.sleep(2)
        f = client.files.get(name=f.name)
    return f


def _ensure_soul_active(client: genai.Client) -> List[Dict[str, str]]:
    if not SOUL_DIR.is_dir():
        return []

    manifest = _load_manifest()
    refs: List[Dict[str, str]] = []

    pdfs = sorted([p for p in SOUL_DIR.iterdir() if p.is_file() and p.name.lower().endswith(".pdf")])

    for p in pdfs:
        fp = _fingerprint(p)
        entry = manifest.get(fp)

        if entry and entry.get("name") and entry.get("uri"):
            if _remote_active(client, entry["name"]):
                refs.append({"name": entry["name"], "uri": entry["uri"], "local": p.name})
                continue

        # Upload (or re-upload)
        uploaded = _upload_and_wait(client, str(p))
        manifest[fp] = {"name": uploaded.name, "uri": uploaded.uri, "local": p.name}
        refs.append({"name": uploaded.name, "uri": uploaded.uri, "local": p.name})

    _save_manifest(manifest)
    return refs


# -----------------------------
# BACKOFF HELPERS (429)
# -----------------------------
def _sleep_backoff(attempt: int) -> None:
    # exponential: 2,4,8,16,... with cap
    s = min(BASE_SLEEP_S * (2 ** attempt), 60)
    time.sleep(s)


def _generate_with_retries(client: genai.Client, model_id: str, content: types.Content) -> str:
    last_err: Optional[Exception] = None

    for attempt in range(MAX_RETRIES):
        try:
            resp = client.models.generate_content(
                model=model_id,
                contents=[content],
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
            return resp.text

        except errors.APIError as e:
            # 429 quota/rate limit
            if getattr(e, "code", None) == 429:
                last_err = e
                _sleep_backoff(attempt)
                continue
            last_err = e
            break

        except Exception as e:
            last_err = e
            break

    raise last_err or RuntimeError("MODEL_CALL_FAIL")


# -----------------------------
# CORE AUDIT
# -----------------------------
def _run_gemini_audit(client: genai.Client, report_path: str) -> Dict[str, Any]:
    soul_refs = _ensure_soul_active(client)
    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    report_file = _upload_and_wait(client, report_path)

    parts: List[types.Part] = []
    for ref in soul_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))
    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))
    parts.append(
        types.Part.from_text(
            text=(
                "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
                "Identify technical discrepancies only. Return ONLY NS-DK-1.0 JSON."
            )
        )
    )

    content = types.Content(role="user", parts=parts)

    # Model pick + fallback if model missing / throttled
    model_id = _pick_model(client)
    try:
        raw_text = _generate_with_retries(client, model_id, content)
    except Exception:
        # fallback: try a flash model if available
        try:
            raw_text = _generate_with_retries(client, "gemini-2.0-flash", content)
        except Exception as e:
            # preserve the real cause: 429, 404, etc.
            name = type(e).__name__
            msg = str(e)
            if "429" in msg:
                return _empty_payload(status="UNKNOWN", notes_extra="MODEL_CALL_FAIL:429")
            return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{name}")

    try:
        raw = json.loads(raw_text)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")

    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)

    if float(payload.get("confidence", 0.0) or 0.0) < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            confidence=float(payload.get("confidence", 0.0) or 0.0),
            notes_extra="CONFIDENCE_GATE_ACTIVE",
        )

    return payload


# -----------------------------
# PUBLIC API
# -----------------------------
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")
        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")
        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        client = _client()
        return _run_gemini_audit(client, file_path)

    except Exception as e:
        # last-resort fail-closed
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
