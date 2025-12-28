"""
NorthStar Hub — Kernel Core (NS-DK-1.0)
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
- Google Native Gemini (google-genai SDK) + Files API
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types


# -----------------------------
# CANON (DO NOT DRIFT)
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}
CONFIDENCE_GATE = 0.70

# Repo paths
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"          # lives inside repo
TMP_DIR = "tmp"
MANIFEST_PATH = "manifests/soul_manifest.json"

# IMPORTANT: Gemini 1.5 models are shut down -> use 2.5 series
PRIMARY_MODEL_ID = "gemini-2.5-pro"
FALLBACK_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.0-flash-001",
]

# Env/Secrets
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"


# -----------------------------
# TIME / PAYLOAD HELPERS
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
    Normalize to NS-DK-1.0 keys and safe defaults (fail-closed).
    """
    p = dict(payload or {})
    p["version"] = KERNEL_VERSION
    p["timestamp"] = p.get("timestamp") or _utc_iso()

    if p.get("status") not in ALLOWED_STATUS:
        p["status"] = "UNKNOWN"

    if p.get("risk_level") not in ALLOWED_RISK:
        p["risk_level"] = "NONE"

    if not isinstance(p.get("findings"), list):
        p["findings"] = []

    conf = p.get("confidence")
    try:
        p["confidence"] = float(conf)
    except Exception:
        p["confidence"] = 0.0

    p["notes"] = NOTES_IMMUTABLE
    return p


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Strips findings that lack evidence (document/page/field).
    If status=RISK_DETECTED but no valid findings => UNKNOWN.
    """
    p = dict(payload or {})
    findings = p.get("findings", [])
    if not isinstance(findings, list):
        p["findings"] = []
        return p

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

        if not doc or page is None or not field:
            continue

        # Reject placeholder "UNKNOWN"
        if str(page).strip().upper() == "UNKNOWN":
            continue
        if str(field).strip().upper() == "UNKNOWN":
            continue

        valid.append(f)

    p["findings"] = valid

    if p.get("status") == "RISK_DETECTED" and not valid:
        p["status"] = "UNKNOWN"
        p["risk_level"] = "NONE"
        p["confidence"] = min(float(p.get("confidence", 0.0) or 0.0), 0.5)

    return p


# -----------------------------
# STREAMLIT-CLOUD FRIENDLY API KEY LOADING
# -----------------------------
def _get_api_key() -> Optional[str]:
    """
    Priority:
    1) Environment variable GEMINI_API_KEY
    2) Streamlit secrets if running inside Streamlit Cloud (optional)
    """
    key = os.getenv(GEMINI_API_KEY_ENV)
    if key:
        return key

    # Optional: allow kernel to work even if user didn't export env var,
    # but stored key in Streamlit secrets.
    try:
        import streamlit as st  # type: ignore
        if "GEMINI_API_KEY" in st.secrets:
            return str(st.secrets["GEMINI_API_KEY"])
    except Exception:
        pass

    return None


def _client() -> genai.Client:
    api_key = _get_api_key()
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (env var or Streamlit secrets).")
    # Stable API channel:
    # (google-genai supports v1 and v1beta; v1 is safer)
    return genai.Client(api_key=api_key, http_options={"api_version": "v1"})


# -----------------------------
# MANIFEST MANAGER (INLINE, SO NO MISMATCH)
# -----------------------------
class ManifestManager:
    """
    Maps local PDF fingerprints -> Gemini uploaded file refs (name, uri).
    Re-uploads when local file changes or remote isn't ACTIVE.
    """

    def __init__(self, manifest_path: str, client: genai.Client):
        self.path = Path(manifest_path)
        self.client = client
        self.data: Dict[str, Dict[str, Any]] = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2), encoding="utf-8")

    def _fingerprint(self, file_path: Path) -> str:
        st = file_path.stat()
        return f"{file_path.name}__{st.st_size}__{int(st.st_mtime)}"

    def _remote_active(self, remote_name: str) -> bool:
        try:
            f = self.client.files.get(name=remote_name)
            return getattr(f.state, "name", "") == "ACTIVE"
        except Exception:
            return False

    def _upload_and_wait(self, file_path: Path, sleep_s: int = 2) -> types.File:
        # IMPORTANT: google-genai uses file=, not path=
        uploaded = self.client.files.upload(file=str(file_path))
        while getattr(uploaded.state, "name", "") == "PROCESSING":
            time.sleep(sleep_s)
            uploaded = self.client.files.get(name=uploaded.name)
        return uploaded

    def ensure_active_pdf_files(self, folder_path: str) -> List[Dict[str, str]]:
        folder = Path(folder_path)
        if not folder.exists() or not folder.is_dir():
            raise FileNotFoundError(f"SOUL_DIR missing: {folder_path}")

        refs: List[Dict[str, str]] = []
        pdfs = sorted([p for p in folder.glob("*.pdf") if p.is_file()])

        for p in pdfs:
            key = self._fingerprint(p)

            # Find any existing entry by exact fingerprint
            entry = self.data.get(key)
            if entry and entry.get("name") and self._remote_active(entry["name"]):
                refs.append({"name": entry["name"], "uri": entry["uri"], "local": p.name})
                continue

            # Upload/re-upload
            uploaded = self._upload_and_wait(p)
            self.data[key] = {
                "name": uploaded.name,
                "uri": uploaded.uri,
                "uploaded_at": int(time.time()),
                "local": p.name,
            }
            refs.append({"name": uploaded.name, "uri": uploaded.uri, "local": p.name})

        self.save()
        return refs


# -----------------------------
# SYSTEM INSTRUCTION (HARD-LOCK)
# -----------------------------
SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies and mismatches between the credit report and SOUL standards.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations, NO action steps, NO dispute-letter writing, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report is unreadable/scan/OCR weak OR missing key pages => status INCOMPLETE.
6) User narrative is NOT evidence. Only PDFs.

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
- If unsure => status UNKNOWN (fail-closed).
""".strip()


# -----------------------------
# GEMINI EXECUTION
# -----------------------------
def _upload_report_and_wait(client: genai.Client, report_path: str) -> types.File:
    # IMPORTANT: google-genai uses file=, not path=
    uploaded = client.files.upload(file=report_path)
    while getattr(uploaded.state, "name", "") == "PROCESSING":
        time.sleep(2)
        uploaded = client.files.get(name=uploaded.name)
    return uploaded


def _call_model(client: genai.Client, model_id: str, parts: List[types.Part]) -> Dict[str, Any]:
    resp = client.models.generate_content(
        model=model_id,
        contents=[types.Content(role="user", parts=parts)],
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )
    # resp.text is the safest cross-version field
    return json.loads(resp.text)


def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # SOUL
    mm = ManifestManager(MANIFEST_PATH, client)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # REPORT
    report_file = _upload_report_and_wait(client, report_path)

    # Build parts: SOUL first, then report, then task text
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

    # Model selection with fallback
    model_candidates = [PRIMARY_MODEL_ID] + FALLBACK_MODELS
    last_err = None

    for mid in model_candidates:
        try:
            raw = _call_model(client, mid, parts)
            raw = _normalize_contract(raw)
            raw = _evidence_gate(raw)

            # Confidence hard gate
            if float(raw.get("confidence", 0.0) or 0.0) < CONFIDENCE_GATE:
                return _empty_payload(
                    status="UNKNOWN",
                    confidence=float(raw.get("confidence", 0.0) or 0.0),
                    notes_extra="CONFIDENCE_GATE_ACTIVE",
                )

            return raw
        except json.JSONDecodeError:
            return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")
        except Exception as e:
            last_err = e
            continue

    # If all models failed
    err_name = type(last_err).__name__ if last_err else "UNKNOWN"
    return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{err_name}")


# -----------------------------
# PUBLIC API (CALLED BY Streamlit main.py)
# -----------------------------
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        # Input guard
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")
        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")
        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        os.makedirs(TMP_DIR, exist_ok=True)
        Path("manifests").mkdir(parents=True, exist_ok=True)

        return _run_gemini_audit(file_path)

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
