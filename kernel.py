"""
NorthStar Hub — Kernel Core (NS-DK-2.1)
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
- Bureau Detector + BTM dynamic loading
"""

from __future__ import annotations

import os
import json
import time
import hashlib
import datetime as dt
import inspect
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from google import genai
from google.genai import types

# -----------------------------
# CANON (DO NOT DRIFT)
# -----------------------------
KERNEL_VERSION = "NS-DK-2.1"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}

CONFIDENCE_GATE = 0.70

# -----------------------------
# PATHS (Repo-local for Alpha)
# -----------------------------
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"
TMP_DIR = "tmp"

# -----------------------------
# GEMINI
# -----------------------------
# Default: use a model that won't 404 on you in v1beta scenarios
MODEL_ID = os.getenv("GEMINI_MODEL_ID", "gemini-2.5-flash")
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"

# -----------------------------
# Bureau detection heuristics
# -----------------------------
BUREAUS = {
    "TRANSUNION": ["transunion", "trans union", "tu report", "how to read transunion", "transunion credit report"],
    "EXPERIAN": ["experian", "experian credit report"],
    "EQUIFAX": ["equifax", "equifax credit report"],
}

# BTM naming heuristics (we will match by keyword)
BTM_HINTS = {
    "TRANSUNION": ["btm", "transunion", "tu"],
    "EXPERIAN": ["btm", "experian", "ex"],
    "EQUIFAX": ["btm", "equifax", "eq"],
}

# -----------------------------
# Utilities
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


def _validate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enforces NS-DK contract and fail-closed behavior.
    Hard gate: confidence < CONFIDENCE_GATE => UNKNOWN (no conclusions).
    """
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

        # Integrity gate (Alpha)
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


# -----------------------------
# SDK-safe upload (fixes `path=` TypeError)
# -----------------------------
def upload_any(client, file_path: str):
    """
    Upload robust (Streamlit Cloud-safe).
    Adapts to the installed SDK signature.
    """
    fn = client.files.upload

    def _wait(f, sleep_s=2):
        while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
            time.sleep(sleep_s)
            f = client.files.get(name=f.name)
        return f

    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())
    except Exception:
        params = []

    # Prefer keyword variants if present
    if "path" in params:
        f = fn(path=file_path)
        return _wait(f)

    if "file" in params:
        try:
            f = fn(file=file_path)
            return _wait(f)
        except TypeError:
            pass
        with open(file_path, "rb") as fh:
            f = fn(file=fh)
            return _wait(f)

    # Positional fallback
    try:
        f = fn(file_path)
        return _wait(f)
    except TypeError:
        with open(file_path, "rb") as fh:
            f = fn(fh)
            return _wait(f)


# -----------------------------
# Manifest Manager (re-upload if remote missing)
# -----------------------------
class ManifestManager:
    """
    Local manifest:
      fingerprint -> {name, uri, uploaded_at, local}

    Re-upload when:
      - local file changed
      - remote missing/not ACTIVE
    """

    def __init__(self, manifest_path: str, client):
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

    def _fingerprint(self, p: Path) -> str:
        st = p.stat()
        base = f"{p.name}__{st.st_size}__{int(st.st_mtime)}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    def _remote_active(self, remote_name: str) -> bool:
        try:
            f = self.client.files.get(name=remote_name)
            return getattr(getattr(f, "state", None), "name", "") == "ACTIVE"
        except Exception:
            return False

    def ensure_active_pdf_files(self, folder_path: str) -> List[Dict[str, str]]:
        folder = Path(folder_path)
        if not folder.exists():
            raise FileNotFoundError(f"SOUL folder missing: {folder_path}")

        refs: List[Dict[str, str]] = []
        for p in sorted(folder.glob("*.pdf")):
            if not p.is_file():
                continue

            key = self._fingerprint(p)
            entry = self.data.get(key)

            # reuse if remote is ACTIVE
            if entry and entry.get("name") and self._remote_active(entry["name"]):
                refs.append({"name": entry["name"], "uri": entry["uri"], "local": p.name})
                continue

            uploaded = upload_any(self.client, str(p))
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
# Bureau Detector (light pre-read)
# -----------------------------
def _detect_bureau_from_bytes(pdf_bytes: bytes) -> str:
    """
    Cheap detector: looks for bureau keywords in first N bytes.
    Works even when text extraction is ugly.
    """
    hay = pdf_bytes[:200_000].lower()
    for bureau, keys in BUREAUS.items():
        for k in keys:
            if k.encode("utf-8") in hay:
                return bureau
    return "UNKNOWN"


def _select_soul_and_btm(soul_files: List[Dict[str, str]], bureau_id: str) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    From all SOUL refs, pick:
      - core standards (always) => anything NOT obviously BTM
      - bureau-specific BTM => best match for bureau_id
    """
    btms: List[Dict[str, str]] = []
    core: List[Dict[str, str]] = []

    for f in soul_files:
        name = (f.get("local") or f.get("name") or "").lower()
        # classify BTM by filename keyword
        if "btm" in name or "translation" in name or "mapping" in name:
            btms.append(f)
        else:
            core.append(f)

    if bureau_id not in BTM_HINTS:
        return core, []  # no BTM

    hints = BTM_HINTS[bureau_id]
    matched: List[Dict[str, str]] = []
    for f in btms:
        nm = (f.get("local") or f.get("name") or "").lower()
        score = sum(1 for h in hints if h in nm)
        if score >= 2:  # strong match
            matched.append(f)

    # fallback: if none scored high, try softer match (any hint)
    if not matched:
        for f in btms:
            nm = (f.get("local") or f.get("name") or "").lower()
            if any(h in nm for h in hints):
                matched.append(f)

    return core, matched


# -----------------------------
# System Instruction (BTM-aware)
# -----------------------------
def _build_system_instruction(bureau_id: str) -> str:
    return f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies and mismatches between:
  (A) the credit report PDF (bureau format may vary)
  (B) SOUL standards PDFs (Metro 2 + any provided bureau translation/mapping docs)

BUREAU CONTEXT:
- Detected Bureau: {bureau_id}
- If a bureau-specific translation/mapping document (BTM) is provided, you MUST use it to interpret bureau-native codes.
- You MUST NOT flag a discrepancy solely because a bureau uses proprietary formatting IF the BTM maps it as valid.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK contract. No extra text.
2) NO recommendations, NO action steps, NO dispute-letter writing, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report text is unreadable/scan/OCR weak => status INCOMPLETE.
6) User narrative is NOT evidence. Only the PDFs.
7) If the report uses bureau-native code/format and BTM states it's valid => DO NOT flag it.
8) If BTM is missing for this bureau => be conservative; prefer UNKNOWN over speculative findings.

NS-DK JSON CONTRACT:
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


def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY environment variable.")
    return genai.Client(api_key=api_key)


def _run_gemini_audit(report_path: str, bureau_id: str) -> Dict[str, Any]:
    client = _client()

    # 1) Ensure SOUL files are active
    mm = ManifestManager(MANIFEST_PATH, client)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # 2) Upload report (STREET)
    report_file = upload_any(client, report_path)

    # 3) Choose core standards + BTM (if available)
    core_refs, btm_refs = _select_soul_and_btm(soul_refs, bureau_id)

    # 4) Build parts: core SOUL + BTM + report + task
    parts: List[Any] = []

    # Core first (Metro2 + standards)
    for ref in core_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    # Then BTM (if any)
    for ref in btm_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    # Then report
    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

    # Task instruction
    parts.append(types.Part.from_text(
        "Perform a technical consistency audit. "
        "Only output evidence-bound findings. "
        "Return ONLY NS-DK JSON."
    ))

    sys_inst = _build_system_instruction(bureau_id)

    try:
        resp = client.models.generate_content(
            model=MODEL_ID,
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                system_instruction=sys_inst,
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{type(e).__name__}")

    # Parse + gates
    try:
        raw = json.loads(resp.text)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")

    raw = _evidence_gate(raw)
    raw = _validate_payload(raw)

    return raw


# -----------------------------
# PUBLIC API (CALLED BY UI)
# -----------------------------
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    """
    Canonical entry point called by Streamlit (main.py)
    """
    try:
        # Input checks
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")

        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        # Ensure folders
        os.makedirs(TMP_DIR, exist_ok=True)
        Path(MANIFEST_PATH).parent.mkdir(parents=True, exist_ok=True)

        # Bureau detection (read bytes safely)
        try:
            with open(file_path, "rb") as f:
                pdf_bytes = f.read()
            bureau_id = _detect_bureau_from_bytes(pdf_bytes)
        except Exception:
            bureau_id = "UNKNOWN"

        # Must have local SOUL dir
        if not os.path.isdir(SOUL_DIR):
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

        return _run_gemini_audit(file_path, bureau_id)

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
