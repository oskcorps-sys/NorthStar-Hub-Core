# kernel.py
"""
NorthStar Hub — Kernel Core (NS-DK-1.0) — V2.1
Scope: TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY
- Evidence-bound JSON output
- Fail-closed behavior
- Confidence Gate
- Optional BTM (Bureau Translation Map) support
"""

from __future__ import annotations

import os
import json
import time
import inspect
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

# -----------------------------
# PATHS (Repo-local Alpha)
# -----------------------------
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"          # (tu nombre actual)
BTM_DIR = os.path.join(SOUL_DIR, "BTM")       # recomendado: 00_NORTHSTAR_SOUL_IMPUT/BTM/
TMP_DIR = "tmp"

MANIFEST_PATH = "manifests/soul_manifest.json"

# -----------------------------
# GEMINI
# -----------------------------
# Si ya estás usando 2.5 flash, déjalo así. Si no, cámbialo desde env MODEL_ID.
MODEL_ID = os.getenv("MODEL_ID", "gemini-2.5-flash")
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"


# -----------------------------
# TIME
# -----------------------------
def _utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# -----------------------------
# PAYLOAD HELPERS
# -----------------------------
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
    Ensures NS-DK-1.0 keys exist and are well-typed.
    IMPORTANT: timestamp/version/notes get hard-overridden later.
    """
    try:
        payload = dict(raw or {})

        # status / risk
        if payload.get("status") not in ALLOWED_STATUS:
            payload["status"] = "UNKNOWN"
        if payload.get("risk_level") not in ALLOWED_RISK:
            payload["risk_level"] = "NONE"

        # findings
        if not isinstance(payload.get("findings"), list):
            payload["findings"] = []

        # confidence
        conf = payload.get("confidence", 0.0)
        payload["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0

        return payload
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="NORMALIZE_EXCEPTION")


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
        ev = f.get("evidence")
        if not isinstance(ev, dict):
            continue

        doc = ev.get("document")
        page = ev.get("page")
        field = ev.get("field")

        # Require all three, not placeholders
        if doc and page and field:
            if str(page).strip().upper() != "UNKNOWN" and str(field).strip().upper() != "UNKNOWN":
                valid.append(f)

    payload["findings"] = valid

    if payload.get("status") == "RISK_DETECTED" and not valid:
        payload["status"] = "UNKNOWN"
        payload["risk_level"] = "NONE"
        payload["confidence"] = min(float(payload.get("confidence", 0.0) or 0.0), 0.5)

    return payload


def _finalize(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    HARD OVERRIDES:
    - version must be canonical
    - timestamp must be runtime UTC
    - notes must remain immutable
    """
    payload = dict(payload or {})
    payload["version"] = KERNEL_VERSION
    payload["timestamp"] = _utc_iso()
    payload["notes"] = NOTES_IMMUTABLE
    return payload


# -----------------------------
# CLIENT
# -----------------------------
def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY environment variable.")
    return genai.Client(api_key=api_key)


# -----------------------------
# ROBUST UPLOAD (SDK-SIGNATURE SAFE)
# -----------------------------
def _wait_active(client: genai.Client, f: Any, sleep_s: int = 2) -> Any:
    while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
        time.sleep(sleep_s)
        f = client.files.get(name=f.name)
    return f


def upload_any(client: genai.Client, file_path: str) -> Any:
    """
    Upload robusto: se adapta a la firma real del SDK instalado.
    Evita: TypeError: unexpected keyword argument 'path' / 'file'
    """
    fn = client.files.upload
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())
    except Exception:
        params = []

    # Prefer keyword "path"
    if "path" in params:
        f = fn(path=file_path)
        return _wait_active(client, f)

    # Prefer keyword "file"
    if "file" in params:
        try:
            f = fn(file=file_path)
            return _wait_active(client, f)
        except TypeError:
            pass
        with open(file_path, "rb") as fh:
            f = fn(file=fh)
            return _wait_active(client, f)

    # Fallback positional
    try:
        f = fn(file_path)
        return _wait_active(client, f)
    except TypeError:
        with open(file_path, "rb") as fh:
            f = fn(fh)
            return _wait_active(client, f)


# -----------------------------
# MANIFEST MANAGER (INLINE, NO EXTRA FILE REQUIRED)
# -----------------------------
class ManifestManager:
    """
    Local manifest:
      fingerprint -> {name, uri, uploaded_at, local}

    Re-upload when:
      - local file changed (size/mtime)
      - remote missing/not ACTIVE
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

    def _fingerprint(self, p: Path) -> str:
        st = p.stat()
        return f"{p.name}__{st.st_size}__{int(st.st_mtime)}"

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

            # reuse if remote ACTIVE
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
# BUREAU DETECTOR + BTM LOADER
# -----------------------------
def _detect_bureau_from_pdf_text(report_path: str) -> str:
    """
    Quick, local detector (no AI) using PyPDF2 if installed.
    Returns: TRANSUNION | EXPERIAN | EQUIFAX | UNKNOWN
    """
    try:
        from PyPDF2 import PdfReader  # type: ignore
        reader = PdfReader(report_path)
        if not reader.pages:
            return "UNKNOWN"
        text = (reader.pages[0].extract_text() or "").upper()
        if "TRANSUNION" in text:
            return "TRANSUNION"
        if "EXPERIAN" in text:
            return "EXPERIAN"
        if "EQUIFAX" in text:
            return "EQUIFAX"
        return "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _load_btm(bureau_id: str) -> Optional[Dict[str, Any]]:
    """
    Looks for BTM JSON files.
    Recommended filenames:
      - BTM_TRANSUNION.json
      - BTM_EXPERIAN.json
      - BTM_EQUIFAX.json
    Location:
      00_NORTHSTAR_SOUL_IMPUT/BTM/
    """
    try:
        if bureau_id not in ("TRANSUNION", "EXPERIAN", "EQUIFAX"):
            return None
        btm_path = os.path.join(BTM_DIR, f"BTM_{bureau_id}.json")
        if not os.path.exists(btm_path):
            return None
        with open(btm_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# -----------------------------
# SYSTEM INSTRUCTION (STRICT)
# -----------------------------
def _system_instruction(bureau_id: str, btm: Optional[Dict[str, Any]]) -> str:
    btm_clause = ""
    if btm:
        # Keep it short; pass the map as JSON inline, but do not let it balloon.
        btm_json = json.dumps(btm, ensure_ascii=False)
        btm_clause = f"""
BUREAU_TRANSLATION_MAP (BTM):
- Bureau detected: {bureau_id}
- Use this BTM to interpret bureau-native/proprietary codes before comparing to Metro 2.
- If a code is bureau-native and has a valid mapping in BTM, DO NOT flag it as inconsistency.
- Only flag CODE_MAPPING_INCONSISTENCY if a bureau-native code has NO valid translation or violates the mapping.
BTM_JSON:
{btm_json}
""".strip()

    return f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies/mismatches between the credit report and provided standards (SOUL PDFs).

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0 contract. No extra text.
2) NO recommendations, NO action steps, NO dispute-letter writing, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report text is unreadable/scan/OCR weak => status INCOMPLETE.
6) User narrative is NOT evidence. Only the PDFs.

{btm_clause}

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
# CORE RUN
# -----------------------------
def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # 1) Bureau detect + BTM load
    bureau_id = _detect_bureau_from_pdf_text(report_path)
    btm = _load_btm(bureau_id)

    # 2) Ensure SOUL PDFs are active (exclude BTM folder if you keep BTM as JSON)
    # Only upload PDFs that are in SOUL_DIR root (manuals/standards).
    mm = ManifestManager(MANIFEST_PATH, client)
    soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # 3) Upload report
    report_file = upload_any(client, report_path)

    # 4) Build parts: SOUL PDFs first, report last, then instruction
    parts: List[types.Part] = []

    for ref in soul_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

    parts.append(
        types.Part.from_text(
            text="Perform a technical consistency audit. Return ONLY NS-DK-1.0 JSON."
        )
    )

    sys_inst = _system_instruction(bureau_id=bureau_id, btm=btm)

    # 5) Model call
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
        # Common: 404 model, 429 rate limit, 400 invalid arg, etc.
        return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{type(e).__name__}")

    # 6) Parse JSON
    try:
        raw = json.loads(resp.text)
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")

    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)

    # 7) Confidence Gate (fail-closed)
    if float(payload.get("confidence", 0.0)) < CONFIDENCE_GATE:
        payload = _empty_payload(
            status="UNKNOWN",
            risk_level="NONE",
            confidence=float(payload.get("confidence", 0.0) or 0.0),
            notes_extra="CONFIDENCE_GATE_ACTIVE",
        )

    # 8) HARD OVERRIDES (kill fake timestamps forever)
    payload = _finalize(payload)
    return payload


# -----------------------------
# PUBLIC API (CALLED BY UI)
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

        os.makedirs(TMP_DIR, exist_ok=True)
        return _run_gemini_audit(file_path)

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
