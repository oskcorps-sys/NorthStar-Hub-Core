"""
NorthStar Hub — Kernel Core (NS-DK-2.2)
Scope: Technical data consistency check only
- Evidence-bound JSON output
- Fail-closed behavior
- Bureau Detector + BTM dynamic loading
- SDK-signature adaptive (prevents TypeError drift)
"""

from __future__ import annotations

import os
import json
import time
import hashlib
import datetime as dt
import inspect
from pathlib import Path
from typing import Any, Dict, List, Tuple

from google import genai
from google.genai import types

# -----------------------------
# CANON
# -----------------------------
KERNEL_VERSION = "NS-DK-2.2"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}
CONFIDENCE_GATE = 0.70

# -----------------------------
# PATHS (repo-local)
# -----------------------------
SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"
TMP_DIR = "tmp"

# -----------------------------
# GEMINI
# -----------------------------
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"
MODEL_ID = os.getenv("GEMINI_MODEL_ID", "gemini-2.5-flash")

# -----------------------------
# Bureau detection heuristics
# -----------------------------
BUREAUS = {
    "TRANSUNION": ["transunion", "trans union", "how to read transunion", "transunion credit report"],
    "EXPERIAN": ["experian", "experian credit report"],
    "EQUIFAX": ["equifax", "equifax credit report"],
}

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


def _short_err(e: Exception, limit: int = 140) -> str:
    s = f"{type(e).__name__}:{str(e)}"
    return s if len(s) <= limit else s[:limit] + "…"


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
    try:
        payload = dict(payload or {})
        payload["version"] = KERNEL_VERSION
        payload["timestamp"] = payload.get("timestamp") or _utc_iso()

        if payload.get("status") not in ALLOWED_STATUS:
            return _empty_payload(status="UNKNOWN", notes_extra="BAD_STATUS")

        if payload.get("risk_level") not in ALLOWED_RISK:
            return _empty_payload(status="UNKNOWN", notes_extra="BAD_RISK_LEVEL")

        if not isinstance(payload.get("findings"), list):
            payload["findings"] = []

        conf = payload.get("confidence")
        payload["confidence"] = float(conf) if isinstance(conf, (int, float)) else 0.0

        if payload["confidence"] < CONFIDENCE_GATE:
            return _empty_payload(
                status="UNKNOWN",
                risk_level="NONE",
                confidence=payload["confidence"],
                notes_extra="CONFIDENCE_GATE_ACTIVE",
            )

        payload["notes"] = NOTES_IMMUTABLE
        return payload
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"VALIDATION_EXCEPTION:{_short_err(e)}")


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
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
# SDK-adaptive Part builders (fix TypeError drift)
# -----------------------------
def _sig_params(fn) -> List[str]:
    try:
        return list(inspect.signature(fn).parameters.keys())
    except Exception:
        return []


def part_text(text: str):
    fn = types.Part.from_text
    params = _sig_params(fn)
    # Most robust: keyword
    if "text" in params:
        return fn(text=text)
    # Fallback: some SDKs accept a single positional string
    return fn(text)


def part_uri(uri: str, mime: str = "application/pdf"):
    fn = types.Part.from_uri
    params = _sig_params(fn)

    # Common keyword names
    if "file_uri" in params and "mime_type" in params:
        return fn(file_uri=uri, mime_type=mime)
    if "uri" in params and "mime_type" in params:
        return fn(uri=uri, mime_type=mime)

    # Positional fallback
    try:
        return fn(uri, mime)
    except TypeError:
        return fn(uri)


# -----------------------------
# SDK-safe upload (fixes `path=` / `file=` TypeError drift)
# -----------------------------
def upload_any(client, file_path: str):
    fn = client.files.upload

    def _wait(f, sleep_s=2):
        while getattr(getattr(f, "state", None), "name", "") == "PROCESSING":
            time.sleep(sleep_s)
            f = client.files.get(name=f.name)
        return f

    params = _sig_params(fn)

    if "path" in params:
        return _wait(fn(path=file_path))

    if "file" in params:
        # string path
        try:
            return _wait(fn(file=file_path))
        except TypeError:
            pass
        # file handle
        with open(file_path, "rb") as fh:
            return _wait(fn(file=fh))

    # positional
    try:
        return _wait(fn(file_path))
    except TypeError:
        with open(file_path, "rb") as fh:
            return _wait(fn(fh))


# -----------------------------
# Manifest Manager
# -----------------------------
class ManifestManager:
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
# Bureau detector
# -----------------------------
def _detect_bureau_from_bytes(pdf_bytes: bytes) -> str:
    hay = pdf_bytes[:200_000].lower()
    for bureau, keys in BUREAUS.items():
        for k in keys:
            if k.encode("utf-8") in hay:
                return bureau
    return "UNKNOWN"


def _select_soul_and_btm(soul_files: List[Dict[str, str]], bureau_id: str) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    btms: List[Dict[str, str]] = []
    core: List[Dict[str, str]] = []

    for f in soul_files:
        name = (f.get("local") or f.get("name") or "").lower()
        if "btm" in name or "translation" in name or "mapping" in name:
            btms.append(f)
        else:
            core.append(f)

    if bureau_id not in BTM_HINTS:
        return core, []

    hints = BTM_HINTS[bureau_id]

    strong: List[Dict[str, str]] = []
    for f in btms:
        nm = (f.get("local") or f.get("name") or "").lower()
        score = sum(1 for h in hints if h in nm)
        if score >= 2:
            strong.append(f)

    if strong:
        return core, strong

    soft = [f for f in btms if any(h in (f.get("local") or f.get("name") or "").lower() for h in hints)]
    return core, soft


def _build_system_instruction(bureau_id: str) -> str:
    return f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies between:
  (A) the credit report PDF (bureau formats vary)
  (B) SOUL standards PDFs (Metro 2 + any provided bureau translation/mapping docs)

BUREAU CONTEXT:
- Detected Bureau: {bureau_id}
- If a bureau-specific translation/mapping document (BTM) is provided, you MUST use it to interpret bureau-native codes.
- Do NOT flag discrepancies solely due to bureau proprietary formatting if BTM maps it as valid.

HARD RULES:
1) OUTPUT ONLY valid JSON (no extra text).
2) NO recommendations, NO action steps, NO credit repair language.
3) Every finding MUST include evidence: document + page + field.
4) If evidence missing/ambiguous => do NOT output the finding.
5) Unreadable/scan/OCR weak => status INCOMPLETE.
6) If unsure => status UNKNOWN (fail-closed).

JSON CONTRACT:
{{
  "version": "{KERNEL_VERSION}",
  "timestamp": "ISO-UTC",
  "status": "OK|RISK_DETECTED|INCOMPLETE|UNKNOWN|SCOPE_LIMITATION",
  "risk_level": "NONE|LOW|MEDIUM|HIGH",
  "findings": [{{"type":"STRING","description":"short technical","evidence":{{"document":"PDF","page":1,"field":"FIELD"}}}}],
  "confidence": 0.0,
  "notes": "{NOTES_IMMUTABLE}"
}}
""".strip()


def _client() -> genai.Client:
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY.")
    return genai.Client(api_key=api_key)


def _run_gemini_audit(report_path: str, bureau_id: str) -> Dict[str, Any]:
    client = _client()

    mm = ManifestManager(MANIFEST_PATH, client)
    try:
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    except Exception as e:
        return _empty_payload(status="INCOMPLETE", notes_extra=f"SOUL_MANIFEST_FAIL:{_short_err(e)}")

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    try:
        report_file = upload_any(client, report_path)
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"UPLOAD_FAIL:{_short_err(e)}")

    core_refs, btm_refs = _select_soul_and_btm(soul_refs, bureau_id)

    parts: List[Any] = []
    for ref in core_refs:
        parts.append(part_uri(ref["uri"], "application/pdf"))
    for ref in btm_refs:
        parts.append(part_uri(ref["uri"], "application/pdf"))

    parts.append(part_uri(report_file.uri, "application/pdf"))
    parts.append(part_text(
        "Perform a technical consistency audit. "
        "Only output evidence-bound findings. "
        "Return ONLY JSON in the required contract."
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
        return _empty_payload(status="UNKNOWN", notes_extra=f"MODEL_CALL_FAIL:{_short_err(e)}")

    try:
        raw = json.loads(resp.text)
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"BAD_JSON_OUTPUT:{_short_err(e)}")

    raw = _evidence_gate(raw)
    raw = _validate_payload(raw)
    return raw


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

        os.makedirs(TMP_DIR, exist_ok=True)
        Path(MANIFEST_PATH).parent.mkdir(parents=True, exist_ok=True)

        if not os.path.isdir(SOUL_DIR):
            return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

        try:
            with open(file_path, "rb") as f:
                pdf_bytes = f.read()
            bureau_id = _detect_bureau_from_bytes(pdf_bytes)
        except Exception:
            bureau_id = "UNKNOWN"

        return _run_gemini_audit(file_path, bureau_id)

    except TypeError as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{_short_err(e)}")
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{_short_err(e)}")
