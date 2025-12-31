"""
NorthStar Hub — Kernel Core (NS-DK-2.1)
Scope: TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY
- Evidence-bound JSON output (NS-DK contract)
- Fail-closed behavior (confidence gate + evidence gate)
- Bureau-aware (Detector + optional BTM injection)
- Google Native Gemini via google-genai SDK
"""

from __future__ import annotations

import os
import json
import time
import inspect
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from google import genai
from google.genai import types

# Optional: PDF pre-read for bureau detection + page count guard
try:
    import PyPDF2
except Exception:
    PyPDF2 = None  # type: ignore


# ============================================================
# CANON (DO NOT DRIFT)
# ============================================================
KERNEL_VERSION = "NS-DK-2.1"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}

CONFIDENCE_GATE = float(os.getenv("NS_CONFIDENCE_GATE", "0.70"))

# ============================================================
# PATHS (Repo-local for Alpha)
# ============================================================
SOUL_DIR = os.getenv("NS_SOUL_DIR", "00_NORTHSTAR_SOUL_IMPUT")  # repo folder
MANIFEST_PATH = os.getenv("NS_MANIFEST_PATH", "manifests/soul_manifest.json")
INSTRUCTIONS_DIR = os.getenv("NS_INSTRUCTIONS_DIR", "instructions")
TMP_DIR = os.getenv("NS_TMP_DIR", "tmp")

# ============================================================
# GEMINI
# ============================================================
# Use what you confirmed: 2.5 flash (override via env if needed)
MODEL_ID = os.getenv("NS_MODEL_ID", "gemini-2.5-flash")
GEMINI_API_KEY_ENV = os.getenv("NS_API_KEY_ENV", "GEMINI_API_KEY")

# Hard safety guard for giant PDFs (Gemini file/page limits)
MAX_PDF_PAGES = int(os.getenv("NS_MAX_PDF_PAGES", "1000"))

# Retry guards (429 / transient)
MAX_RETRIES = int(os.getenv("NS_MAX_RETRIES", "3"))
BASE_BACKOFF_S = float(os.getenv("NS_BACKOFF_S", "2.0"))


# ============================================================
# UTILITIES
# ============================================================
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


def _coerce_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _coerce_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


# ============================================================
# API KEY RESOLUTION (Streamlit Cloud-safe)
# ============================================================
def _get_api_key() -> Optional[str]:
    # 1) Environment (recommended)
    k = os.getenv(GEMINI_API_KEY_ENV)
    if k:
        return k

    # 2) Streamlit secrets fallback (does NOT require streamlit import in main app)
    try:
        import streamlit as st  # type: ignore

        if GEMINI_API_KEY_ENV in st.secrets:
            return str(st.secrets[GEMINI_API_KEY_ENV])
    except Exception:
        pass

    return None


def _client() -> genai.Client:
    api_key = _get_api_key()
    if not api_key:
        raise RuntimeError("MISSING_API_KEY")
    return genai.Client(api_key=api_key)


# ============================================================
# SDK-SIGNATURE SAFE UPLOAD (solves: Files.upload() path error)
# ============================================================
def _upload_any(client: genai.Client, file_path: str):
    fn = client.files.upload

    def _wait_active(f, sleep_s: float = 1.5):
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

    # prefer keyword that exists
    if "path" in params:
        f = fn(path=file_path)
        return _wait_active(f)

    if "file" in params:
        # try file=path
        try:
            f = fn(file=file_path)
            return _wait_active(f)
        except TypeError:
            pass
        # try file=handle
        with open(file_path, "rb") as fh:
            f = fn(file=fh)
            return _wait_active(f)

    # fallback positional
    try:
        f = fn(file_path)
        return _wait_active(f)
    except TypeError:
        with open(file_path, "rb") as fh:
            f = fn(fh)
            return _wait_active(f)


# ============================================================
# MANIFEST MANAGER (local fingerprint -> remote file refs)
# ============================================================
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
            raise FileNotFoundError(f"SOUL_FOLDER_MISSING:{folder_path}")

        refs: List[Dict[str, str]] = []

        for p in sorted(folder.glob("*.pdf")):
            if not p.is_file():
                continue

            key = self._fingerprint(p)
            entry = self.data.get(key)

            # reuse if remote ACTIVE
            if entry and entry.get("name") and self._remote_active(str(entry["name"])):
                refs.append({"name": str(entry["name"]), "uri": str(entry["uri"]), "local": p.name})
                continue

            # upload/reupload
            uploaded = _upload_any(self.client, str(p))

            self.data[key] = {
                "name": uploaded.name,
                "uri": uploaded.uri,
                "uploaded_at": int(time.time()),
                "local": p.name,
            }
            refs.append({"name": uploaded.name, "uri": uploaded.uri, "local": p.name})

        self.save()
        return refs


# ============================================================
# BUREAU DETECTOR + BTM SELECTOR
# ============================================================
def _detect_bureau_from_pdf(pdf_path: str) -> str:
    """
    Returns one of: EXPERIAN | EQUIFAX | TRANSUNION | UNKNOWN
    Uses first-page text if PyPDF2 is available. Fail-closed to UNKNOWN.
    """
    if PyPDF2 is None:
        return "UNKNOWN"

    try:
        with open(pdf_path, "rb") as f:
            r = PyPDF2.PdfReader(f)
            if not r.pages:
                return "UNKNOWN"
            txt = (r.pages[0].extract_text() or "").upper()

        if "EXPERIAN" in txt:
            return "EXPERIAN"
        if "EQUIFAX" in txt:
            return "EQUIFAX"
        if "TRANSUNION" in txt or "TRANS UNION" in txt:
            return "TRANSUNION"

        return "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _pdf_page_count(pdf_path: str) -> Optional[int]:
    if PyPDF2 is None:
        return None
    try:
        with open(pdf_path, "rb") as f:
            r = PyPDF2.PdfReader(f)
            return len(r.pages)
    except Exception:
        return None


def _select_btm_files(soul_folder: str, bureau_id: str) -> List[Path]:
    """
    Finds BTM files inside SOUL folder.
    Expected naming (flexible):
      - BTM_TRANSUNION.pdf
      - BTM_EXPERIAN.pdf
      - BTM_EQUIFAX.pdf
    Also accepts:
      - BTM_TU.pdf / BTM_EXP.pdf / BTM_EQ.pdf
    """
    folder = Path(soul_folder)
    if not folder.exists():
        return []

    bureau_id = (bureau_id or "UNKNOWN").upper()
    patterns: List[str] = []

    if bureau_id == "TRANSUNION":
        patterns = ["BTM_TRANSUNION*.pdf", "BTM_TU*.pdf"]
    elif bureau_id == "EXPERIAN":
        patterns = ["BTM_EXPERIAN*.pdf", "BTM_EXP*.pdf"]
    elif bureau_id == "EQUIFAX":
        patterns = ["BTM_EQUIFAX*.pdf", "BTM_EQ*.pdf"]
    else:
        return []

    out: List[Path] = []
    for pat in patterns:
        out.extend(sorted(folder.glob(pat)))

    # de-dup
    seen = set()
    uniq: List[Path] = []
    for p in out:
        if p.name not in seen:
            uniq.append(p)
            seen.add(p.name)
    return uniq


# ============================================================
# SYSTEM INSTRUCTION (file-driven; defaults embedded)
# ============================================================
_DEFAULT_BASE = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.
MISSION: Detect technical inconsistencies and mismatches between the credit report and provided standards.

HARD RULES:
1) OUTPUT ONLY valid JSON following the NS-DK contract. No extra text.
2) NO recommendations, NO action steps, NO dispute letters, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous -> DO NOT output the finding.
5) If report is unreadable/scan/OCR weak or key sections missing -> status INCOMPLETE.
6) User narrative is NOT evidence. Only the PDFs.
""".strip()

_DEFAULT_BUREAU_RULES = """
BUREAU DIALECT NORMALIZATION RULES:
- Metro 2 is the universal baseline, but bureaus may present proprietary "dialects".
- If a field/value is a known bureau dialect AND the BTM mapping defines it as acceptable:
  -> DO NOT flag as inconsistency.
- If a field/value appears proprietary AND there is NO mapping available:
  -> flag as BTM_MISSING_MAPPING (evidence-bound) OR return UNKNOWN if ambiguous.
- Only flag METRO2_MISMATCH when:
  a) BTM exists for the bureau (or bureau is UNKNOWN), AND
  b) the value violates Metro 2 per SOUL standards, AND
  c) evidence is explicit (document/page/field).

FINDING TYPES (examples; you can use others if justified and evidence-bound):
- METRO2_MISMATCH
- BTM_MISSING_MAPPING
- CODE_MAPPING_INCONSISTENCY
- DATA_FORMAT_INCONSISTENCY
- DATA_VALUE_INCONSISTENCY
- TEMPORAL_INCONSISTENCY
""".strip()

_DEFAULT_CONTRACT = f"""
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
- If unreadable/missing key pages => status INCOMPLETE.
- If unsure => status UNKNOWN (fail-closed).
""".strip()


def _read_text_if_exists(path: Path) -> Optional[str]:
    try:
        if path.exists() and path.is_file():
            return path.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    return None


def _build_system_instruction(bureau_id: str) -> str:
    """
    Builds final system instruction using instruction files if present;
    otherwise uses embedded defaults.
    """
    instr_dir = Path(INSTRUCTIONS_DIR)

    base = _read_text_if_exists(instr_dir / "nsdk_base.md") or _DEFAULT_BASE
    rules = _read_text_if_exists(instr_dir / "nsdk_bureau_rules.md") or _DEFAULT_BUREAU_RULES
    contract = _read_text_if_exists(instr_dir / "nsdk_output_contract.json") or _DEFAULT_CONTRACT

    bureau_line = f"BUREAU_ID: {bureau_id}\n"
    return "\n\n".join([base, bureau_line, rules, contract]).strip()


# ============================================================
# CONTRACT NORMALIZATION + GATES
# ============================================================
def _normalize_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensures the kernel ALWAYS returns a schema-compliant NS-DK payload.
    """
    p = dict(payload or {})

    p["version"] = KERNEL_VERSION
    p["timestamp"] = p.get("timestamp") or _utc_iso()

    status = str(p.get("status") or "UNKNOWN").upper()
    risk = str(p.get("risk_level") or "NONE").upper()

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

    p["confidence"] = _coerce_float(p.get("confidence"), 0.0)
    p["notes"] = NOTES_IMMUTABLE

    return p


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Strips findings missing required evidence fields.
    If status=RISK_DETECTED but no valid findings => UNKNOWN.
    """
    p = dict(payload or {})
    findings = p.get("findings", [])
    if not isinstance(findings, list):
        p["findings"] = []
        return p

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

        if doc and field and page is not None:
            page_str = str(page).strip().upper()
            field_str = str(field).strip().upper()
            if page_str != "UNKNOWN" and field_str != "UNKNOWN":
                # coerce numeric page if possible
                ev["page"] = _coerce_int(page, default=page)  # keeps original if not int
                f["evidence"] = ev
                valid.append(f)

    p["findings"] = valid

    if p.get("status") == "RISK_DETECTED" and not valid:
        p["status"] = "UNKNOWN"
        p["risk_level"] = "NONE"
        p["confidence"] = min(_coerce_float(p.get("confidence"), 0.0), 0.5)

    return p


def _confidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(payload or {})
    c = _coerce_float(p.get("confidence"), 0.0)
    if c < CONFIDENCE_GATE:
        return _empty_payload(
            status="UNKNOWN",
            risk_level="NONE",
            confidence=c,
            notes_extra="CONFIDENCE_GATE_ACTIVE",
        )
    return p


# ============================================================
# MODEL CALL (with retries)
# ============================================================
def _call_model_with_retries(client: genai.Client, model_id: str, contents, config) -> str:
    last_err: Optional[Exception] = None
    for i in range(MAX_RETRIES + 1):
        try:
            resp = client.models.generate_content(
                model=model_id,
                contents=contents,
                config=config,
            )
            return resp.text
        except Exception as e:
            last_err = e
            # Backoff for rate limit / transient failures
            sleep_s = BASE_BACKOFF_S * (2 ** i)
            time.sleep(sleep_s)
    raise RuntimeError(f"MODEL_CALL_FAIL:{type(last_err).__name__}") from last_err


# ============================================================
# CORE AUDIT (Gemini)
# ============================================================
def _run_gemini_audit(report_path: str) -> Dict[str, Any]:
    client = _client()

    # 0) Page limit guard (hard fail-closed)
    pages = _pdf_page_count(report_path)
    if pages is not None and pages > MAX_PDF_PAGES:
        return _empty_payload(status="INCOMPLETE", notes_extra=f"PDF_PAGE_LIMIT_EXCEEDED:{pages}>{MAX_PDF_PAGES}")

    # 1) Detect bureau (for BTM injection + rules)
    bureau_id = _detect_bureau_from_pdf(report_path)

    # 2) Upload SOUL PDFs (base standards + BTM if present)
    mm = ManifestManager(MANIFEST_PATH, client)

    if not os.path.isdir(SOUL_DIR):
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_DIR_MISSING")

    # Base SOUL PDFs (ALL PDFs in SOUL_DIR)
    try:
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)
    except Exception as e:
        return _empty_payload(status="INCOMPLETE", notes_extra=f"SOUL_MANIFEST_FAIL:{type(e).__name__}")

    if not soul_refs:
        return _empty_payload(status="INCOMPLETE", notes_extra="SOUL_NO_PDFS_FOUND")

    # 2b) Ensure bureau-specific BTM exists (optional but recommended)
    # If BTM exists in folder, it will already be in soul_refs because we upload *.pdf.
    # Still: we want to explicitly *signal* bureau context in instruction.
    btm_local_files = _select_btm_files(SOUL_DIR, bureau_id)
    btm_present = len(btm_local_files) > 0

    # 3) Upload report
    try:
        report_file = _upload_any(client, report_path)
    except Exception as e:
        return _empty_payload(status="INCOMPLETE", notes_extra=f"REPORT_UPLOAD_FAIL:{type(e).__name__}")

    # 4) Build system instruction (file-driven)
    system_instruction = _build_system_instruction(bureau_id)
    if not system_instruction:
        return _empty_payload(status="INCOMPLETE", notes_extra="MISSING_SYSTEM_INSTRUCTION")

    # 5) Assemble parts (SOUL first, then report, then task)
    parts: List[types.Part] = []

    # Attach SOUL refs (all PDFs in SOUL_DIR)
    for ref in soul_refs:
        parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

    # Attach report
    parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

    # Task
    task = (
        "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
        "If BTM is present for the bureau, treat bureau dialect as valid when BTM maps it. "
        "Return ONLY NS-DK JSON."
    )
    # IMPORTANT: keyword form avoids Part.from_text positional TypeError across SDK versions
    parts.append(types.Part.from_text(text=task))

    # 6) Call model (strict JSON)
    try:
        raw_text = _call_model_with_retries(
            client,
            MODEL_ID,
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=str(e))

    # 7) Parse + normalize + gates
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON_OUTPUT")

    payload = _normalize_contract(raw)
    payload = _evidence_gate(payload)
    payload = _confidence_gate(payload)

    # add lightweight trace (still within notes, non-prescriptive)
    # keeps the canon, adds minimal context without leaking prompt
    if payload["notes"] == NOTES_IMMUTABLE:
        extra = f"BUREAU:{bureau_id}"
        if btm_present:
            extra += "|BTM:ON"
        payload["notes"] = f"{NOTES_IMMUTABLE} | {extra}"

    return payload


# ============================================================
# PUBLIC API (called by Streamlit main.py)
# ============================================================
def audit_credit_report(file_path: str) -> Dict[str, Any]:
    """
    Canonical entry point called by Streamlit (main.py)
    Input: local PDF path
    Output: NS-DK dict
    """
    try:
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")

        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")

        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        os.makedirs(TMP_DIR, exist_ok=True)
        Path(MANIFEST_PATH).parent.mkdir(parents=True, exist_ok=True)

        return _run_gemini_audit(file_path)

    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")


if __name__ == "__main__":
    print("NorthStar Hub Kernel Initialized...")
    print(_empty_payload(status="OK", risk_level="NONE", confidence=1.0))
