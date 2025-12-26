from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict

from google import genai
from google.genai import types

from manifest_manager import ManifestManager

# -----------------------------
# CANON (DO NOT DRIFT)
# -----------------------------
KERNEL_VERSION = "NS-DK-1.0"
NOTES_IMMUTABLE = "TECHNICAL_DATA_CONSISTENCY_CHECK_ONLY"
CONFIDENCE_GATE = 0.70

ALLOWED_STATUS = {"OK", "RISK_DETECTED", "INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"}
ALLOWED_RISK = {"NONE", "LOW", "MEDIUM", "HIGH"}

SOUL_DIR = "00_NORTHSTAR_SOUL_IMPUT"
MANIFEST_PATH = "manifests/soul_manifest.json"
MODEL_ID = "gemini-1.5-pro"


def _utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _client() -> genai.Client:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("MISSING_GEMINI_API_KEY")
    return genai.Client(api_key=api_key)


def _empty_payload(status="UNKNOWN", risk_level="NONE", confidence=0.0, notes_extra="") -> Dict[str, Any]:
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
    try:
        payload = dict(payload or {})
        status = payload.get("status", "UNKNOWN")
        risk = payload.get("risk_level", "NONE")
        findings = payload.get("findings", [])
        conf = payload.get("confidence", 0.0)

        if status not in ALLOWED_STATUS:
            status = "UNKNOWN"
        if risk not in ALLOWED_RISK:
            risk = "NONE"
        if not isinstance(findings, list):
            findings = []

        try:
            conf = float(conf)
        except Exception:
            conf = 0.0

        return {
            "version": KERNEL_VERSION,
            "timestamp": _utc_iso(),
            "status": status,
            "risk_level": risk,
            "findings": findings,
            "confidence": conf,
            "notes": NOTES_IMMUTABLE,
        }
    except Exception:
        return _empty_payload(status="UNKNOWN", notes_extra="NORMALIZE_FAIL")


def _evidence_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    findings = payload.get("findings", [])
    if not isinstance(findings, list):
        payload["findings"] = []
        return payload

    valid = []
    for f in findings:
        if not isinstance(f, dict):
            continue
        ev = f.get("evidence", {})
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


SYSTEM_INSTRUCTION = f"""
ROLE: Principal Technical Data Consistency Auditor (NorthStar Hub).
SCOPE: {NOTES_IMMUTABLE}. Not legal advice. Not financial advice. Not credit repair.

HARD RULES:
1) OUTPUT ONLY valid JSON following NS-DK-1.0. No extra text.
2) NO recommendations, NO action steps, NO dispute letters, NO lender suggestions.
3) Every finding MUST include evidence: document + page + field.
4) If evidence is missing/ambiguous, do NOT output the finding.
5) If report is unreadable / scanned / OCR weak => status INCOMPLETE.
6) If unsure => status UNKNOWN (fail-closed).

ALLOWED ENUMS:
status: OK|RISK_DETECTED|INCOMPLETE|UNKNOWN|SCOPE_LIMITATION
risk_level: NONE|LOW|MEDIUM|HIGH
""".strip()


def audit_credit_report(file_path: str) -> Dict[str, Any]:
    try:
        if not file_path or not isinstance(file_path, str):
            return _empty_payload(status="INCOMPLETE", notes_extra="BAD_INPUT")
        if not os.path.exists(file_path):
            return _empty_payload(status="INCOMPLETE", notes_extra="FILE_NOT_FOUND")
        if not file_path.lower().endswith(".pdf"):
            return _empty_payload(status="INCOMPLETE", notes_extra="NOT_PDF")

        client = _client()

        # 1) Ensure SOUL active
        mm = ManifestManager(MANIFEST_PATH, client)
        soul_refs = mm.ensure_active_pdf_files(SOUL_DIR)

        # 2) Upload report and wait
        report_file = client.files.upload(path=file_path)
        while getattr(report_file.state, "name", "") == "PROCESSING":
            time.sleep(2)
            report_file = client.files.get(name=report_file.name)

        # 3) Build context (SOUL first, REPORT last)
        parts = []
        for ref in soul_refs:
            parts.append(types.Part.from_uri(file_uri=ref["uri"], mime_type="application/pdf"))

        parts.append(types.Part.from_uri(file_uri=report_file.uri, mime_type="application/pdf"))

        parts.append(types.Part.from_text(
            "Perform a technical Metro 2 consistency audit of the attached credit report against SOUL standards. "
            "Return ONLY NS-DK-1.0 JSON."
        ))

        response = client.models.generate_content(
            model=MODEL_ID,
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )

        raw = json.loads(response.text)

        payload = _normalize_contract(raw)
        payload = _evidence_gate(payload)

        if float(payload.get("confidence", 0.0)) < CONFIDENCE_GATE:
            return _empty_payload(
                status="UNKNOWN",
                risk_level="NONE",
                confidence=payload.get("confidence", 0.0),
                notes_extra="CONFIDENCE_GATE",
            )

        return payload

    except json.JSONDecodeError:
        return _empty_payload(status="UNKNOWN", notes_extra="BAD_JSON")
    except Exception as e:
        return _empty_payload(status="UNKNOWN", notes_extra=f"KERNEL_FAIL:{type(e).__name__}")
