from __future__ import annotations

import os
import uuid
import traceback
import streamlit as st

from kernel import audit_credit_report, KERNEL_VERSION, NOTES_IMMUTABLE

# -----------------------------
# STREAMLIT CONFIG
# -----------------------------
st.set_page_config(
    page_title="NorthStar Hub | Forensic Audit (Alpha)",
    page_icon="⚖️",
    layout="wide",
)

st.title("⚖️ NorthStar Hub (Alpha)")
st.caption(f"Kernel: {KERNEL_VERSION} | Mode: {NOTES_IMMUTABLE}")
st.divider()

# -----------------------------
# SECRETS → ENV (Streamlit Cloud)
# Kernel reads GEMINI_API_KEY from env.
# -----------------------------
if "GEMINI_API_KEY" in st.secrets and st.secrets["GEMINI_API_KEY"]:
    os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]

# -----------------------------
# HELPERS
# -----------------------------
TMP_DIR = "tmp"


def _save_uploaded_pdf(uploaded) -> str:
    os.makedirs(TMP_DIR, exist_ok=True)
    safe_name = "".join(ch for ch in (uploaded.name or "report.pdf") if ch.isalnum() or ch in ("-", "_", ".", " ")).strip()
    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"
    unique = uuid.uuid4().hex[:10]
    path = os.path.join(TMP_DIR, f"{unique}__{safe_name}")
    with open(path, "wb") as f:
        f.write(uploaded.getbuffer())
    return path


def _cleanup_file(path: str) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# -----------------------------
# LAYOUT
# -----------------------------
col1, col2 = st.columns([1, 2], gap="large")

with col1:
    st.subheader("📂 Ingest Evidence")
    uploaded = st.file_uploader("Upload Credit Report (PDF)", type=["pdf"])

    st.caption("This tool performs a **technical data consistency check** only. It does not provide legal, financial, or credit repair advice.")

    run = False
    if uploaded:
        st.success("PDF received. Ready to run audit.")
        run = st.button("🚀 Run Forensic Audit", use_container_width=True)

    if run:
        if "GEMINI_API_KEY" not in os.environ or not os.environ["GEMINI_API_KEY"]:
            st.error("Missing GEMINI_API_KEY. Add it in Streamlit Secrets before running.")
        else:
            tmp_path = ""
            try:
                tmp_path = _save_uploaded_pdf(uploaded)

                with st.spinner("Running forensic audit (NS-DK-1.0)..."):
                    result = audit_credit_report(tmp_path)

                st.session_state["audit_result"] = result
                st.session_state["last_file"] = uploaded.name

                st.success("Audit completed (fail-closed rules active).")

            except Exception as e:
                st.error(f"Kernel execution failed: {type(e).__name__}")
                st.code(traceback.format_exc())
                st.session_state["audit_result"] = {
                    "version": KERNEL_VERSION,
                    "timestamp": "",
                    "status": "UNKNOWN",
                    "risk_level": "NONE",
                    "findings": [],
                    "confidence": 0.0,
                    "notes": f"{NOTES_IMMUTABLE} | UI_FAIL:{type(e).__name__}",
                }
            finally:
                _cleanup_file(tmp_path)

with col2:
    st.subheader("🔍 Audit Results")

    res = st.session_state.get("audit_result")
    if not res:
        st.info("Waiting for report ingestion...")
    else:
        status = res.get("status", "UNKNOWN")
        risk = res.get("risk_level", "NONE")
        conf = float(res.get("confidence", 0.0) or 0.0)
        findings = res.get("findings") or []

        # Headline metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("Status", status)
        m2.metric("Risk Level", risk)
        m3.metric("Confidence", f"{conf*100:.1f}%")

        st.progress(conf, text=f"Confidence Gate Active (>= 70%). Current: {conf*100:.1f}%")
        st.divider()

        # Findings detail (visualization only)
        if findings:
            for fnd in findings:
                f_type = fnd.get("type", "UNKNOWN_FINDING")
                with st.expander(f"🚩 {f_type}", expanded=True):
                    st.write(f"**Description:** {fnd.get('description','')}")
                    st.json(fnd.get("evidence", {}))
        else:
            if status == "OK":
                st.success("No technical inconsistencies detected under current ruleset.")
            elif status in ("INCOMPLETE", "UNKNOWN", "SCOPE_LIMITATION"):
                st.warning("Insufficient evidence to reach a conclusion. No best-effort output was produced.")
            else:
                st.info("No findings available for this run.")

        st.divider()
        st.caption(f"Timestamp: {res.get('timestamp','')} | {res.get('notes','')}")
