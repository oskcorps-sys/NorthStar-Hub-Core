import os
import re
import streamlit as st
from PyPDF2 import PdfReader

from kernel import audit_credit_report, KERNEL_VERSION, NOTES_IMMUTABLE

# -----------------------------
# Bureau Detector
# -----------------------------
PATTERNS = {
    "EXPERIAN": r"\bexperian\b",
    "EQUIFAX": r"\bequifax\b",
    "TRANSUNION": r"\btransunion\b|\btrans union\b",
}

def detect_bureau(path: str) -> str:
    try:
        r = PdfReader(path)
        text = (r.pages[0].extract_text() or "").lower()
        for k, p in PATTERNS.items():
            if re.search(p, text):
                return k
    except Exception:
        pass
    return "UNKNOWN"


# -----------------------------
# UI
# -----------------------------
st.set_page_config(
    page_title="NorthStar Hub | Forensic Audit",
    page_icon="⚖️",
    layout="wide",
)

st.title("⚖️ NorthStar Hub (Alpha)")
st.caption(f"Kernel: {KERNEL_VERSION} | {NOTES_IMMUTABLE}")
st.divider()

col1, col2 = st.columns([1, 2], gap="large")

with col1:
    st.subheader("📂 Evidence")
    uploaded = st.file_uploader("Upload Credit Report (PDF)", type=["pdf"])

    if uploaded:
        os.makedirs("tmp", exist_ok=True)
        path = os.path.join("tmp", uploaded.name)
        with open(path, "wb") as f:
            f.write(uploaded.getbuffer())

        bureau = detect_bureau(path)
        st.info(f"Bureau detected: {bureau}")

        if st.button("🚀 Run Audit", use_container_width=True):
            with st.spinner("Running forensic audit..."):
                res = audit_credit_report(path, bureau)
            st.session_state["res"] = res

with col2:
    res = st.session_state.get("res")
    if not res:
        st.info("Awaiting report...")
    else:
        st.metric("Status", res["status"])
        st.metric("Risk", res["risk_level"])
        st.metric("Confidence", f"{res['confidence']*100:.1f}%")

        for f in res.get("findings", []):
            with st.expander(f["type"], True):
                st.write(f["description"])
                st.json(f["evidence"])

        st.caption(res["notes"])
