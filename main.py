
import os
import streamlit as st
from kernel import audit_credit_report, KERNEL_VERSION, NOTES_IMMUTABLE

st.set_page_config(page_title="NorthStar Hub | Forensic Audit (Alpha)", page_icon="⚖️", layout="wide")

st.title("⚖️ NorthStar Hub (Alpha)")
st.caption(f"Kernel: {KERNEL_VERSION} | Mode: {NOTES_IMMUTABLE}")
st.divider()

col1, col2 = st.columns([1, 2], gap="large")

with col1:
    st.subheader("📂 Ingest Evidence")
    uploaded = st.file_uploader("Upload Credit Report (PDF)", type=["pdf"])

    if uploaded:
        st.success("PDF received. Ready to run audit.")
        if st.button("🚀 Run Forensic Audit", use_container_width=True):
            os.makedirs("tmp", exist_ok=True)
            tmp_path = os.path.join("tmp", uploaded.name)
            with open(tmp_path, "wb") as f:
                f.write(uploaded.getbuffer())

            with st.spinner("Running forensic audit..."):
                result = audit_credit_report(tmp_path)

            st.session_state["audit_result"] = result

with col2:
    st.subheader("🔍 Audit Results")

    res = st.session_state.get("audit_result")
    if not res:
        st.info("Waiting for report ingestion...")
    else:
        status = res.get("status", "UNKNOWN")
        risk = res.get("risk_level", "NONE")
        conf = float(res.get("confidence", 0.0))
        findings = res.get("findings", [])

        m1, m2, m3 = st.columns(3)
        m1.metric("Status", status)
        m2.metric("Risk Level", risk)
        m3.metric("Confidence", f"{conf*100:.1f}%")

        st.progress(conf, text=f"Confidence Gate (>= 70%). Current: {conf*100:.1f}%")
        st.divider()

        if findings:
            for fnd in findings:
                f_type = fnd.get("type", "UNKNOWN_FINDING")
                with st.expander(f"🚩 {f_type}", expanded=True):
                    st.write(f"**Description:** {fnd.get('description','')}")
                    st.json(fnd.get("evidence", {}))
        else:
            if status == "OK":
                st.success("No technical inconsistencies detected under current ruleset.")
            elif status in ("INCOMPLETE", "UNKNOWN"):
                st.warning("Insufficient evidence to reach a conclusion. No best-effort output was produced.")
            else:
                st.info("No findings available for this run.")

        st.divider()
        st.caption(f"Timestamp: {res.get('timestamp')} | {res.get('notes')}")
