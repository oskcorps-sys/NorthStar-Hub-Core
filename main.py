import streamlit as st
import os
from kernel import audit_credit_report, KERNEL_VERSION, NOTES_IMMUTABLE

# -----------------------------
# UI CONFIG
# -----------------------------
st.set_page_config(
    page_title="NorthStar Hub | Forensic Audit (Alpha)",
    page_icon="⚖️",
    layout="wide"
)

st.title("⚖️ NorthStar Hub (Alpha)")
st.caption(f"Kernel: {KERNEL_VERSION} | Mode: {NOTES_IMMUTABLE}")
st.divider()

# ... (el resto del código sigue igual, pero asegúrate de que el 'import' inicial esté al borde)
