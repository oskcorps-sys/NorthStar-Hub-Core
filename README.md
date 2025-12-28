# NorthStar Hub Core (Alpha) — NS-DK-1.0

Scope: Technical data consistency check only.
Outputs are evidence-bound JSON. Fail-closed behavior.

## Repo Layout
- `main.py` Streamlit UI
- `kernel.py` NS-DK-1.0 engine (Gemini 2.5 Flash)
- `manifest_manager.py` SOUL upload + manifest
- `00_NORTHSTAR_SOUL_IMPUT/` Put SOUL PDFs here (Metro 2 manuals, standards)
- `manifests/` Local cache for remote file references
- `tmp/` Temp uploads from UI

## Streamlit Cloud
1) Deploy from GitHub.
2) Set Secret:
   - `GEMINI_API_KEY = <your_key>`
3) Ensure `00_NORTHSTAR_SOUL_IMPUT/` contains at least 1 PDF.

Run locally:
```bash
pip install -r requirements.txt
streamlit run main.py
