import json
import time
import inspect
from pathlib import Path
from typing import Dict, List, Optional

try:
    from PyPDF2 import PdfReader
except Exception:
    PdfReader = None


def _pdf_page_count(path: str) -> Optional[int]:
    if PdfReader is None:
        return None
    try:
        r = PdfReader(path)
        return len(r.pages)
    except Exception:
        return None


def upload_any(client, file_path: str, sleep_s: int = 2):
    """
    Upload robusto (Streamlit Cloud-safe).
    Se adapta a la firma real del SDK instalado (google-genai).
    - Soporta: upload(path=...), upload(file=...), upload(<positional>)
    - Espera ACTIVE (sale de PROCESSING)
    """
    fn = client.files.upload

    def _wait(f):
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

    # Preferred keyword: path
    if "path" in params:
        f = fn(path=file_path)
        return _wait(f)

    # Alternate keyword: file
    if "file" in params:
        # Try passing str
        try:
            f = fn(file=file_path)
            return _wait(f)
        except TypeError:
            pass
        # Try file handle
        with open(file_path, "rb") as fh:
            f = fn(file=fh)
            return _wait(f)

    # Positional fallbacks
    try:
        f = fn(file_path)
        return _wait(f)
    except TypeError:
        with open(file_path, "rb") as fh:
            f = fn(fh)
            return _wait(f)


class ManifestManager:
    """
    Local manifest:
      fingerprint -> {name, uri, uploaded_at, local, pages}

    Re-upload when:
      - local file changed (size/mtime)
      - remote missing/not ACTIVE

    Also:
      - Skips PDFs above max_pages (default 1000) to avoid Gemini File limits.
    """

    def __init__(self, manifest_path: str, client, max_pages: int = 1000):
        self.path = Path(manifest_path)
        self.client = client
        self.max_pages = max_pages
        self.data: Dict[str, Dict] = self._load()

    def _load(self) -> Dict[str, Dict]:
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

            pages = _pdf_page_count(str(p))
            if pages is not None and pages > self.max_pages:
                # Skip huge PDFs to avoid 400 INVALID_ARGUMENT (page limit)
                continue

            key = self._fingerprint(p)
            entry = self.data.get(key)

            # Reuse if remote is ACTIVE
            if entry and entry.get("name") and self._remote_active(entry["name"]):
                refs.append({"name": entry["name"], "uri": entry["uri"], "local": p.name})
                continue

            uploaded = upload_any(self.client, str(p))

            self.data[key] = {
                "name": uploaded.name,
                "uri": uploaded.uri,
                "uploaded_at": int(time.time()),
                "local": p.name,
                "pages": pages,
            }
            refs.append({"name": uploaded.name, "uri": uploaded.uri, "local": p.name})

        self.save()
        return refs
