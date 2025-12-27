import json
import time
from pathlib import Path
from typing import Dict, List


def _upload_any(client, file_path: str):
    """
    Streamlit Cloud + google-genai SDK can vary.
    This tries multiple upload signatures until one works.
    Returns the uploaded file object.
    """
    # 1) Positional (many SDKs support this)
    try:
        return client.files.upload(file_path)
    except TypeError:
        pass

    # 2) Keyword path=
    try:
        return client.files.upload(path=file_path)
    except TypeError:
        pass

    # 3) Keyword file= (some SDKs use this)
    try:
        return client.files.upload(file=file_path)
    except TypeError:
        pass

    # 4) File handle
    with open(file_path, "rb") as fh:
        try:
            return client.files.upload(file=fh)
        except TypeError as e:
            raise TypeError(
                "Files.upload() signature mismatch in this environment. "
                "Tried positional, path=, file=, and file-handle."
            ) from e


class ManifestManager:
    """
    Keeps a local manifest mapping local PDF fingerprints -> Gemini File name/uri.
    Re-uploads when:
      - file changed (mtime/size)
      - remote missing or not ACTIVE
    """

    def __init__(self, manifest_path: str, client):
        self.path = Path(manifest_path)
        self.client = client
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

    def _fingerprint(self, file_path: Path) -> str:
        st = file_path.stat()
        return f"{file_path.name}__{st.st_size}__{int(st.st_mtime)}"

    def _remote_active(self, remote_name: str) -> bool:
        try:
            f = self.client.files.get(name=remote_name)
            return getattr(f.state, "name", "") == "ACTIVE"
        except Exception:
            return False

    def ensure_active_pdf_files(
        self,
        folder_path: str,
        glob_pattern: str = "*.pdf",
        wait_processing: bool = True,
        sleep_s: int = 2
    ) -> List[Dict[str, str]]:
        """
        Returns list: [{"name": "...", "uri": "...", "local": "..."}]
        """
        folder = Path(folder_path)
        if not folder.exists():
            raise FileNotFoundError(f"Knowledge folder missing: {folder_path}")

        refs: List[Dict[str, str]] = []

        for p in sorted(folder.glob(glob_pattern)):
            if not p.is_file():
                continue

            key = self._fingerprint(p)
            entry = self.data.get(key)

            # If manifest has it and remote is ACTIVE, reuse it
            if entry and entry.get("name") and self._remote_active(entry["name"]):
                refs.append({"name": entry["name"], "uri": entry["uri"], "local": p.name})
                continue

            # Upload / Re-upload
            uploaded = _upload_any(self.client, str(p))

            if wait_processing:
                while getattr(uploaded.state, "name", "") == "PROCESSING":
                    time.sleep(sleep_s)
                    uploaded = self.client.files.get(name=uploaded.name)

            self.data[key] = {
                "name": uploaded.name,
                "uri": uploaded.uri,
                "uploaded_at": int(time.time()),
                "local": p.name,
            }
            refs.append({"name": uploaded.name, "uri": uploaded.uri, "local": p.name})

        self.save()
        return refs
