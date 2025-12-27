import json
import time
from pathlib import Path
from typing import Dict, List


class ManifestManager:
    """
    Keeps a local manifest mapping local PDF fingerprints -> Gemini File name/uri.
    Re-uploads when:
      - local file changed (mtime/size)
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
            return getattr(getattr(f, "state", None), "name", "") == "ACTIVE"
        except Exception:
            return False

    def _upload_pdf(self, p: Path, wait_processing: bool = True, sleep_s: int = 2):
        # ✅ IMPORTANT: Your SDK does NOT accept path=... so we upload via file=
        with open(str(p), "rb") as fh:
            uploaded = self.client.files.upload(file=fh)

        if wait_processing:
            while getattr(getattr(uploaded, "state", None), "name", "") == "PROCESSING":
                time.sleep(sleep_s)
                uploaded = self.client.files.get(name=uploaded.name)

        return uploaded

    def ensure_active_pdf_files(
        self,
        folder_path: str,
        glob_pattern: str = "*.pdf",
        wait_processing: bool = True,
        sleep_s: int = 2,
    ) -> List[Dict[str, str]]:
        """
        Returns list of refs:
          [{"name": "...", "uri": "...", "local": "..."}]
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

            # If we already have a remote reference, verify it
            if entry and entry.get("name") and self._remote_active(entry["name"]):
                refs.append({"name": entry["name"], "uri": entry["uri"], "local": p.name})
                continue

            # Upload (or re-upload)
            uploaded = self._upload_pdf(p, wait_processing=wait_processing, sleep_s=sleep_s)

            # Store/overwrite manifest for this fingerprint
            self.data[key] = {
                "name": uploaded.name,
                "uri": uploaded.uri,
                "uploaded_at": int(time.time()),
                "local": p.name,
            }
            refs.append({"name": uploaded.name, "uri": uploaded.uri, "local": p.name})

        self.save()
        return refs
