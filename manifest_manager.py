import json
import time
import inspect
from pathlib import Path
from typing import Dict, List


def upload_any(client, file_path: str):
    fn = client.files.upload

    def _wait(f):
        while getattr(f.state, "name", "") == "PROCESSING":
            time.sleep(2)
            f = client.files.get(name=f.name)
        return f

    try:
        sig = inspect.signature(fn)
        params = sig.parameters
    except Exception:
        params = {}

    if "path" in params:
        return _wait(fn(path=file_path))

    if "file" in params:
        try:
            return _wait(fn(file=file_path))
        except TypeError:
            with open(file_path, "rb") as fh:
                return _wait(fn(file=fh))

    with open(file_path, "rb") as fh:
        return _wait(fn(fh))


class ManifestManager:
    def __init__(self, manifest_path: str, client):
        self.path = Path(manifest_path)
        self.client = client
        self.data = self._load()

    def _load(self):
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except Exception:
                pass
        return {}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2))

    def _fingerprint(self, p: Path):
        st = p.stat()
        return f"{p.name}__{st.st_size}__{int(st.st_mtime)}"

    def ensure_active_pdf_files(self, folder: str) -> List[Dict[str, str]]:
        refs = []
        base = Path(folder)

        for p in sorted(base.glob("*.pdf")):
            key = self._fingerprint(p)
            entry = self.data.get(key)

            if entry:
                try:
                    f = self.client.files.get(name=entry["name"])
                    if getattr(f.state, "name", "") == "ACTIVE":
                        refs.append(entry)
                        continue
                except Exception:
                    pass

            uploaded = upload_any(self.client, str(p))
            self.data[key] = {
                "name": uploaded.name,
                "uri": uploaded.uri,
                "local": p.name,
                "uploaded_at": int(time.time()),
            }
            refs.append(self.data[key])

        self.save()
        return refs
