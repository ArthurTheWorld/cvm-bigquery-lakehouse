"""Dublês de teste compartilhados."""
import base64
import hashlib
import io
import zipfile


def make_zip(files: dict) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def latin1_csv(header: str, rows: list) -> bytes:
    return ("\r\n".join([header] + rows) + "\r\n").encode("latin-1")


class Response:
    def __init__(self, status_code, content=b""):
        self.status_code, self.content = status_code, content


class FakeSession:
    """GET programável: {url: bytes | int(status)}; URL desconhecida -> 404."""

    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, **kwargs):
        self.calls.append(url)
        value = self.routes.get(url, 404)
        return Response(value) if isinstance(value, int) else Response(200, value)


class FakeBlob:
    def __init__(self, store, name):
        self.store, self.name, self.md5_hash = store, name, None

    def exists(self):
        return self.name in self.store

    def reload(self):
        self.md5_hash = base64.b64encode(hashlib.md5(self.store[self.name]).digest()).decode()

    def upload_from_string(self, data, content_type=None):
        self.store[self.name] = data


class FakeBucket:
    def __init__(self):
        self.store = {}

    def blob(self, name):
        return FakeBlob(self.store, name)
