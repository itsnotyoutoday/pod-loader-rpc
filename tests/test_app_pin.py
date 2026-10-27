"""Pinned TLS for the app client: the right certificate connects, a wrong one is refused
INSIDE the handshake, before the token is written. Loopback only (127.0.0.1); skipped when
no `openssl` binary is available to make a throwaway certificate."""
import hashlib
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, "src")
from pod_loader.app import Client, Unreachable
from pod_loader.base_loader import pin_mismatch, pinned_ssl_context

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


check("no pin → no special context", pinned_ssl_context("") is None)
check("pin_mismatch: match is empty, colons and case ignored",
      pin_mismatch(b"x", hashlib.sha256(b"x").hexdigest().upper()) == ""
      and pin_mismatch(b"x", ":".join(hashlib.sha256(b"x").hexdigest()[i:i + 2]
                                      for i in range(0, 64, 2))) == "")
check("pin_mismatch: a mismatch says so", "does not match" in pin_mismatch(b"x", "00" * 32))

if not shutil.which("openssl"):
    print("  (skipped the live handshake: no openssl binary)")
    print(f"\n  {sum(ok)}/{len(ok)} passed")
    sys.exit(0 if all(ok) else 1)

tmp = Path(tempfile.mkdtemp())
subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                "-subj", "/CN=pin-test", "-keyout", str(tmp / "k.pem"),
                "-out", str(tmp / "c.pem")], check=True, capture_output=True)
der = ssl.PEM_cert_to_DER_cert((tmp / "c.pem").read_text())
good = hashlib.sha256(der).hexdigest()

server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
server_ctx.load_cert_chain(tmp / "c.pem", tmp / "k.pem")
lsock = socket.socket()
lsock.bind(("127.0.0.1", 0))
lsock.listen(8)
port = lsock.getsockname()[1]
received = []


def serve():
    while True:
        try:
            raw, _ = lsock.accept()
        except OSError:
            return
        try:
            with server_ctx.wrap_socket(raw, server_side=True) as s:
                data = s.recv(65536)
                received.append(data)
                body = b'{"job_id": "j", "state": "ready"}'
                s.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                          b"Content-Length: " + str(len(body)).encode() +
                          b"\r\nConnection: close\r\n\r\n" + body)
        except (ssl.SSLError, OSError):
            received.append(b"")


threading.Thread(target=serve, daemon=True).start()
url = f"https://127.0.0.1:{port}"

with Client(url, "secret-token", fingerprint=good, timeout=5) as pc:
    check("the pinned certificate connects", pc.get("j")["state"] == "ready")
check("…and the token was sent", any(b"secret-token" in r for r in received))

received.clear()
try:
    with Client(url, "secret-token", fingerprint="00" * 32, timeout=5) as pc:
        pc.get("j")
    check("a wrong pin is refused", False)
except Unreachable as e:
    check("a wrong pin is refused as Unreachable, by the pin", "does not match" in str(e))
check("the token never left on the refused connection",
      not any(b"secret-token" in r for r in received))

try:
    with Client(url, "secret-token", timeout=5) as pc:
        pc.get("j")
    check("without a pin a self-signed endpoint fails CA verification", False)
except Unreachable:
    check("without a pin a self-signed endpoint fails CA verification", True)

lsock.close()
print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
