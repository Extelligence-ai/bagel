"""Opt-in, same-origin browser setup on a loopback-published Bagel server."""

from __future__ import annotations

import asyncio
import json
import pathlib
import secrets
import threading
import time
from urllib.parse import urlsplit

from filelock import FileLock, Timeout
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response

from settings import settings
from src.sink.publish import identity, pairing, require_fleet

CSRF = secrets.token_hex(32)
MAX_BODY = 4096
HEADERS = {
    "Cache-Control": "no-store",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; connect-src 'self'; "
        "style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
    ),
}


class BrowserPairing:
    """Own one enrollment worker; persist identity before making outbound requests."""

    def __init__(self, directory: pathlib.Path, enroll_url: str) -> None:
        """Use the installer-configured endpoint, never a browser-selected destination."""
        self.directory = directory
        self.enroll_url = enroll_url.rstrip("/")
        self.mutex = threading.Lock()
        self.state: dict = {"status": "ready"}
        self.worker: threading.Thread | None = None

    def status(self) -> dict:
        """Expose only public state, never the pairing secret or private key."""
        with self.mutex:
            if identity.is_enrolled(self.directory) and self.state["status"] != "connected":
                return {"status": "enrolled"}
            return dict(self.state)

    def start(self, url: str | None = None, code: str | None = None) -> dict:
        """Start or resume the same durable attempt without replacing active work."""
        require_fleet()
        with self.mutex:
            if identity.is_enrolled(self.directory):
                raise ValueError("This controller is already connected.")
            if self.worker and self.worker.is_alive():
                raise ValueError("A connection is already in progress. Return to its fleet page.")
            self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.directory.chmod(0o700)
            lock = FileLock(str(self.directory / ".pairing.lock"), thread_local=False)
            lock.acquire(timeout=0)
            try:
                if url is None and code is None:
                    pending = json.loads((self.directory / "pairing-pending.json").read_text())
                    url = pending["url"]
                else:
                    pending = None
                if (
                    not self.enroll_url
                    or not isinstance(url, str)
                    or url.rstrip("/") != self.enroll_url
                ):
                    raise ValueError("This controller is not configured for that fleet service.")
                if pending is None:
                    pending = pairing.prepare(self.directory, self.enroll_url, code)
                self.state = {
                    "status": "connecting",
                    "public_key_sha256": pending["public_key_sha256"],
                    "controller_id": pending["request"]["discovery"]["controller_id"],
                }
                self.worker = threading.Thread(target=self._run, args=(pending, lock), daemon=True)
                self.worker.start()
                return dict(self.state)
            except Exception:
                lock.release()
                raise

    def _set(self, **values: object) -> None:
        with self.mutex:
            self.state.update(values)

    def _run(self, pending: dict, lock: FileLock) -> None:
        try:
            deadline = time.monotonic() + 900
            while time.monotonic() < deadline:
                try:
                    result = pairing.exchange(pending)
                except ConnectionError:
                    self._set(status="retrying")
                    time.sleep(3)
                    continue
                if result.get("status") == "issued":
                    pairing.install(self.directory, pending, result)
                    active = False
                    # A resumed request may finish while the MCP server is still booting.
                    for _ in range(10):
                        if asyncio.run(pairing.activate()):
                            active = True
                            break
                        time.sleep(3)
                    self._set(status="connected", streams_reloaded=active)
                    return
                if result.get("status") != "awaiting_confirmation":
                    raise ValueError("Unexpected fleet response")
                self._set(status="awaiting_confirmation")
                time.sleep(3)
            self._set(status="paused")
        except Exception:
            # Server bodies and exception strings may contain credentials.
            self._set(status="error")
        finally:
            lock.release()


def _allowed(request: Request, *, mutation: bool = False) -> bool:
    host = request.headers.get("host", "")
    try:
        hostname = urlsplit("http://" + host).hostname
    except ValueError:
        return False
    if hostname not in {"localhost", "127.0.0.1", "::1"}:
        return False
    if mutation:
        return (
            request.headers.get("origin") == f"{request.url.scheme}://{host}"
            and secrets.compare_digest(request.headers.get("x-bagel-csrf", ""), CSRF)
            and request.headers.get("content-type", "").split(";")[0] == "application/json"
        )
    return True


def register(server: object) -> BrowserPairing | None:  # noqa: C901 -- small, scoped route handlers
    """Register only when explicitly enabled; publish this server on loopback only."""
    if not settings.FLEET_PAIRING_UI_ENABLED:
        return None
    manager = BrowserPairing(
        pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY), settings.FLEET_ENROLL_URL or ""
    )

    @server.custom_route("/fleet/connect", methods=["GET"])
    async def page(request: Request) -> Response:
        if not _allowed(request):
            return Response(status_code=403, headers=HEADERS)
        source = pathlib.Path(__file__).with_name("pairing_ui.html").read_text()
        return HTMLResponse(source.replace("__CSRF__", CSRF), headers=HEADERS)

    @server.custom_route("/fleet/connect/app.js", methods=["GET"])
    async def script(request: Request) -> Response:
        if not _allowed(request):
            return Response(status_code=403, headers=HEADERS)
        return Response(
            pathlib.Path(__file__).with_name("pairing_ui.js").read_text(),
            media_type="application/javascript",
            headers=HEADERS,
        )

    @server.custom_route("/fleet/connect/status", methods=["GET"])
    async def status(request: Request) -> Response:
        if not _allowed(request):
            return Response(status_code=403, headers=HEADERS)
        return JSONResponse(manager.status(), headers=HEADERS)

    @server.custom_route("/fleet/connect/start", methods=["POST"])
    async def start(request: Request) -> Response:
        if not _allowed(request, mutation=True):
            return Response(status_code=403, headers=HEADERS)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > MAX_BODY:
                return Response(status_code=413, headers=HEADERS)
        try:
            data = json.loads(body)
            if not isinstance(data, dict) or set(data) not in ({"url", "code"}, {"resume"}):
                raise ValueError("Invalid connection request.")
            if "resume" in data:
                result = await asyncio.to_thread(manager.start)
            elif isinstance(data["url"], str) and isinstance(data["code"], str):
                result = await asyncio.to_thread(manager.start, data["url"], data["code"])
            else:
                raise ValueError("Invalid connection request.")
            return JSONResponse(result, headers=HEADERS)
        except (ValueError, OSError, Timeout, RuntimeError):
            return JSONResponse(
                {
                    "error": (
                        "Unable to start. Check that this Bagel is configured for your fleet "
                        "service and no other connection is in progress."
                    )
                },
                status_code=409,
                headers=HEADERS,
            )

    if (manager.directory / "pairing-pending.json").exists() and not identity.is_enrolled(
        manager.directory
    ):
        try:
            manager.start()
        except (ValueError, OSError, Timeout, RuntimeError):
            pass  # Setup remains available for an explicit retry.
    return manager
