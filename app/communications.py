import json
import logging
import os
from queue import Empty, Queue
from threading import Event, Lock, Thread
from uuid import uuid4

from websockets.exceptions import WebSocketException
from websockets.sync.client import ClientConnection, connect

logger = logging.getLogger("gr.communications")

RECONNECT_SECONDS = 3.0
MAX_MESSAGE_BYTES = 1_048_576


class CommunicationsError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class CommunicationsClient:
    def __init__(self, url: str, token: str, service_name: str, timeout: float = 3.0) -> None:
        self._url = url
        self._token = token
        self._service_name = service_name
        self._timeout = timeout
        self._lock = Lock()
        self._socket: ClientConnection | None = None
        self._pending: dict[str, tuple[ClientConnection, Queue]] = {}
        self._stopped = Event()
        self._supervisor: Thread | None = None

    def start(self) -> None:
        if not self._token:
            logger.warning("Falta INTERNAL_SERVICE_TOKEN: no se abre la conexión con el hub")
            return
        self._stopped.clear()
        self._supervisor = Thread(target=self._supervise, daemon=True)
        self._supervisor.start()

    def stop(self) -> None:
        self._stopped.set()
        with self._lock:
            socket, self._socket = self._socket, None
        if socket is not None:
            socket.close()
        if self._supervisor is not None:
            self._supervisor.join(timeout=self._timeout + RECONNECT_SECONDS)
            self._supervisor = None

    def request(self, target: str, action: str, payload: dict) -> dict:
        socket = self._connection()
        request_id = str(uuid4())
        answers: Queue = Queue(maxsize=1)
        self._pending[request_id] = (socket, answers)
        try:
            self._send(
                socket,
                {"type": "request", "id": request_id, "target": target, "action": action, "payload": payload},
            )
            if self._socket is not socket:
                raise CommunicationsError("HUB_NO_DISPONIBLE", "Se perdió la conexión con el hub")
            frame = answers.get(timeout=self._timeout)
        except Empty as exc:
            raise CommunicationsError("TIEMPO_AGOTADO", f"{target} no respondió a tiempo") from exc
        finally:
            self._pending.pop(request_id, None)

        if frame.get("ok"):
            return frame.get("payload") or {}
        error = frame.get("error") or {}
        raise CommunicationsError(error.get("code", "ERROR_INTERNO"), error.get("message", "Respuesta inválida"))

    def emit(self, name: str, audience: dict, payload: dict) -> None:
        self._send(
            self._connection(),
            {"type": "event", "name": name, "audience": audience, "payload": payload},
        )

    def _supervise(self) -> None:
        while not self._stopped.is_set():
            try:
                self._connection()
            except CommunicationsError as exc:
                logger.warning("Hub de comunicaciones no disponible, se reintenta: %s", exc)
            self._stopped.wait(RECONNECT_SECONDS)

    def _connection(self) -> ClientConnection:
        with self._lock:
            if self._socket is not None:
                return self._socket
            if not self._token:
                raise CommunicationsError("HUB_NO_CONFIGURADO", "Falta INTERNAL_SERVICE_TOKEN")
            try:
                socket = connect(
                    self._url,
                    additional_headers={"X-Internal-Token": self._token, "X-Service-Name": self._service_name},
                    open_timeout=self._timeout,
                    close_timeout=1,
                    max_size=MAX_MESSAGE_BYTES,
                    proxy=None,
                )
            except (OSError, WebSocketException) as exc:
                raise CommunicationsError("HUB_NO_DISPONIBLE", str(exc)) from exc
            self._socket = socket
            Thread(target=self._read, args=(socket,), daemon=True).start()
            return socket

    def _send(self, socket: ClientConnection, frame: dict) -> None:
        try:
            socket.send(json.dumps(frame))
        except (OSError, WebSocketException) as exc:
            self._release(socket)
            raise CommunicationsError("HUB_NO_DISPONIBLE", str(exc)) from exc

    def _read(self, socket: ClientConnection) -> None:
        try:
            for message in socket:
                self._receive(message)
        except (OSError, WebSocketException) as exc:
            logger.warning("Conexión con el hub interrumpida: %s", exc)
        finally:
            self._release(socket)

    def _receive(self, message: str | bytes) -> None:
        try:
            frame = json.loads(message)
        except ValueError:
            return
        if not isinstance(frame, dict) or frame.get("type") != "response":
            return
        pending = self._pending.get(str(frame.get("id")))
        if pending is not None and pending[1].empty():
            pending[1].put_nowait(frame)

    def _release(self, socket: ClientConnection) -> None:
        with self._lock:
            if self._socket is socket:
                self._socket = None
        failure = {"ok": False, "error": {"code": "HUB_NO_DISPONIBLE", "message": "Se perdió la conexión con el hub"}}
        for origin, answers in list(self._pending.values()):
            if origin is socket and answers.empty():
                answers.put_nowait(failure)


def communications_client(service_name: str) -> CommunicationsClient:
    return CommunicationsClient(
        url=os.getenv("COMMUNICATIONS_WS_URL", "ws://localhost:4600/internal/ws"),
        token=os.getenv("INTERNAL_SERVICE_TOKEN", ""),
        service_name=service_name,
        timeout=float(os.getenv("COMMUNICATIONS_TIMEOUT_SECONDS", "3")),
    )
