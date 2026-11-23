import json
from contextlib import contextmanager
from threading import Event, Thread

import pytest
from websockets.sync.server import serve

from app.communications import CommunicationsClient, CommunicationsError

TOKEN = "token-interno"


@contextmanager
def hub(handler):
    server = serve(handler, "127.0.0.1", 0)
    Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"ws://127.0.0.1:{server.socket.getsockname()[1]}/internal/ws"
    finally:
        server.shutdown()


def client_for(url: str) -> CommunicationsClient:
    return CommunicationsClient(url, TOKEN, "billing-microservice", timeout=0.5)


def answering(**response):
    def handler(socket):
        for message in socket:
            socket.send(json.dumps({"type": "response", "id": json.loads(message)["id"], **response}))

    return handler


def test_request_identifies_the_service_and_returns_the_payload():
    received = {}

    def handler(socket):
        received["headers"] = socket.request.headers
        frame = json.loads(socket.recv())
        received["frame"] = frame
        socket.send(json.dumps({"type": "response", "id": frame["id"], "ok": True, "payload": {"id": 7}}))
        list(socket)

    with hub(handler) as url:
        client = client_for(url)
        assert client.request("user-microservice", "users.get", {"id": 7}) == {"id": 7}
        client.stop()

    assert received["headers"]["X-Internal-Token"] == TOKEN
    assert received["headers"]["X-Service-Name"] == "billing-microservice"
    assert received["frame"]["target"] == "user-microservice"
    assert received["frame"]["action"] == "users.get"


def test_request_reports_the_remote_error():
    with hub(answering(ok=False, error={"code": "NO_ENCONTRADO", "message": "No existe"})) as url:
        client = client_for(url)
        with pytest.raises(CommunicationsError) as error:
            client.request("user-microservice", "users.get", {"id": 7})
        client.stop()
    assert error.value.code == "NO_ENCONTRADO"


def test_request_times_out():
    with hub(lambda socket: list(socket)) as url:
        client = client_for(url)
        with pytest.raises(CommunicationsError) as error:
            client.request("user-microservice", "users.get", {"id": 7})
        client.stop()
    assert error.value.code == "TIEMPO_AGOTADO"


def test_request_fails_when_the_hub_disconnects():
    with hub(lambda socket: socket.recv()) as url:
        client = client_for(url)
        with pytest.raises(CommunicationsError) as error:
            client.request("user-microservice", "users.get", {"id": 7})
        client.stop()
    assert error.value.code == "HUB_NO_DISPONIBLE"


def test_request_fails_when_the_hub_is_down():
    with pytest.raises(CommunicationsError) as error:
        client_for("ws://127.0.0.1:1/internal/ws").request("user-microservice", "users.get", {"id": 7})
    assert error.value.code == "HUB_NO_DISPONIBLE"


def test_emit_sends_the_event():
    received = {}
    done = Event()

    def handler(socket):
        received.update(json.loads(socket.recv()))
        done.set()
        list(socket)

    with hub(handler) as url:
        client = client_for(url)
        client.emit("contacto.creado", {"roles": ["ADMINISTRACION"]}, {"id": 5})
        assert done.wait(2)
        client.stop()
    assert received == {
        "type": "event",
        "name": "contacto.creado",
        "audience": {"roles": ["ADMINISTRACION"]},
        "payload": {"id": 5},
    }
