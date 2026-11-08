import hashlib
import hmac
import json
import time
from unittest.mock import Mock
from uuid import uuid4

from fastapi.testclient import TestClient

from ticketing.api import app, service, settings


def test_openapi_and_auth():
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    assert "HTTPBearer" in schema["components"]["securitySchemes"]
    assert "/v1/holds" in schema["paths"]
    assert "202" in schema["paths"]["/v1/holds"]["post"]["responses"]
    assert "/v1/reservation-commands/{event_id}/{command_id}" in schema["paths"]
    assert client.get(f"/v1/orders/{uuid4()}").status_code in {401, 403}


def test_callback_signature_and_duplicate_payload_forwarding():
    store = Mock()
    store.callback.return_value = {"status": "duplicate"}
    app.dependency_overrides[service] = lambda: store
    try:
        client = TestClient(app)
        body = {
            "callback_id": str(uuid4()),
            "payment_id": str(uuid4()),
            "order_id": str(uuid4()),
            "amount": 100,
            "currency": "THB",
            "outcome": "SUCCEEDED",
        }
        raw = json.dumps(body).encode()
        timestamp = str(int(time.time()))
        signature = hmac.new(
            settings.webhook_secret.encode(), timestamp.encode() + b"." + raw, hashlib.sha256
        ).hexdigest()
        headers = {
            "X-Payment-Timestamp": timestamp,
            "X-Payment-Signature": signature,
            "Content-Type": "application/json",
        }
        assert client.post("/v1/webhooks/payments", content=raw, headers=headers).status_code == 200
        store.callback.assert_called_once_with(body)
        headers["X-Payment-Signature"] = "invalid"
        assert client.post("/v1/webhooks/payments", content=raw, headers=headers).status_code == 401
        store.callback.assert_called_once()
    finally:
        app.dependency_overrides.clear()
