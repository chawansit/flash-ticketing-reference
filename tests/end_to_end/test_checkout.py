import os
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import jwt
import pytest

from ticketing.config import Settings

pytestmark = pytest.mark.e2e


def test_http_payment_kafka_ticket():
    url = os.getenv("E2E_API_URL")
    if not url:
        pytest.skip("E2E_API_URL not configured")
    subject = "e2e-" + uuid4().hex
    token = jwt.encode(
        {
            "sub": subject,
            "aud": "ticketing",
            "iss": "ticketing",
            "exp": datetime.now(UTC) + timedelta(minutes=5),
        },
        Settings().jwt_secret,
        algorithm="HS256",
    )
    with httpx.Client(base_url=url, headers={"Authorization": "Bearer " + token}, timeout=5) as client:
        events = client.get("/v1/events").json()
        assert events, "Run the seed command before E2E tests"
        event = next((event for event in events if event["title"] == "Bangkok Demo Concert"), None)
        assert event is not None, "Run the demo seed command before E2E tests"
        seats = client.get(f"/v1/events/{event['id']}/seats").json()["seats"]
        available = [s for s in seats if s["status"] == "AVAILABLE"]
        assert available, "Seed event has no free seats"
        held = client.post(
            "/v1/holds",
            headers={"Idempotency-Key": str(uuid4())},
            json={"event_id": event["id"], "seat_ids": [available[-1]["seat_id"]]},
        )
        assert held.status_code == 201, held.text
        order = held.json()["order_id"]
        result = client.post(
            f"/v1/orders/{order}/payments",
            headers={"Idempotency-Key": str(uuid4())},
            json={"duplicates": 3, "delay_seconds": 0},
        )
        assert result.status_code == 202, result.text
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            current = client.get(f"/v1/orders/{order}").json()
            if current["status"] == "FULFILLED":
                assert len(current["tickets"]) == 1
                return
            time.sleep(0.3)
        pytest.fail(f"Fulfillment did not complete: {current}")
