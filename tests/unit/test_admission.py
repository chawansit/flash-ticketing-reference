from fastapi.testclient import TestClient

from ticketing.api import app, settings


def test_full_admission_rejects_before_database_or_auth_dependencies():
    app.state.reserve_inflight = settings.reserve_concurrency
    try:
        response = TestClient(app).post("/v1/holds", json={})
        assert response.status_code == 503
        assert response.json()["code"] == "ADMISSION_FULL"
        assert "Server-Timing" in response.headers
        assert app.state.reserve_inflight == settings.reserve_concurrency
    finally:
        app.state.reserve_inflight = 0
