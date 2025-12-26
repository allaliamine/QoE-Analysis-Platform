from __future__ import annotations

from fastapi.testclient import TestClient


class _DummyModel:
    def __init__(self, value: float):
        self._value = value

    def predict(self, X):  # sklearn-like API
        return [self._value]


def test_qoe_to_class_boundaries():
    from api.cloud_gaming import main as cg

    assert cg.qoe_to_class(1.99) == "Poor"
    assert cg.qoe_to_class(2.0) == "Fair"
    assert cg.qoe_to_class(2.99) == "Fair"
    assert cg.qoe_to_class(3.0) == "Good"
    assert cg.qoe_to_class(3.99) == "Good"
    assert cg.qoe_to_class(4.0) == "Excellent"


def test_health_and_predict_success(monkeypatch):
    from api.cloud_gaming import main as cg

    monkeypatch.setattr(cg.joblib, "load", lambda _: _DummyModel(3.7))

    with TestClient(cg.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["model_loaded"] is True

        payload = {
            "CPU_usage": 50,
            "GPU_usage": 40,
            "Bandwidth_MBps": 25.5,
            "Latency_ms": 35,
            "FrameRate_fps": 60,
            "Jitter_ms": 3,
        }
        r = client.post("/predict", json=payload)
        assert r.status_code == 200
        body = r.json()
        assert body["qoe_score"] == 3.7
        assert body["qoe_class"] == "Good"


def test_predict_validation_error(monkeypatch):
    from api.cloud_gaming import main as cg

    monkeypatch.setattr(cg.joblib, "load", lambda _: _DummyModel(3.0))

    with TestClient(cg.app) as client:
        payload = {
            "CPU_usage": -1,  # invalid
            "GPU_usage": 40,
            "Bandwidth_MBps": 25.5,
            "Latency_ms": 35,
            "FrameRate_fps": 60,
            "Jitter_ms": 3,
        }
        r = client.post("/predict", json=payload)
        assert r.status_code == 422
