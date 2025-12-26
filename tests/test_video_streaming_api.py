from __future__ import annotations

from fastapi.testclient import TestClient


class _DummyModel:
    def __init__(self, value: float):
        self._value = value

    def predict(self, X):
        return [self._value]


def test_qoe_to_class_boundaries():
    from api.video_streaming import main as vs

    assert vs.qoe_to_class(1.99) == "Bad"
    assert vs.qoe_to_class(2.0) == "Poor"
    assert vs.qoe_to_class(2.99) == "Poor"
    assert vs.qoe_to_class(3.0) == "Fair"
    assert vs.qoe_to_class(3.99) == "Fair"
    assert vs.qoe_to_class(4.0) == "Good"
    assert vs.qoe_to_class(4.49) == "Good"
    assert vs.qoe_to_class(4.5) == "Excellent"


def test_health_and_predict_success(monkeypatch):
    from api.video_streaming import main as vs

    monkeypatch.setattr(vs.joblib, "load", lambda _: _DummyModel(4.6))

    with TestClient(vs.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["model_loaded"] is True

        payload = {
            "throughput": 2000,
            "avg_bitrate": 1500,
            "delay_qos": 50,
            "jitter": 5,
            "packet_loss": 0,
        }
        r = client.post("/predict", json=payload)
        assert r.status_code == 200
        body = r.json()
        assert body["qoe_score"] == 4.6
        assert body["qoe_class"] == "Excellent"
