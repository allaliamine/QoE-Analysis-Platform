from __future__ import annotations

import pytest


def test_load_config_cloud_gaming_has_expected_keys():
    from ml.training.train import load_config

    cfg = load_config("cloud_gaming")
    assert "dataset" in cfg
    assert "model" in cfg
    assert "models_to_train" in cfg

    assert cfg["dataset"]["name"] == "cloud_gaming"
    assert isinstance(cfg["dataset"]["features"], list)
    assert len(cfg["dataset"]["features"]) > 0


@pytest.mark.parametrize(
    "model_type",
    ["LinearRegression", "ElasticNet", "RandomForestRegressor", "GradientBoostingRegressor"],
)
def test_get_model_supported_types(model_type):
    from ml.training.train import get_model

    model = get_model({"type": model_type, "params": {}})
    assert model is not None


def test_get_model_unknown_type_raises():
    from ml.training.train import get_model

    with pytest.raises(ValueError):
        get_model({"type": "NotARealModel", "params": {}})
