"""
Unified Training Script for QoE Prediction Models
Supports both Cloud Gaming and Video Streaming datasets via config files.

Usage:
    python train.py --config cloud_gaming
    python train.py --config video_streaming
"""

import os
import sys
import argparse
import yaml
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.utils.class_weight import compute_sample_weight
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def load_config(config_name: str) -> dict:
    """Load configuration from YAML file."""
    config_path = os.path.join(PROJECT_ROOT, "ml", "config", f"{config_name}.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_data(config: dict):
    """Load and split data based on config."""
    data_path = os.path.join(PROJECT_ROOT, config["dataset"]["path"])
    df = pd.read_csv(data_path)
    
    target_col = config["dataset"]["target_column"]
    feature_cols = config["dataset"]["features"]
    
    X = df[feature_cols]
    y = df[target_col]
    
    test_size = config["model"]["test_size"]
    random_state = config["model"]["random_state"]
    
    return train_test_split(X, y, test_size=test_size, random_state=random_state)


def get_model(model_config: dict):
    """Create model instance based on config."""
    model_type = model_config["type"]
    params = model_config.get("params", {})
    
    model_classes = {
        "LinearRegression": LinearRegression,
        "ElasticNet": ElasticNet,
        "RandomForestRegressor": RandomForestRegressor,
        "GradientBoostingRegressor": GradientBoostingRegressor
    }
    
    if model_type not in model_classes:
        raise ValueError(f"Unknown model type: {model_type}")
    
    # For linear models we will wrap in pipeline with scaler
    if model_type in ["LinearRegression", "ElasticNet"]:
        return Pipeline([
            ("scaler", StandardScaler()),
            ("model", model_classes[model_type](**params))
        ])
    
    return model_classes[model_type](**params)


def train(config_name: str):
    """Train all models specified in config and save them."""
    print(f"\n{'='*60}")
    print(f"Training models for: {config_name}")
    print(f"{'='*60}\n")
    
    config = load_config(config_name)
    X_train, X_test, y_train, y_test = load_data(config)
    
    print(f"Dataset: {config['dataset']['name']}")
    print(f"Training samples: {len(X_train)}")
    print(f"Test samples: {len(X_test)}")
    print(f"Features: {config['dataset']['features']}")
    print(f"Target: {config['dataset']['target_column']}\n")
    
    # Compute sample weights for balanced training
    sample_weights = compute_sample_weight(
        class_weight="balanced",
        y=y_train.round().astype(int)
    )
    
    # Create output directory
    output_dir = os.path.join(PROJECT_ROOT, config["model"]["output_dir"])
    os.makedirs(output_dir, exist_ok=True)
    
    dataset_name = config["dataset"]["name"]
    trained_models = []
    
    for model_config in config["models_to_train"]:
        model_name = model_config["name"]
        model = get_model(model_config)
        
        print(f"Training {model_name}...", end=" ")
        
        try:
            # Train model with appropriate method
            if isinstance(model, Pipeline):
                model.fit(X_train, y_train, model__sample_weight=sample_weights)
            elif isinstance(model, RandomForestRegressor):
                model.fit(X_train, y_train, sample_weight=sample_weights)
            else:
                model.fit(X_train, y_train)
            
            # Save model with dataset prefix
            model_filename = f"{dataset_name}_{model_name}.pkl"
            model_path = os.path.join(output_dir, model_filename)
            joblib.dump(model, model_path)
            trained_models.append(model_filename)
            print(f"✓ Saved: {model_filename}")
            
        except Exception as e:
            print(f"✗ Failed: {e}")
    
    print(f"\n{'='*60}")
    print(f"Training complete! {len(trained_models)} models saved to {output_dir}")
    print(f"{'='*60}\n")
    
    return trained_models


def main():
    parser = argparse.ArgumentParser(description="Train QoE prediction models")
    parser.add_argument(
        "--config", 
        type=str, 
        required=True,
        choices=["cloud_gaming", "video_streaming"],
        help="Configuration to use (cloud_gaming or video_streaming)"
    )
    args = parser.parse_args()
    
    train(args.config)


if __name__ == "__main__":
    main()
