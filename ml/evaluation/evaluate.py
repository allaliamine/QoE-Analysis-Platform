"""
Unified Evaluation Script for QoE Prediction Models
Supports both Cloud Gaming and Video Streaming datasets via config files.
Evaluates all trained models, selects the best one, and deletes the others.

Usage:
    python evaluate.py --config cloud_gaming
    python evaluate.py --config video_streaming
"""

import os
import sys
import argparse
import yaml
import pandas as pd
import numpy as np
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

# Get project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_config(config_name: str) -> dict:
    """Load configuration from YAML file."""
    config_path = os.path.join(PROJECT_ROOT, "ml", "config", f"{config_name}.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def score_to_class(score, dataset_name):
    """Convert score to quality class based on dataset type."""
    if dataset_name == "cloud_gaming":
        # QoE score classification
        if score < 2:
            return "Poor"
        elif score < 3:
            return "Fair"
        elif score < 4:
            return "Good"
        else:
            return "Excellent"
    else:
        # MOS score classification (ITU-T P.800 scale)
        if score < 2:
            return "Bad"
        elif score < 3:
            return "Poor"
        elif score < 4:
            return "Fair"
        elif score < 4.5:
            return "Good"
        else:
            return "Excellent"


def load_test_data(config: dict):
    """Load and prepare test data based on config."""
    data_path = os.path.join(PROJECT_ROOT, config["dataset"]["path"])
    df = pd.read_csv(data_path)
    
    target_col = config["dataset"]["target_column"]
    feature_cols = config["dataset"]["features"]
    
    X = df[feature_cols]
    y = df[target_col]
    
    test_size = config["model"]["test_size"]
    random_state = config["model"]["random_state"]
    
    _, X_test, _, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
    return X_test, y_test


def evaluate_model(model, X_test, y_test, model_name, dataset_name):
    """Evaluate a single model and return metrics."""
    predictions = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    r2 = r2_score(y_test, predictions)
    
    # Classification accuracy
    true_classes = y_test.apply(lambda x: score_to_class(x, dataset_name))
    pred_classes = pd.Series(predictions).apply(lambda x: score_to_class(x, dataset_name))
    class_accuracy = (true_classes.values == pred_classes.values).mean()
    
    return {
        "Model": model_name,
        "MAE": mae,
        "RMSE": rmse,
        "R2": r2,
        "Class_Accuracy": class_accuracy
    }


def evaluate(config_name: str):
    """Evaluate all models for a dataset and keep only the best one."""
    print(f"\n{'='*70}")
    print(f"Evaluating {config_name.replace('_', ' ').title()} QoE Models")
    print(f"{'='*70}\n")
    
    config = load_config(config_name)
    dataset_name = config["dataset"]["name"]
    model_dir = os.path.join(PROJECT_ROOT, config["model"]["output_dir"])
    final_model_name = config["model"]["output_name"]
    
    X_test, y_test = load_test_data(config)
    print(f"Dataset: {dataset_name}")
    print(f"Test samples: {len(X_test)}\n")
    
    # Find all models for this dataset
    model_files = [f for f in os.listdir(model_dir) 
                   if f.startswith(f"{dataset_name}_") and f.endswith(".pkl")]
    
    if not model_files:
        print("No trained models found! Please run training first.")
        return None
    
    print(f"Found {len(model_files)} models to evaluate:\n")
    
    results = []
    for model_file in model_files:
        model_path = os.path.join(model_dir, model_file)
        try:
            model = joblib.load(model_path)
            metrics = evaluate_model(model, X_test, y_test, model_file, dataset_name)
            results.append(metrics)
        except Exception as e:
            print(f"Error loading {model_file}: {e}")
    
    if not results:
        print("No models could be evaluated!")
        return None
    
    # Create results DataFrame
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values("RMSE")
    
    print("Model Evaluation Results:")
    print("-" * 70)
    print(results_df.to_string(index=False))
    print("-" * 70)
    
    # Find best model (lowest RMSE)
    best_model_name = results_df.iloc[0]["Model"]
    best_rmse = results_df.iloc[0]["RMSE"]
    best_r2 = results_df.iloc[0]["R2"]
    best_accuracy = results_df.iloc[0]["Class_Accuracy"]
    
    print(f"\n🏆 Best Model: {best_model_name}")
    print(f"   RMSE: {best_rmse:.4f}")
    print(f"   R2 Score: {best_r2:.4f}")
    print(f"   Class Accuracy: {best_accuracy:.2%}")
    
    # Save best model with final name
    best_model_path = os.path.join(model_dir, best_model_name)
    final_model_path = os.path.join(model_dir, final_model_name)
    
    best_model = joblib.load(best_model_path)
    joblib.dump(best_model, final_model_path)
    print(f"\n✓ Saved best model as: {final_model_name}")
    
    # Delete all other models (including the original best model file)
    print("\nCleaning up other models...")
    for model_file in model_files:
        model_path = os.path.join(model_dir, model_file)
        try:
            os.remove(model_path)
            print(f"   ✗ Deleted: {model_file}")
        except Exception as e:
            print(f"   Error deleting {model_file}: {e}")
    
    print(f"\n{'='*70}")
    print(f"Evaluation complete!")
    print(f"Final model saved: {final_model_path}")
    print(f"{'='*70}\n")
    
    return final_model_path


def main():
    parser = argparse.ArgumentParser(description="Evaluate QoE prediction models")
    parser.add_argument(
        "--config", 
        type=str, 
        required=True,
        choices=["cloud_gaming", "video_streaming"],
        help="Configuration to use (cloud_gaming or video_streaming)"
    )
    args = parser.parse_args()
    
    evaluate(args.config)


if __name__ == "__main__":
    main()
