"""
FastAPI endpoint for Cloud Gaming QoE Prediction
"""

import os
import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(
    title="Cloud Gaming QoE Prediction API",
    description="Predict Quality of Experience (QoE) score for cloud gaming sessions",
    version="1.0.0"
)

# Load model at startup
MODEL_PATH = os.environ.get("MODEL_PATH", "/app/models/cloud_gaming_model.pkl")
model = None


@app.on_event("startup")
async def load_model():
    global model
    try:
        model = joblib.load(MODEL_PATH)
        print(f"Model loaded successfully from {MODEL_PATH}")
    except Exception as e:
        print(f"Error loading model: {e}")
        raise RuntimeError(f"Failed to load model: {e}")


class CloudGamingInput(BaseModel):
    CPU_usage: float = Field(..., ge=0, le=100, description="CPU usage percentage (0-100)")
    GPU_usage: float = Field(..., ge=0, le=100, description="GPU usage percentage (0-100)")
    Bandwidth_MBps: float = Field(..., ge=0, description="Bandwidth in MBps")
    Latency_ms: float = Field(..., ge=0, description="Latency in milliseconds")
    FrameRate_fps: float = Field(..., ge=0, description="Frame rate in FPS")
    Jitter_ms: float = Field(..., ge=0, description="Jitter in milliseconds")


class PredictionResponse(BaseModel):
    """Response containing QoE prediction"""
    qoe_score: float = Field(..., description="Predicted QoE score (1-5)")
    qoe_class: str = Field(..., description="QoE quality class")


def qoe_to_class(score: float) -> str:
    """Convert QoE score to quality class."""
    if score < 2:
        return "Poor"
    elif score < 3:
        return "Fair"
    elif score < 4:
        return "Good"
    else:
        return "Excellent"


@app.get("/health")
async def health_check():
    return {"status": "healthy", "model_loaded": model is not None}


@app.post("/predict", response_model=PredictionResponse)
async def predict_qoe(input_data: CloudGamingInput):
    """
    Predict QoE score for a cloud gaming session.
    Returns the predicted QoE score (1-5 scale) and quality class.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        features = np.array([[
            input_data.CPU_usage,
            input_data.GPU_usage,
            input_data.Bandwidth_MBps,
            input_data.Latency_ms,
            input_data.FrameRate_fps,
            input_data.Jitter_ms
        ]])
        
        prediction = model.predict(features)[0]
        
        return PredictionResponse(
            qoe_score=round(prediction, 2),
            qoe_class=qoe_to_class(prediction)
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "service": "Cloud Gaming QoE Prediction API",
        "version": "1.0.0",
        "endpoints": {
            "/predict": "POST - Predict QoE score",
            "/health": "GET - Health check",
            "/docs": "GET - API documentation"
        }
    }
