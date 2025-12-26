"""
FastAPI endpoint for Video Streaming QoE Prediction
"""

import os
import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import pandas as pd

app = FastAPI(
    title="Video Streaming QoE Prediction API",
    description="Predict QoE for video streaming sessions",
    version="1.0.0"
)

# Load model at startup
MODEL_PATH = os.environ.get("MODEL_PATH", "/app/models/video_streaming_model.pkl")
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


class VideoStreamingInput(BaseModel):
    throughput: float = Field(..., ge=0, description="Network throughput in bps")
    avg_bitrate: float = Field(..., ge=0, description="Average video bitrate in kbps")
    delay_qos: float = Field(..., ge=0, description="QoS delay in milliseconds")
    jitter: float = Field(..., ge=0, description="Network jitter in milliseconds")
    packet_loss: float = Field(..., ge=0, description="Packet loss count")


class PredictionResponse(BaseModel):
    """Response containing QoE prediction"""
    qoe_score: float = Field(..., description="Predicted QoE score (1-5)")
    qoe_class: str = Field(..., description="QoE quality class")


def qoe_to_class(score: float) -> str:
    """Convert QoE score to quality class"""
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


@app.get("/health")
async def health_check():
    return {"status": "healthy", "model_loaded": model is not None}


@app.post("/predict", response_model=PredictionResponse)
async def predict_qoe(input_data: VideoStreamingInput):
    """
    Predict QoE score for a video streaming session.
    Returns the predicted QoE score (1-5 scale) and quality class.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        features = np.array([[
            input_data.throughput,
            input_data.avg_bitrate,
            input_data.delay_qos,
            input_data.jitter,
            input_data.packet_loss
        ]])

        FEATURES = ["throughput", "avg_bitrate", "delay_qos", "jitter", "packet_loss"]

        # inside predict_qoe
        features = pd.DataFrame([{
            "throughput": input_data.throughput,
            "avg_bitrate": input_data.avg_bitrate,
            "delay_qos": input_data.delay_qos,
            "jitter": input_data.jitter,
            "packet_loss": input_data.packet_loss,
        }], columns=FEATURES)
        
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
        "service": "Video Streaming QoE Prediction API",
        "version": "1.0.0",
        "endpoints": {
            "/predict": "POST - Predict QoE score",
            "/health": "GET - Health check",
            "/docs": "GET - API documentation"
        }
    }
