-- Cloud Gaming QoE Predictions Table
CREATE TABLE IF NOT EXISTS predictions.cloud_gaming_predictions (
    id UUID DEFAULT generateUUIDv4(),
    CPU_usage Int32,
    GPU_usage Int32,
    Bandwidth_MBps Float64,
    Latency_ms Int32,
    FrameRate_fps Int32,
    Jitter_ms Int32,
    predicted_qoe Float64,
    qoe_class String,
    prediction_timestamp DateTime DEFAULT now(),
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ingestion_timestamp, id)
PARTITION BY toYYYYMM(ingestion_timestamp);

-- Video Streaming QoE Predictions Table
CREATE TABLE IF NOT EXISTS predictions.video_streaming_predictions (
    id UUID DEFAULT generateUUIDv4(),
    throughput Float64,
    avg_bitrate Float64,
    delay_qos Float64,
    jitter Float64,
    packet_loss Float64,
    predicted_qoe Float64,
    qoe_class String,
    prediction_timestamp DateTime DEFAULT now(),
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ingestion_timestamp, id)
PARTITION BY toYYYYMM(ingestion_timestamp);
