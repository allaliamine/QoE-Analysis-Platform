-- ========================================
-- RAW DATA LAYER
-- ========================================

-- Cloud Gaming KPI Raw Data Table
CREATE TABLE IF NOT EXISTS raw_data.cloud_gaming_raw (
    id UUID DEFAULT generateUUIDv4(),
    CPU_usage Int32,
    GPU_usage Int32,
    Bandwidth_MBps Float64,
    Latency_ms Int32,
    FrameRate_fps Int32,
    Jitter_ms Int32,
    processing_time DateTime DEFAULT now(),
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ingestion_timestamp, id)
PARTITION BY toYYYYMM(ingestion_timestamp);

-- Video Streaming KPI Raw Data Table
CREATE TABLE IF NOT EXISTS raw_data.video_streaming_raw (
    id UUID DEFAULT generateUUIDv4(),
    throughput Float64,
    avg_bitrate Float64,
    delay_qos Float64,
    jitter Float64,
    packet_loss Float64,
    processing_time DateTime DEFAULT now(),
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ingestion_timestamp, id)
PARTITION BY toYYYYMM(ingestion_timestamp);
