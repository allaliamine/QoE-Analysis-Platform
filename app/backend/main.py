import os
import asyncio
import json
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from clickhouse_connect import get_client

CLICKHOUSE_HOST = "host.docker.internal" # use clickhouse when container based
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_USER = "qoe_user"
CLICKHOUSE_PASSWORD = "qoe_password"
CLICKHOUSE_DB = "predictions"

app = FastAPI()

ch = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB,
)

def iso_now():
    return datetime.now(timezone.utc).isoformat()

async def sse_stream(table_name: str):
    """
    Streams once per second:
      - delta_counts: counts of qoe_class for new rows since last timestamp
      - points: new (ingestion_timestamp, predicted_qoe) points since last timestamp
    Uses ingestion_timestamp as the incremental cursor.
    """
    # Start slightly in the past to avoid "empty first screen"
    since = datetime.now(timezone.utc)

    while True:
        # Query new rows since last cursor
        # 1) delta counts by qoe_class
        counts_sql = f"""
        SELECT
          qoe_class,
          count() AS c,
          max(ingestion_timestamp) AS max_ts
        FROM {table_name}
        WHERE ingestion_timestamp > %(since)s
        GROUP BY qoe_class
        """

        # 2) points for chart
        points_sql = f"""
        SELECT
          ingestion_timestamp,
          predicted_qoe
        FROM {table_name}
        WHERE ingestion_timestamp > %(since)s
        ORDER BY ingestion_timestamp ASC
        LIMIT 500
        """

        try:
            counts_rows = ch.query(counts_sql, parameters={"since": since}).result_rows
            points_rows = ch.query(points_sql, parameters={"since": since}).result_rows

            delta_counts = {}
            max_ts = None

            for qoe_class, c, row_max_ts in counts_rows:
                delta_counts[str(qoe_class)] = int(c)
                if row_max_ts is not None:
                    # row_max_ts is naive datetime in server timezone usually; treat as UTC-ish in docker
                    # If your ClickHouse uses UTC (common), this is fine.
                    if max_ts is None or row_max_ts > max_ts:
                        max_ts = row_max_ts

            points = []
            for ts, qoe in points_rows:
                # Convert datetime to ISO string
                if isinstance(ts, datetime):
                    ts_iso = ts.replace(tzinfo=timezone.utc).isoformat()
                else:
                    ts_iso = str(ts)
                points.append({"ts": ts_iso, "qoe": float(qoe)})

            # Move cursor forward safely
            if max_ts is not None and max_ts > since.replace(tzinfo=None):
                since = max_ts.replace(tzinfo=timezone.utc)

            payload = {
                "ts": iso_now(),
                "delta_counts": delta_counts,
                "points": points,
            }

        except Exception as e:
            payload = {"ts": iso_now(), "error": str(e), "delta_counts": {}, "points": []}

        # SSE format: "data: <json>\n\n"
        yield f"data: {json.dumps(payload)}\n\n"
        await asyncio.sleep(1)

@app.get("/api/stream/cloud-gaming")
async def stream_cloud_gaming():
    return StreamingResponse(
        sse_stream("predictions.cloud_gaming_predictions"),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )

@app.get("/api/stream/video-streaming")
async def stream_video_streaming():
    return StreamingResponse(
        sse_stream("predictions.video_streaming_predictions"),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )

@app.get("/health")
def health():
    return {"ok": True}
