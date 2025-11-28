from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
import requests
from fastapi import FastAPI
import json
import asyncio
from pathlib import Path

app = FastAPI()

KSQL_URL = "http://ksqldb-server:8088"

@app.on_event("startup")
async def startup_event():
    """
    Initialize ksqlDB streams and materialized views
    """
    
    # create order_created stream
    requests.post(f"{KSQL_URL}/ksql", json={
        "ksql": "CREATE STREAM IF NOT EXISTS order_created_stream (order_id VARCHAR, amount DOUBLE) WITH (KAFKA_TOPIC='order_created', VALUE_FORMAT='JSON');" 
    })

    # create Materialized view from order stream
    requests.post(f"{KSQL_URL}/ksql", json={
        "ksql": "CREATE TABLE IF NOT EXISTS order_stats AS SELECT 'global' AS id, COUNT(*) AS total_orders, SUM(amount) AS total_amount FROM order_created_stream GROUP BY 'global' EMIT CHANGES;"
    })

def order_stats():
    """
    Query ksqlDB for aggregated order stats
    """
    res = requests.post(f"{KSQL_URL}/query", json={
        "ksql": "SELECT total_orders, total_amount from order_stats WHERE id='global';"
    })
    data = res.json()
    if len(data) > 1:
        columns = data[1]["row"]["columns"]
        stats = {"total_orders": columns[0], "total_amount": round(columns[1], 2)}
        return stats

@app.get("/stats")
async def get_stats():
    """
    REST API endpoint for order statistics
    """
    return order_stats()


@app.get("/stats/stream")
async def stream_stats():
    """
    SSE endpoint for real time streaming of order stats
    """
    async def event_generator():
        while True:
            stats = order_stats()
            yield f"data: {json.dumps(stats)}\n\n"

            await asyncio.sleep(5)
            
    return StreamingResponse(event_generator(), media_type="text/event-stream")


# Serve static files (frontend)
frontend_path = Path(__file__).parent.parent / "frontend"
app.mount("/", StaticFiles(directory=str(frontend_path), html=True), name="static")


