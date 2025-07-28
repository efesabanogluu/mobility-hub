# FastAPI: Lightweight web framework for building REST APIs
from fastapi import FastAPI, HTTPException
import redis
import json

app = FastAPI(title="Real-time Metrics API")

# Redis bağlantısı
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.get("/health")
def health_check():
    """
    Health check endpoint.
    Pings Redis to confirm connectivity.
    """
    try:
        redis_client.ping()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")


@app.get("/metrics/{entity}/{id}")
def get_metric(entity: str, id: str):
    """
    Retrieve real-time metrics for a specific entity and ID.
    Example: /metrics/driver/driver_123
    """
    key = f"{entity}:{id}"  # Construct Redis key format (e.g., driver:driver_123)
    val = redis_client.get(key)

    if not val:
        # Return 404 if the metric is not found in Redis
        raise HTTPException(status_code=404, detail=f"No metric found for {key}")
    return json.loads(val)


@app.get("/metrics/{entity}")
def list_keys(entity: str):
    """
    List all metric keys for a given entity type.
    Useful for discovering which IDs are available in Redis.
    """
    # Pattern match all keys like 'driver:*', 'vehicle_type:*' etc.
    keys = redis_client.keys(f"{entity}:*")
    return {
        "count": len(keys),
        "keys": keys
    }
