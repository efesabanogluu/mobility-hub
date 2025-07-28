import json
from pyflink.common import Row
from pyflink.datastream.functions import ReduceFunction
from redis import Redis
from datetime import datetime

# === Redis bağlantısı ===
redis_client = Redis(host="redis", port=6379, db=0)

# === JSON'dan Row parse ===
def parse_enriched_json(event_json, key_name):
    event = json.loads(event_json)
    print(Row(
        event.get(key_name, "unknown"),
        float(event.get("duration_min", 0) or 0),
        float(event.get("distance_km", 0) or 0),
        float(event.get("price", 0) or 0),
        1.0,
        event.get("start_time", "unknown")

    ) )
    return Row(
        event.get(key_name, "unknown"),
        float(event.get("duration_min", 0) or 0),
        float(event.get("distance_km", 0) or 0),
        float(event.get("price", 0) or 0),
        1.0,
        event.get("start_time", "unknown")
    )

# === Ortak Reduce Function ===

class AggregateMetrics(ReduceFunction):
    def reduce(self, v1, v2):
        def earlier(t1, t2):
            try:
                return t1 if datetime.fromisoformat(t1) < datetime.fromisoformat(t2) else t2
            except:
                return t1 or t2

        return Row(
            v1[0],  # key
            v1[1] + v2[1],  # duration
            v1[2] + v2[2],  # distance
            v1[3] + v2[3],  # price
            v1[4] + v2[4],  # count
            earlier(v1[5], v2[5])  # earliest start_time
        )

# === Formatlama Fonksiyonları ===

def format_driver(row):
    return json.dumps({
        "driver_id": row[0],
        "trip_count": int(row[4]),
        "avg_duration": row[1] / row[4] if row[4] else 0,
        "total_distance": row[2],
        "total_revenue": row[3],
        "start_time": row[5]

    })

def format_passenger(row):
    return json.dumps({
        "passenger_id": row[0],
        "trip_count": int(row[4]),
        "avg_duration": row[1] / row[4] if row[4] else 0,
        "total_distance": row[2],
        "total_revenue": row[3],
        "start_time": row[5]

    })

def format_vehicle_type(row):
    return json.dumps({
        "vehicle_type": row[0],
        "trip_count": int(row[4]),
        "avg_duration": row[1] / row[4] if row[4] else 0,
        "total_distance": row[2],
        "total_revenue": row[3],
        "start_time": row[5]
    })

def format_h3(row):
    return json.dumps({
        "h3_region": row[0],
        "trip_count": int(row[4]),
        "avg_duration": row[1] / row[4] if row[4] else 0,
        "total_distance": row[2],
        "total_revenue": row[3],
        "start_time": row[5]

    })

def write_json_to_file(path):
    """
    returns a lambda that appends to a file line-by-line
    """
    import os
    os.makedirs(os.path.dirname(path), exist_ok=True)
    def inner(value):
        print("📁 writing to file:", path, "val:", value)
        with open(path, "a", encoding="utf-8") as f:
            f.write(value + "\n")
    return inner

def safe_float(val):
    try:
        return float(val)
    except:
        return 0.0