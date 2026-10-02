"""Kafka dashboard backend (FastAPI) with optional Mock Mode."""
import asyncio
import json
import os
import random
import time
import uuid

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# SET THIS TO True TO RUN WITHOUT KAFKA INSTALLED
MOCK_MODE = True

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
BASE = {"bootstrap.servers": BOOTSTRAP}

app = FastAPI(title="Kafka Dashboard")

if not MOCK_MODE:
    from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient(BASE)
    producer = Producer(BASE)
    meta_consumer = Consumer({**BASE, "group.id": "dashboard-meta", "enable.auto.commit": False})

# Mock in-memory state
mock_topics = {
    "metrics": {"partitions": 3, "messages": 150},
    "orders": {"partitions": 2, "messages": 45},
}


@app.get("/api/overview")
def overview():
    if MOCK_MODE:
        topics = []
        for name, meta in mock_topics.items():
            parts = meta["partitions"]
            total_msgs = meta["messages"]
            topics.append({
                "name": name,
                "partitions": parts,
                "replicas": 1,
                "messages": total_msgs,
                "end_offset": total_msgs,
                "per_partition": [{"p": p, "low": 0, "high": total_msgs // parts} for p in range(parts)],
            })
        return {
            "bootstrap": "Mock Mode (No Kafka required)",
            "ts": time.time(),
            "controller": 1,
            "brokers": [{"id": 1, "host": "localhost", "port": 9092}],
            "topics": topics,
        }

    try:
        md = admin.list_topics(timeout=5)
    except KafkaException as e:
        raise HTTPException(503, f"Cannot reach Kafka at {BOOTSTRAP}: {e}")

    topics = []
    for name, t in sorted(md.topics.items()):
        if name.startswith("__"):
            continue
        out = {}
        for p in t.partitions.keys():
            low, high = meta_consumer.get_watermark_offsets(TopicPartition(name, p), timeout=5)
            out[p] = (low, high)
        topics.append({
            "name": name,
            "partitions": len(t.partitions),
            "replicas": len(next(iter(t.partitions.values())).replicas) if t.partitions else 0,
            "messages": sum(h - l for l, h in out.values()),
            "end_offset": sum(h for _, h in out.values()),
            "per_partition": [{"p": p, "low": l, "high": h} for p, (l, h) in sorted(out.items())],
        })

    brokers = [{"id": b.id, "host": b.host, "port": b.port} for b in md.brokers.values()]
    return {
        "bootstrap": BOOTSTRAP,
        "ts": time.time(),
        "controller": md.controller_id,
        "brokers": brokers,
        "topics": topics,
    }


@app.get("/api/groups")
def groups():
    if MOCK_MODE:
        return [
            {
                "group": "analytics-group",
                "state": "Stable",
                "lag": [{"topic": "metrics", "lag": 3}],
                "total_lag": 3,
            }
        ]

    result = []
    listing = admin.list_consumer_groups().result()
    ids = [g.group_id for g in listing.valid if g.group_id != "dashboard-meta"]
    if not ids:
        return []
    from confluent_kafka import ConsumerGroupTopicPartitions
    futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g) for g in ids])
    states = {g.group_id: str(g.state).split(".")[-1] for g in listing.valid}
    for gid, fut in futures.items():
        try:
            tps = fut.result().topic_partitions
        except Exception:
            continue
        per_topic: dict = {}
        for tp in tps:
            if tp.offset < 0:
                continue
            _, high = meta_consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5)
            per_topic[tp.topic] = per_topic.get(tp.topic, 0) + max(high - tp.offset, 0)
        result.append({
            "group": gid,
            "state": states.get(gid, "UNKNOWN"),
            "lag": [{"topic": t, "lag": l} for t, l in per_topic.items()],
            "total_lag": sum(per_topic.values()),
        })
    return result


class NewTopicReq(BaseModel):
    name: str
    partitions: int = 1


@app.post("/api/topics")
def create_topic(req: NewTopicReq):
    if MOCK_MODE:
        mock_topics[req.name] = {"partitions": req.partitions, "messages": 0}
        return {"ok": True}

    fut = admin.create_topics([NewTopic(req.name, num_partitions=req.partitions, replication_factor=1)])[req.name]
    try:
        fut.result()
    except Exception as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


@app.delete("/api/topics/{name}")
def delete_topic(name: str):
    if MOCK_MODE:
        if name in mock_topics:
            del mock_topics[name]
        return {"ok": True}

    try:
        admin.delete_topics([name])[name].result()
    except Exception as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


class ProduceReq(BaseModel):
    topic: str
    key: str | None = None
    value: str


@app.post("/api/produce")
def produce(req: ProduceReq):
    if MOCK_MODE:
        if req.topic not in mock_topics:
            mock_topics[req.topic] = {"partitions": 1, "messages": 0}
        mock_topics[req.topic]["messages"] += 1
        return {"ok": True}

    err = {}
    def cb(e, m):
        if e:
            err["e"] = str(e)
    producer.produce(req.topic, value=req.value.encode(), key=req.key.encode() if req.key else None, callback=cb)
    producer.flush(5)
    if err:
        raise HTTPException(400, err["e"])
    return {"ok": True}


@app.websocket("/ws/messages/{topic}")
async def stream_messages(ws: WebSocket, topic: str):
    await ws.accept()
    if MOCK_MODE:
        offset = 0
        try:
            while True:
                await asyncio.sleep(1)
                offset += 1
                await ws.send_json({
                    "type": "message",
                    "partition": 0,
                    "offset": offset,
                    "ts": int(time.time() * 1000),
                    "key": f"key-{random.randint(1, 5)}",
                    "value": json.dumps({"sensor_id": "A1", "temp": round(random.uniform(20.0, 30.0), 2)}),
                })
        except Exception:
            pass
        return

    c = Consumer({**BASE, "group.id": f"dashboard-view-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False, "auto.offset.reset": "latest"})
    try:
        md = c.list_topics(topic, timeout=5)
        parts = md.topics[topic].partitions.keys()
        assignment = []
        for p in parts:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
            assignment.append(TopicPartition(topic, p, max(low, high - 10)))
        c.assign(assignment)
        while True:
            msg = await asyncio.to_thread(c.poll, 0.5)
            if msg is None:
                await ws.send_json({"type": "ping"})
                continue
            if msg.error():
                continue
            await ws.send_json({
                "type": "message",
                "partition": msg.partition(),
                "offset": msg.offset(),
                "ts": msg.timestamp()[1],
                "key": msg.key().decode(errors="replace") if msg.key() else None,
                "value": msg.value().decode(errors="replace") if msg.value() else None,
            })
    except Exception:
        pass
    finally:
        c.close()


static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir)

app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
def index():
    index_file = os.path.join(static_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "FastAPI is running in Mock Mode."}