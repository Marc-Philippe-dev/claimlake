"""Source 3 - simulated First Notice Of Loss (FNOL) events from the mobile app.

One producer, three targets:
  --sink kafka      local Kafka in Docker (default bootstrap localhost:9092)
  --sink eventhubs  Azure Event Hubs through its Kafka endpoint (port 9093, SASL_SSL)
  --sink files      JSON lines in <landing>/fnol (no Docker needed)

Examples:
  python scripts/send_fnol_events.py --policies data/pas/policies_initial.csv --count 500
  python scripts/send_fnol_events.py --policies ... --sink files --landing data/landing --count 200
  EVENTHUB_NAMESPACE=evhns-claimslake-dev EVENTHUB_CONNECTION_STRING="Endpoint=sb://..." \\
    python scripts/send_fnol_events.py --policies ... --sink eventhubs --rate 5
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

CHANNELS = ["MOBILE_APP", "PHONE", "AGENT", "WEB"]
CLAIM_TYPES = ["COLLISION", "BRIS_DE_GLACE", "VOL", "INCENDIE", "CATASTROPHE_NATURELLE", "AUTRE"]


def make_event(policy_ids: list[int], regions: dict[int, str]) -> dict:
    policy_id = random.choice(policy_ids)
    now = datetime.now(timezone.utc)
    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),
        "policy_id": policy_id,
        "incident_ts": (now - timedelta(hours=random.randint(0, 72))).isoformat(),
        "channel": random.choices(CHANNELS, weights=[0.45, 0.3, 0.15, 0.1])[0],
        "claim_type": random.choice(CLAIM_TYPES),
        "estimated_amount": round(random.lognormvariate(7.0, 1.0), 2),
        "region": regions.get(policy_id),
        "injured_persons": random.choices([0, 1, 2], weights=[0.9, 0.08, 0.02])[0],
    }
    if random.random() < 0.01:  # deliberate anomaly, dropped by an expectation
        event["policy_id"] = None
    return event


def kafka_producer(sink: str, bootstrap: str):
    from confluent_kafka import Producer

    if sink == "kafka":
        return Producer({"bootstrap.servers": bootstrap})
    namespace = os.environ["EVENTHUB_NAMESPACE"]
    return Producer(
        {
            "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": os.environ["EVENTHUB_CONNECTION_STRING"],
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--policies", required=True)
    parser.add_argument("--sink", choices=["kafka", "eventhubs", "files"], default="kafka")
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "fnol-events"))
    parser.add_argument("--landing", default="data/landing", type=Path)
    parser.add_argument("--rate", type=float, default=20.0, help="events per second")
    parser.add_argument("--count", type=int, default=0, help="stop after N events (0 = forever)")
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    policies = pd.read_csv(args.policies, usecols=["policy_id", "region"])
    policy_ids = policies["policy_id"].astype(int).tolist()
    regions = dict(zip(policies["policy_id"].astype(int), policies["region"], strict=True))

    producer = None
    buffer: list[str] = []
    if args.sink in ("kafka", "eventhubs"):
        producer = kafka_producer(args.sink, args.bootstrap)

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            event = make_event(policy_ids, regions)
            payload = json.dumps(event)
            if producer is not None:
                producer.produce(args.topic, key=str(event["policy_id"]), value=payload)
                producer.poll(0)
            else:
                buffer.append(payload)
                if len(buffer) >= 100:
                    _flush_file(args.landing, buffer)
            sent += 1
            if args.rate > 0:
                time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        if producer is not None:
            producer.flush(10)
        elif buffer:
            _flush_file(args.landing, buffer)
    print(f"FNOL: {sent} event(s) sent to {args.sink}")


def _flush_file(landing: Path, buffer: list[str]) -> None:
    out = landing / "fnol"
    out.mkdir(parents=True, exist_ok=True)
    name = datetime.now(timezone.utc).strftime("fnol_%Y%m%dT%H%M%S%f.json")
    (out / name).write_text("\n".join(buffer) + "\n")
    buffer.clear()


if __name__ == "__main__":
    main()
