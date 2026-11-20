#!/usr/bin/env python3
"""向 Kafka 写模拟订单事件，字段与 Flink Kafka 源表一致。

依赖：pip install kafka-python==2.2.15
用法：
  python produce_orders.py --bootstrap 127.0.0.1:9092 --topic order-events --count 20
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta

CATEGORIES = {"数码": 9001, "家电": 9002, "图书": 9003, "服饰": 9004}
CHANNELS = ["douyin", "wechat", "organic", "ads"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "order-events"))
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--sleep", type=float, default=0.2, help="每条之间暂停秒数")
    args = parser.parse_args()

    from kafka import KafkaProducer  # 延迟导入，便于 --help 不依赖 kafka

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=20,
    )
    base = datetime.now().replace(microsecond=0)
    for i in range(args.count):
        category = random.choice(list(CATEGORIES))
        event_time = base + timedelta(seconds=i)
        event = {
            "event_id": int(time.time() * 1000) + i,
            "user_id": random.randint(100, 120),
            "product_id": CATEGORIES[category],
            "category": category,
            "amount": round(random.uniform(10, 3000), 2),
            "quantity": random.randint(1, 3),
            "channel": random.choice(CHANNELS),
            "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        producer.send(args.topic, event)
        print(json.dumps(event, ensure_ascii=False))
        time.sleep(args.sleep)
    producer.flush()
    producer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
