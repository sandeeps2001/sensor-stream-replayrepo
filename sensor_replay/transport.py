"""Pluggable transport: in-memory (default, no dependencies) or Kafka.

The in-memory transport uses the simulated arrival times as the ingestion clock,
so runs are fast and fully reproducible. The Kafka transport stamps real wall-clock
ingestion time on the consumer side.
"""
import json
import time


class InMemoryTransport:
    def __init__(self):
        self._queue = []

    def send(self, event_dict, arrival_ts):
        self._queue.append((arrival_ts, event_dict))

    def consume(self):
        for arrival_ts, ev in sorted(self._queue, key=lambda x: x[0]):
            yield ev, arrival_ts


class KafkaTransport:
    def __init__(self, topic="sensor-events", servers="localhost:9092"):
        from kafka import KafkaProducer, KafkaConsumer  # pip install kafka-python
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=servers,
                                      value_serializer=lambda v: json.dumps(v).encode())
        self.consumer = KafkaConsumer(topic, bootstrap_servers=servers,
                                      auto_offset_reset="earliest", consumer_timeout_ms=3000,
                                      value_deserializer=lambda b: json.loads(b.decode()))

    def send(self, event_dict, arrival_ts=None):
        # key by device so one device's events stay in one partition (ordered per partition)
        self.producer.send(self.topic, value=event_dict, key=event_dict["device_id"].encode())

    def consume(self):
        self.producer.flush()
        for msg in self.consumer:
            yield msg.value, time.time()
