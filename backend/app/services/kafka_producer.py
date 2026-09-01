import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, List
from kafka import KafkaProducer as PyKafkaProducer
from backend.app.core.config import settings

logger = logging.getLogger("kafka_producer")
logging.basicConfig(level=logging.INFO)

class KafkaProducerService:
    _producer = None
    _buffer: List[Dict[str, Any]] = []

    @classmethod
    def get_producer(cls):
        if cls._producer is None:
            try:
                bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
                logger.info(f"[KAFKA PRODUCER] Attempting connection to Kafka bootstrap servers: {bootstrap_servers}")
                cls._producer = PyKafkaProducer(
                    bootstrap_servers=bootstrap_servers.split(','),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    request_timeout_ms=3000,
                    max_block_ms=3000
                )
                logger.info("[KAFKA PRODUCER] Connected to Kafka broker successfully.")
            except Exception as e:
                logger.warning(f"[KAFKA PRODUCER] Kafka broker unavailable ({e}). Using direct streaming buffer fallback.")
                cls._producer = None
        return cls._producer

    @classmethod
    def publish_event(cls, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publishes one JSON event to Kafka topic cloud-cost-events.
        Ensures event_id, dataset_id, batch_id, and event_timestamp exist.
        """
        event_id = event.get('event_id') or f"evt-{uuid.uuid4().hex[:12]}"
        event['event_id'] = event_id
        event['dataset_id'] = event.get('dataset_id', 'ds_sample_test')
        event['batch_id'] = event.get('batch_id', f"batch_{uuid.uuid4().hex[:8]}")
        event['operation'] = str(event.get('operation', 'INSERT')).upper()
        if 'event_timestamp' not in event or not event['event_timestamp']:
            event['event_timestamp'] = datetime.utcnow().isoformat()

        topic = settings.KAFKA_TOPIC
        producer = cls.get_producer()

        if producer:
            try:
                key = event_id
                future = producer.send(topic, key=key, value=event)
                # Wait briefly for delivery confirmation
                metadata = future.get(timeout=3)
                logger.info(f"[KAFKA PRODUCER] Published event {event_id} to topic {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
                return {
                    "status": "published",
                    "event_id": event_id,
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "kafka_active": True
                }
            except Exception as e:
                logger.warning(f"[KAFKA PRODUCER] Failed to send to Kafka broker ({e}). Queuing to buffer.")

        # Fallback buffer handling if Kafka broker connection is unreachable or timed out
        cls._buffer.append(event)
        logger.info(f"[KAFKA PRODUCER] Buffered event {event_id} locally (total buffer size: {len(cls._buffer)})")

        if not settings.KAFKA_ENABLED:
            return {
                "status": "buffered",
                "event_id": event_id,
                "topic": topic,
                "fallback_mode": True,
                "kafka_active": False,
                "consumer_result": "Kafka processing is disabled in this environment."
            }
        
        # Trigger immediate processing via consumer engine
        from backend.app.services.kafka_consumer import KafkaConsumerService
        from backend.app.db.database import SessionLocal
        
        db = SessionLocal()
        try:
            consumer_res = KafkaConsumerService.process_single_event(event, db)
            return {
                "status": "published",
                "event_id": event_id,
                "topic": topic,
                "fallback_mode": True,
                "kafka_active": False,
                "consumer_result": consumer_res
            }
        finally:
            db.close()

    @classmethod
    def publish_batch(cls, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Publishes a list of events to Kafka topic.
        """
        results = []
        published_count = 0
        for evt in events:
            res = cls.publish_event(evt)
            results.append(res)
            if res.get("status") == "published":
                published_count += 1

        return {
            "total": len(events),
            "published": published_count,
            "results": results
        }
