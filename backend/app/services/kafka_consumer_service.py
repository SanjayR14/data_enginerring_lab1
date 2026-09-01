import threading
import logging
import json
import time
from kafka import KafkaConsumer
from sqlalchemy.orm import Session

from backend.app.core.config import settings
from backend.app.db.database import SessionLocal
from backend.app.services.airflow_service import AirflowOrchestratorService

logger = logging.getLogger("kafka_consumer_daemon")
logging.basicConfig(level=logging.INFO)

def consume_events():
    logger.info("Kafka consumer daemon thread started.")
    consumer = None
    while True:
        try:
            if not consumer:
                consumer = KafkaConsumer(
                    settings.KAFKA_TOPIC,
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    group_id=settings.KAFKA_GROUP_ID,
                    auto_offset_reset='earliest',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                logger.info("Successfully connected to Kafka.")

            for msg in consumer:
                event = msg.value
                logger.info(f"Received Kafka event: {event}")
                
                dataset_id = event.get('dataset_id')
                if dataset_id:
                    with SessionLocal() as db:
                        logger.info(f"Triggering Airflow DAG for dataset: {dataset_id}")
                        try:
                            AirflowOrchestratorService.trigger_dag_run(dataset_id=dataset_id, db=db)
                        except Exception as e:
                            logger.error(f"Failed to trigger Airflow DAG: {e}")

        except Exception as e:
            logger.error(f"Kafka consumer error: {e}. Retrying in 5s...")
            time.sleep(5)
            consumer = None

def start_kafka_consumer_daemon():
    thread = threading.Thread(target=consume_events, daemon=True)
    thread.start()
