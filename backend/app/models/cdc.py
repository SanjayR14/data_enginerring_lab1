from sqlalchemy import Column, String, Integer, DateTime, Text, Float, Boolean
from datetime import datetime
import uuid
from backend.app.db.database import Base

class KafkaEventAuditModel(Base):
    __tablename__ = "kafka_event_audit"

    event_id = Column(String, primary_key=True, index=True)
    dataset_id = Column(String, index=True, nullable=False, default="ds_streaming")
    batch_id = Column(String, index=True, nullable=False, default=lambda: f"batch_{uuid.uuid4().hex[:8]}")
    business_key = Column(String, index=True, nullable=False)
    operation = Column(String, nullable=False)  # INSERT, UPDATE, DELETE
    event_timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    received_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="PENDING")  # SUCCESS, DUPLICATE_EVENT, DLQ_INVALID, FAILED, OUT_OF_ORDER_SKIPPED
    error_message = Column(Text, nullable=True)
    raw_event = Column(Text, nullable=True)

class PipelineEventMetricsModel(Base):
    __tablename__ = "pipeline_event_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    received_count = Column(Integer, default=0)
    processed_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    duplicate_count = Column(Integer, default=0)
    dlq_count = Column(Integer, default=0)
    insert_count = Column(Integer, default=0)
    update_count = Column(Integer, default=0)
    delete_count = Column(Integer, default=0)
    avg_processing_time_ms = Column(Float, default=0.0)
    last_event_timestamp = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
