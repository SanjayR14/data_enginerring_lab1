from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime

class CloudCostEventRequest(BaseModel):
    event_id: Optional[str] = Field(None, description="Unique event identifier (auto-generated if omitted)")
    dataset_id: str = Field("ds_sample_test", description="Target dataset identifier")
    batch_id: Optional[str] = Field(None, description="Batch identifier")
    operation: str = Field("INSERT", description="CDC Operation: INSERT, UPDATE, or DELETE")
    event_timestamp: Optional[str] = Field(None, description="ISO-8601 timestamp of event occurrence")
    record: Dict[str, Any] = Field(..., description="Cloud cost record dimension and metric attributes")

class CloudCostBatchEventRequest(BaseModel):
    events: List[CloudCostEventRequest]

class EventStatusResponse(BaseModel):
    event_id: str
    dataset_id: str
    batch_id: str
    business_key: str
    operation: str
    event_timestamp: datetime
    received_at: datetime
    processed_at: Optional[datetime] = None
    status: str
    error_message: Optional[str] = None

class KafkaStatusResponse(BaseModel):
    status: str
    bootstrap_servers: str
    topic: str
    consumer_group: str
    dlq_topic: str
    kafka_connected: bool
    active_consumers: int

class KafkaMetricsResponse(BaseModel):
    received_count: int
    processed_count: int
    success_count: int
    failed_count: int
    duplicate_count: int
    dlq_count: int
    insert_count: int
    update_count: int
    delete_count: int
    avg_processing_time_ms: float
    last_event_timestamp: Optional[datetime] = None

class CdcRecordHistoryResponse(BaseModel):
    business_key: str
    record_count: int
    is_deleted: bool
    current_state: Optional[Dict[str, Any]] = None
    history: List[Dict[str, Any]] = []
