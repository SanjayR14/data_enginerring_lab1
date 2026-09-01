import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from backend.app.core.config import settings
from backend.app.models.cdc import KafkaEventAuditModel, PipelineEventMetricsModel
from backend.app.services.cdc_service import CDCService

logger = logging.getLogger("kafka_consumer")
logging.basicConfig(level=logging.INFO)

class KafkaConsumerService:
    @classmethod
    def validate_event(cls, event: Dict[str, Any]) -> (bool, Optional[str]):
        """
        Validates incoming streaming event quality against Section 21 constraints.
        Returns (is_valid, error_message).
        """
        if not isinstance(event, dict):
            return False, "Payload is not a valid JSON object"

        event_id = event.get('event_id')
        if not event_id or not str(event_id).strip():
            return False, "Missing or empty required field: event_id"

        operation = str(event.get('operation', '')).upper()
        if operation not in ["INSERT", "UPDATE", "DELETE"]:
            return False, f"Invalid operation '{operation}'. Must be INSERT, UPDATE, or DELETE"

        record = event.get('record')
        if not isinstance(record, dict) or not record:
            return False, "Missing or empty 'record' dictionary in event"

        # Check critical fields
        critical_fields = ['cloud_provider', 'account_id', 'project_id', 'service']
        for cf in critical_fields:
            if not record.get(cf):
                return False, f"Missing critical record dimension: {cf}"

        # Check numeric coercibility & non-negativity
        try:
            net_cost = float(record.get('net_cost', 0.0))
            if net_cost < 0:
                return False, f"Invalid negative net_cost: {net_cost}"
        except (ValueError, TypeError):
            return False, f"Non-numeric net_cost value: {record.get('net_cost')}"

        try:
            usage_qty = float(record.get('usage_quantity', 0.0))
            if usage_qty < 0:
                return False, f"Invalid negative usage_quantity: {usage_qty}"
        except (ValueError, TypeError):
            return False, f"Non-numeric usage_quantity value: {record.get('usage_quantity')}"

        # Percentage validation if provided
        for pct_col in ['savings_plan_coverage_pct', 'reserved_instance_coverage_pct', 'discount_rate_pct']:
            if pct_col in record and record[pct_col] is not None:
                try:
                    val = float(record[pct_col])
                    if val < 0.0 or val > 100.0:
                        return False, f"Percentage field {pct_col} out of range 0..100: {val}"
                except (ValueError, TypeError):
                    return False, f"Non-numeric percentage in {pct_col}: {record[pct_col]}"

        return True, None

    @classmethod
    def process_single_event(cls, event: Dict[str, Any], db: Session) -> Dict[str, Any]:
        """
        Processes a single event with Validation, Idempotency Check, Retry Logic, and DLQ routing.
        """
        if not settings.KAFKA_ENABLED:
            return {
                "event_id": str(event.get('event_id', 'unknown')),
                "status": "DISABLED",
                "message": "Kafka processing is disabled in this environment."
            }

        start_time = time.time()
        event_id = str(event.get('event_id', 'unknown'))
        operation = str(event.get('operation', 'INSERT')).upper()
        dataset_id = str(event.get('dataset_id', 'ds_sample_test'))
        batch_id = str(event.get('batch_id', 'batch_cdc'))
        raw_record = event.get('record', {})

        business_key = CDCService.generate_business_key(raw_record) if raw_record else "UNKNOWN_KEY"

        # Parse event timestamp
        event_ts_str = event.get('event_timestamp')
        try:
            event_ts = datetime.fromisoformat(event_ts_str.replace('Z', '+00:00')) if event_ts_str else datetime.utcnow()
        except Exception:
            event_ts = datetime.utcnow()

        # Step 1: Idempotency Check in kafka_event_audit
        existing_audit = db.query(KafkaEventAuditModel).filter(
            KafkaEventAuditModel.event_id == event_id
        ).first()

        if existing_audit and existing_audit.status == "SUCCESS":
            logger.info(f"[KAFKA CONSUMER] Event {event_id} already processed successfully. Skipping as DUPLICATE_EVENT.")
            # Record duplicate audit entry
            dup_audit = KafkaEventAuditModel(
                event_id=f"{event_id}_dup_{int(time.time()*1000)}",
                dataset_id=dataset_id,
                batch_id=batch_id,
                business_key=business_key,
                operation=operation,
                event_timestamp=event_ts,
                received_at=datetime.utcnow(),
                processed_at=datetime.utcnow(),
                status="DUPLICATE_EVENT",
                error_message=f"Duplicate event_id '{event_id}' detected and ignored for idempotency.",
                raw_event=json.dumps(event)
            )
            db.add(dup_audit)
            cls._update_metrics(db, duplicate=True)
            db.commit()
            return {
                "event_id": event_id,
                "status": "DUPLICATE_EVENT",
                "message": f"Duplicate event '{event_id}' ignored for idempotency."
            }

        # Step 2: Validate Event Quality
        is_valid, val_error = cls.validate_event(event)
        if not is_valid:
            logger.warning(f"[KAFKA CONSUMER] Event {event_id} validation failed: {val_error}. Routing to DLQ.")
            
            # Send to DLQ Topic/Buffer
            dlq_entry = {
                "event_id": event_id,
                "original_event": event,
                "error": val_error,
                "failed_stage": "STREAMING_VALIDATION",
                "timestamp": datetime.utcnow().isoformat()
            }
            cls._send_to_dlq(dlq_entry)

            audit = KafkaEventAuditModel(
                event_id=event_id,
                dataset_id=dataset_id,
                batch_id=batch_id,
                business_key=business_key,
                operation=operation,
                event_timestamp=event_ts,
                received_at=datetime.utcnow(),
                processed_at=datetime.utcnow(),
                status="DLQ_INVALID",
                error_message=val_error,
                raw_event=json.dumps(event)
            )
            db.merge(audit)
            cls._update_metrics(db, dlq=True, failed=True)
            db.commit()

            return {
                "event_id": event_id,
                "status": "DLQ_INVALID",
                "error": val_error,
                "message": "Event failed validation and was routed to Dead Letter Queue (cloud-cost-events-dlq)."
            }

        # Step 3: CDC Processing with Retry Logic (Max 3 attempts)
        max_retries = 3
        last_error = None
        cdc_res = None

        for attempt in range(1, max_retries + 1):
            try:
                cdc_res = CDCService.process_cdc_event(event, db)
                break
            except Exception as e:
                last_error = str(e)
                logger.warning(f"[KAFKA CONSUMER] Attempt {attempt}/{max_retries} failed for event {event_id}: {e}")
                time.sleep(0.2 * attempt)

        proc_time_ms = round((time.time() - start_time) * 1000.0, 2)

        if cdc_res and cdc_res.get("status") in ["SUCCESS", "OUT_OF_ORDER_SKIPPED"]:
            status_code = cdc_res["status"]
            msg = cdc_res.get("message", "")
            
            audit = KafkaEventAuditModel(
                event_id=event_id,
                dataset_id=dataset_id,
                batch_id=batch_id,
                business_key=business_key,
                operation=operation,
                event_timestamp=event_ts,
                received_at=datetime.utcnow(),
                processed_at=datetime.utcnow(),
                status=status_code,
                error_message=msg if status_code == "OUT_OF_ORDER_SKIPPED" else None,
                raw_event=json.dumps(event)
            )
            db.merge(audit)
            
            cls._update_metrics(
                db,
                success=True,
                op=operation,
                proc_time_ms=proc_time_ms
            )
            db.commit()

            return {
                "event_id": event_id,
                "business_key": business_key,
                "status": status_code,
                "operation": operation,
                "message": msg,
                "processing_time_ms": proc_time_ms
            }
        else:
            err_msg = last_error or (cdc_res.get("message") if cdc_res else "CDC processing failure")
            logger.error(f"[KAFKA CONSUMER] Event {event_id} failed processing after retries: {err_msg}")

            # Send to DLQ
            cls._send_to_dlq({
                "event_id": event_id,
                "original_event": event,
                "error": err_msg,
                "failed_stage": "CDC_DELTA_MERGE",
                "timestamp": datetime.utcnow().isoformat()
            })

            audit = KafkaEventAuditModel(
                event_id=event_id,
                dataset_id=dataset_id,
                batch_id=batch_id,
                business_key=business_key,
                operation=operation,
                event_timestamp=event_ts,
                received_at=datetime.utcnow(),
                processed_at=datetime.utcnow(),
                status="FAILED",
                error_message=err_msg,
                raw_event=json.dumps(event)
            )
            db.merge(audit)
            cls._update_metrics(db, failed=True, dlq=True, proc_time_ms=proc_time_ms)
            db.commit()

            return {
                "event_id": event_id,
                "status": "FAILED",
                "error": err_msg,
                "message": "Processing failed and event was routed to Dead Letter Queue."
            }

    @classmethod
    def _send_to_dlq(cls, dlq_payload: Dict[str, Any]):
        """
        Sends invalid/failed events to DLQ topic cloud-cost-events-dlq.
        """
        dlq_topic = settings.KAFKA_DLQ_TOPIC
        try:
            from backend.app.services.kafka_producer import KafkaProducerService
            producer = KafkaProducerService.get_producer()
            if producer:
                producer.send(dlq_topic, value=dlq_payload)
                logger.info(f"[DLQ] Routed failed event {dlq_payload.get('event_id')} to topic {dlq_topic}")
        except Exception as e:
            logger.error(f"[DLQ] Error sending to DLQ topic {dlq_topic}: {e}")

    @classmethod
    def _update_metrics(
        cls,
        db: Session,
        success: bool = False,
        failed: bool = False,
        duplicate: bool = False,
        dlq: bool = False,
        op: str = "INSERT",
        proc_time_ms: float = 0.0
    ):
        metrics = db.query(PipelineEventMetricsModel).first()
        if not metrics:
            metrics = PipelineEventMetricsModel(
                received_count=0,
                processed_count=0,
                success_count=0,
                failed_count=0,
                duplicate_count=0,
                dlq_count=0,
                insert_count=0,
                update_count=0,
                delete_count=0,
                avg_processing_time_ms=0.0
            )
            db.add(metrics)

        metrics.received_count = (metrics.received_count or 0) + 1
        metrics.processed_count = (metrics.processed_count or 0) + 1
        if success:
            metrics.success_count = (metrics.success_count or 0) + 1
            if op == "INSERT":
                metrics.insert_count = (metrics.insert_count or 0) + 1
            elif op == "UPDATE":
                metrics.update_count = (metrics.update_count or 0) + 1
            elif op == "DELETE":
                metrics.delete_count = (metrics.delete_count or 0) + 1
        if failed:
            metrics.failed_count = (metrics.failed_count or 0) + 1
        if duplicate:
            metrics.duplicate_count = (metrics.duplicate_count or 0) + 1
        if dlq:
            metrics.dlq_count = (metrics.dlq_count or 0) + 1

        curr_avg = metrics.avg_processing_time_ms or 0.0
        if proc_time_ms > 0:
            if curr_avg > 0:
                metrics.avg_processing_time_ms = round((curr_avg + proc_time_ms) / 2.0, 2)
            else:
                metrics.avg_processing_time_ms = proc_time_ms

        metrics.last_event_timestamp = datetime.utcnow()
