import sys
import os
import json
from datetime import datetime

# Add root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from backend.app.db.database import SessionLocal, init_db
from backend.app.services.kafka_producer import KafkaProducerService
from backend.app.services.kafka_consumer import KafkaConsumerService
from backend.app.services.cdc_service import CDCService
from backend.app.models.cdc import KafkaEventAuditModel, PipelineEventMetricsModel

def run_phase3_cdc_tests():
    print("========================================================")
    print("RUNNING PHASE 3 — KAFKA STREAMING & CDC VERIFICATION TESTS")
    print("========================================================")

    init_db()
    db = SessionLocal()

    # Clean existing test audit records
    db.query(KafkaEventAuditModel).delete()
    db.commit()

    test_dataset = "ds_sample_test"
    # Clean up test dataset directory to start with a fresh Delta/Parquet state
    import shutil
    for d in ["./data/delta", "./data/databricks_delta"]:
        if os.path.exists(d):
            shutil.rmtree(d)

    test_event_id = "evt-test-001"

    sample_record = {
        "date": "2026-08-01",
        "cloud_provider": "AWS",
        "account_id": "123456789012",
        "project_id": "prj-data-warehouse-prod",
        "environment": "production",
        "region": "us-east-1",
        "service": "AmazonEC2",
        "resource_type": "Compute",
        "usage_quantity": 720,
        "usage_unit": "Hrs",
        "list_cost": 1440.0,
        "net_cost": 1224.0,
        "budget_amount": 40000.0,
        "currency": "USD"
    }

    business_key = CDCService.generate_business_key(sample_record)
    print(f"\n1. Generated Business Key:\n   {business_key}")

    # ----------------------------------------------------
    # TEST STEP 1: INSERT EVENT
    # ----------------------------------------------------
    print("\n--- TEST STEP 1: INSERT Event ---")
    insert_event = {
        "event_id": test_event_id,
        "dataset_id": test_dataset,
        "batch_id": "batch_cdc_001",
        "operation": "INSERT",
        "event_timestamp": "2026-08-11T10:00:00Z",
        "record": sample_record
    }

    pub_res = KafkaProducerService.publish_event(insert_event)
    print(f"Publish Result: {pub_res}")

    # Check Silver State
    history = CDCService.get_cdc_history(business_key, dataset_id=test_dataset)
    print(f"Silver State after INSERT: net_cost={history['current_state'].get('net_cost')}, is_deleted={history['is_deleted']}")
    assert history["current_state"] is not None
    assert history["is_deleted"] is False
    assert float(history["current_state"]["net_cost"]) == 1224.0

    # ----------------------------------------------------
    # TEST STEP 2: DUPLICATE EVENT (IDEMPOTENCY)
    # ----------------------------------------------------
    print("\n--- TEST STEP 2: Duplicate Event (Idempotency) ---")
    dup_res = KafkaConsumerService.process_single_event(insert_event, db)
    print(f"Duplicate Consumer Result: {dup_res}")
    assert dup_res["status"] == "DUPLICATE_EVENT", f"Expected DUPLICATE_EVENT, got {dup_res['status']}"

    # Verify record count in Silver didn't change/duplicate
    history_dup = CDCService.get_cdc_history(business_key, dataset_id=test_dataset)
    assert history_dup["current_state"]["net_cost"] == 1224.0, "Duplicate event unexpectedly mutated state!"

    # ----------------------------------------------------
    # TEST STEP 3: UPDATE EVENT
    # ----------------------------------------------------
    print("\n--- TEST STEP 3: UPDATE Event ---")
    updated_record = dict(sample_record)
    updated_record["net_cost"] = 1500.0
    updated_record["usage_quantity"] = 800

    update_event = {
        "event_id": "evt-test-002",
        "dataset_id": test_dataset,
        "batch_id": "batch_cdc_002",
        "operation": "UPDATE",
        "event_timestamp": "2026-08-11T10:05:00Z",
        "record": updated_record
    }

    upd_res = KafkaProducerService.publish_event(update_event)
    print(f"Update Event Result: {upd_res}")

    history_upd = CDCService.get_cdc_history(business_key, dataset_id=test_dataset)
    print(f"Silver State after UPDATE: net_cost={history_upd['current_state'].get('net_cost')}, last_operation={history_upd['current_state'].get('last_operation')}")
    assert float(history_upd["current_state"]["net_cost"]) == 1500.0
    assert history_upd["current_state"]["last_operation"] == "UPDATE"

    # ----------------------------------------------------
    # TEST STEP 4: DELETE EVENT (SOFT DELETE)
    # ----------------------------------------------------
    print("\n--- TEST STEP 4: DELETE Event (Soft Delete) ---")
    delete_event = {
        "event_id": "evt-test-003",
        "dataset_id": test_dataset,
        "batch_id": "batch_cdc_003",
        "operation": "DELETE",
        "event_timestamp": "2026-08-11T10:10:00Z",
        "record": updated_record
    }

    del_res = KafkaProducerService.publish_event(delete_event)
    print(f"Delete Event Result: {del_res}")

    history_del = CDCService.get_cdc_history(business_key, dataset_id=test_dataset)
    print(f"Silver State after DELETE: is_deleted={history_del['is_deleted']}, deleted_at={history_del['current_state'].get('deleted_at')}")
    assert history_del["is_deleted"] is True

    # ----------------------------------------------------
    # TEST STEP 5: INVALID EVENT -> DLQ
    # ----------------------------------------------------
    print("\n--- TEST STEP 5: Invalid Event -> DLQ Routing ---")
    invalid_event = {
        "event_id": "evt-invalid-004",
        "dataset_id": test_dataset,
        "batch_id": "batch_cdc_004",
        "operation": "INSERT",
        "event_timestamp": "2026-08-11T10:15:00Z",
        "record": {
            "cloud_provider": "AWS",
            "account_id": "123456789012",
            # Missing critical 'project_id' and 'service'
            "net_cost": -50.0 # Negative cost invalid!
        }
    }

    invalid_pub = KafkaProducerService.publish_event(invalid_event)
    print(f"Invalid Event Result: {invalid_pub}")
    assert invalid_pub["consumer_result"]["status"] == "DLQ_INVALID"

    # ----------------------------------------------------
    # TEST STEP 6: OUT OF ORDER EVENT
    # ----------------------------------------------------
    print("\n--- TEST STEP 6: Out-of-Order Event Handling ---")
    older_event = {
        "event_id": "evt-older-005",
        "dataset_id": test_dataset,
        "batch_id": "batch_cdc_005",
        "operation": "UPDATE",
        "event_timestamp": "2026-08-11T09:00:00Z", # Older than 10:10:00Z delete timestamp
        "record": sample_record
    }

    ooo_pub = KafkaProducerService.publish_event(older_event)
    print(f"Out of Order Result: {ooo_pub}")
    assert ooo_pub["consumer_result"]["status"] == "OUT_OF_ORDER_SKIPPED"

    # ----------------------------------------------------
    # AUDIT LOGS AND METRICS VERIFICATION
    # ----------------------------------------------------
    print("\n--- VERIFYING KAFKA AUDIT LOGS & METRICS ---")
    audits = db.query(KafkaEventAuditModel).all()
    print(f"Total Audit Entries: {len(audits)}")
    for a in audits:
        print(f"  Event: {a.event_id} | Op: {a.operation} | Status: {a.status} | Key: {a.business_key[:30]}...")

    metrics = db.query(PipelineEventMetricsModel).first()
    if metrics:
        print(f"Metrics: Processed={metrics.processed_count}, Success={metrics.success_count}, Inserts={metrics.insert_count}, Updates={metrics.update_count}, Deletes={metrics.delete_count}, Duplicates={metrics.duplicate_count}, DLQ={metrics.dlq_count}")

    print("\n========================================================")
    print("ALL PHASE 3 KAFKA & CDC VERIFICATION TESTS PASSED SUCCESSFULLY!")
    print("========================================================\n")
    db.close()

if __name__ == "__main__":
    run_phase3_cdc_tests()
