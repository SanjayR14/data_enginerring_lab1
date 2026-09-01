import requests
import time
import os
import uuid
import sqlite3

BASE_URL = 'http://localhost:8000/api'
ORIGINAL_CSV = 'data/sample/cloud_cost_dataset.csv'
uid = uuid.uuid4().hex[:6]
TEST_CSV = f'data/sample/test_e2e_sqlite_{uid}.csv'

def run_test():
    print("--- STARTING E2E INGESTION TEST (SQLITE) ---")
    
    # 1. Create unique 100-row CSV
    with open(ORIGINAL_CSV, 'rb') as f_in:
        lines = f_in.readlines()
        
    with open(TEST_CSV, 'wb') as f_out:
        f_out.writelines(lines)
        # append 90 more rows of the valid second line but inject a unique id
        valid_row_str = lines[1].decode('utf-8')
        # We replace the project_id (e.g., 'prj-data-warehouse-prod') with something unique
        parts = valid_row_str.split(',')
        if len(parts) > 9:
            parts[9] = f"proj_{uid}"
            
        unique_row = (','.join(parts)).encode('utf-8')
        for _ in range(90):
            f_out.write(unique_row)

    # 2. Upload CSV
    print(f"Uploading new dataset: {TEST_CSV}")
    with open(TEST_CSV, 'rb') as f:
        res = requests.post(f'{BASE_URL}/datasets/upload', files={'file': f})
    
    os.remove(TEST_CSV)
    
    if res.status_code not in (200, 201):
        print(f"FAIL: Upload failed: {res.text}")
        return
    
    dataset = res.json()
    dataset_id = dataset['id']
    print(f"PASS: Upload successful. Dataset ID: {dataset_id}")

    # 3. Wait for Airflow Pipeline to Complete
    print("Waiting for Airflow pipeline to complete (this may take a minute)...")
    pipeline_completed = False
    for _ in range(30):
        time.sleep(5)
        try:
            st_res = requests.get(f'{BASE_URL}/pipeline/status/{dataset_id}')
            if st_res.status_code == 200:
                st = st_res.json()
                print(f"  Current Status: {st['status']} - {st['current_stage']}")
                if st['status'] in ('SUCCESS', 'FAILED'):
                    pipeline_completed = True
                    break
        except Exception as e:
            pass
    
    if not pipeline_completed:
        print("FAIL: Pipeline did not complete in time.")
    else:
        print("PASS: Airflow Pipeline triggered and finished execution.")

    # Connect to SQLite
    print("Connecting to SQLite to verify database records...")
    db_path = "data/cloud_cost.db"
    if not os.path.exists(db_path):
        print(f"FAIL: SQLite database not found at {db_path}")
        return
        
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 4. Verify SQLite Dataset
    cur.execute("SELECT id, status, row_count FROM datasets WHERE id = ?", (dataset_id,))
    ds_row = cur.fetchone()
    if ds_row:
        print(f"PASS: SQLite datasets table has record: ID={ds_row[0]}, Status={ds_row[1]}, Rows={ds_row[2]}")
    else:
        print(f"FAIL: SQLite datasets table missing record for {dataset_id}")

    # 5. Verify SQLite Pipeline Runs
    cur.execute("SELECT run_id, status FROM pipeline_runs WHERE dataset_id = ?", (dataset_id,))
    pr_row = cur.fetchone()
    if pr_row:
        print(f"PASS: SQLite pipeline_runs table has record: RunID={pr_row[0]}, Status={pr_row[1]}")
    else:
        print(f"FAIL: SQLite pipeline_runs missing record for {dataset_id}")

    conn.close()
    print("--- E2E INGESTION TEST COMPLETE ---")

if __name__ == '__main__':
    run_test()
