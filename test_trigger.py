import requests, time, os, uuid

base_url = 'http://localhost:8000/api'
original_file = 'data/sample/cloud_cost_dataset.csv'
file_path = f'data/sample/test_{uuid.uuid4().hex[:6]}.csv'

with open(original_file, 'rb') as f_in:
    content = f_in.read()
    
# Append a dummy line to change the hash
with open(file_path, 'wb') as f_out:
    f_out.write(content)
    f_out.write(b'\n2026-01-01,AWS,12345,dummy,1.0')

print(f'Uploading dataset: {file_path}')
with open(file_path, 'rb') as f:
    res = requests.post(f'{base_url}/datasets/upload', files={'file': f})

os.remove(file_path)

if res.status_code not in (200, 201):
    print(f'Upload failed: {res.text}')
    exit(1)

dataset = res.json()
dataset_id = dataset['id']
print(f'Upload successful! Dataset ID: {dataset_id}')

print('Checking pipeline status...')
for _ in range(5):
    time.sleep(2)
    status_res = requests.get(f'{base_url}/pipeline/status/{dataset_id}')
    if status_res.status_code == 200:
        status_data = status_res.json()
        print(f"Pipeline Status: {status_data['status']}, Current Stage: {status_data['current_stage']}")
    else:
        print(f'Failed to get status: {status_res.status_code}')
