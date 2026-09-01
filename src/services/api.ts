import {
  Dataset,
  DatasetDetail,
  DatasetPreview,
  PipelineStatus,
  PipelineProcessResponse,
  DataQualitySummary,
  QuarantineRecord,
  DatasetProfile,
  CloudCostEventRequest,
  EventStatus,
  KafkaStatus,
  KafkaMetrics,
  CdcRecordHistory
} from '../types';

const API_BASE = '/api';

export async function fetchHealth(): Promise<{ status: string; database: string }> {
  const res = await fetch(`${API_BASE}/health`);
  if (!res.ok) throw new Error('Failed to fetch backend health');
  return res.json();
}

export async function uploadDataset(file: File): Promise<Dataset> {
  const formData = new FormData();
  formData.append('file', file);

  const res = await fetch(`${API_BASE}/datasets/upload`, {
    method: 'POST',
    body: formData,
  });

  if (!res.ok) {
    const errorData = await res.json().catch(() => ({ detail: 'Upload failed' }));
    throw new Error(errorData.detail || 'Dataset upload failed');
  }

  return res.json();
}

export async function fetchDatasets(): Promise<Dataset[]> {
  const res = await fetch(`${API_BASE}/datasets`);
  if (!res.ok) throw new Error('Failed to fetch datasets list');
  return res.json();
}

export async function fetchDatasetDetail(datasetId: string): Promise<DatasetDetail> {
  const res = await fetch(`${API_BASE}/datasets/${datasetId}`);
  if (!res.ok) throw new Error('Failed to fetch dataset details');
  return res.json();
}

export async function fetchDatasetPreview(
  datasetId: string,
  limit: number = 10,
  layer: 'bronze' | 'silver' = 'bronze'
): Promise<DatasetPreview> {
  const res = await fetch(`${API_BASE}/datasets/${datasetId}/preview?limit=${limit}&layer=${layer}`);
  if (!res.ok) throw new Error('Failed to fetch dataset preview');
  return res.json();
}

export async function triggerPipelineProcess(datasetId: string): Promise<PipelineProcessResponse> {
  const res = await fetch(`${API_BASE}/pipeline/process/${datasetId}`, {
    method: 'POST',
  });
  if (!res.ok) {
    const errorData = await res.json().catch(() => ({ detail: 'Pipeline execution failed' }));
    throw new Error(errorData.detail || 'Failed to trigger pipeline processing');
  }
  return res.json();
}

export async function fetchPipelineStatus(datasetId: string): Promise<PipelineStatus> {
  const res = await fetch(`${API_BASE}/pipeline/status/${datasetId}`);
  if (!res.ok) throw new Error('Failed to fetch pipeline status');
  return res.json();
}

export async function fetchDataQualitySummary(datasetId: string): Promise<DataQualitySummary> {
  const res = await fetch(`${API_BASE}/datasets/${datasetId}/data-quality`);
  if (!res.ok) throw new Error('Failed to fetch data quality summary');
  return res.json();
}

export async function fetchQuarantineRecords(datasetId: string, limit: number = 50): Promise<QuarantineRecord[]> {
  const res = await fetch(`${API_BASE}/datasets/${datasetId}/quarantine?limit=${limit}`);
  if (!res.ok) throw new Error('Failed to fetch quarantine records');
  return res.json();
}

export async function fetchDatasetProfile(datasetId: string): Promise<DatasetProfile> {
  const res = await fetch(`${API_BASE}/datasets/${datasetId}/profile`);
  if (!res.ok) throw new Error('Failed to fetch dataset profile');
  return res.json();
}

export function getSampleDatasetUrl(): string {
  return `${API_BASE}/datasets/sample/download`;
}

// -------------------------------------------------------------------
// PHASE 3: KAFKA STREAMING & CDC API FUNCTIONS
// -------------------------------------------------------------------

export async function publishCloudCostEvent(event: CloudCostEventRequest): Promise<any> {
  const res = await fetch(`${API_BASE}/events/cloud-cost`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(event)
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: 'Failed to publish event' }));
    throw new Error(err.detail || 'Failed to publish event to Kafka');
  }
  return res.json();
}

export async function publishCloudCostBatch(events: CloudCostEventRequest[]): Promise<any> {
  const res = await fetch(`${API_BASE}/events/cloud-cost/batch`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ events })
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: 'Failed to publish batch' }));
    throw new Error(err.detail || 'Failed to publish batch events');
  }
  return res.json();
}

export async function fetchEventStatus(eventId: string): Promise<EventStatus> {
  const res = await fetch(`${API_BASE}/events/${eventId}`);
  if (!res.ok) throw new Error(`Event ID ${eventId} not found`);
  return res.json();
}

export async function fetchKafkaStatus(): Promise<KafkaStatus> {
  const res = await fetch(`${API_BASE}/kafka/status`);
  if (!res.ok) throw new Error('Failed to fetch Kafka status');
  return res.json();
}

export async function fetchKafkaMetrics(): Promise<KafkaMetrics> {
  const res = await fetch(`${API_BASE}/kafka/metrics`);
  if (!res.ok) throw new Error('Failed to fetch Kafka metrics');
  return res.json();
}

export async function fetchCdcRecordHistory(businessKey: string, datasetId: string = 'ds_sample_test'): Promise<CdcRecordHistory> {
  const encodedKey = encodeURIComponent(businessKey);
  const res = await fetch(`${API_BASE}/cdc/records/${encodedKey}?dataset_id=${datasetId}`);
  if (!res.ok) throw new Error('Failed to fetch CDC record history');
  return res.json();
}

export async function fetchCdcAuditLogs(limit: number = 50, statusFilter?: string): Promise<EventStatus[]> {
  const url = statusFilter 
    ? `${API_BASE}/cdc/audit?limit=${limit}&status_filter=${statusFilter}`
    : `${API_BASE}/cdc/audit?limit=${limit}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error('Failed to fetch CDC audit logs');
  return res.json();
}

