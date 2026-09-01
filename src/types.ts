export interface ColumnDetail {
  name: string;
  data_type: string;
}

export interface Dataset {
  id: string;
  filename: string;
  original_filename: string;
  file_size: number;
  file_type: string;
  row_count: number;
  column_count: number;
  status: string;
  upload_timestamp: string;
  file_hash?: string;
  columns: string[];
}

export interface DatasetDetail extends Dataset {
  storage_path: string;
  column_details: ColumnDetail[];
}

export interface DatasetPreview {
  dataset_id: string;
  original_filename: string;
  row_count: number;
  column_count: number;
  columns: string[];
  column_types: Record<string, string>;
  preview_data: Record<string, any>[];
}

export interface AirflowTaskStatusItem {
  task_id: string;
  status: 'QUEUED' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'UPSTREAM_FAILED' | 'SKIPPED';
  started_at?: string;
  completed_at?: string;
  duration_seconds: number;
  error_message?: string;
}

export interface PipelineStatus {
  run_id?: string;
  dag_run_id?: string;
  dataset_id: string;
  batch_id?: string;
  status: string;
  current_stage: string;
  message: string;
  last_updated: string;
  tasks?: AirflowTaskStatusItem[];
  input_records?: number;
  bronze_records?: number;
  valid_records?: number;
  quarantined_records?: number;
  silver_records?: number;
  gold_records?: number;
  failed_stage?: string;
  error_message?: string;
  databricks_executed?: boolean;
  kafka_events?: number;
  processed?: number;
  failed?: number;
  duplicates?: number;
}

export interface CloudCostEventRequest {
  event_id?: string;
  dataset_id: string;
  batch_id?: string;
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  event_timestamp?: string;
  record: Record<string, any>;
}

export interface EventStatus {
  event_id: string;
  dataset_id: string;
  batch_id: string;
  business_key: string;
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  event_timestamp: string;
  received_at: string;
  processed_at?: string;
  status: 'SUCCESS' | 'DUPLICATE_EVENT' | 'DLQ_INVALID' | 'FAILED' | 'OUT_OF_ORDER_SKIPPED' | 'PENDING';
  error_message?: string;
}

export interface KafkaStatus {
  status: string;
  bootstrap_servers: string;
  topic: string;
  consumer_group: string;
  dlq_topic: string;
  kafka_connected: boolean;
  active_consumers: number;
}

export interface KafkaMetrics {
  received_count: number;
  processed_count: number;
  success_count: number;
  failed_count: number;
  duplicate_count: number;
  dlq_count: number;
  insert_count: number;
  update_count: number;
  delete_count: number;
  avg_processing_time_ms: number;
  last_event_timestamp?: string;
}

export interface CdcRecordHistory {
  business_key: string;
  record_count: number;
  is_deleted: boolean;
  current_state?: Record<string, any>;
  history: Record<string, any>[];
}

export interface PipelineProcessResponse {
  run_id: string;
  dataset_id: string;
  batch_id: string;
  dag_id?: string;
  dag_run_id?: string;
  status: string;
  current_stage: string;
  message: string;
  started_at: string;
}

export interface DataQualityCheckItem {
  check_name: string;
  status: string;
  records_checked: number;
  records_failed: number;
  failure_percentage: number;
}

export interface DataQualitySummary {
  dataset_id: string;
  batch_id: string;
  total_records: number;
  valid_records: number;
  quarantined_records: number;
  duplicate_records: number;
  null_issues: number;
  invalid_values: number;
  quality_score: number;
  checks: DataQualityCheckItem[];
}

export interface QuarantineRecord {
  id: number;
  record_hash?: string;
  failure_reason: string;
  failed_at: string;
  original_record: string;
}

export interface DatasetProfile {
  dataset_id: string;
  row_count: number;
  column_count: number;
  null_counts: Record<string, number>;
  distinct_counts: Record<string, number>;
  numeric_stats: Record<string, { min: number; max: number; mean: number; median: number; std: number }>;
  categorical_frequencies: Record<string, Record<string, number>>;
  duplicate_count: number;
  correlation_matrix: Record<string, Record<string, number | null>>;
  outliers: Record<string, { count: number; lower_bound: number; upper_bound: number; example_values: number[] }>;
}
