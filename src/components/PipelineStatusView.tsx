import React, { useEffect, useState } from 'react';
import { fetchPipelineStatus, triggerPipelineProcess } from '../services/api';
import { PipelineStatus, Dataset, AirflowTaskStatusItem } from '../types';
import {
  GitPullRequest,
  CheckCircle2,
  Cpu,
  Workflow,
  AlertCircle,
  Play,
  RefreshCw,
  Zap,
  Clock,
  Check,
  XCircle,
  AlertTriangle,
  Server,
  Layers,
  Database
} from 'lucide-react';

interface PipelineStatusViewProps {
  datasets: Dataset[];
}

export const PipelineStatusView: React.FC<PipelineStatusViewProps> = ({ datasets }) => {
  const [selectedDatasetId, setSelectedDatasetId] = useState<string>(datasets[0]?.id || '');
  const [statusData, setStatusData] = useState<PipelineStatus | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [isProcessing, setIsProcessing] = useState<boolean>(false);
  const [actionMessage, setActionMessage] = useState<string | null>(null);

  const loadStatus = async () => {
    if (!selectedDatasetId) return;
    setIsLoading(true);
    try {
      const data = await fetchPipelineStatus(selectedDatasetId);
      setStatusData(data);
    } catch {
      setStatusData(null);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadStatus();
  }, [selectedDatasetId]);

  // Auto-polling when pipeline is active
  useEffect(() => {
    let interval: any = null;
    if (statusData?.status === 'RUNNING' || statusData?.status === 'QUEUED' || isProcessing) {
      interval = setInterval(() => {
        loadStatus();
      }, 2000);
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [selectedDatasetId, statusData?.status, isProcessing]);

  const handleRunPipeline = async () => {
    if (!selectedDatasetId) return;
    setIsProcessing(true);
    setActionMessage('Triggering Apache Airflow Orchestrated Pipeline (cloud_cost_etl_pipeline)...');
    try {
      const res = await triggerPipelineProcess(selectedDatasetId);
      setActionMessage(`Airflow DAG Triggered Successfully! DAG Run ID: ${res.dag_run_id}`);
      await loadStatus();
    } catch (err: any) {
      setActionMessage(`Pipeline execution failed: ${err.message}`);
    } finally {
      setIsProcessing(false);
    }
  };

  const isCompleted = statusData?.status === 'SUCCESS' || statusData?.status === 'COMPLETED' || statusData?.status === 'PROCESSED_GOLD';

  const defaultTasks: AirflowTaskStatusItem[] = [
    { task_id: 'check_dataset', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'validate_schema', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'create_batch', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'load_bronze', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'bronze_quality_check', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'clean_data', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'feature_engineering', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'silver_quality_check', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'dimension_load', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'fact_load', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'warehouse_quality_check', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'refresh_olap_aggregates', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'warehouse_verification', status: 'QUEUED', duration_seconds: 0 },
    { task_id: 'update_pipeline_status', status: 'QUEUED', duration_seconds: 0 },
  ];

  const tasksToDisplay = statusData?.tasks && statusData.tasks.length > 0 ? statusData.tasks : defaultTasks;

  const getTaskBadge = (status: string) => {
    switch (status) {
      case 'SUCCESS':
        return (
          <span className="px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-emerald-100 text-emerald-800 border border-emerald-200 flex items-center space-x-1">
            <CheckCircle2 className="w-3 h-3 text-emerald-600" />
            <span>SUCCESS</span>
          </span>
        );
      case 'RUNNING':
        return (
          <span className="px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-blue-100 text-blue-800 border border-blue-200 flex items-center space-x-1">
            <RefreshCw className="w-3 h-3 text-blue-600 animate-spin" />
            <span>RUNNING</span>
          </span>
        );
      case 'FAILED':
        return (
          <span className="px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-rose-100 text-rose-800 border border-rose-200 flex items-center space-x-1">
            <XCircle className="w-3 h-3 text-rose-600" />
            <span>FAILED</span>
          </span>
        );
      case 'UPSTREAM_FAILED':
        return (
          <span className="px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-amber-100 text-amber-800 border border-amber-200 flex items-center space-x-1">
            <AlertTriangle className="w-3 h-3 text-amber-600" />
            <span>UPSTREAM_FAILED</span>
          </span>
        );
      default:
        return (
          <span className="px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-slate-100 text-slate-500 border border-slate-200 flex items-center space-x-1">
            <Clock className="w-3 h-3 text-slate-400" />
            <span>QUEUED</span>
          </span>
        );
    }
  };

  return (
    <div id="pipeline-status-view" className="space-y-6 max-w-5xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 pb-2 border-b border-slate-200">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight flex items-center space-x-2">
            <Workflow className="w-6 h-6 text-indigo-600" />
            <span>Apache Airflow Orchestration & Pipeline Engine</span>
          </h1>
          <p className="text-slate-500 text-sm mt-1">
            Phase 4 Central Workflow Orchestrator (`cloud_cost_etl_pipeline` DAG).
          </p>
        </div>
      </div>

      {datasets.length === 0 ? (
        <div className="bg-white border border-slate-200 rounded-xl p-10 text-center space-y-3 shadow-sm">
          <AlertCircle className="w-10 h-10 text-amber-500 mx-auto" />
          <p className="text-slate-900 font-bold text-sm">No Datasets Available for Pipeline Execution</p>
          <p className="text-slate-500 text-xs">Upload a dataset first to trigger the Airflow ETL pipeline.</p>
        </div>
      ) : (
        <>
          {/* Dataset Selector & Trigger Bar */}
          <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4 shadow-sm">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
              <div>
                <label htmlFor="pipeline-dataset-select" className="text-xs font-bold text-slate-700 block mb-1 uppercase tracking-wider">
                  Select Target Dataset:
                </label>
                <div className="flex items-center space-x-2">
                  <select
                    id="pipeline-dataset-select"
                    value={selectedDatasetId}
                    onChange={(e) => setSelectedDatasetId(e.target.value)}
                    className="bg-white border border-slate-300 text-slate-800 text-xs font-semibold rounded-lg px-4 py-2.5 w-full sm:w-80 focus:outline-none focus:border-indigo-500 shadow-2xs"
                  >
                    {datasets.map((d) => (
                      <option key={d.id} value={d.id}>
                        {d.original_filename} ({d.id})
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={loadStatus}
                    className="p-2.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg text-xs font-bold transition"
                    title="Refresh Pipeline Status"
                  >
                    <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
                  </button>
                </div>
              </div>

              {/* Action Button */}
              <div className="flex flex-col items-end space-y-1">
                <button
                  id="btn-process-pipeline"
                  onClick={handleRunPipeline}
                  disabled={isProcessing || statusData?.status === 'RUNNING'}
                  className={`px-5 py-3 rounded-lg font-bold text-xs shadow-md transition flex items-center space-x-2 ${
                    isProcessing || statusData?.status === 'RUNNING'
                      ? 'bg-indigo-400 text-white cursor-not-allowed'
                      : 'bg-indigo-600 hover:bg-indigo-700 text-white active:scale-98'
                  }`}
                >
                  <Play className={`w-4 h-4 fill-current ${isProcessing ? 'animate-spin' : ''}`} />
                  <span>{isProcessing || statusData?.status === 'RUNNING' ? 'Airflow DAG Running...' : 'Trigger Airflow Pipeline DAG'}</span>
                </button>
                <span className="text-[10px] text-slate-400">DAG ID: cloud_cost_etl_pipeline</span>
              </div>
            </div>

            {actionMessage && (
              <div className={`p-3 rounded-lg text-xs font-medium border ${actionMessage.includes('failed') ? 'bg-rose-50 border-rose-200 text-rose-800' : 'bg-indigo-50 border-indigo-200 text-indigo-900'}`}>
                {actionMessage}
              </div>
            )}

            {/* Current Stage Banner */}
            {statusData && (
              <div className="p-4 bg-slate-50 border border-slate-200 rounded-xl space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold text-indigo-700 flex items-center space-x-1.5">
                    <Zap className="w-3.5 h-3.5" />
                    <span>Active DAG Stage: {statusData.current_stage}</span>
                  </span>
                  <div className="flex items-center space-x-2">
                    {statusData.dag_run_id && (
                      <span className="font-mono text-[10px] bg-slate-200 text-slate-700 px-2 py-0.5 rounded font-semibold">
                        {statusData.dag_run_id}
                      </span>
                    )}
                    <span className={`px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider ${
                      isCompleted ? 'bg-emerald-100 text-emerald-800 border border-emerald-200' :
                      statusData.status === 'FAILED' ? 'bg-rose-100 text-rose-800 border border-rose-200' :
                      'bg-indigo-100 text-indigo-800 border border-indigo-200'
                    }`}>
                      {statusData.status}
                    </span>
                  </div>
                </div>
                <p className="text-xs text-slate-600 leading-relaxed">{statusData.message}</p>
              </div>
            )}
          </div>

          {/* Records Summary Metrics Grid */}
          {statusData && (
            <div className="space-y-3">
              <div className="grid grid-cols-2 sm:grid-cols-6 gap-3">
                <div className="bg-white border border-slate-200 rounded-xl p-3.5 shadow-2xs space-y-1">
                  <span className="text-[10px] font-bold text-slate-500 uppercase block">Raw Input</span>
                  <span className="text-xl font-extrabold text-slate-900">{statusData.input_records || 0}</span>
                  <span className="text-[10px] text-slate-400 block">Staging CSV</span>
                </div>

                <div className="bg-white border border-slate-200 rounded-xl p-3.5 shadow-2xs space-y-1">
                  <span className="text-[10px] font-bold text-amber-700 uppercase block">Bronze Raw</span>
                  <span className="text-xl font-extrabold text-amber-600">{statusData.bronze_records || 0}</span>
                  <span className="text-[10px] text-amber-800 block font-medium">Parquet Storage</span>
                </div>

                <div className="bg-white border border-slate-200 rounded-xl p-3.5 shadow-2xs space-y-1">
                  <span className="text-[10px] font-bold text-emerald-700 uppercase block">Valid Rows</span>
                  <span className="text-xl font-extrabold text-emerald-600">{statusData.valid_records || 0}</span>
                  <span className="text-[10px] text-emerald-800 block font-medium">Passed Quality</span>
                </div>

                <div className="bg-white border border-slate-200 rounded-xl p-3.5 shadow-2xs space-y-1">
                  <span className="text-[10px] font-bold text-rose-700 uppercase block font-sans">Quarantined</span>
                  <span className="text-xl font-extrabold text-rose-600">{statusData.quarantined_records || 0}</span>
                  <span className="text-[10px] text-rose-800 block font-medium">Isolated Invalid</span>
                </div>

                <div className="bg-white border border-slate-200 rounded-xl p-3.5 shadow-2xs space-y-1">
                  <span className="text-[10px] font-bold text-blue-700 uppercase block">Silver Clean</span>
                  <span className="text-xl font-extrabold text-blue-600">{statusData.silver_records || 0}</span>
                  <span className="text-[10px] text-blue-800 block font-medium">Feature Eng.</span>
                </div>

                <div className="bg-white border border-slate-200 rounded-xl p-3.5 shadow-2xs space-y-1">
                  <span className="text-[10px] font-bold text-indigo-700 uppercase block">Gold Summary</span>
                  <span className="text-xl font-extrabold text-indigo-600">{statusData.gold_records || 0}</span>
                  <span className="text-[10px] text-indigo-800 block font-medium">Gold Warehouse</span>
                </div>
              </div>

              {/* Streaming Stats Bar */}
              <div className="bg-gradient-to-r from-slate-900 to-indigo-950 text-white rounded-xl p-4 border border-indigo-800/40 flex flex-col sm:flex-row sm:items-center justify-between gap-3 text-xs">
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-emerald-400 animate-ping" />
                  <span className="font-bold text-white uppercase tracking-wider">Phase 3 Real-time CDC Stream Status:</span>
                </div>
                <div className="flex items-center gap-4 font-mono">
                  <span>Kafka Events: <strong className="text-indigo-300">{statusData.kafka_events || 0}</strong></span>
                  <span>Merged: <strong className="text-emerald-400">{statusData.processed || 0}</strong></span>
                  <span>Duplicates Filtered: <strong className="text-amber-300">{statusData.duplicates || 0}</strong></span>
                  <span>DLQ Invalid: <strong className="text-rose-400">{statusData.failed || 0}</strong></span>
                </div>
              </div>
            </div>
          )}

          {/* Detailed 12-Task Airflow DAG Execution Lifecycle */}
          <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4 shadow-sm">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-bold text-slate-900 uppercase tracking-wider flex items-center space-x-2">
                <Workflow className="w-4 h-4 text-indigo-600" />
                <span>12-Task Airflow DAG Execution Pipeline</span>
              </h3>
              <span className="text-xs text-slate-500 font-mono">DAG: cloud_cost_etl_pipeline</span>
            </div>

            <div className="space-y-2">
              {tasksToDisplay.map((task, idx) => (
                <div
                  key={task.task_id}
                  className={`p-3.5 rounded-xl border flex items-center justify-between transition ${
                    task.status === 'SUCCESS'
                      ? 'bg-emerald-50/40 border-emerald-200 text-slate-900'
                      : task.status === 'RUNNING'
                      ? 'bg-blue-50/60 border-blue-300 text-slate-900 shadow-2xs'
                      : task.status === 'FAILED'
                      ? 'bg-rose-50/60 border-rose-300 text-slate-900'
                      : task.status === 'UPSTREAM_FAILED'
                      ? 'bg-amber-50/40 border-amber-200 text-slate-700'
                      : 'bg-slate-50/50 border-slate-200 text-slate-500'
                  }`}
                >
                  <div className="flex items-center space-x-3">
                    <div className={`w-7 h-7 rounded-full flex items-center justify-center font-bold text-xs ${
                      task.status === 'SUCCESS' ? 'bg-emerald-600 text-white' :
                      task.status === 'RUNNING' ? 'bg-blue-600 text-white animate-pulse' :
                      task.status === 'FAILED' ? 'bg-rose-600 text-white' :
                      'bg-slate-200 text-slate-600'
                    }`}>
                      {idx + 1}
                    </div>
                    <div>
                      <span className="text-xs font-bold font-mono text-slate-900">{task.task_id}</span>
                      {task.error_message && (
                        <p className="text-[11px] text-rose-600 mt-0.5 font-medium">{task.error_message}</p>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center space-x-3">
                    {task.duration_seconds > 0 && (
                      <span className="text-[10px] text-slate-400 font-mono">
                        {task.duration_seconds.toFixed(2)}s
                      </span>
                    )}
                    {getTaskBadge(task.status)}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Databricks Target Schema Details */}
          <div className="bg-slate-900 text-slate-200 border border-slate-800 rounded-xl p-6 space-y-4 shadow-xl">
            <div className="flex items-center space-x-2">
              <Cpu className="w-5 h-5 text-amber-400" />
              <h3 className="font-bold text-white text-sm uppercase tracking-wider">Databricks Delta Lake Catalog Projection</h3>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
              <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg space-y-1">
                <span className="text-[10px] text-amber-400 font-bold uppercase block">Catalog & Schema</span>
                <p className="font-mono text-slate-200 font-semibold">cloud_cost_catalog.cloud_analytics</p>
              </div>

              <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg space-y-1">
                <span className="text-[10px] text-blue-400 font-bold uppercase block">Synchronized Delta & Gold Warehouse Tables</span>
                <p className="font-mono text-slate-300 text-[11px]">
                  bronze_cloud_cost_raw, silver_cloud_cost_clean, gold_cloud_cost_summary, quarantine_cloud_cost_records, airflow_task_instances
                </p>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
};
