import React, { useState, useEffect } from 'react';
import {
  Radio,
  Zap,
  RefreshCw,
  CheckCircle,
  AlertTriangle,
  XCircle,
  Copy,
  Clock,
  Database,
  ArrowRight,
  Sliders,
  Send,
  Eye,
  ShieldCheck,
  Activity,
  Layers,
  Search,
  Filter,
  Check,
  Info
} from 'lucide-react';
import {
  KafkaStatus,
  KafkaMetrics,
  EventStatus,
  CdcRecordHistory,
  CloudCostEventRequest
} from '../types';
import {
  fetchKafkaStatus,
  fetchKafkaMetrics,
  fetchCdcAuditLogs,
  publishCloudCostEvent,
  fetchCdcRecordHistory
} from '../services/api';

interface StreamingCdcViewProps {
  selectedDatasetId?: string;
  onRefreshPipeline?: () => void;
}

export const StreamingCdcView: React.FC<StreamingCdcViewProps> = ({
  selectedDatasetId = 'ds_sample_test',
  onRefreshPipeline
}) => {
  const [kafkaStatus, setKafkaStatus] = useState<KafkaStatus | null>(null);
  const [kafkaMetrics, setKafkaMetrics] = useState<KafkaMetrics | null>(null);
  const [auditLogs, setAuditLogs] = useState<EventStatus[]>([]);
  const [statusFilter, setStatusFilter] = useState<string>('ALL');
  const [loading, setLoading] = useState<boolean>(true);
  const [publishing, setPublishing] = useState<boolean>(false);
  const [lastResponse, setLastResponse] = useState<any | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Inspector State
  const [inspectKey, setInspectKey] = useState<string | null>(null);
  const [historyData, setHistoryData] = useState<CdcRecordHistory | null>(null);
  const [loadingHistory, setLoadingHistory] = useState<boolean>(false);

  // Event Form State
  const [operation, setOperation] = useState<'INSERT' | 'UPDATE' | 'DELETE'>('INSERT');
  const [eventIdOverride, setEventIdOverride] = useState<string>('');
  const [eventTimestamp, setEventTimestamp] = useState<string>('');
  
  const [formData, setFormData] = useState({
    date: new Date().toISOString().split('T')[0],
    cloud_provider: 'AWS',
    account_id: '123456789012',
    project_id: 'prj-data-warehouse-prod',
    environment: 'production',
    region: 'us-east-1',
    service: 'AmazonEC2',
    resource_type: 'Compute',
    usage_quantity: 720,
    usage_unit: 'Hrs',
    list_cost: 1440.0,
    net_cost: 1224.0,
    budget_amount: 40000.0,
    currency: 'USD'
  });

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);
      const [kStatus, kMetrics, aLogs] = await Promise.all([
        fetchKafkaStatus(),
        fetchKafkaMetrics(),
        fetchCdcAuditLogs(50, statusFilter === 'ALL' ? undefined : statusFilter)
      ]);
      setKafkaStatus(kStatus);
      setKafkaMetrics(kMetrics);
      setAuditLogs(aLogs);
    } catch (err: any) {
      setError(err.message || 'Failed to load Kafka streaming state');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 5000); // Poll every 5s for real-time feel
    return () => clearInterval(interval);
  }, [statusFilter]);

  const handleInputChange = (field: string, val: any) => {
    setFormData(prev => ({
      ...prev,
      [field]: val
    }));
  };

  const handlePublish = async () => {
    try {
      setPublishing(true);
      setError(null);
      
      const payload: CloudCostEventRequest = {
        event_id: eventIdOverride.trim() || undefined,
        dataset_id: selectedDatasetId,
        operation: operation,
        event_timestamp: eventTimestamp.trim() || undefined,
        record: formData
      };

      const res = await publishCloudCostEvent(payload);
      setLastResponse(res);
      await loadData();
      if (onRefreshPipeline) onRefreshPipeline();
    } catch (err: any) {
      setError(err.message || 'Failed to publish streaming event');
    } finally {
      setPublishing(false);
    }
  };

  const applyPreset = (type: 'AWS_INSERT' | 'GCP_UPDATE' | 'AZURE_DELETE' | 'DUPLICATE' | 'MALFORMED' | 'OUT_OF_ORDER') => {
    const nowISO = new Date().toISOString();
    if (type === 'AWS_INSERT') {
      setOperation('INSERT');
      setEventIdOverride(`evt-aws-${Date.now().toString().slice(-6)}`);
      setEventTimestamp(nowISO);
      setFormData({
        date: new Date().toISOString().split('T')[0],
        cloud_provider: 'AWS',
        account_id: '123456789012',
        project_id: 'prj-analytics-prod',
        environment: 'production',
        region: 'us-east-1',
        service: 'AmazonEC2',
        resource_type: 'Compute',
        usage_quantity: 720,
        usage_unit: 'Hrs',
        list_cost: 1440.0,
        net_cost: 1224.0,
        budget_amount: 50000.0,
        currency: 'USD'
      });
    } else if (type === 'GCP_UPDATE') {
      setOperation('UPDATE');
      setEventIdOverride(`evt-gcp-${Date.now().toString().slice(-6)}`);
      setEventTimestamp(nowISO);
      setFormData({
        date: new Date().toISOString().split('T')[0],
        cloud_provider: 'AWS', // Match AWS business key to update it!
        account_id: '123456789012',
        project_id: 'prj-analytics-prod',
        environment: 'production',
        region: 'us-east-1',
        service: 'AmazonEC2',
        resource_type: 'Compute',
        usage_quantity: 850,
        usage_unit: 'Hrs',
        list_cost: 1700.0,
        net_cost: 1450.0, // Updated cost!
        budget_amount: 50000.0,
        currency: 'USD'
      });
    } else if (type === 'AZURE_DELETE') {
      setOperation('DELETE');
      setEventIdOverride(`evt-del-${Date.now().toString().slice(-6)}`);
      setEventTimestamp(nowISO);
      setFormData({
        date: new Date().toISOString().split('T')[0],
        cloud_provider: 'AWS',
        account_id: '123456789012',
        project_id: 'prj-analytics-prod',
        environment: 'production',
        region: 'us-east-1',
        service: 'AmazonEC2',
        resource_type: 'Compute',
        usage_quantity: 0,
        usage_unit: 'Hrs',
        list_cost: 0,
        net_cost: 0,
        budget_amount: 0,
        currency: 'USD'
      });
    } else if (type === 'DUPLICATE') {
      // Re-use an existing event ID if available
      const existingId = auditLogs.length > 0 ? auditLogs[0].event_id : 'evt-duplicate-preset';
      setOperation('INSERT');
      setEventIdOverride(existingId);
      setEventTimestamp(nowISO);
    } else if (type === 'MALFORMED') {
      setOperation('INSERT');
      setEventIdOverride(`evt-malformed-${Date.now().toString().slice(-4)}`);
      setEventTimestamp(nowISO);
      setFormData(prev => ({
        ...prev,
        project_id: '', // Empty project_id triggers DLQ validation error!
        net_cost: -500 // Negative cost!
      }));
    } else if (type === 'OUT_OF_ORDER') {
      setOperation('UPDATE');
      setEventIdOverride(`evt-ooo-${Date.now().toString().slice(-4)}`);
      // Timestamp from yesterday
      const yesterday = new Date(Date.now() - 86400000).toISOString();
      setEventTimestamp(yesterday);
    }
  };

  const handleInspectKey = async (bKey: string) => {
    try {
      setInspectKey(bKey);
      setLoadingHistory(true);
      const res = await fetchCdcRecordHistory(bKey, selectedDatasetId);
      setHistoryData(res);
    } catch (err: any) {
      console.error(err);
    } finally {
      setLoadingHistory(false);
    }
  };

  const getStatusBadge = (st: string) => {
    switch (st) {
      case 'SUCCESS':
        return <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-100 text-emerald-800 border border-emerald-200"><CheckCircle className="w-3 h-3" /> Processed & Merged</span>;
      case 'DUPLICATE_EVENT':
        return <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-amber-100 text-amber-800 border border-amber-200"><Copy className="w-3 h-3" /> Duplicate Skipped</span>;
      case 'DLQ_INVALID':
        return <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-rose-100 text-rose-800 border border-rose-200"><XCircle className="w-3 h-3" /> Routed to DLQ</span>;
      case 'OUT_OF_ORDER_SKIPPED':
        return <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-indigo-100 text-indigo-800 border border-indigo-200"><Clock className="w-3 h-3" /> Out-of-Order Skipped</span>;
      default:
        return <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-slate-100 text-slate-800 border border-slate-200"><Clock className="w-3 h-3" /> {st}</span>;
    }
  };

  return (
    <div className="space-y-6">
      {/* Non-Coder Explanation Banner */}
      <div className="bg-gradient-to-r from-slate-900 via-indigo-950 to-slate-900 text-white rounded-xl p-6 shadow-md border border-indigo-900/40">
        <div className="flex items-start gap-4">
          <div className="p-3 bg-indigo-500/20 rounded-lg text-indigo-400 border border-indigo-500/30">
            <Radio className="w-6 h-6 animate-pulse" />
          </div>
          <div className="space-y-1 flex-1">
            <div className="flex items-center gap-3">
              <h2 className="text-xl font-bold tracking-tight text-white">Phase 3 — Apache Kafka Real-Time Ingestion & CDC Pipeline</h2>
              <span className="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 flex items-center gap-1">
                <Activity className="w-3 h-3" /> Live Event Processor
              </span>
            </div>
            <p className="text-sm text-slate-300 leading-relaxed">
              Unlike batch CSV uploads, real-time cloud data continuously streams into <strong>Apache Kafka topics</strong>. 
              Our <strong>Change Data Capture (CDC) engine</strong> processes each <code>INSERT</code>, <code>UPDATE</code>, or <code>DELETE</code> event, 
              guarantees <strong>idempotency (no duplicates)</strong>, handles out-of-order logs, and executes <strong>Delta Lake MERGE operations</strong> on Databricks Silver tables instantly.
            </p>
          </div>
        </div>
      </div>

      {/* Error Alert */}
      {error && (
        <div className="p-4 bg-rose-50 border border-rose-200 text-rose-800 rounded-lg text-sm flex items-center gap-3">
          <AlertTriangle className="w-5 h-5 text-rose-600 flex-shrink-0" />
          <span className="flex-1">{error}</span>
        </div>
      )}

      {/* Infrastructure & Status Metrics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm space-y-2">
          <div className="flex items-center justify-between text-slate-500 text-xs font-semibold uppercase tracking-wider">
            <span>Kafka Connection</span>
            <Radio className={`w-4 h-4 ${kafkaStatus?.kafka_connected ? 'text-emerald-600' : 'text-amber-500'}`} />
          </div>
          <div className="text-lg font-bold text-slate-900 truncate">
            {kafkaStatus?.status || 'INITIALIZING'}
          </div>
          <div className="text-xs text-slate-500 flex items-center gap-1">
            <span className="font-mono text-indigo-600 font-semibold">{kafkaStatus?.topic || 'cloud-cost-events'}</span>
          </div>
        </div>

        <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm space-y-2">
          <div className="flex items-center justify-between text-slate-500 text-xs font-semibold uppercase tracking-wider">
            <span>Events Received</span>
            <Zap className="w-4 h-4 text-indigo-600" />
          </div>
          <div className="text-2xl font-black text-slate-900">
            {kafkaMetrics?.received_count || 0}
          </div>
          <div className="text-xs text-slate-500">
            Success: <span className="font-semibold text-emerald-600">{kafkaMetrics?.success_count || 0}</span> | DLQ: <span className="font-semibold text-rose-600">{kafkaMetrics?.dlq_count || 0}</span>
          </div>
        </div>

        <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm space-y-2">
          <div className="flex items-center justify-between text-slate-500 text-xs font-semibold uppercase tracking-wider">
            <span>CDC Operations</span>
            <Layers className="w-4 h-4 text-blue-600" />
          </div>
          <div className="text-2xl font-black text-slate-900">
            {kafkaMetrics?.insert_count || 0} <span className="text-xs font-normal text-slate-500">Inserts</span>
          </div>
          <div className="text-xs text-slate-500">
            Updates: <span className="font-semibold text-blue-600">{kafkaMetrics?.update_count || 0}</span> | Deletes: <span className="font-semibold text-purple-600">{kafkaMetrics?.delete_count || 0}</span>
          </div>
        </div>

        <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm space-y-2">
          <div className="flex items-center justify-between text-slate-500 text-xs font-semibold uppercase tracking-wider">
            <span>Idempotency & Speed</span>
            <ShieldCheck className="w-4 h-4 text-emerald-600" />
          </div>
          <div className="text-2xl font-black text-slate-900">
            {kafkaMetrics?.duplicate_count || 0} <span className="text-xs font-normal text-slate-500">Duplicates Filtered</span>
          </div>
          <div className="text-xs text-slate-500">
            Avg Processing Time: <span className="font-semibold text-slate-700">{kafkaMetrics?.avg_processing_time_ms || 0} ms</span>
          </div>
        </div>
      </div>

      {/* Main Grid: Producer Form & Presets */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Presets & Simulator Column */}
        <div className="lg:col-span-1 bg-white p-6 rounded-xl border border-slate-200 shadow-sm space-y-4">
          <div className="flex items-center gap-2 border-b border-slate-100 pb-3">
            <Sliders className="w-5 h-5 text-indigo-600" />
            <h3 className="font-bold text-slate-900 text-base">Quick Streaming Presets</h3>
          </div>

          <p className="text-xs text-slate-500 leading-relaxed">
            Click any button below to instantly populate realistic cloud cost events and test pipeline behaviors:
          </p>

          <div className="space-y-2.5">
            <button
              type="button"
              onClick={() => applyPreset('AWS_INSERT')}
              className="w-full text-left p-3 rounded-lg border border-slate-200 hover:border-indigo-300 hover:bg-indigo-50/50 transition-colors flex items-center justify-between text-xs font-medium text-slate-800 group"
            >
              <div>
                <span className="font-semibold text-slate-900 block">1. Send AWS Insert Event</span>
                <span className="text-slate-500 text-[11px]">New EC2 compute log to Bronze</span>
              </div>
              <ArrowRight className="w-4 h-4 text-slate-400 group-hover:text-indigo-600" />
            </button>

            <button
              type="button"
              onClick={() => applyPreset('GCP_UPDATE')}
              className="w-full text-left p-3 rounded-lg border border-slate-200 hover:border-blue-300 hover:bg-blue-50/50 transition-colors flex items-center justify-between text-xs font-medium text-slate-800 group"
            >
              <div>
                <span className="font-semibold text-slate-900 block">2. Send Cost Update Event</span>
                <span className="text-slate-500 text-[11px]">Delta MERGE update in Silver</span>
              </div>
              <ArrowRight className="w-4 h-4 text-slate-400 group-hover:text-blue-600" />
            </button>

            <button
              type="button"
              onClick={() => applyPreset('AZURE_DELETE')}
              className="w-full text-left p-3 rounded-lg border border-slate-200 hover:border-purple-300 hover:bg-purple-50/50 transition-colors flex items-center justify-between text-xs font-medium text-slate-800 group"
            >
              <div>
                <span className="font-semibold text-slate-900 block">3. Send Resource Delete Event</span>
                <span className="text-slate-500 text-[11px]">Soft delete in Silver (is_deleted=true)</span>
              </div>
              <ArrowRight className="w-4 h-4 text-slate-400 group-hover:text-purple-600" />
            </button>

            <button
              type="button"
              onClick={() => applyPreset('DUPLICATE')}
              className="w-full text-left p-3 rounded-lg border border-slate-200 hover:border-amber-300 hover:bg-amber-50/50 transition-colors flex items-center justify-between text-xs font-medium text-slate-800 group"
            >
              <div>
                <span className="font-semibold text-slate-900 block">4. Test Idempotency (Duplicate)</span>
                <span className="text-slate-500 text-[11px]">Sends duplicate event ID</span>
              </div>
              <ArrowRight className="w-4 h-4 text-slate-400 group-hover:text-amber-600" />
            </button>

            <button
              type="button"
              onClick={() => applyPreset('MALFORMED')}
              className="w-full text-left p-3 rounded-lg border border-slate-200 hover:border-rose-300 hover:bg-rose-50/50 transition-colors flex items-center justify-between text-xs font-medium text-slate-800 group"
            >
              <div>
                <span className="font-semibold text-slate-900 block">5. Test Dead Letter Queue (DLQ)</span>
                <span className="text-slate-500 text-[11px]">Missing project_id & negative cost</span>
              </div>
              <ArrowRight className="w-4 h-4 text-slate-400 group-hover:text-rose-600" />
            </button>

            <button
              type="button"
              onClick={() => applyPreset('OUT_OF_ORDER')}
              className="w-full text-left p-3 rounded-lg border border-slate-200 hover:border-indigo-300 hover:bg-indigo-50/50 transition-colors flex items-center justify-between text-xs font-medium text-slate-800 group"
            >
              <div>
                <span className="font-semibold text-slate-900 block">6. Test Out-of-Order Log</span>
                <span className="text-slate-500 text-[11px]">Older timestamp event skipped</span>
              </div>
              <ArrowRight className="w-4 h-4 text-slate-400 group-hover:text-indigo-600" />
            </button>
          </div>
        </div>

        {/* Kafka Producer Form */}
        <div className="lg:col-span-2 bg-white p-6 rounded-xl border border-slate-200 shadow-sm space-y-4">
          <div className="flex items-center justify-between border-b border-slate-100 pb-3">
            <div className="flex items-center gap-2">
              <Send className="w-5 h-5 text-indigo-600" />
              <h3 className="font-bold text-slate-900 text-base">Kafka Event Producer Form</h3>
            </div>
            <span className="text-xs text-slate-500 font-mono">Topic: {kafkaStatus?.topic || 'cloud-cost-events'}</span>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1">CDC Operation</label>
              <select
                value={operation}
                onChange={(e) => setOperation(e.target.value as any)}
                className="w-full text-xs p-2 rounded-lg border border-slate-300 bg-slate-50 focus:bg-white focus:ring-2 focus:ring-indigo-500 focus:outline-none font-bold"
              >
                <option value="INSERT">INSERT (New Record)</option>
                <option value="UPDATE">UPDATE (Cost/Usage Merge)</option>
                <option value="DELETE">DELETE (Soft Delete)</option>
              </select>
            </div>

            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1">Event ID (Optional)</label>
              <input
                type="text"
                value={eventIdOverride}
                onChange={(e) => setEventIdOverride(e.target.value)}
                placeholder="Auto-generated if blank"
                className="w-full text-xs p-2 rounded-lg border border-slate-300 bg-slate-50 focus:bg-white font-mono"
              />
            </div>

            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1">Event Timestamp</label>
              <input
                type="text"
                value={eventTimestamp}
                onChange={(e) => setEventTimestamp(e.target.value)}
                placeholder="ISO timestamp (Optional)"
                className="w-full text-xs p-2 rounded-lg border border-slate-300 bg-slate-50 focus:bg-white font-mono"
              />
            </div>
          </div>

          <div className="border-t border-slate-100 pt-3">
            <label className="block text-xs font-bold text-slate-800 mb-2">Record Dimension Attributes (Business Key Factors)</label>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
              <div>
                <label className="block text-[11px] text-slate-500">Cloud Provider</label>
                <select
                  value={formData.cloud_provider}
                  onChange={(e) => handleInputChange('cloud_provider', e.target.value)}
                  className="w-full text-xs p-1.5 rounded border border-slate-200"
                >
                  <option value="AWS">AWS</option>
                  <option value="GCP">GCP</option>
                  <option value="Azure">Azure</option>
                </select>
              </div>

              <div>
                <label className="block text-[11px] text-slate-500">Account ID</label>
                <input
                  type="text"
                  value={formData.account_id}
                  onChange={(e) => handleInputChange('account_id', e.target.value)}
                  className="w-full text-xs p-1.5 rounded border border-slate-200 font-mono"
                />
              </div>

              <div>
                <label className="block text-[11px] text-slate-500">Project ID</label>
                <input
                  type="text"
                  value={formData.project_id}
                  onChange={(e) => handleInputChange('project_id', e.target.value)}
                  className="w-full text-xs p-1.5 rounded border border-slate-200 font-mono"
                />
              </div>

              <div>
                <label className="block text-[11px] text-slate-500">Environment</label>
                <input
                  type="text"
                  value={formData.environment}
                  onChange={(e) => handleInputChange('environment', e.target.value)}
                  className="w-full text-xs p-1.5 rounded border border-slate-200"
                />
              </div>

              <div>
                <label className="block text-[11px] text-slate-500">Region</label>
                <input
                  type="text"
                  value={formData.region}
                  onChange={(e) => handleInputChange('region', e.target.value)}
                  className="w-full text-xs p-1.5 rounded border border-slate-200 font-mono"
                />
              </div>

              <div>
                <label className="block text-[11px] text-slate-500">Service</label>
                <input
                  type="text"
                  value={formData.service}
                  onChange={(e) => handleInputChange('service', e.target.value)}
                  className="w-full text-xs p-1.5 rounded border border-slate-200"
                />
              </div>

              <div>
                <label className="block text-[11px] text-slate-500">Usage Quantity</label>
                <input
                  type="number"
                  value={formData.usage_quantity}
                  onChange={(e) => handleInputChange('usage_quantity', parseFloat(e.target.value) || 0)}
                  className="w-full text-xs p-1.5 rounded border border-slate-200 font-semibold text-slate-800"
                />
              </div>

              <div>
                <label className="block text-[11px] text-slate-500">Net Cost ($)</label>
                <input
                  type="number"
                  value={formData.net_cost}
                  onChange={(e) => handleInputChange('net_cost', parseFloat(e.target.value) || 0)}
                  className="w-full text-xs p-1.5 rounded border border-slate-200 font-semibold text-indigo-700"
                />
              </div>
            </div>
          </div>

          <div className="flex items-center justify-between pt-2">
            <button
              type="button"
              onClick={handlePublish}
              disabled={publishing}
              className="px-5 py-2.5 rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white font-semibold text-xs flex items-center gap-2 shadow-sm disabled:opacity-50 transition-colors"
            >
              {publishing ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
              <span>{publishing ? 'Publishing to Kafka...' : 'Publish Event to Kafka'}</span>
            </button>

            {lastResponse && (
              <div className="text-xs text-slate-600 flex items-center gap-2 bg-slate-50 px-3 py-1.5 rounded border border-slate-200 font-mono">
                <Check className="w-3.5 h-3.5 text-emerald-600" />
                <span>Result: {lastResponse.consumer_result?.status || lastResponse.status}</span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* CDC Audit Log Table */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden space-y-0">
        <div className="p-5 border-b border-slate-200 flex flex-col sm:flex-row sm:items-center justify-between gap-4 bg-slate-50/50">
          <div>
            <h3 className="font-bold text-slate-900 text-base flex items-center gap-2">
              <Database className="w-5 h-5 text-indigo-600" />
              <span>Kafka Event Audit Log & CDC Pipeline Execution History</span>
            </h3>
            <p className="text-xs text-slate-500 mt-0.5">
              Every event consumed from Kafka is recorded in the <code>kafka_event_audit</code> ledger with exact execution timestamp & status.
            </p>
          </div>

          <div className="flex items-center gap-2">
            <Filter className="w-4 h-4 text-slate-400" />
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="text-xs p-2 rounded-lg border border-slate-300 bg-white font-semibold"
            >
              <option value="ALL">All Statuses</option>
              <option value="SUCCESS">SUCCESS</option>
              <option value="DUPLICATE_EVENT">DUPLICATE_EVENT</option>
              <option value="DLQ_INVALID">DLQ_INVALID</option>
              <option value="OUT_OF_ORDER_SKIPPED">OUT_OF_ORDER_SKIPPED</option>
            </select>

            <button
              onClick={loadData}
              className="p-2 text-slate-600 hover:text-slate-900 bg-white border border-slate-300 rounded-lg"
              title="Refresh Audit Log"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            </button>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="bg-slate-100/70 border-b border-slate-200 text-slate-600 font-semibold uppercase tracking-wider text-[11px]">
                <th className="p-3">Event ID</th>
                <th className="p-3">Operation</th>
                <th className="p-3">Business Key</th>
                <th className="p-3">Status</th>
                <th className="p-3">Received At</th>
                <th className="p-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 text-slate-700">
              {auditLogs.length === 0 ? (
                <tr>
                  <td colSpan={6} className="p-8 text-center text-slate-400">
                    No Kafka events consumed yet. Click a Quick Streaming Preset above to publish your first event!
                  </td>
                </tr>
              ) : (
                auditLogs.map((log) => (
                  <tr key={log.event_id + (log.processed_at || '')} className="hover:bg-slate-50/80 transition-colors">
                    <td className="p-3 font-mono font-semibold text-slate-900">{log.event_id}</td>
                    <td className="p-3">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-extrabold ${
                        log.operation === 'INSERT' ? 'bg-emerald-100 text-emerald-800' :
                        log.operation === 'UPDATE' ? 'bg-blue-100 text-blue-800' :
                        'bg-purple-100 text-purple-800'
                      }`}>
                        {log.operation}
                      </span>
                    </td>
                    <td className="p-3 font-mono text-slate-600 text-[11px] max-w-xs truncate" title={log.business_key}>
                      {log.business_key}
                    </td>
                    <td className="p-3">{getStatusBadge(log.status)}</td>
                    <td className="p-3 font-mono text-slate-500 text-[11px]">
                      {new Date(log.received_at).toLocaleTimeString()}
                    </td>
                    <td className="p-3 text-right">
                      <button
                        onClick={() => handleInspectKey(log.business_key)}
                        className="px-2.5 py-1 rounded bg-slate-100 hover:bg-indigo-50 text-slate-700 hover:text-indigo-700 border border-slate-200 font-semibold text-[11px] inline-flex items-center gap-1 transition-colors"
                      >
                        <Eye className="w-3.5 h-3.5" /> Inspect CDC
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* CDC Lineage Modal / Drawer */}
      {inspectKey && (
        <div className="fixed inset-0 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-2xl shadow-2xl border border-slate-200 w-full max-w-3xl overflow-hidden max-h-[85vh] flex flex-col">
            <div className="p-5 bg-slate-900 text-white flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Layers className="w-5 h-5 text-indigo-400" />
                <h3 className="font-bold text-base">CDC Delta State & Lineage Inspector</h3>
              </div>
              <button
                onClick={() => { setInspectKey(null); setHistoryData(null); }}
                className="p-1 rounded text-slate-400 hover:text-white"
              >
                ✕
              </button>
            </div>

            <div className="p-6 overflow-y-auto space-y-5 flex-1">
              <div>
                <label className="block text-xs font-semibold text-slate-500 uppercase">Target Business Key</label>
                <div className="font-mono text-xs font-bold text-indigo-900 bg-indigo-50 p-2.5 rounded-lg border border-indigo-200 mt-1 break-all">
                  {inspectKey}
                </div>
              </div>

              {loadingHistory ? (
                <div className="p-8 text-center text-slate-500 flex items-center justify-center gap-2">
                  <RefreshCw className="w-5 h-5 animate-spin text-indigo-600" />
                  <span>Fetching CDC history from Silver Delta Table...</span>
                </div>
              ) : historyData ? (
                <div className="space-y-4">
                  {/* Current State Summary */}
                  <div className="p-4 bg-slate-50 rounded-xl border border-slate-200 space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="font-bold text-slate-800 text-sm">Current Delta Lake Silver Record State</span>
                      {historyData.is_deleted ? (
                        <span className="px-2.5 py-0.5 rounded-full text-xs font-bold bg-rose-100 text-rose-800">Soft Deleted (is_deleted=true)</span>
                      ) : (
                        <span className="px-2.5 py-0.5 rounded-full text-xs font-bold bg-emerald-100 text-emerald-800">Active Record</span>
                      )}
                    </div>

                    {historyData.current_state ? (
                      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs pt-2">
                        <div>
                          <span className="text-slate-400 text-[10px] block">Net Cost</span>
                          <span className="font-mono font-bold text-indigo-700 text-sm">${historyData.current_state.net_cost}</span>
                        </div>
                        <div>
                          <span className="text-slate-400 text-[10px] block">Usage Quantity</span>
                          <span className="font-mono font-bold text-slate-800">{historyData.current_state.usage_quantity}</span>
                        </div>
                        <div>
                          <span className="text-slate-400 text-[10px] block">Last Operation</span>
                          <span className="font-mono font-bold text-slate-800">{historyData.current_state.last_operation}</span>
                        </div>
                        <div>
                          <span className="text-slate-400 text-[10px] block">Updated At</span>
                          <span className="font-mono text-slate-600 text-[11px]">{new Date(historyData.current_state.updated_at).toLocaleTimeString()}</span>
                        </div>
                      </div>
                    ) : (
                      <div className="text-xs text-slate-400">No active state in Silver layer.</div>
                    )}
                  </div>

                  {/* Audit Event Lineage List */}
                  <div>
                    <h4 className="font-bold text-slate-900 text-xs uppercase tracking-wider mb-2">CDC Event Lineage History ({historyData.record_count} Events)</h4>
                    <div className="space-y-2">
                      {historyData.history.map((h, i) => (
                        <div key={i} className="p-3 rounded-lg border border-slate-200 bg-white flex items-center justify-between text-xs">
                          <div className="flex items-center gap-3">
                            <span className="font-mono text-slate-400 text-[11px]">#{i+1}</span>
                            <span className={`px-2 py-0.5 rounded text-[10px] font-extrabold ${
                              h.operation === 'INSERT' ? 'bg-emerald-100 text-emerald-800' :
                              h.operation === 'UPDATE' ? 'bg-blue-100 text-blue-800' :
                              'bg-purple-100 text-purple-800'
                            }`}>
                              {h.operation}
                            </span>
                            <span className="font-mono text-slate-700">{h.event_id}</span>
                          </div>
                          <div className="font-mono text-slate-500 text-[11px]">
                            {new Date(h.event_timestamp || h.received_at).toLocaleString()}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              ) : null}
            </div>

            <div className="p-4 bg-slate-50 border-t border-slate-200 text-right">
              <button
                onClick={() => { setInspectKey(null); setHistoryData(null); }}
                className="px-4 py-2 bg-slate-800 hover:bg-slate-900 text-white rounded-lg font-semibold text-xs"
              >
                Close Inspector
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
