import React, { useEffect, useState } from 'react';
import { fetchDataQualitySummary, fetchQuarantineRecords } from '../services/api';
import { DataQualitySummary, QuarantineRecord, Dataset } from '../types';
import { ShieldCheck, ShieldAlert, AlertTriangle, CheckCircle2, XCircle, FileSearch, RefreshCw, Layers } from 'lucide-react';

interface DataQualityViewProps {
  datasets: Dataset[];
}

export const DataQualityView: React.FC<DataQualityViewProps> = ({ datasets }) => {
  const [selectedDatasetId, setSelectedDatasetId] = useState<string>(datasets[0]?.id || '');
  const [summary, setSummary] = useState<DataQualitySummary | null>(null);
  const [quarantine, setQuarantine] = useState<QuarantineRecord[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [selectedQuarantineRecord, setSelectedQuarantineRecord] = useState<QuarantineRecord | null>(null);

  const loadData = async () => {
    if (!selectedDatasetId) return;
    setIsLoading(true);
    setErrorMsg(null);
    try {
      const [sumData, qData] = await Promise.all([
        fetchDataQualitySummary(selectedDatasetId),
        fetchQuarantineRecords(selectedDatasetId, 50).catch(() => [])
      ]);
      setSummary(sumData);
      setQuarantine(qData);
    } catch (err: any) {
      setSummary(null);
      setQuarantine([]);
      setErrorMsg(err.message || 'Data quality execution summary not found for this dataset. Please run the ETL pipeline first.');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadData();
  }, [selectedDatasetId]);

  return (
    <div id="data-quality-view" className="space-y-6 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-2 border-b border-slate-200">
        <div>
          <div className="flex items-center space-x-2">
            <ShieldCheck className="w-6 h-6 text-blue-600" />
            <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Data Quality & Validation Engine</h1>
          </div>
          <p className="text-slate-500 text-sm mt-1">
            Automated schema validation, null value auditing, range checks, deduplication, and quarantine management.
          </p>
        </div>

        {/* Dataset Selector */}
        {datasets.length > 0 && (
          <div className="flex items-center space-x-2">
            <select
              id="dq-dataset-select"
              value={selectedDatasetId}
              onChange={(e) => setSelectedDatasetId(e.target.value)}
              className="bg-white border border-slate-300 text-slate-800 text-xs font-semibold rounded-lg px-4 py-2.5 shadow-2xs focus:outline-none focus:border-blue-500"
            >
              {datasets.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.original_filename} ({d.id})
                </option>
              ))}
            </select>
            <button
              onClick={loadData}
              disabled={isLoading}
              className="p-2.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg text-xs font-bold transition flex items-center"
              title="Refresh Data Quality Checks"
            >
              <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
            </button>
          </div>
        )}
      </div>

      {errorMsg ? (
        <div className="bg-amber-50 border border-amber-200 rounded-xl p-8 text-center space-y-3">
          <ShieldAlert className="w-10 h-10 text-amber-500 mx-auto" />
          <h3 className="text-slate-900 font-bold text-sm">ETL Pipeline Pending Execution</h3>
          <p className="text-amber-800 text-xs max-w-md mx-auto">{errorMsg}</p>
        </div>
      ) : summary ? (
        <>
          {/* Top KPI Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-2">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Overall Quality Score</span>
              <div className="flex items-baseline space-x-2">
                <span className={`text-3xl font-extrabold ${summary.quality_score >= 90 ? 'text-emerald-600' : summary.quality_score >= 70 ? 'text-amber-600' : 'text-rose-600'}`}>
                  {summary.quality_score}%
                </span>
                <span className="text-xs text-slate-400">cleanliness</span>
              </div>
              <div className="w-full bg-slate-100 h-1.5 rounded-full overflow-hidden">
                <div
                  className={`h-full ${summary.quality_score >= 90 ? 'bg-emerald-500' : summary.quality_score >= 70 ? 'bg-amber-500' : 'bg-rose-500'}`}
                  style={{ width: `${summary.quality_score}%` }}
                />
              </div>
            </div>

            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-2">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Total Records Checked</span>
              <span className="text-3xl font-extrabold text-slate-900">{summary.total_records.toLocaleString()}</span>
              <span className="text-xs text-slate-400 block">Bronze Raw Input</span>
            </div>

            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-2">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Passed Valid Records</span>
              <span className="text-3xl font-extrabold text-emerald-600">{summary.valid_records.toLocaleString()}</span>
              <span className="text-xs text-emerald-700 block font-medium">Passed to Silver Layer</span>
            </div>

            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-2">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Quarantined Records</span>
              <span className="text-3xl font-extrabold text-rose-600">{summary.quarantined_records.toLocaleString()}</span>
              <span className="text-xs text-rose-700 block font-medium">Isolated for Inspection</span>
            </div>
          </div>

          {/* 12 Data Quality Validation Checks Table */}
          <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4 shadow-sm">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-bold text-slate-900 uppercase tracking-wider">Automated Rule-Based Quality Audits</h3>
                <p className="text-xs text-slate-500">12 comprehensive data quality validation checks executed against raw input batch</p>
              </div>
              <span className="text-xs font-mono bg-slate-100 text-slate-700 px-3 py-1 rounded-md border border-slate-200 font-bold">
                Batch ID: {summary.batch_id}
              </span>
            </div>

            <div className="overflow-x-auto border border-slate-200 rounded-lg">
              <table className="w-full text-left text-xs text-slate-700">
                <thead className="bg-slate-50 border-b border-slate-200 text-[10px] uppercase tracking-wider text-slate-500 font-bold">
                  <tr>
                    <th className="p-3">Audit Check Rule</th>
                    <th className="p-3">Status</th>
                    <th className="p-3 text-right">Checked</th>
                    <th className="p-3 text-right">Failed</th>
                    <th className="p-3 text-right">Failure Rate</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {summary.checks.map((chk, idx) => (
                    <tr key={idx} className="hover:bg-slate-50/80 transition">
                      <td className="p-3 font-semibold text-slate-900">{chk.check_name}</td>
                      <td className="p-3">
                        {chk.status === 'PASS' ? (
                          <span className="inline-flex items-center space-x-1 px-2.5 py-0.5 rounded-full text-[10px] font-bold bg-emerald-100 text-emerald-800 border border-emerald-200">
                            <CheckCircle2 className="w-3 h-3" />
                            <span>PASS</span>
                          </span>
                        ) : (
                          <span className="inline-flex items-center space-x-1 px-2.5 py-0.5 rounded-full text-[10px] font-bold bg-rose-100 text-rose-800 border border-rose-200">
                            <XCircle className="w-3 h-3" />
                            <span>FAIL</span>
                          </span>
                        )}
                      </td>
                      <td className="p-3 text-right font-mono">{chk.records_checked.toLocaleString()}</td>
                      <td className={`p-3 text-right font-mono font-bold ${chk.records_failed > 0 ? 'text-rose-600' : 'text-slate-400'}`}>
                        {chk.records_failed.toLocaleString()}
                      </td>
                      <td className="p-3 text-right font-mono">
                        <span className={`px-2 py-0.5 rounded ${chk.failure_percentage > 0 ? 'bg-rose-50 text-rose-700 font-bold' : 'text-slate-400'}`}>
                          {chk.failure_percentage}%
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Quarantined Records Table */}
          <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4 shadow-sm">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-bold text-slate-900 uppercase tracking-wider flex items-center space-x-2">
                  <ShieldAlert className="w-4 h-4 text-rose-600" />
                  <span>Quarantined Records Table (`quarantine_cloud_cost_records`)</span>
                </h3>
                <p className="text-xs text-slate-500">
                  Records failing critical validation checks isolated from Silver layer processing
                </p>
              </div>
              <span className="text-xs font-bold text-rose-600 bg-rose-50 border border-rose-200 px-3 py-1 rounded-md">
                {quarantine.length} Isolated Records
              </span>
            </div>

            {quarantine.length === 0 ? (
              <div className="p-8 text-center bg-emerald-50/50 border border-emerald-200 rounded-lg space-y-1">
                <CheckCircle2 className="w-8 h-8 text-emerald-600 mx-auto" />
                <p className="text-slate-900 font-bold text-xs">Zero Quarantined Records!</p>
                <p className="text-slate-500 text-[11px]">All records passed quality checks cleanly into the Silver Delta Table.</p>
              </div>
            ) : (
              <div className="overflow-x-auto border border-slate-200 rounded-lg">
                <table className="w-full text-left text-xs text-slate-700">
                  <thead className="bg-slate-50 border-b border-slate-200 text-[10px] uppercase tracking-wider text-slate-500 font-bold">
                    <tr>
                      <th className="p-3">Failure Reason</th>
                      <th className="p-3">Deterministic Record Hash</th>
                      <th className="p-3">Quarantined At</th>
                      <th className="p-3 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 font-mono">
                    {quarantine.map((item) => (
                      <tr key={item.id} className="hover:bg-slate-50 transition">
                        <td className="p-3">
                          <span className="px-2.5 py-1 rounded text-[10px] font-bold bg-rose-100 text-rose-800 border border-rose-200">
                            {item.failure_reason}
                          </span>
                        </td>
                        <td className="p-3 text-slate-600 text-[11px]">
                          {item.record_hash ? item.record_hash.substring(0, 16) + '...' : 'N/A'}
                        </td>
                        <td className="p-3 text-slate-500 text-[11px]">
                          {new Date(item.failed_at).toLocaleString()}
                        </td>
                        <td className="p-3 text-right">
                          <button
                            onClick={() => setSelectedQuarantineRecord(item)}
                            className="inline-flex items-center space-x-1 text-xs font-sans font-bold text-blue-600 hover:text-blue-800 underline"
                          >
                            <FileSearch className="w-3.5 h-3.5" />
                            <span>Inspect Payload</span>
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      ) : null}

      {/* Payload Modal */}
      {selectedQuarantineRecord && (
        <div className="fixed inset-0 z-50 bg-slate-900/60 backdrop-blur-xs flex items-center justify-center p-4">
          <div className="bg-white rounded-xl border border-slate-200 p-6 max-w-xl w-full space-y-4 shadow-xl">
            <div className="flex items-center justify-between border-b pb-3">
              <h3 className="font-bold text-slate-900 text-sm flex items-center space-x-2">
                <AlertTriangle className="w-4 h-4 text-rose-600" />
                <span>Quarantined Record Inspection</span>
              </h3>
              <button
                onClick={() => setSelectedQuarantineRecord(null)}
                className="text-slate-400 hover:text-slate-600 text-xs font-bold"
              >
                ✕ Close
              </button>
            </div>
            <div className="space-y-2">
              <div className="flex justify-between text-xs">
                <span className="text-slate-500 font-medium">Failure Reason:</span>
                <span className="font-bold text-rose-600">{selectedQuarantineRecord.failure_reason}</span>
              </div>
              <div className="flex justify-between text-xs font-mono">
                <span className="text-slate-500">Record Hash:</span>
                <span className="text-slate-800 text-[11px]">{selectedQuarantineRecord.record_hash}</span>
              </div>
              <div className="mt-2">
                <label className="text-[10px] font-bold text-slate-500 uppercase block mb-1">Raw Record Payload (JSON):</label>
                <pre className="bg-slate-900 text-emerald-400 p-3 rounded-lg text-xs overflow-x-auto font-mono max-h-60">
                  {JSON.stringify(JSON.parse(selectedQuarantineRecord.original_record || '{}'), null, 2)}
                </pre>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
