import React from 'react';
import { Dataset } from '../types';
import { UploadCloud, FileSpreadsheet, Layers, Clock, ArrowUpRight, Eye, RefreshCw, Server, Database, Activity } from 'lucide-react';

interface DashboardViewProps {
  datasets: Dataset[];
  onNavigateUpload: () => void;
  onPreviewDataset: (datasetId: string) => void;
  onRefresh: () => void;
  isLoading: boolean;
}

export const DashboardView: React.FC<DashboardViewProps> = ({
  datasets,
  onNavigateUpload,
  onPreviewDataset,
  onRefresh,
  isLoading,
}) => {
  const totalRows = datasets.reduce((acc, d) => acc + d.row_count, 0);
  const totalCols = datasets.length > 0 ? Math.max(...datasets.map(d => d.column_count)) : 0;

  return (
    <div id="dashboard-view" className="space-y-6">
      {/* Top Banner & Title */}
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 pb-2 border-b border-slate-200">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Executive Cost Data Control Center</h1>
          <p className="text-slate-500 text-sm mt-1">
            Automated dataset ingestion, validation, and metadata tracking for multi-cloud budget streams.
          </p>
        </div>

        <div className="flex items-center space-x-3 shrink-0">
          <button
            id="refresh-datasets-btn"
            onClick={onRefresh}
            disabled={isLoading}
            className="flex items-center space-x-2 px-3.5 py-2 rounded-lg text-xs font-semibold bg-white text-slate-700 hover:bg-slate-50 border border-slate-200 shadow-sm transition"
          >
            <RefreshCw className={`w-3.5 h-3.5 text-slate-500 ${isLoading ? 'animate-spin text-blue-600' : ''}`} />
            <span>Refresh Data</span>
          </button>

          <button
            id="go-to-upload-btn"
            onClick={onNavigateUpload}
            className="flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold bg-slate-900 text-white hover:bg-slate-800 shadow-sm transition"
          >
            <UploadCloud className="w-4 h-4" />
            <span>Upload Dataset</span>
          </button>
        </div>
      </div>

      {/* Metrics Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5">
        <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-sm space-y-3">
          <div className="flex items-center justify-between text-slate-500">
            <span className="text-xs font-bold uppercase tracking-wider text-slate-400">Total Datasets</span>
            <div className="p-2 bg-blue-50 text-blue-600 rounded-lg">
              <FileSpreadsheet className="w-4 h-4" />
            </div>
          </div>
          <div className="text-2xl font-bold text-slate-900">{datasets.length}</div>
          <p className="text-xs text-slate-500">Indexed in SQLite Metadata Store</p>
        </div>

        <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-sm space-y-3">
          <div className="flex items-center justify-between text-slate-500">
            <span className="text-xs font-bold uppercase tracking-wider text-slate-400">Rows Ingested</span>
            <div className="p-2 bg-emerald-50 text-emerald-600 rounded-lg">
              <Layers className="w-4 h-4" />
            </div>
          </div>
          <div className="text-2xl font-bold text-slate-900">{totalRows.toLocaleString()}</div>
          <p className="text-xs text-slate-500">Across verified CSV files</p>
        </div>

        <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-sm space-y-3">
          <div className="flex items-center justify-between text-slate-500">
            <span className="text-xs font-bold uppercase tracking-wider text-slate-400">Max Column Schema</span>
            <div className="p-2 bg-purple-50 text-purple-600 rounded-lg">
              <ArrowUpRight className="w-4 h-4" />
            </div>
          </div>
          <div className="text-2xl font-bold text-slate-900">{totalCols} Fields</div>
          <p className="text-xs text-slate-500">Multi-cloud spend attributes</p>
        </div>

        <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-sm space-y-3">
          <div className="flex items-center justify-between text-slate-500">
            <span className="text-xs font-bold uppercase tracking-wider text-slate-400">Pipeline State</span>
            <div className="p-2 bg-indigo-50 text-indigo-600 rounded-lg">
              <Clock className="w-4 h-4" />
            </div>
          </div>
          <div className="text-2xl font-bold text-slate-900">Phase 1</div>
          <p className="text-xs text-emerald-600 font-semibold">Ingestion Engine Operational</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* Dataset Table Section */}
        <div className="lg:col-span-8 bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
            <div>
              <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider">Dataset Repository</h3>
              <p className="text-xs text-slate-500 mt-0.5">Uploaded cloud budget datasets stored in local repository</p>
            </div>
            <span className="text-xs font-bold text-blue-600 bg-blue-50 px-2.5 py-1 rounded-md border border-blue-100">
              {datasets.length} Files
            </span>
          </div>

          {datasets.length === 0 ? (
            <div className="p-12 text-center space-y-4">
              <div className="w-12 h-12 bg-slate-100 text-slate-400 rounded-full flex items-center justify-center mx-auto">
                <FileSpreadsheet className="w-6 h-6" />
              </div>
              <div>
                <p className="text-slate-800 font-semibold text-sm">No datasets uploaded yet</p>
                <p className="text-slate-500 text-xs mt-1">Upload your multi-cloud cost CSV file to begin analysis</p>
              </div>
              <button
                id="empty-upload-action"
                onClick={onNavigateUpload}
                className="px-4 py-2 bg-slate-900 hover:bg-slate-800 text-white rounded-lg text-xs font-medium inline-flex items-center space-x-2 shadow-sm transition"
              >
                <UploadCloud className="w-4 h-4" />
                <span>Go to Dataset Upload</span>
              </button>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table id="datasets-table" className="w-full text-left border-collapse">
                <thead>
                  <tr className="bg-slate-50 text-[10px] font-bold text-slate-400 uppercase tracking-wider border-b border-slate-100">
                    <th className="px-5 py-3">Dataset / Filename</th>
                    <th className="px-5 py-3">Rows</th>
                    <th className="px-5 py-3">Columns</th>
                    <th className="px-5 py-3">Size</th>
                    <th className="px-5 py-3">Status</th>
                    <th className="px-5 py-3 text-right">Action</th>
                  </tr>
                </thead>
                <tbody className="text-xs text-slate-600 divide-y divide-slate-100">
                  {datasets.map((d) => (
                    <tr key={d.id} className="hover:bg-slate-50/80 transition-colors">
                      <td className="px-5 py-3.5 font-medium text-slate-900">
                        <div className="flex items-center space-x-3">
                          <div className="w-7 h-7 bg-slate-100 rounded flex items-center justify-center text-slate-500 shrink-0">
                            <FileSpreadsheet className="w-3.5 h-3.5" />
                          </div>
                          <div className="min-w-0">
                            <p className="font-semibold text-slate-900 truncate">{d.original_filename}</p>
                            <span className="text-[10px] text-blue-600 font-mono font-bold bg-blue-50 px-1.5 py-0.5 rounded">
                              ID: {d.id}
                            </span>
                          </div>
                        </div>
                      </td>
                      <td className="px-5 py-3.5 font-mono font-semibold text-slate-900">{d.row_count.toLocaleString()}</td>
                      <td className="px-5 py-3.5 font-mono text-slate-600">{d.column_count}</td>
                      <td className="px-5 py-3.5 font-mono text-slate-500">{(d.file_size / 1024).toFixed(1)} KB</td>
                      <td className="px-5 py-3.5">
                        <span className="bg-green-100 text-green-700 px-2 py-0.5 rounded text-[10px] font-bold uppercase border border-green-200">
                          {d.status}
                        </span>
                      </td>
                      <td className="px-5 py-3.5 text-right">
                        <button
                          id={`btn-preview-${d.id}`}
                          onClick={() => onPreviewDataset(d.id)}
                          className="inline-flex items-center space-x-1 px-2.5 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-800 rounded-lg text-xs font-semibold border border-slate-200 transition"
                        >
                          <Eye className="w-3.5 h-3.5 text-blue-600" />
                          <span>Preview</span>
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Telemetry Card */}
        <div className="lg:col-span-4 space-y-6">
          <div className="bg-slate-900 rounded-xl p-6 text-white shadow-md relative overflow-hidden">
            <div className="absolute -right-4 -bottom-4 w-28 h-28 bg-blue-500/10 rounded-full blur-2xl pointer-events-none" />

            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-bold tracking-tight">System Telemetry</h3>
              <span className="px-2 py-0.5 rounded bg-slate-800 text-[10px] font-mono text-blue-400 border border-slate-700">
                v1.0.0-phase1
              </span>
            </div>

            <div className="space-y-4">
              <div>
                <div className="flex justify-between text-[10px] uppercase font-bold text-slate-400 mb-1">
                  <span>Storage (SQLite)</span>
                  <span className="text-slate-300">Active</span>
                </div>
                <div className="h-1.5 w-full bg-slate-800 rounded-full overflow-hidden">
                  <div className="h-full bg-blue-500 w-[35%]" />
                </div>
              </div>

              <div>
                <div className="flex justify-between text-[10px] uppercase font-bold text-slate-400 mb-1">
                  <span>Ingestion Load</span>
                  <span className="text-emerald-400">Optimal</span>
                </div>
                <div className="h-1.5 w-full bg-slate-800 rounded-full overflow-hidden">
                  <div className="h-full bg-emerald-500 w-[15%]" />
                </div>
              </div>

              <div className="pt-3 border-t border-slate-800 space-y-2 text-xs text-slate-300">
                <div className="flex items-center justify-between text-[11px]">
                  <span className="text-slate-400">Server Backend:</span>
                  <span className="font-medium text-white">FastAPI (Python 3.10)</span>
                </div>
                <div className="flex items-center justify-between text-[11px]">
                  <span className="text-slate-400">Database Engine:</span>
                  <span className="font-medium text-white">SQLite Metadata Store</span>
                </div>
                <div className="flex items-center justify-between text-[11px]">
                  <span className="text-slate-400">Unified Ingress:</span>
                  <span className="font-medium text-white">Express Proxy Port 3000</span>
                </div>
              </div>
            </div>

            <div className="mt-5 pt-3 border-t border-slate-800 flex items-center justify-between text-[10px] text-slate-400">
              <div className="flex items-center space-x-1.5">
                <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
                <span>ALL SYSTEMS READY</span>
              </div>
              <span>Phase 1 Deployment</span>
            </div>
          </div>

          <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-sm space-y-3">
            <h3 className="text-xs font-bold text-slate-700 uppercase tracking-wider">Quick Actions</h3>
            <div className="space-y-2">
              <button
                onClick={onNavigateUpload}
                className="w-full text-left p-3 rounded-lg border border-slate-200 hover:border-blue-300 hover:bg-blue-50/50 transition flex items-center justify-between group"
              >
                <div>
                  <p className="text-xs font-semibold text-slate-900 group-hover:text-blue-600">Import CSV Dataset</p>
                  <p className="text-[11px] text-slate-500">Parse headers & calculate file hash</p>
                </div>
                <UploadCloud className="w-4 h-4 text-slate-400 group-hover:text-blue-600" />
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

