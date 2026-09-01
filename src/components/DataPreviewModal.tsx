import React, { useEffect, useState } from 'react';
import { fetchDatasetDetail, fetchDatasetPreview } from '../services/api';
import { DatasetDetail, DatasetPreview } from '../types';
import { X, FileSpreadsheet, Table, Loader2, Layers, Sparkles } from 'lucide-react';

interface DataPreviewModalProps {
  datasetId: string;
  onClose: () => void;
}

export const DataPreviewModal: React.FC<DataPreviewModalProps> = ({ datasetId, onClose }) => {
  const [detail, setDetail] = useState<DatasetDetail | null>(null);
  const [preview, setPreview] = useState<DatasetPreview | null>(null);
  const [layer, setLayer] = useState<'bronze' | 'silver'>('bronze');
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const loadPreviewData = async (selectedLayer: 'bronze' | 'silver') => {
    setIsLoading(true);
    setError(null);
    try {
      const [detailData, previewData] = await Promise.all([
        fetchDatasetDetail(datasetId),
        fetchDatasetPreview(datasetId, 10, selectedLayer),
      ]);
      setDetail(detailData);
      setPreview(previewData);
    } catch (err: any) {
      setError(err.message || 'Failed to load dataset details');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadPreviewData(layer);
  }, [datasetId, layer]);

  return (
    <div id="data-preview-modal" className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-xs">
      <div className="bg-white border border-slate-200 rounded-2xl w-full max-w-5xl max-h-[90vh] flex flex-col shadow-2xl overflow-hidden">
        {/* Modal Header */}
        <div className="p-5 border-b border-slate-200 flex items-center justify-between bg-slate-50/80">
          <div className="flex items-center space-x-3">
            <div className="p-2 bg-blue-100 text-blue-700 rounded-lg">
              <FileSpreadsheet className="w-5 h-5" />
            </div>
            <div>
              <h3 className="text-base font-bold text-slate-900">{detail?.original_filename || 'Dataset Inspection'}</h3>
              <p className="text-xs text-slate-500 font-mono">ID: {datasetId}</p>
            </div>
          </div>

          <button
            id="close-preview-modal-btn"
            onClick={onClose}
            className="p-2 text-slate-400 hover:text-slate-600 hover:bg-slate-200/60 rounded-xl transition"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Modal Content Body */}
        <div className="p-6 overflow-y-auto space-y-6 flex-1 bg-white">
          {/* Layer Selector Switch */}
          <div className="flex items-center justify-between bg-slate-100 p-1.5 rounded-xl border border-slate-200">
            <span className="text-xs font-bold text-slate-700 px-3 uppercase tracking-wider flex items-center space-x-2">
              <Layers className="w-4 h-4 text-blue-600" />
              <span>Select Storage Layer:</span>
            </span>

            <div className="flex space-x-1">
              <button
                id="btn-preview-layer-bronze"
                onClick={() => setLayer('bronze')}
                className={`px-4 py-2 text-xs font-bold rounded-lg transition flex items-center space-x-1.5 ${
                  layer === 'bronze'
                    ? 'bg-amber-600 text-white shadow-sm'
                    : 'text-slate-600 hover:bg-slate-200'
                }`}
              >
                <span>Bronze Layer (Raw CSV)</span>
              </button>

              <button
                id="btn-preview-layer-silver"
                onClick={() => setLayer('silver')}
                className={`px-4 py-2 text-xs font-bold rounded-lg transition flex items-center space-x-1.5 ${
                  layer === 'silver'
                    ? 'bg-blue-600 text-white shadow-sm'
                    : 'text-slate-600 hover:bg-slate-200'
                }`}
              >
                <Sparkles className="w-3.5 h-3.5" />
                <span>Silver Layer (Cleaned & Feature Engineered)</span>
              </button>
            </div>
          </div>

          {isLoading ? (
            <div className="py-20 text-center space-y-3">
              <Loader2 className="w-8 h-8 text-blue-600 animate-spin mx-auto" />
              <p className="text-sm text-slate-500">Loading dataset preview for {layer.toUpperCase()} layer...</p>
            </div>
          ) : error ? (
            <div className="p-4 bg-red-50 border border-red-200 rounded-xl text-red-700 text-sm">
              {error}
            </div>
          ) : (
            <>
              {/* Metadata Summary Grid */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                <div className="p-3.5 bg-slate-50 border border-slate-200 rounded-xl">
                  <span className="text-[10px] text-slate-500 uppercase font-bold">Total Rows</span>
                  <p className="text-lg font-bold text-emerald-700 font-mono">{preview?.row_count?.toLocaleString() || detail?.row_count?.toLocaleString()}</p>
                </div>
                <div className="p-3.5 bg-slate-50 border border-slate-200 rounded-xl">
                  <span className="text-[10px] text-slate-500 uppercase font-bold">Total Columns</span>
                  <p className="text-lg font-bold text-blue-700 font-mono">{preview?.column_count || detail?.column_count}</p>
                </div>
                <div className="p-3.5 bg-slate-50 border border-slate-200 rounded-xl">
                  <span className="text-[10px] text-slate-500 uppercase font-bold">Storage Layer</span>
                  <p className="text-xs font-bold text-slate-900 font-mono mt-1 uppercase">
                    {layer === 'silver' ? 'Silver Parquet Delta' : 'Bronze Raw CSV'}
                  </p>
                </div>
                <div className="p-3.5 bg-slate-50 border border-slate-200 rounded-xl">
                  <span className="text-[10px] text-slate-500 uppercase font-bold">Engineered Features</span>
                  <p className="text-xs font-bold text-indigo-600 mt-1">
                    {layer === 'silver' ? 'total_savings, effective_discount_pct, budget_remaining, cost_risk_level' : 'Standard Raw Fields'}
                  </p>
                </div>
              </div>

              {/* Data Table Preview */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <h4 className="text-xs font-bold uppercase tracking-wider text-slate-700 flex items-center space-x-2">
                    <Table className="w-4 h-4 text-blue-600" />
                    <span>Top 10 Rows Data Preview ({layer.toUpperCase()} Layer)</span>
                  </h4>
                  <span className="text-[11px] text-slate-500">Showing first 10 records</span>
                </div>

                <div className="border border-slate-200 rounded-xl overflow-x-auto bg-white">
                  {preview && preview.preview_data.length > 0 ? (
                    <table className="w-full text-left text-xs border-collapse">
                      <thead className="bg-slate-50 text-slate-500 uppercase font-mono text-[10px] font-bold border-b border-slate-200">
                        <tr>
                          {preview.columns.map((col) => (
                            <th key={col} className={`py-2.5 px-4 whitespace-nowrap ${
                              ['total_savings', 'effective_discount_pct', 'budget_remaining', 'cost_risk_level', 'high_budget_utilization_flag', 'cost_per_usage'].includes(col)
                                ? 'bg-indigo-50/80 text-indigo-900 font-bold border-x border-indigo-100'
                                : ''
                            }`}>
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100 font-mono text-[11px] text-slate-700">
                        {preview.preview_data.map((row, idx) => (
                          <tr key={idx} className="hover:bg-slate-50/80 transition-colors">
                            {preview.columns.map((col) => (
                              <td key={col} className={`py-2.5 px-4 whitespace-nowrap ${
                                ['total_savings', 'effective_discount_pct', 'budget_remaining', 'cost_risk_level', 'high_budget_utilization_flag', 'cost_per_usage'].includes(col)
                                  ? 'bg-indigo-50/30 text-indigo-950 font-bold border-x border-indigo-100/60'
                                  : ''
                              }`}>
                                {col === 'cost_risk_level' ? (
                                  <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                                    row[col] === 'HIGH' ? 'bg-rose-100 text-rose-800' : row[col] === 'MEDIUM' ? 'bg-amber-100 text-amber-800' : 'bg-emerald-100 text-emerald-800'
                                  }`}>
                                    {row[col]}
                                  </span>
                                ) : row[col] !== null && row[col] !== undefined ? (
                                  String(row[col])
                                ) : (
                                  <span className="text-slate-400 font-normal">null</span>
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <div className="py-8 text-center text-slate-500 text-xs">
                      No records found for layer '{layer}'. If Silver layer is selected, run the ETL pipeline first.
                    </div>
                  )}
                </div>
              </div>
            </>
          )}
        </div>

        {/* Modal Footer */}
        <div className="p-4 bg-slate-50 border-t border-slate-200 flex justify-end">
          <button
            id="modal-close-bottom-btn"
            onClick={onClose}
            className="px-4 py-2 bg-slate-900 hover:bg-slate-800 text-white text-xs font-semibold rounded-lg shadow-sm transition"
          >
            Close Window
          </button>
        </div>
      </div>
    </div>
  );
};
