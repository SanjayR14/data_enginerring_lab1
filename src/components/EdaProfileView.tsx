import React, { useEffect, useState } from 'react';
import { fetchDatasetProfile, fetchDatasetDetail } from '../services/api';
import { DatasetProfile, DatasetDetail, Dataset } from '../types';
import { BarChart3, Database, Layers, RefreshCw, AlertCircle } from 'lucide-react';

interface EdaProfileViewProps {
  datasets: Dataset[];
}

export const EdaProfileView: React.FC<EdaProfileViewProps> = ({ datasets }) => {
  const [selectedDatasetId, setSelectedDatasetId] = useState<string>(datasets[0]?.id || '');
  const [profile, setProfile] = useState<DatasetProfile | null>(null);
  const [datasetDetail, setDatasetDetail] = useState<DatasetDetail | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const loadProfile = async () => {
    if (!selectedDatasetId) return;
    setIsLoading(true);
    setErrorMsg(null);
    try {
      const [profData, detailData] = await Promise.all([
        fetchDatasetProfile(selectedDatasetId),
        fetchDatasetDetail(selectedDatasetId).catch(() => null)
      ]);
      setProfile(profData);
      setDatasetDetail(detailData);
    } catch (err: any) {
      setProfile(null);
      setErrorMsg(err.message || 'Failed to load profiling metrics.');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadProfile();
  }, [selectedDatasetId]);

  return (
    <div id="eda-profile-view" className="space-y-6 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-2 border-b border-slate-200">
        <div>
          <div className="flex items-center space-x-2">
            <BarChart3 className="w-6 h-6 text-blue-600" />
            <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Exploratory Data Analysis & Dataset Profiling</h1>
          </div>
          <p className="text-slate-500 text-sm mt-1">
            Statistical distribution metrics, null value profiling, distinct cardinality, and feature value distributions.
          </p>
        </div>

        {/* Dataset Selector */}
        {datasets.length > 0 && (
          <div className="flex items-center space-x-2">
            <select
              id="eda-dataset-select"
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
              onClick={loadProfile}
              disabled={isLoading}
              className="p-2.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg text-xs font-bold transition flex items-center"
            >
              <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
            </button>
          </div>
        )}
      </div>

      {errorMsg ? (
        <div className="bg-amber-50 border border-amber-200 rounded-xl p-8 text-center space-y-3">
          <AlertCircle className="w-10 h-10 text-amber-500 mx-auto" />
          <h3 className="text-slate-900 font-bold text-sm">Profiling Unavailable</h3>
          <p className="text-amber-800 text-xs max-w-md mx-auto">{errorMsg}</p>
        </div>
      ) : profile ? (
        <>
          {/* Top Summary Metrics */}
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-4">
            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-1">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Total Row Count</span>
              <span className="text-3xl font-extrabold text-slate-900">{profile.row_count.toLocaleString()}</span>
              <span className="text-xs text-slate-400 block">Ingested Records</span>
            </div>

            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-1">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Feature Columns</span>
              <span className="text-3xl font-extrabold text-blue-600">{profile.column_count}</span>
              <span className="text-xs text-slate-400 block">Total Attributes</span>
            </div>

            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-1">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Duplicate Rows</span>
              <span className={`text-3xl font-extrabold ${profile.duplicate_count > 0 ? 'text-amber-600' : 'text-emerald-600'}`}>{profile.duplicate_count}</span>
              <span className="text-xs text-slate-400 block">Full-Row Duplicates</span>
            </div>

            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-1">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Numeric Metrics</span>
              <span className="text-3xl font-extrabold text-emerald-600">{Object.keys(profile.numeric_stats).length}</span>
              <span className="text-xs text-slate-400 block">Quantitative Features</span>
            </div>

            <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-2xs space-y-1">
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 block">Categorical Attributes</span>
              <span className="text-3xl font-extrabold text-indigo-600">{Object.keys(profile.categorical_frequencies).length}</span>
              <span className="text-xs text-slate-400 block">Dimensions & Categories</span>
            </div>
          </div>

          {/* Numeric Feature Summary Statistics Table */}
          <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4 shadow-sm">
            <div>
              <h3 className="text-sm font-bold text-slate-900 uppercase tracking-wider">Numeric Feature Statistical Summary</h3>
              <p className="text-xs text-slate-500">Min, max, mean, median, and standard deviation calculated across cleaned numeric features</p>
            </div>

            <div className="overflow-x-auto border border-slate-200 rounded-lg">
              <table className="w-full text-left text-xs text-slate-700 font-mono">
                <thead className="bg-slate-50 border-b border-slate-200 text-[10px] uppercase tracking-wider text-slate-500 font-bold font-sans">
                  <tr>
                    <th className="p-3">Feature Name</th>
                    <th className="p-3 text-right">Min</th>
                    <th className="p-3 text-right">Max</th>
                    <th className="p-3 text-right">Mean</th>
                    <th className="p-3 text-right">Median</th>
                    <th className="p-3 text-right">Std Dev</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {Object.entries(profile.numeric_stats).map(([col, stats]: [string, any]) => (
                    <tr key={col} className="hover:bg-slate-50 transition">
                      <td className="p-3 font-sans font-bold text-slate-900">{col}</td>
                      <td className="p-3 text-right text-slate-600">{stats.min.toLocaleString()}</td>
                      <td className="p-3 text-right text-slate-600">{stats.max.toLocaleString()}</td>
                      <td className="p-3 text-right text-blue-700 font-bold">{stats.mean.toLocaleString()}</td>
                      <td className="p-3 text-right text-slate-800">{stats.median.toLocaleString()}</td>
                      <td className="p-3 text-right text-slate-500">{stats.std.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Categorical Value Distribution Cards */}
          <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4 shadow-sm">
            <div>
              <h3 className="text-sm font-bold text-slate-900 uppercase tracking-wider">Categorical Dimension Frequencies</h3>
              <p className="text-xs text-slate-500">Top distinct categorical values and relative sample frequencies</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {Object.entries(profile.categorical_frequencies).map(([catCol, freqMap]) => (
                <div key={catCol} className="p-4 bg-slate-50 border border-slate-200 rounded-lg space-y-3">
                  <div className="flex justify-between items-center border-b border-slate-200 pb-2">
                    <span className="font-bold text-xs text-slate-800 uppercase tracking-wider">{catCol}</span>
                    <span className="text-[10px] font-mono bg-white border border-slate-200 px-2 py-0.5 rounded text-slate-600">
                      Distinct: {profile.distinct_counts[catCol] || 0}
                    </span>
                  </div>

                  <div className="space-y-2">
                    {Object.entries(freqMap).map(([val, count]) => {
                      const pct = Math.round((count / profile.row_count) * 100);
                      return (
                        <div key={val} className="space-y-1">
                          <div className="flex justify-between text-xs">
                            <span className="text-slate-700 font-medium">{val}</span>
                            <span className="text-slate-500 font-mono text-[11px]">{count} ({pct}%)</span>
                          </div>
                          <div className="w-full bg-slate-200 h-1.5 rounded-full overflow-hidden">
                            <div className="bg-blue-600 h-full rounded-full" style={{ width: `${pct}%` }} />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Correlation Matrix */}
          {Object.keys(profile.correlation_matrix).length > 0 && (
            <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4 shadow-sm">
              <div>
                <h3 className="text-sm font-bold text-slate-900 uppercase tracking-wider">Numeric Feature Correlation Matrix</h3>
                <p className="text-xs text-slate-500">Pearson correlation coefficient between numeric features (-1 to +1)</p>
              </div>

              <div className="overflow-x-auto border border-slate-200 rounded-lg">
                <table className="w-full text-left text-[11px] text-slate-700 font-mono">
                  <thead className="bg-slate-50 border-b border-slate-200 text-[10px] uppercase tracking-wider text-slate-500 font-bold font-sans">
                    <tr>
                      <th className="p-2"></th>
                      {Object.keys(profile.correlation_matrix).map((col) => (
                        <th key={col} className="p-2 text-right whitespace-nowrap">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {Object.entries(profile.correlation_matrix).map(([rowCol, rowVals]) => (
                      <tr key={rowCol} className="hover:bg-slate-50 transition">
                        <td className="p-2 font-sans font-bold text-slate-900 whitespace-nowrap">{rowCol}</td>
                        {Object.keys(profile.correlation_matrix).map((colCol) => {
                          const v = rowVals[colCol];
                          const abs = v === null ? 0 : Math.abs(v);
                          const bg = v === null ? '' : v > 0 ? `rgba(37,99,235,${abs * 0.5})` : `rgba(220,38,38,${abs * 0.5})`;
                          return (
                            <td key={colCol} className="p-2 text-right" style={{ backgroundColor: bg }}>
                              {v === null ? '—' : v.toFixed(2)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Outlier Detection */}
          <div className="bg-white border border-slate-200 rounded-xl p-6 space-y-4 shadow-sm">
            <div>
              <h3 className="text-sm font-bold text-slate-900 uppercase tracking-wider">Outlier Detection (IQR Method)</h3>
              <p className="text-xs text-slate-500">Values falling outside 1.5x the interquartile range for each numeric feature</p>
            </div>

            {Object.keys(profile.outliers).length === 0 ? (
              <p className="text-xs text-slate-400 italic">No statistical outliers detected in this dataset.</p>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {Object.entries(profile.outliers).map(([col, info]: [string, any]) => (
                  <div key={col} className="p-4 bg-amber-50 border border-amber-200 rounded-lg space-y-2">
                    <div className="flex justify-between items-center">
                      <span className="font-bold text-xs text-slate-800 uppercase tracking-wider">{col}</span>
                      <span className="text-[10px] font-mono bg-white border border-amber-200 px-2 py-0.5 rounded text-amber-700">
                        {info.count} outlier{info.count !== 1 ? 's' : ''}
                      </span>
                    </div>
                    <p className="text-[11px] text-slate-600">
                      Expected range: {info.lower_bound.toLocaleString()} to {info.upper_bound.toLocaleString()}
                    </p>
                    <p className="text-[11px] font-mono text-amber-800">
                      e.g. {info.example_values.map(v => v.toLocaleString()).join(', ')}
                    </p>
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      ) : null}
    </div>
  );
};
