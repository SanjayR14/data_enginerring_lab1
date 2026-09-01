import React, { useState, useEffect } from 'react';
import {
  Layers, Filter, BarChart3, PieChart, Database, RefreshCw,
  CheckCircle2, AlertTriangle, TrendingUp, Table, Grid,
  Zap, Info, ArrowDown, ArrowUp, Activity, Sparkles, Sliders
} from 'lucide-react';
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, Cell, PieChart as RePieChart, Pie
} from 'recharts';

type OlapTab = 'rollup' | 'drilldown' | 'slice' | 'dice' | 'pivot' | 'top' | 'budget' | 'savings' | 'anomalies' | 'compare';

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4'];

export const OlapAnalyzerView: React.FC = () => {
  const [activeTab, setActiveTab] = useState<OlapTab>('rollup');
  const [loading, setLoading] = useState<boolean>(false);
  const [cubeInfo, setCubeInfo] = useState<any>(null);
  
  // Control States
  const [rollupDim, setRollupDim] = useState<string>('time');
  const [rollupLevel, setRollupLevel] = useState<string>('month');
  const [rollupMeasure, setRollupMeasure] = useState<string>('net_cost');

  const [drillHierarchy, setDrillHierarchy] = useState<string>('time');
  const [drillCurrent, setDrillCurrent] = useState<string>('year');
  const [drillNext, setDrillNext] = useState<string>('quarter');

  const [sliceDim, setSliceDim] = useState<string>('cloud_provider');
  const [sliceVal, setSliceVal] = useState<string>('AWS');

  const [diceProviders, setDiceProviders] = useState<string[]>(['AWS', 'Azure']);
  const [diceDepts, setDiceDepts] = useState<string[]>(['Engineering', 'Finance']);
  const [diceEnv, setDiceEnv] = useState<string>('production');

  const [pivotRows, setPivotRows] = useState<string>('department');
  const [pivotCols, setPivotCols] = useState<string>('cloud_provider');

  const [topCat, setTopCat] = useState<string>('projects');
  const [topN, setTopN] = useState<number>(10);

  const [compareDim, setCompareDim] = useState<string>('cloud_provider');

  // Query Results
  const [queryResult, setQueryResult] = useState<any>(null);

  useEffect(() => {
    fetchCubeSummary();
    runQuery();
  }, [activeTab]);

  const fetchCubeSummary = async () => {
    try {
      const res = await fetch('/api/olap/cube');
      if (res.ok) {
        const data = await res.json();
        setCubeInfo(data);
      }
    } catch (e) {
      console.error('Failed to fetch cube summary:', e);
    }
  };

  const runQuery = async () => {
    setLoading(true);
    setQueryResult(null);
    try {
      let url = '';
      if (activeTab === 'rollup') {
        url = `/api/olap/rollup?dimension=${rollupDim}&level=${rollupLevel}&measure=${rollupMeasure}`;
      } else if (activeTab === 'drilldown') {
        url = `/api/olap/drilldown?hierarchy=${drillHierarchy}&current_level=${drillCurrent}&next_level=${drillNext}`;
      } else if (activeTab === 'slice') {
        url = `/api/olap/slice?dimension=${sliceDim}&value=${encodeURIComponent(sliceVal)}`;
      } else if (activeTab === 'dice') {
        const payload = {
          filters: {
            cloud_provider: diceProviders,
            department: diceDepts,
            environment: diceEnv
          }
        };
        const res = await fetch('/api/olap/dice', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (res.ok) setQueryResult(await res.json());
        setLoading(false);
        return;
      } else if (activeTab === 'pivot') {
        url = `/api/olap/pivot?rows=${pivotRows}&columns=${pivotCols}&measure=${rollupMeasure}`;
      } else if (activeTab === 'top') {
        url = `/api/olap/top?category=${topCat}&n=${topN}&measure=${rollupMeasure}`;
      } else if (activeTab === 'budget') {
        url = `/api/olap/budget`;
      } else if (activeTab === 'savings') {
        url = `/api/olap/savings`;
      } else if (activeTab === 'anomalies') {
        url = `/api/olap/anomalies`;
      } else if (activeTab === 'compare') {
        url = `/api/olap/compare?dimension=${compareDim}`;
      }

      if (url) {
        const res = await fetch(url);
        if (res.ok) {
          const data = await res.json();
          setQueryResult(data);
        }
      }
    } catch (e) {
      console.error('OLAP Query execution failed:', e);
    } finally {
      setLoading(false);
    }
  };

  const handleRefreshAggregates = async () => {
    setLoading(true);
    try {
      await fetch('/api/olap/cube');
      await runQuery();
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto text-slate-100">
      {/* Header & Data Cube Overview Banner */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <div className="flex items-center space-x-3">
            <div className="p-2.5 bg-blue-500/20 text-blue-400 rounded-lg border border-blue-500/30">
              <Layers className="w-6 h-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold tracking-tight text-white flex items-center gap-2">
                Multidimensional Data Cube & OLAP Analyzer
                <span className="text-xs font-semibold px-2.5 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20">
                  cloud_cost_cube
                </span>
              </h1>
              <p className="text-sm text-slate-400 mt-0.5">
                No-code multidimensional analysis: Roll-up, Drill-down, Slice, Dice, Pivot, and Rule-Based Business Interpretation.
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center space-x-3">
          <button
            onClick={handleRefreshAggregates}
            disabled={loading}
            className="flex items-center space-x-2 px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-sm font-medium rounded-lg border border-slate-700 transition"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            <span>Refresh OLAP Aggregates</span>
          </button>
        </div>
      </div>

      {/* Cube Summary Stat Cards */}
      {cubeInfo && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 flex items-center justify-between">
            <div>
              <p className="text-xs text-slate-400 uppercase tracking-wider font-medium">Cube Name</p>
              <p className="text-lg font-bold text-white mt-1">{cubeInfo.cube_name}</p>
              <p className="text-[11px] text-slate-500 mt-0.5">{cubeInfo.dimensions_available?.length || 12} Dimensions</p>
            </div>
            <div className="p-3 bg-slate-800 rounded-lg text-blue-400">
              <Database className="w-5 h-5" />
            </div>
          </div>

          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 flex items-center justify-between">
            <div>
              <p className="text-xs text-slate-400 uppercase tracking-wider font-medium">Cube Base Records</p>
              <p className="text-lg font-bold text-emerald-400 mt-1">{cubeInfo.record_count?.toLocaleString() || 0} rows</p>
              <p className="text-[11px] text-slate-500 mt-0.5">Base Grain Active</p>
            </div>
            <div className="p-3 bg-emerald-500/10 rounded-lg text-emerald-400">
              <Layers className="w-5 h-5" />
            </div>
          </div>

          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 flex items-center justify-between">
            <div>
              <p className="text-xs text-slate-400 uppercase tracking-wider font-medium">Total Cube Net Spend</p>
              <p className="text-lg font-bold text-white mt-1">${cubeInfo.total_net_cost?.toLocaleString() || '0.00'}</p>
              <p className="text-[11px] text-slate-500 mt-0.5">USD Multi-Cloud Total</p>
            </div>
            <div className="p-3 bg-blue-500/10 rounded-lg text-blue-400">
              <TrendingUp className="w-5 h-5" />
            </div>
          </div>

          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 flex items-center justify-between">
            <div>
              <p className="text-xs text-slate-400 uppercase tracking-wider font-medium">Allocated Budget</p>
              <p className="text-lg font-bold text-amber-400 mt-1">${cubeInfo.total_budget?.toLocaleString() || '0.00'}</p>
              <p className="text-[11px] text-slate-500 mt-0.5">Target Cap</p>
            </div>
            <div className="p-3 bg-amber-500/10 rounded-lg text-amber-400">
              <Zap className="w-5 h-5" />
            </div>
          </div>
        </div>
      )}

      {/* OLAP Operation Navigation Bar */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-2 flex flex-wrap gap-1">
        {[
          { id: 'rollup', label: 'Roll-up', icon: ArrowUp },
          { id: 'drilldown', label: 'Drill-down', icon: ArrowDown },
          { id: 'slice', label: 'Slice', icon: Filter },
          { id: 'dice', label: 'Dice', icon: Grid },
          { id: 'pivot', label: 'Pivot Matrix', icon: Table },
          { id: 'top', label: 'Top-N Ranking', icon: BarChart3 },
          { id: 'budget', label: 'Budget Analysis', icon: Zap },
          { id: 'savings', label: 'Savings & Discounts', icon: Sparkles },
          { id: 'anomalies', label: 'Anomalies OLAP', icon: AlertTriangle },
          { id: 'compare', label: 'Side-by-Side Compare', icon: Activity }
        ].map(tab => {
          const Icon = tab.icon;
          const isActive = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as OlapTab)}
              className={`flex items-center space-x-2 px-3.5 py-2 text-xs font-medium rounded-lg transition-all ${
                isActive
                  ? 'bg-blue-600 text-white shadow-md shadow-blue-500/20'
                  : 'text-slate-400 hover:text-white hover:bg-slate-800'
              }`}
            >
              <Icon className="w-3.5 h-3.5" />
              <span>{tab.label}</span>
            </button>
          );
        })}
      </div>

      {/* Non-Coder Interactive Control Panel */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
        <div className="flex items-center justify-between border-b border-slate-800 pb-3">
          <h2 className="text-sm font-semibold text-white uppercase tracking-wider flex items-center gap-2">
            <Sliders className="w-4 h-4 text-blue-400" />
            OLAP Query Parameters ({activeTab.toUpperCase()})
          </h2>
          <button
            onClick={runQuery}
            disabled={loading}
            className="px-4 py-1.5 bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold rounded-lg shadow transition"
          >
            {loading ? 'Executing...' : 'RUN ANALYSIS'}
          </button>
        </div>

        {/* Tab Specific Controls */}
        {activeTab === 'rollup' && (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <div>
              <label className="block text-xs text-slate-400 mb-1">Dimension</label>
              <select
                value={rollupDim}
                onChange={e => setRollupDim(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2 focus:ring-1 focus:ring-blue-500"
              >
                <option value="time">Time Hierarchy</option>
                <option value="cloud_provider">Cloud Provider</option>
                <option value="organization">Organization</option>
                <option value="service">Resource Service</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Target Aggregation Level</label>
              <select
                value={rollupLevel}
                onChange={e => setRollupLevel(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2 focus:ring-1 focus:ring-blue-500"
              >
                <option value="month">Month (Day → Month)</option>
                <option value="quarter">Quarter (Month → Quarter)</option>
                <option value="year">Year (Quarter → Year)</option>
                <option value="cloud_provider">Provider (Account → Provider)</option>
                <option value="business_unit">Business Unit (Dept → BU)</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Target Measure</label>
              <select
                value={rollupMeasure}
                onChange={e => setRollupMeasure(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2 focus:ring-1 focus:ring-blue-500"
              >
                <option value="net_cost">Net Cost ($)</option>
                <option value="total_savings">Total Savings ($)</option>
                <option value="budget_amount">Allocated Budget ($)</option>
              </select>
            </div>
          </div>
        )}

        {activeTab === 'drilldown' && (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <div>
              <label className="block text-xs text-slate-400 mb-1">Analytical Hierarchy</label>
              <select
                value={drillHierarchy}
                onChange={e => setDrillHierarchy(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2 focus:ring-1 focus:ring-blue-500"
              >
                <option value="time">TIME (Year → Quarter → Month → Day)</option>
                <option value="cloud">CLOUD (Provider → Account → Project)</option>
                <option value="organization">ORGANIZATION (BU → Dept → Cost Center)</option>
                <option value="resource">RESOURCE (Service → Resource Type)</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Current Parent Level</label>
              <input
                type="text"
                value={drillCurrent}
                onChange={e => setDrillCurrent(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
                placeholder="e.g. 2023 or AWS"
              />
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Next Granular Level</label>
              <input
                type="text"
                value={drillNext}
                onChange={e => setDrillNext(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
                placeholder="e.g. quarter or month"
              />
            </div>
          </div>
        )}

        {activeTab === 'slice' && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-slate-400 mb-1">Fixed Dimension</label>
              <select
                value={sliceDim}
                onChange={e => setSliceDim(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2 focus:ring-1 focus:ring-blue-500"
              >
                <option value="cloud_provider">Cloud Provider</option>
                <option value="department">Department</option>
                <option value="environment">Environment</option>
                <option value="region">Region</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Fixed Slice Value</label>
              <input
                type="text"
                value={sliceVal}
                onChange={e => setSliceVal(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
                placeholder="e.g. AWS or Engineering or production"
              />
            </div>
          </div>
        )}

        {activeTab === 'dice' && (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <div>
              <label className="block text-xs text-slate-400 mb-2 font-medium">Cloud Providers</label>
              <div className="space-y-1.5 bg-slate-800 p-2.5 rounded-lg border border-slate-700">
                {['AWS', 'Azure', 'GCP'].map(p => (
                  <label key={p} className="flex items-center space-x-2 text-xs cursor-pointer">
                    <input
                      type="checkbox"
                      checked={diceProviders.includes(p)}
                      onChange={e => {
                        if (e.target.checked) setDiceProviders([...diceProviders, p]);
                        else setDiceProviders(diceProviders.filter(x => x !== p));
                      }}
                      className="rounded border-slate-600 text-blue-600 focus:ring-0"
                    />
                    <span>{p}</span>
                  </label>
                ))}
              </div>
            </div>

            <div>
              <label className="block text-xs text-slate-400 mb-2 font-medium">Departments</label>
              <div className="space-y-1.5 bg-slate-800 p-2.5 rounded-lg border border-slate-700">
                {['Engineering', 'Finance', 'Marketing', 'Product', 'HR'].map(d => (
                  <label key={d} className="flex items-center space-x-2 text-xs cursor-pointer">
                    <input
                      type="checkbox"
                      checked={diceDepts.includes(d)}
                      onChange={e => {
                        if (e.target.checked) setDiceDepts([...diceDepts, d]);
                        else setDiceDepts(diceDepts.filter(x => x !== d));
                      }}
                      className="rounded border-slate-600 text-blue-600 focus:ring-0"
                    />
                    <span>{d}</span>
                  </label>
                ))}
              </div>
            </div>

            <div>
              <label className="block text-xs text-slate-400 mb-2 font-medium">Environment</label>
              <select
                value={diceEnv}
                onChange={e => setDiceEnv(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
              >
                <option value="production">production</option>
                <option value="staging">staging</option>
                <option value="development">development</option>
              </select>
            </div>
          </div>
        )}

        {activeTab === 'pivot' && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-slate-400 mb-1">Rows Dimension</label>
              <select
                value={pivotRows}
                onChange={e => setPivotRows(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
              >
                <option value="department">Department</option>
                <option value="service">Service</option>
                <option value="region">Region</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Columns Dimension</label>
              <select
                value={pivotCols}
                onChange={e => setPivotCols(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
              >
                <option value="cloud_provider">Cloud Provider</option>
                <option value="environment">Environment</option>
              </select>
            </div>
          </div>
        )}

        {activeTab === 'top' && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-slate-400 mb-1">Category Ranking</label>
              <select
                value={topCat}
                onChange={e => setTopCat(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
              >
                <option value="projects">Top Expensive Projects</option>
                <option value="services">Top Expensive Services</option>
                <option value="departments">Top Spending Departments</option>
                <option value="regions">Top Regions</option>
                <option value="accounts">Top Accounts</option>
                <option value="anomalies">Top Anomalous Projects</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Limit (N)</label>
              <input
                type="number"
                value={topN}
                onChange={e => setTopN(Number(e.target.value))}
                className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
                min={1}
                max={20}
              />
            </div>
          </div>
        )}

        {activeTab === 'compare' && (
          <div>
            <label className="block text-xs text-slate-400 mb-1">Comparative Axis</label>
            <select
              value={compareDim}
              onChange={e => setCompareDim(e.target.value)}
              className="w-full bg-slate-800 border border-slate-700 text-white text-xs rounded-lg p-2"
            >
              <option value="cloud_provider">AWS vs Azure vs GCP</option>
              <option value="department">Engineering vs Finance vs Marketing</option>
              <option value="environment">production vs staging vs development</option>
            </select>
          </div>
        )}
      </div>

      {/* Rule-Based Business Interpretation Output Box */}
      {queryResult && queryResult.interpretation && (
        <div className="bg-gradient-to-r from-blue-950/40 via-slate-900 to-indigo-950/40 border border-blue-500/30 rounded-xl p-4 shadow-lg flex items-start space-x-3">
          <div className="p-2 bg-blue-500/20 text-blue-400 rounded-lg shrink-0 mt-0.5">
            <Sparkles className="w-5 h-5" />
          </div>
          <div>
            <h3 className="text-xs font-bold uppercase tracking-wider text-blue-400 flex items-center gap-1.5">
              Rule-Based Business Interpretation
              <span className="text-[10px] font-normal px-2 py-0.2 rounded bg-blue-500/10 text-blue-300">
                Deterministic Engine (Non-LLM)
              </span>
            </h3>
            <p className="text-sm font-medium text-slate-200 mt-1 leading-relaxed">
              {queryResult.interpretation}
            </p>
          </div>
        </div>
      )}

      {/* Query Results & Data Visualizations */}
      {loading ? (
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-12 text-center text-slate-400">
          <RefreshCw className="w-8 h-8 animate-spin mx-auto text-blue-400 mb-3" />
          <p className="text-sm">Querying Cloud Data Cube & OLAP Aggregates...</p>
        </div>
      ) : queryResult ? (
        <div className="space-y-6">
          {/* Chart Rendering */}
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl">
            <h3 className="text-sm font-semibold text-white uppercase tracking-wider mb-4 flex items-center justify-between">
              <span>Data Cube Visualization</span>
              <span className="text-xs text-slate-500 font-normal">
                {queryResult.operation || activeTab.toUpperCase()}
              </span>
            </h3>

            <div className="h-72 w-full">
              <ResponsiveContainer width="100%" height="100%">
                {activeTab === 'pivot' && queryResult.matrix ? (
                  <BarChart data={queryResult.matrix}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey={queryResult.rows_dimension || 'department'} stroke="#94a3b8" fontSize={12} />
                    <YAxis stroke="#94a3b8" fontSize={12} />
                    <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', color: '#fff' }} />
                    <Legend />
                    {(queryResult.columns || []).map((col: string, idx: number) => (
                      <Bar key={col} dataKey={col} fill={COLORS[idx % COLORS.length]} name={col} />
                    ))}
                  </BarChart>
                ) : activeTab === 'budget' && queryResult.department_budget_breakdown ? (
                  <BarChart data={queryResult.department_budget_breakdown}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey="department" stroke="#94a3b8" fontSize={12} />
                    <YAxis stroke="#94a3b8" fontSize={12} />
                    <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', color: '#fff' }} />
                    <Legend />
                    <Bar dataKey="budget_amount" fill="#3b82f6" name="Budget ($)" />
                    <Bar dataKey="net_cost" fill="#f59e0b" name="Actual Cost ($)" />
                  </BarChart>
                ) : activeTab === 'compare' && queryResult.comparison_data ? (
                  <BarChart data={queryResult.comparison_data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey={queryResult.dimension} stroke="#94a3b8" fontSize={12} />
                    <YAxis stroke="#94a3b8" fontSize={12} />
                    <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', color: '#fff' }} />
                    <Legend />
                    <Bar dataKey="net_cost" fill="#3b82f6" name="Net Cost ($)" />
                    <Bar dataKey="total_savings" fill="#10b981" name="Savings ($)" />
                  </BarChart>
                ) : queryResult.data ? (
                  <BarChart data={queryResult.data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey={Object.keys(queryResult.data[0] || {})[0] || 'label'} stroke="#94a3b8" fontSize={12} />
                    <YAxis stroke="#94a3b8" fontSize={12} />
                    <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', color: '#fff' }} />
                    <Bar dataKey={queryResult.measure || 'net_cost'} fill="#3b82f6">
                      {queryResult.data.map((_: any, index: number) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                ) : (
                  <div className="flex items-center justify-center h-full text-slate-500 text-sm">
                    No data array returned for chart visualization.
                  </div>
                )}
              </ResponsiveContainer>
            </div>
          </div>

          {/* Detailed Data Output Table */}
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 overflow-x-auto shadow-xl">
            <h3 className="text-sm font-semibold text-white uppercase tracking-wider mb-3">
              Analytical Results Table
            </h3>
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="border-b border-slate-800 text-slate-400 bg-slate-800/50">
                  {queryResult.matrix ? (
                    <>
                      <th className="p-2.5 font-medium">{queryResult.rows_dimension}</th>
                      {(queryResult.columns || []).map((col: string) => (
                        <th key={col} className="p-2.5 font-medium text-right">{col} ($)</th>
                      ))}
                    </>
                  ) : queryResult.department_budget_breakdown ? (
                    <>
                      <th className="p-2.5 font-medium">Department</th>
                      <th className="p-2.5 font-medium text-right">Budget ($)</th>
                      <th className="p-2.5 font-medium text-right">Actual Cost ($)</th>
                      <th className="p-2.5 font-medium text-right">Remaining ($)</th>
                      <th className="p-2.5 font-medium text-right">Utilization (%)</th>
                      <th className="p-2.5 font-medium text-center">Status</th>
                    </>
                  ) : (
                    Object.keys((queryResult.data && queryResult.data[0]) || {}).map(k => (
                      <th key={k} className="p-2.5 font-medium capitalize">{k.replace('_', ' ')}</th>
                    ))
                  )}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 text-slate-300">
                {queryResult.matrix ? (
                  queryResult.matrix.map((row: any, idx: number) => (
                    <tr key={idx} className="hover:bg-slate-800/30">
                      <td className="p-2.5 font-medium text-white">{row[queryResult.rows_dimension || 'department']}</td>
                      {(queryResult.columns || []).map((col: string) => (
                        <td key={col} className="p-2.5 text-right font-mono text-slate-200">
                          ${(row[col] || 0).toLocaleString()}
                        </td>
                      ))}
                    </tr>
                  ))
                ) : queryResult.department_budget_breakdown ? (
                  queryResult.department_budget_breakdown.map((row: any, idx: number) => (
                    <tr key={idx} className="hover:bg-slate-800/30">
                      <td className="p-2.5 font-medium text-white">{row.department}</td>
                      <td className="p-2.5 text-right font-mono">${row.budget_amount?.toLocaleString()}</td>
                      <td className="p-2.5 text-right font-mono">${row.net_cost?.toLocaleString()}</td>
                      <td className="p-2.5 text-right font-mono">${row.budget_remaining?.toLocaleString()}</td>
                      <td className="p-2.5 text-right font-mono font-bold text-amber-400">{row.budget_utilization_pct}%</td>
                      <td className="p-2.5 text-center">
                        <span className={`px-2 py-0.5 rounded text-[10px] font-semibold ${
                          row.status === 'Over Budget' ? 'bg-red-500/20 text-red-400 border border-red-500/30' :
                          row.status === 'Near Budget' ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30' :
                          'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                        }`}>
                          {row.status}
                        </span>
                      </td>
                    </tr>
                  ))
                ) : queryResult.data ? (
                  queryResult.data.map((row: any, idx: number) => (
                    <tr key={idx} className="hover:bg-slate-800/30">
                      {Object.keys(row).map(k => (
                        <td key={k} className="p-2.5 font-mono text-slate-300">
                          {typeof row[k] === 'number'
                            ? (k.includes('cost') || k.includes('savings') || k.includes('budget') ? `$${row[k].toLocaleString()}` : row[k])
                            : String(row[k])}
                        </td>
                      ))}
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} className="p-4 text-center text-slate-500">No output rows.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
    </div>
  );
};
