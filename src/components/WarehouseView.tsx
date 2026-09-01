import React, { useState, useEffect } from 'react';
import { Database, Layers, GitBranch, Table, Cpu, CheckCircle, AlertTriangle, ArrowRight, RefreshCw, Play, FileText, Code, PieChart } from 'lucide-react';
import { OlapAnalyzerView } from './OlapAnalyzerView';

interface WarehouseSummary {
  status: string;
  catalog: string;
  schema: string;
  storage_format: string;
  fact_table: string;
  fact_record_count: number;
  total_net_cost_usd: number;
  total_savings_usd: number;
  anomalies_count: number;
  dimension_counts: Record<string, number>;
  grain: string;
}

interface AnalyticalQuery {
  id: number;
  title: string;
  sql: string;
  result: any[];
}

export const WarehouseView: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'olap_cube' | 'overview' | 'star' | 'snowflake' | 'queries' | 'oltp' | 'scd2'>('olap_cube');
  const [summary, setSummary] = useState<WarehouseSummary | null>(null);
  const [queries, setQueries] = useState<AnalyticalQuery[]>([]);
  const [selectedQueryId, setSelectedQueryId] = useState<number>(1);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [scdResult, setScdResult] = useState<any | null>(null);
  const [scdLoading, setScdLoading] = useState<boolean>(false);

  const fetchWarehouseData = async () => {
    setIsLoading(true);
    try {
      const [sumRes, qRes] = await Promise.all([
        fetch('/api/warehouse/summary'),
        fetch('/api/warehouse/queries')
      ]);
      if (sumRes.ok) setSummary(await sumRes.json());
      if (qRes.ok) setQueries(await qRes.json());
    } catch (err) {
      console.error("Failed to load warehouse data", err);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchWarehouseData();
  }, []);

  const handleRunScd2Demo = async () => {
    setScdLoading(true);
    try {
      const res = await fetch('/api/warehouse/scd2-demo?project_id=prj-analytics&new_environment=staging', { method: 'POST' });
      if (res.ok) {
        const data = await res.json();
        setScdResult(data);
        fetchWarehouseData();
      }
    } catch (err) {
      console.error(err);
    } finally {
      setScdLoading(false);
    }
  };

  const selectedQuery = queries.find(q => q.id === selectedQueryId) || queries[0];

  return (
    <div className="space-y-6">
      {/* Header Banner */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <div className="flex items-center space-x-3 mb-2">
            <span className="bg-blue-500/20 text-blue-400 border border-blue-500/30 text-xs font-bold px-2.5 py-1 rounded-full flex items-center gap-1.5">
              <Database className="w-3.5 h-3.5" /> Phase 5
            </span>
            <span className="text-slate-400 text-xs font-mono">
              Catalog: cloud_cost_catalog.cloud_warehouse
            </span>
          </div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Data Warehouse & OLAP Dimensional Model</h1>
          <p className="text-sm text-slate-400 mt-1 max-w-3xl">
            Star Schema & Snowflake Schema OLAP Data Warehouse implemented in Databricks Delta Lake. Features surrogate keys, SCD Type 2 tracking, and 15 analytical query engine views.
          </p>
        </div>
        <div className="flex items-center space-x-3">
          <button
            onClick={fetchWarehouseData}
            disabled={isLoading}
            className="flex items-center space-x-2 bg-slate-800 hover:bg-slate-700 text-white px-4 py-2 rounded-lg text-sm font-medium border border-slate-700 transition"
          >
            <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
            <span>Refresh Warehouse</span>
          </button>
        </div>
      </div>

      {/* Metrics Banner */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg">
          <div className="flex items-center justify-between">
            <span className="text-xs text-slate-400 font-semibold uppercase tracking-wider">Fact Table Rows</span>
            <Table className="w-5 h-5 text-blue-400" />
          </div>
          <div className="mt-2 text-2xl font-bold text-white">
            {summary?.fact_record_count.toLocaleString() || '0'}
          </div>
          <div className="mt-1 text-[11px] text-slate-400 font-mono">
            fact_cloud_cost (Delta Lake)
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg">
          <div className="flex items-center justify-between">
            <span className="text-xs text-slate-400 font-semibold uppercase tracking-wider">Total Warehouse Net Spend</span>
            <span className="text-emerald-400 text-xs font-mono font-bold">USD</span>
          </div>
          <div className="mt-2 text-2xl font-bold text-emerald-400">
            ${summary?.total_net_cost_usd.toLocaleString(undefined, { minimumFractionDigits: 2 }) || '0.00'}
          </div>
          <div className="mt-1 text-[11px] text-slate-400 font-mono">
            Savings: ${summary?.total_savings_usd.toLocaleString(undefined, { minimumFractionDigits: 2 }) || '0.00'}
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg">
          <div className="flex items-center justify-between">
            <span className="text-xs text-slate-400 font-semibold uppercase tracking-wider">Loaded Dimensions</span>
            <Layers className="w-5 h-5 text-indigo-400" />
          </div>
          <div className="mt-2 text-2xl font-bold text-white">
            {summary ? Object.keys(summary.dimension_counts).length : 0} Tables
          </div>
          <div className="mt-1 text-[11px] text-slate-400">
            Surrogate Keys & Key=0 Strategy
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg">
          <div className="flex items-center justify-between">
            <span className="text-xs text-slate-400 font-semibold uppercase tracking-wider">Anomalous Items</span>
            <AlertTriangle className="w-5 h-5 text-amber-400" />
          </div>
          <div className="mt-2 text-2xl font-bold text-amber-400">
            {summary?.anomalies_count || 0} Records
          </div>
          <div className="mt-1 text-[11px] text-slate-400">
            Budget Utilization &gt;= 90%
          </div>
        </div>
      </div>

      {/* View Selector Tabs */}
      <div className="border-b border-slate-800 flex space-x-2 overflow-x-auto pb-1">
        {[
          { id: 'olap_cube', label: 'Data Cube & OLAP Analyzer', icon: PieChart },
          { id: 'overview', label: 'Warehouse Overview', icon: Database },
          { id: 'star', label: 'Star Schema Visualizer', icon: Layers },
          { id: 'snowflake', label: 'Snowflake Schema', icon: GitBranch },
          { id: 'queries', label: '15 Analytical Queries', icon: Code },
          { id: 'oltp', label: 'OLTP vs OLAP Architecture', icon: Cpu },
          { id: 'scd2', label: 'SCD Type 2 Simulator', icon: RefreshCw },
        ].map((tab) => {
          const Icon = tab.icon;
          const isSelected = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as any)}
              className={`flex items-center space-x-2 px-4 py-2.5 rounded-lg text-sm font-medium transition whitespace-nowrap ${
                isSelected
                  ? 'bg-blue-600 text-white shadow-md'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/60'
              }`}
            >
              <Icon className="w-4 h-4" />
              <span>{tab.label}</span>
            </button>
          );
        })}
      </div>

      {/* TAB 0: OLAP DATA CUBE ANALYZER */}
      {activeTab === 'olap_cube' && (
        <OlapAnalyzerView />
      )}

      {/* TAB 1: OVERVIEW */}
      {activeTab === 'overview' && (
        <div className="space-y-6">
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl">
            <h3 className="text-lg font-bold text-white mb-2">Fact Table Defined Grain</h3>
            <div className="bg-blue-950/40 border border-blue-800/40 rounded-lg p-4 font-mono text-sm text-blue-300">
              "{summary?.grain || 'ONE ROW REPRESENTS ONE CLOUD COST RECORD FOR A SPECIFIC DATE, ACCOUNT, PROJECT, ENVIRONMENT, PROVIDER, REGION, SERVICE, AND RESOURCE TYPE'}"
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl">
              <h3 className="text-base font-bold text-white mb-4 flex items-center justify-between">
                <span>Dimension Table Row Counts</span>
                <span className="text-xs text-slate-400 font-mono">Key=0 Included</span>
              </h3>
              <div className="space-y-3">
                {summary && Object.entries(summary.dimension_counts).map(([dim, count]) => (
                  <div key={dim} className="flex items-center justify-between p-3 bg-slate-800/50 rounded-lg border border-slate-700/50">
                    <span className="font-mono text-sm text-slate-200">{dim}</span>
                    <span className="bg-blue-500/10 text-blue-400 font-mono font-bold text-xs px-2.5 py-1 rounded-full border border-blue-500/20">
                      {count} rows
                    </span>
                  </div>
                ))}
              </div>
            </div>

            <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl space-y-4">
              <h3 className="text-base font-bold text-white">Warehouse Specifications & Features</h3>

              <div className="p-4 bg-slate-800/40 rounded-lg border border-slate-700/50 space-y-2">
                <div className="text-sm font-semibold text-slate-200">Surrogate Key Strategy</div>
                <p className="text-xs text-slate-400">
                  Every dimension uses an integer surrogate key (e.g. <code className="text-blue-300">20260115</code> for dates, <code className="text-blue-300">1, 2, 3</code> for entities). Isolates analytical queries from source system key changes.
                </p>
              </div>

              <div className="p-4 bg-slate-800/40 rounded-lg border border-slate-700/50 space-y-2">
                <div className="text-sm font-semibold text-slate-200">Unknown Member Handling</div>
                <p className="text-xs text-slate-400">
                  Default record inserted at <code className="text-blue-300">Key = 0</code> (<code className="text-blue-300">UNKNOWN</code>) across all dimensions to prevent foreign key resolution failures during batch loads.
                </p>
              </div>

              <div className="p-4 bg-slate-800/40 rounded-lg border border-slate-700/50 space-y-2">
                <div className="text-sm font-semibold text-slate-200">Idempotent MERGE ETL</div>
                <p className="text-xs text-slate-400">
                  Determines record hashes and uses delta merge logic to ensure zero duplicate fact rows even when re-executing Airflow DAG pipelines.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* TAB 2: STAR SCHEMA */}
      {activeTab === 'star' && (
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl space-y-6">
          <div className="flex items-center justify-between border-b border-slate-800 pb-4">
            <div>
              <h3 className="text-lg font-bold text-white">Star Schema Architectural Model</h3>
              <p className="text-xs text-slate-400 mt-0.5">Central Fact Table (<code className="text-blue-400">fact_cloud_cost</code>) surrounded by 9 denormalized dimensions</p>
            </div>
            <span className="text-xs bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 px-3 py-1 rounded-full font-mono">
              Databricks Delta Table
            </span>
          </div>

          {/* Star Diagram Visualizer */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {/* Left Dimensions */}
            <div className="space-y-4">
              <div className="bg-slate-800/80 border border-blue-500/30 rounded-lg p-4 shadow">
                <div className="flex items-center justify-between text-sm font-bold text-blue-300 font-mono">
                  <span>dim_date</span>
                  <span className="text-[10px] text-slate-400">PK: date_key (int)</span>
                </div>
                <div className="text-xs text-slate-400 mt-2 space-y-1 font-mono text-[11px]">
                  <div>• date, year, quarter, month</div>
                  <div>• month_name, week, day</div>
                  <div>• is_month_start, is_month_end</div>
                </div>
              </div>

              <div className="bg-slate-800/80 border border-blue-500/30 rounded-lg p-4 shadow">
                <div className="flex items-center justify-between text-sm font-bold text-blue-300 font-mono">
                  <span>dim_cloud</span>
                  <span className="text-[10px] text-slate-400">PK: cloud_key (int)</span>
                </div>
                <div className="text-xs text-slate-400 mt-2 space-y-1 font-mono text-[11px]">
                  <div>• cloud_provider (AWS/GCP/AZURE)</div>
                  <div>• provider_group</div>
                </div>
              </div>

              <div className="bg-slate-800/80 border border-blue-500/30 rounded-lg p-4 shadow">
                <div className="flex items-center justify-between text-sm font-bold text-blue-300 font-mono">
                  <span>dim_account</span>
                  <span className="text-[10px] text-slate-400">PK: account_key (int)</span>
                </div>
                <div className="text-xs text-slate-400 mt-2 space-y-1 font-mono text-[11px]">
                  <div>• account_id (Natural Key)</div>
                  <div>• account_name, cloud_key</div>
                  <div>• effective_date, expiry_date, is_current</div>
                </div>
              </div>
            </div>

            {/* Center FACT Table */}
            <div className="bg-blue-950/40 border-2 border-blue-500 rounded-xl p-5 shadow-2xl flex flex-col justify-between space-y-4">
              <div>
                <div className="flex items-center justify-between border-b border-blue-800/60 pb-3">
                  <span className="font-bold text-white text-base tracking-tight font-mono">fact_cloud_cost</span>
                  <span className="text-xs bg-blue-500/20 text-blue-300 font-mono px-2 py-0.5 rounded">FACT TABLE</span>
                </div>

                <div className="mt-3 text-xs text-slate-300 font-semibold uppercase tracking-wider">Foreign Keys (Surrogate)</div>
                <div className="mt-1 space-y-1 text-xs font-mono text-blue-300 bg-slate-900/60 p-2.5 rounded border border-slate-800">
                  <div>• date_key FK -&gt; dim_date</div>
                  <div>• cloud_key FK -&gt; dim_cloud</div>
                  <div>• account_key FK -&gt; dim_account</div>
                  <div>• project_key FK -&gt; dim_project</div>
                  <div>• organization_key FK -&gt; dim_org</div>
                  <div>• location_key FK -&gt; dim_location</div>
                  <div>• service_key FK -&gt; dim_service</div>
                  <div>• environment_key FK -&gt; dim_env</div>
                  <div>• currency_key FK -&gt; dim_currency</div>
                </div>

                <div className="mt-3 text-xs text-slate-300 font-semibold uppercase tracking-wider">Additive Measures</div>
                <div className="mt-1 space-y-1 text-xs font-mono text-emerald-400 bg-slate-900/60 p-2.5 rounded border border-slate-800">
                  <div>• net_cost, list_cost, discount_amount</div>
                  <div>• reserved_savings, spot_savings, total_savings</div>
                  <div>• budget_amount, budget_remaining</div>
                  <div>• budget_utilization_pct, is_anomaly</div>
                </div>
              </div>

              <div className="text-[11px] text-slate-400 text-center font-mono border-t border-blue-800/40 pt-2">
                Grain: 1 Row per Date/Account/Project/Svc/Res
              </div>
            </div>

            {/* Right Dimensions */}
            <div className="space-y-4">
              <div className="bg-slate-800/80 border border-blue-500/30 rounded-lg p-4 shadow">
                <div className="flex items-center justify-between text-sm font-bold text-blue-300 font-mono">
                  <span>dim_project</span>
                  <span className="text-[10px] text-slate-400">PK: project_key (int)</span>
                </div>
                <div className="text-xs text-slate-400 mt-2 space-y-1 font-mono text-[11px]">
                  <div>• project_id, account_key</div>
                  <div>• environment</div>
                  <div>• effective_date, expiry_date, is_current</div>
                </div>
              </div>

              <div className="bg-slate-800/80 border border-blue-500/30 rounded-lg p-4 shadow">
                <div className="flex items-center justify-between text-sm font-bold text-blue-300 font-mono">
                  <span>dim_organization</span>
                  <span className="text-[10px] text-slate-400">PK: organization_key (int)</span>
                </div>
                <div className="text-xs text-slate-400 mt-2 space-y-1 font-mono text-[11px]">
                  <div>• business_unit</div>
                  <div>• department</div>
                  <div>• cost_center</div>
                </div>
              </div>

              <div className="bg-slate-800/80 border border-blue-500/30 rounded-lg p-4 shadow">
                <div className="flex items-center justify-between text-sm font-bold text-blue-300 font-mono">
                  <span>dim_service</span>
                  <span className="text-[10px] text-slate-400">PK: service_key (int)</span>
                </div>
                <div className="text-xs text-slate-400 mt-2 space-y-1 font-mono text-[11px]">
                  <div>• service (e.g. EC2, BigQuery)</div>
                  <div>• resource_type</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* TAB 3: SNOWFLAKE SCHEMA */}
      {activeTab === 'snowflake' && (
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl space-y-6">
          <div>
            <h3 className="text-lg font-bold text-white">Snowflake Schema Normalized Hierarchy</h3>
            <p className="text-xs text-slate-400 mt-0.5">Normalized dimensional trees reducing data redundancy across parent entity hierarchies</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Hierarchy 1 */}
            <div className="bg-slate-800/60 border border-slate-700 rounded-xl p-5 space-y-4">
              <h4 className="text-sm font-bold text-blue-300 uppercase tracking-wider font-mono">
                Hierarchy 1: Organization Structure
              </h4>

              <div className="space-y-3 font-mono text-xs">
                <div className="bg-slate-900 p-3 rounded border border-slate-700">
                  <div className="text-slate-400 text-[10px]">Level 1 (Root)</div>
                  <div className="font-bold text-white">dim_business_unit</div>
                  <div className="text-blue-400 text-[11px]">PK: business_unit_key | Attributes: business_unit</div>
                </div>

                <div className="flex justify-center"><ArrowRight className="w-4 h-4 text-slate-500 rotate-90" /></div>

                <div className="bg-slate-900 p-3 rounded border border-slate-700">
                  <div className="text-slate-400 text-[10px]">Level 2</div>
                  <div className="font-bold text-white">dim_department</div>
                  <div className="text-blue-400 text-[11px]">PK: department_key | FK: business_unit_key</div>
                </div>

                <div className="flex justify-center"><ArrowRight className="w-4 h-4 text-slate-500 rotate-90" /></div>

                <div className="bg-slate-900 p-3 rounded border border-slate-700">
                  <div className="text-slate-400 text-[10px]">Level 3 (Leaf connected to Fact)</div>
                  <div className="font-bold text-white">dim_cost_center</div>
                  <div className="text-blue-400 text-[11px]">PK: cost_center_key | FK: department_key</div>
                </div>
              </div>
            </div>

            {/* Hierarchy 2 */}
            <div className="bg-slate-800/60 border border-slate-700 rounded-xl p-5 space-y-4">
              <h4 className="text-sm font-bold text-blue-300 uppercase tracking-wider font-mono">
                Hierarchy 2: Cloud Infrastructure Hierarchy
              </h4>

              <div className="space-y-3 font-mono text-xs">
                <div className="bg-slate-900 p-3 rounded border border-slate-700">
                  <div className="text-slate-400 text-[10px]">Level 1 (Root)</div>
                  <div className="font-bold text-white">dim_cloud</div>
                  <div className="text-blue-400 text-[11px]">PK: cloud_key | Attributes: cloud_provider, provider_group</div>
                </div>

                <div className="flex justify-center"><ArrowRight className="w-4 h-4 text-slate-500 rotate-90" /></div>

                <div className="bg-slate-900 p-3 rounded border border-slate-700">
                  <div className="text-slate-400 text-[10px]">Level 2</div>
                  <div className="font-bold text-white">dim_account</div>
                  <div className="text-blue-400 text-[11px]">PK: account_key | FK: cloud_key</div>
                </div>

                <div className="flex justify-center"><ArrowRight className="w-4 h-4 text-slate-500 rotate-90" /></div>

                <div className="bg-slate-900 p-3 rounded border border-slate-700">
                  <div className="text-slate-400 text-[10px]">Level 3 (Leaf connected to Fact)</div>
                  <div className="font-bold text-white">dim_project</div>
                  <div className="text-blue-400 text-[11px]">PK: project_key | FK: account_key</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* TAB 4: 15 ANALYTICAL QUERIES */}
      {activeTab === 'queries' && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Query Selector List */}
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-xl space-y-2 max-h-[600px] overflow-y-auto">
            <h3 className="text-xs font-bold text-slate-400 uppercase tracking-wider px-2 mb-2">
              15 Analytical Queries Engine
            </h3>
            {queries.map((q) => (
              <button
                key={q.id}
                onClick={() => setSelectedQueryId(q.id)}
                className={`w-full text-left px-3 py-2.5 rounded-lg text-xs font-medium transition flex items-center justify-between ${
                  selectedQueryId === q.id
                    ? 'bg-blue-600 text-white font-bold shadow-sm'
                    : 'text-slate-300 hover:bg-slate-800'
                }`}
              >
                <span className="truncate">{q.title}</span>
                <Play className="w-3 h-3 shrink-0 ml-2" />
              </button>
            ))}
          </div>

          {/* Query Result Panel */}
          <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl space-y-4">
            {selectedQuery && (
              <>
                <div className="flex items-center justify-between border-b border-slate-800 pb-3">
                  <h3 className="text-base font-bold text-white">{selectedQuery.title}</h3>
                  <span className="text-xs font-mono bg-blue-500/10 text-blue-400 px-2.5 py-1 rounded border border-blue-500/20">
                    {selectedQuery.result.length} Rows
                  </span>
                </div>

                {/* SQL Box */}
                <div>
                  <div className="text-xs text-slate-400 font-mono mb-1">Generated Warehouse SQL</div>
                  <pre className="bg-slate-950 p-3 rounded-lg border border-slate-800 font-mono text-xs text-emerald-400 overflow-x-auto">
                    {selectedQuery.sql}
                  </pre>
                </div>

                {/* Data Grid */}
                <div>
                  <div className="text-xs text-slate-400 font-mono mb-2">Execution Results Data Grid</div>
                  <div className="overflow-x-auto max-h-[320px] rounded-lg border border-slate-800">
                    <table className="w-full text-left border-collapse font-mono text-xs">
                      <thead className="bg-slate-800 text-slate-300 uppercase text-[10px]">
                        <tr>
                          {selectedQuery.result.length > 0 && Object.keys(selectedQuery.result[0]).map((col) => (
                            <th key={col} className="p-2.5 border-b border-slate-700">{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800 text-slate-200">
                        {selectedQuery.result.map((row, idx) => (
                          <tr key={idx} className="hover:bg-slate-800/40">
                            {Object.values(row).map((val: any, vIdx) => (
                              <td key={vIdx} className="p-2.5 font-mono text-slate-300">
                                {typeof val === 'number' ? val.toLocaleString() : String(val)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      )}

      {/* TAB 5: OLTP VS OLAP */}
      {activeTab === 'oltp' && (
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl space-y-6">
          <h3 className="text-lg font-bold text-white">OLTP vs OLAP Architectural Boundary</h3>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-slate-800/60 border border-slate-700 rounded-xl p-5 space-y-3">
              <div className="flex items-center space-x-2 text-blue-400 font-bold font-mono">
                <Database className="w-5 h-5" />
                <span>OLTP Layer (SQLite)</span>
              </div>
              <p className="text-xs text-slate-300">
                Operational control database managing dataset uploads, pipeline run state ledgers, Kafka event auditing, and entity control metadata.
              </p>
              <ul className="text-xs text-slate-400 space-y-1 font-mono list-disc list-inside pt-2">
                <li>datasets</li>
                <li>pipeline_runs</li>
                <li>airflow_task_instances</li>
                <li>kafka_event_audit</li>
              </ul>
            </div>

            <div className="bg-slate-800/60 border border-blue-500/40 rounded-xl p-5 space-y-3">
              <div className="flex items-center space-x-2 text-emerald-400 font-bold font-mono">
                <Layers className="w-5 h-5" />
                <span>OLAP Layer (Databricks Delta Warehouse)</span>
              </div>
              <p className="text-xs text-slate-300">
                Analytical Data Warehouse housing Star/Snowflake schema Delta tables optimized for heavy multi-dimensional aggregation and financial reporting.
              </p>
              <ul className="text-xs text-slate-400 space-y-1 font-mono list-disc list-inside pt-2">
                <li>fact_cloud_cost</li>
                <li>dim_date, dim_cloud, dim_account, dim_project</li>
                <li>dim_organization, dim_service, dim_location</li>
              </ul>
            </div>
          </div>
        </div>
      )}

      {/* TAB 6: SCD TYPE 2 SIMULATOR */}
      {activeTab === 'scd2' && (
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl space-y-6">
          <div>
            <h3 className="text-lg font-bold text-white">Slowly Changing Dimension (SCD Type 2) Interactive Simulator</h3>
            <p className="text-xs text-slate-400 mt-1">
              Demonstrates historical record versioning on <code className="text-blue-400">dim_project</code>. When an attribute changes (e.g. project environment from production to staging), the existing record is expired (<code className="text-amber-400">is_current=false</code>) and a new surrogate key record is created.
            </p>
          </div>

          <div className="p-5 bg-slate-800/60 rounded-xl border border-slate-700 flex flex-col sm:flex-row items-center justify-between gap-4">
            <div>
              <div className="text-sm font-bold text-white">Target Project: prj-analytics</div>
              <div className="text-xs text-slate-400">Action: Update environment to 'staging' via SCD Type 2</div>
            </div>
            <button
              onClick={handleRunScd2Demo}
              disabled={scdLoading}
              className="bg-blue-600 hover:bg-blue-500 text-white font-bold text-xs px-5 py-2.5 rounded-lg shadow transition flex items-center space-x-2"
            >
              <RefreshCw className={`w-4 h-4 ${scdLoading ? 'animate-spin' : ''}`} />
              <span>Simulate SCD Type 2 Update</span>
            </button>
          </div>

          {scdResult && (
            <div className="bg-slate-950 p-4 rounded-xl border border-emerald-500/40 text-xs font-mono space-y-2">
              <div className="text-emerald-400 font-bold flex items-center space-x-2">
                <CheckCircle className="w-4 h-4" />
                <span>SCD Type 2 Record Update Executed Successfully!</span>
              </div>
              <pre className="text-slate-300">{JSON.stringify(scdResult, null, 2)}</pre>
            </div>
          )}
        </div>
      )}
    </div>
  );
};
