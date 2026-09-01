import React from 'react';
import { LayoutDashboard, UploadCloud, GitPullRequest, ShieldCheck, BarChart3, PieChart, Lock, Sparkles, Radio } from 'lucide-react';

export type NavTab = 'dashboard' | 'upload' | 'pipeline' | 'streaming' | 'quality' | 'eda' | 'analytics';

interface SidebarProps {
  activeTab: NavTab;
  setActiveTab: (tab: NavTab) => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ activeTab, setActiveTab }) => {
  const mainNav = [
    { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
    { id: 'upload', label: 'Dataset Upload', icon: UploadCloud },
    { id: 'pipeline', label: 'Airflow & Databricks ETL', icon: GitPullRequest, tag: 'Phase 4' },
    { id: 'analytics', label: 'Data Warehouse & OLAP', icon: PieChart, tag: 'Phase 6', isNew: true },
    { id: 'quality', label: 'Data Quality & Audits', icon: ShieldCheck, tag: 'Phase 2' },
    { id: 'eda', label: 'EDA & Profiling', icon: BarChart3, tag: 'Phase 2' },
  ];

  const futureNav: any[] = [];

  return (
    <aside id="app-sidebar" className="w-64 bg-slate-900 text-slate-300 flex flex-col shrink-0 shadow-xl border-r border-slate-800 h-full">
      {/* Brand Header */}
      <div className="p-6 flex items-center space-x-3 border-b border-slate-800">
        <div className="w-8 h-8 bg-blue-500 rounded flex items-center justify-center font-bold text-white shadow-lg text-sm">
          C
        </div>
        <div>
          <span className="font-bold text-white tracking-tight uppercase text-xs block">
            Cloud Intelligence
          </span>
          <span className="text-[10px] text-slate-400 block">Databricks Engine v2.0</span>
        </div>
      </div>

      {/* Nav Menu */}
      <nav className="flex-1 py-6 px-4 space-y-1 overflow-y-auto">
        <div className="pb-2 px-3">
          <span className="text-[10px] uppercase font-semibold text-slate-500 tracking-wider">
            Active Core Modules
          </span>
        </div>

        {mainNav.map((item) => {
          const Icon = item.icon;
          const isSelected = activeTab === item.id;

          return (
            <button
              key={item.id}
              id={`nav-btn-${item.id}`}
              onClick={() => setActiveTab(item.id as NavTab)}
              className={`w-full flex items-center justify-between px-3 py-2.5 rounded-md text-sm transition-colors ${
                isSelected
                  ? 'bg-blue-600 text-white font-medium shadow-sm'
                  : 'text-slate-300 hover:bg-slate-800 hover:text-white'
              }`}
            >
              <div className="flex items-center space-x-3">
                <Icon className="w-4 h-4 shrink-0" />
                <span>{item.label}</span>
              </div>
              {item.tag && (
                <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${
                  item.tag === 'Phase 3' 
                    ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-400/30' 
                    : 'bg-blue-500/20 text-blue-300 border border-blue-400/30'
                }`}>
                  {item.tag}
                </span>
              )}
            </button>
          );
        })}

        <div className="pt-6 pb-2 px-3">
          <span className="text-[10px] uppercase font-semibold text-slate-500 tracking-wider">
            Future Architecture
          </span>
        </div>

        {futureNav.map((item) => {
          const Icon = item.icon;
          const isSelected = activeTab === item.id;

          return (
            <button
              key={item.id}
              id={`nav-btn-${item.id}`}
              onClick={() => setActiveTab(item.id as NavTab)}
              className={`w-full flex items-center justify-between px-3 py-2.5 rounded-md text-sm transition-colors ${
                isSelected
                  ? 'bg-blue-600 text-white font-medium shadow-sm'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/80'
              }`}
            >
              <div className="flex items-center space-x-3">
                <Icon className="w-4 h-4 shrink-0" />
                <span>{item.label}</span>
              </div>
              <div className="flex items-center space-x-1">
                <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700/60">
                  {item.tag}
                </span>
                <Lock className="w-3 h-3 text-slate-500" />
              </div>
            </button>
          );
        })}
      </nav>

      {/* Deployment Footer Card */}
      <div className="mt-auto p-4 bg-slate-950/50 border-t border-slate-800/80">
        <div className="bg-emerald-900/30 border border-emerald-500/20 p-3 rounded-lg text-slate-300 space-y-1">
          <p className="text-[10px] font-bold text-emerald-400 uppercase tracking-wider flex items-center space-x-1">
            <Sparkles className="w-3 h-3" />
            <span>Phase 6 Engine Active</span>
          </p>
          <p className="text-[11px] leading-relaxed text-slate-300">
            Batch ETL + OLAP Data Cube + Analytics Dashboard live.
          </p>
        </div>
      </div>
    </aside>
  );
};
