import React from 'react';
import { Database, Activity, Cloud, ChevronRight, Bell } from 'lucide-react';

interface HeaderProps {
  healthStatus: { status: string; database: string } | null;
}

export const Header: React.FC<HeaderProps> = ({ healthStatus }) => {
  const isHealthy = healthStatus?.status === 'healthy';

  return (
    <header id="app-header" className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-8 shrink-0 shadow-sm text-slate-900">
      <div className="flex items-center space-x-3 text-sm">
        <div className="flex items-center space-x-2">
          <div className="w-7 h-7 bg-blue-600 rounded flex items-center justify-center font-bold text-white shadow-sm text-xs">
            C
          </div>
          <span className="text-slate-500 font-medium">Workspace</span>
        </div>
        <ChevronRight className="w-3.5 h-3.5 text-slate-400" />
        <span className="font-bold text-slate-900 tracking-tight">Cloud Cost Intelligence</span>
        <span className="hidden sm:inline-block px-2 py-0.5 rounded text-[10px] font-bold bg-blue-50 text-blue-600 border border-blue-200 uppercase tracking-wider">
          Phase 1
        </span>
      </div>

      <div className="flex items-center space-x-4">
        <div className="hidden md:flex items-center space-x-2 px-3 py-1.5 rounded-lg bg-slate-100 border border-slate-200 text-xs">
          <Database className="w-3.5 h-3.5 text-blue-600" />
          <span className="text-slate-700 font-medium">SQLite Metadata Store</span>
        </div>

        <div className={`flex items-center space-x-2 px-3 py-1.5 rounded-lg text-xs font-semibold ${
          isHealthy 
            ? 'bg-emerald-50 text-emerald-700 border border-emerald-200' 
            : 'bg-amber-50 text-amber-700 border border-amber-200'
        }`}>
          <span className={`w-2 h-2 rounded-full ${isHealthy ? 'bg-emerald-500 animate-pulse' : 'bg-amber-500'}`} />
          <span>{isHealthy ? 'API READY' : 'CONNECTING...'}</span>
        </div>

        <div className="pl-2 border-l border-slate-200 flex items-center space-x-3">
          <button className="p-2 text-slate-400 hover:text-slate-600 rounded-lg hover:bg-slate-100 transition">
            <Bell className="w-4 h-4" />
          </button>
          <div className="w-8 h-8 rounded-full bg-slate-200 border border-slate-300 flex items-center justify-center font-bold text-slate-600 text-xs shadow-sm">
            CI
          </div>
        </div>
      </div>
    </header>
  );
};

