import React from 'react';
import { Clock, ShieldCheck, BarChart3, PieChart, Sparkles } from 'lucide-react';

interface PlaceholderViewProps {
  title: string;
  description: string;
  phaseTag: string;
  features: string[];
  iconType: 'quality' | 'eda' | 'analytics';
}

export const PlaceholderView: React.FC<PlaceholderViewProps> = ({
  title,
  description,
  phaseTag,
  features,
  iconType,
}) => {
  const getIcon = () => {
    switch (iconType) {
      case 'quality':
        return ShieldCheck;
      case 'eda':
        return BarChart3;
      case 'analytics':
        return PieChart;
      default:
        return Sparkles;
    }
  };

  const Icon = getIcon();

  return (
    <div className="max-w-4xl mx-auto space-y-6 py-6">
      <div className="bg-white border border-slate-200 rounded-xl p-8 text-center space-y-6 shadow-sm">
        <div className="w-16 h-16 bg-blue-50 text-blue-600 rounded-full flex items-center justify-center mx-auto border border-blue-100 shadow-2xs">
          <Icon className="w-8 h-8" />
        </div>

        <div className="space-y-2">
          <div className="inline-flex items-center space-x-2 px-3 py-1 rounded-full bg-slate-100 text-slate-700 border border-slate-200 text-xs font-semibold uppercase tracking-wider">
            <Clock className="w-3.5 h-3.5 text-slate-500" />
            <span>{phaseTag} Feature Roadmap</span>
          </div>
          <h2 className="text-2xl font-bold text-slate-900 tracking-tight">{title}</h2>
          <p className="text-slate-500 text-xs max-w-xl mx-auto leading-relaxed">{description}</p>
        </div>

        <div className="p-5 bg-slate-50 border border-slate-200 rounded-xl text-left max-w-lg mx-auto space-y-3">
          <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block">
            Upcoming Features in {phaseTag}:
          </span>
          <ul className="space-y-2 text-xs text-slate-600">
            {features.map((feat, i) => (
              <li key={i} className="flex items-start space-x-2">
                <span className="text-blue-600 font-bold mt-0.5">•</span>
                <span>{feat}</span>
              </li>
            ))}
          </ul>
        </div>

        <div className="p-3 bg-slate-50 border border-slate-100 rounded-lg text-slate-400 text-xs font-medium italic">
          "Scheduled for activation in future roadmap release phases"
        </div>
      </div>
    </div>
  );
};

