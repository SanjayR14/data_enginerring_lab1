import React, { useState, useEffect } from 'react';
import { fetchHealth, fetchDatasets } from './services/api';
import { Dataset } from './types';
import { Header } from './components/Header';
import { Sidebar, NavTab } from './components/Sidebar';
import { DashboardView } from './components/DashboardView';
import { UploadView } from './components/UploadView';
import { PipelineStatusView } from './components/PipelineStatusView';

import { DataQualityView } from './components/DataQualityView';
import { EdaProfileView } from './components/EdaProfileView';
import { WarehouseView } from './components/WarehouseView';
import { PlaceholderView } from './components/PlaceholderView';
import { DataPreviewModal } from './components/DataPreviewModal';

export default function App() {
  const [activeTab, setActiveTab] = useState<NavTab>('dashboard');
  const [healthStatus, setHealthStatus] = useState<{ status: string; database: string } | null>(null);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [previewDatasetId, setPreviewDatasetId] = useState<string | null>(null);

  const loadBackendData = async () => {
    setIsLoading(true);
    try {
      const [health, datasetList] = await Promise.all([
        fetchHealth().catch(() => null),
        fetchDatasets().catch(() => []),
      ]);
      setHealthStatus(health);
      setDatasets(datasetList);
    } catch (err) {
      console.error('Failed to connect to backend', err);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadBackendData();
  }, []);

  const handleUploadSuccess = (newDataset: Dataset) => {
    setDatasets((prev) => {
      const filtered = prev.filter((d) => d.id !== newDataset.id);
      return [newDataset, ...filtered];
    });
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans antialiased selection:bg-blue-600 selection:text-white">
      <Header healthStatus={healthStatus} />

      <div className="flex flex-1 overflow-hidden">
        <Sidebar activeTab={activeTab} setActiveTab={setActiveTab} />

        <main className="flex-1 p-6 overflow-y-auto bg-slate-950/90">
          {activeTab === 'dashboard' && (
            <DashboardView
              datasets={datasets}
              onNavigateUpload={() => setActiveTab('upload')}
              onPreviewDataset={(id) => setPreviewDatasetId(id)}
              onRefresh={loadBackendData}
              isLoading={isLoading}
            />
          )}

          {activeTab === 'upload' && (
            <UploadView onUploadSuccess={handleUploadSuccess} />
          )}

          {activeTab === 'pipeline' && (
            <PipelineStatusView datasets={datasets} />
          )}



          {activeTab === 'quality' && (
            <DataQualityView datasets={datasets} />
          )}

          {activeTab === 'eda' && (
            <EdaProfileView datasets={datasets} />
          )}

          {activeTab === 'analytics' && (
            <WarehouseView />
          )}
        </main>
      </div>

      {/* Modal for dataset preview */}
      {previewDatasetId && (
        <DataPreviewModal
          datasetId={previewDatasetId}
          onClose={() => setPreviewDatasetId(null)}
        />
      )}
    </div>
  );
}
