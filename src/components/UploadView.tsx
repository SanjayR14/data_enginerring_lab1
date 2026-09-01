import React, { useState, useRef } from 'react';
import { uploadDataset, getSampleDatasetUrl } from '../services/api';
import { Dataset, DatasetPreview } from '../types';
import { UploadCloud, FileSpreadsheet, CheckCircle2, AlertCircle, Download, FileText, Columns, Eye, Loader2 } from 'lucide-react';

interface UploadViewProps {
  onUploadSuccess: (dataset: Dataset) => void;
}

export const UploadView: React.FC<UploadViewProps> = ({ onUploadSuccess }) => {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState<boolean>(false);
  const [isUploading, setIsUploading] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [uploadedDataset, setUploadedDataset] = useState<Dataset | null>(null);
  const [previewData, setPreviewData] = useState<DatasetPreview | null>(null);
  const [isLoadingPreview, setIsLoadingPreview] = useState<boolean>(false);

  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileSelect = (file: File) => {
    setErrorMsg(null);
    setUploadedDataset(null);
    setPreviewData(null);

    if (!file.name.toLowerCase().endsWith('.csv')) {
      setErrorMsg('Invalid file type: Please select a valid CSV dataset file (.csv).');
      setSelectedFile(null);
      return;
    }

    if (file.size === 0) {
      setErrorMsg('Invalid dataset: The selected file is empty (0 bytes).');
      setSelectedFile(null);
      return;
    }

    setSelectedFile(file);
  };

  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      handleFileSelect(e.dataTransfer.files[0]);
    }
  };

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = () => {
    setIsDragging(false);
  };

  const handleUploadSubmit = async () => {
    if (!selectedFile) return;

    setIsUploading(true);
    setErrorMsg(null);

    try {
      const result = await uploadDataset(selectedFile);
      setUploadedDataset(result);
      onUploadSuccess(result);

      // Load preview for uploaded dataset
      fetchPreview(result.id);
    } catch (err: any) {
      setErrorMsg(err.message || 'Dataset upload failed. Please verify CSV formatting.');
    } finally {
      setIsUploading(false);
    }
  };

  const fetchPreview = async (datasetId: string) => {
    setIsLoadingPreview(true);
    try {
      const res = await fetch(`/api/datasets/${datasetId}/preview?limit=10`);
      if (res.ok) {
        const data = await res.json();
        setPreviewData(data);
      }
    } catch {
      // Ignore preview fetch errors
    } finally {
      setIsLoadingPreview(false);
    }
  };

  return (
    <div id="upload-view" className="space-y-6 max-w-5xl mx-auto">
      {/* Title Header */}
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 pb-2 border-b border-slate-200">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Ingest Dataset</h1>
          <p className="text-slate-500 text-sm mt-1">
            Upload your cloud budget and spending CSV to start the automated pipeline.
          </p>
        </div>

        <a
          id="download-sample-csv-link"
          href={getSampleDatasetUrl()}
          download="cloud_cost_sample_dataset.csv"
          className="inline-flex items-center space-x-2 px-4 py-2 bg-white hover:bg-slate-50 text-slate-700 border border-slate-200 rounded-lg text-xs font-semibold shadow-sm transition shrink-0"
        >
          <Download className="w-4 h-4 text-blue-600" />
          <span>Download Sample CSV</span>
        </a>
      </div>

      {/* Main Upload Box */}
      <div className="bg-white border border-slate-200 rounded-xl p-6 shadow-sm space-y-6">
        <div
          id="csv-dropzone"
          onDrop={handleDrop}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onClick={() => fileInputRef.current?.click()}
          className={`border-2 border-dashed rounded-xl p-10 flex flex-col items-center justify-center space-y-4 cursor-pointer transition-all bg-gradient-to-b from-white to-slate-50/50 ${
            isDragging
              ? 'border-blue-500 bg-blue-50/50 scale-[1.01]'
              : 'border-slate-200 hover:border-blue-300'
          }`}
        >
          <input
            id="csv-file-input"
            ref={fileInputRef}
            type="file"
            accept=".csv"
            className="hidden"
            onChange={(e) => {
              if (e.target.files && e.target.files[0]) {
                handleFileSelect(e.target.files[0]);
              }
            }}
          />

          <div className="w-14 h-14 bg-blue-50 text-blue-600 rounded-full flex items-center justify-center mb-1 shadow-sm border border-blue-100">
            <UploadCloud className="w-7 h-7" />
          </div>

          <div className="text-center space-y-1">
            <p className="font-semibold text-slate-800 text-sm">Click to upload or drag and drop</p>
            <p className="text-xs text-slate-500">Cloud Budget CSV (Max 500MB)</p>
          </div>

          <button
            type="button"
            className="bg-slate-900 text-white px-6 py-2 rounded-lg text-xs font-medium hover:bg-slate-800 transition-shadow shadow-sm"
          >
            Select Dataset
          </button>
        </div>

        {/* Selected File Details Banner */}
        {selectedFile && (
          <div className="p-4 bg-slate-50 border border-slate-200 rounded-xl flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div className="flex items-center space-x-3">
              <div className="p-2.5 bg-blue-100 text-blue-700 rounded-lg">
                <FileText className="w-5 h-5" />
              </div>
              <div>
                <p className="text-sm font-bold text-slate-900">{selectedFile.name}</p>
                <div className="flex items-center space-x-3 text-xs text-slate-500 mt-0.5">
                  <span>Size: {(selectedFile.size / 1024).toFixed(1)} KB</span>
                  <span>Type: CSV</span>
                  <span className="text-emerald-700 font-bold">Ready for upload</span>
                </div>
              </div>
            </div>

            <button
              id="upload-dataset-btn"
              onClick={handleUploadSubmit}
              disabled={isUploading}
              className="px-5 py-2.5 bg-slate-900 hover:bg-slate-800 text-white font-semibold text-xs rounded-lg shadow-sm inline-flex items-center justify-center space-x-2 transition shrink-0"
            >
              {isUploading ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin text-blue-400" />
                  <span>Processing & Ingesting...</span>
                </>
              ) : (
                <>
                  <UploadCloud className="w-4 h-4" />
                  <span>Ingest Dataset</span>
                </>
              )}
            </button>
          </div>
        )}

        {/* Error Alert */}
        {errorMsg && (
          <div className="p-4 bg-red-50 border border-red-200 rounded-xl text-red-700 text-xs flex items-center space-x-3">
            <AlertCircle className="w-5 h-5 shrink-0 text-red-600" />
            <span>{errorMsg}</span>
          </div>
        )}

        {/* Upload Success Details */}
        {uploadedDataset && (
          <div className="space-y-6 pt-4 border-t border-slate-200">
            <div id="upload-success-banner" className="p-4 bg-emerald-50 border border-emerald-200 rounded-xl text-emerald-800 text-sm font-semibold flex items-center space-x-3">
              <CheckCircle2 className="w-5 h-5 shrink-0 text-emerald-600" />
              <span>✓ Dataset uploaded successfully and metadata indexed in SQLite Metadata Store!</span>
            </div>

            {/* Ingestion Stats Cards */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              <div className="p-4 bg-slate-50 border border-slate-200 rounded-xl space-y-1">
                <span className="text-[10px] text-slate-500 uppercase font-bold">Total Rows</span>
                <p className="text-xl font-bold text-slate-900 font-mono">{uploadedDataset.row_count.toLocaleString()}</p>
              </div>

              <div className="p-4 bg-slate-50 border border-slate-200 rounded-xl space-y-1">
                <span className="text-[10px] text-slate-500 uppercase font-bold">Total Columns</span>
                <p className="text-xl font-bold text-slate-900 font-mono">{uploadedDataset.column_count}</p>
              </div>

              <div className="p-4 bg-slate-50 border border-slate-200 rounded-xl space-y-1">
                <span className="text-[10px] text-slate-500 uppercase font-bold">File Size</span>
                <p className="text-xl font-bold text-slate-900 font-mono">{(uploadedDataset.file_size / 1024).toFixed(1)} KB</p>
              </div>

              <div className="p-4 bg-slate-50 border border-slate-200 rounded-xl space-y-1">
                <span className="text-[10px] text-slate-500 uppercase font-bold">Upload Time</span>
                <p className="text-xs font-semibold text-slate-700 mt-2">
                  {new Date(uploadedDataset.upload_timestamp).toLocaleTimeString()}
                </p>
              </div>
            </div>

            {/* Detected Columns Schema */}
            <div className="bg-slate-50 border border-slate-200 rounded-xl p-4 space-y-3">
              <div className="flex items-center space-x-2 text-slate-800 font-semibold text-xs">
                <Columns className="w-4 h-4 text-blue-600" />
                <span>Detected CSV Column Schema ({uploadedDataset.columns.length} Fields)</span>
              </div>
              <div className="flex flex-wrap gap-1.5 max-h-32 overflow-y-auto p-1">
                {uploadedDataset.columns.map((col) => (
                  <span key={col} className="px-2.5 py-1 bg-white border border-slate-200 text-slate-700 font-mono text-[11px] rounded-md shadow-2xs">
                    {col}
                  </span>
                ))}
              </div>
            </div>

            {/* Live Data Preview Table */}
            {previewData && (
              <div className="bg-white border border-slate-200 rounded-xl overflow-hidden shadow-sm space-y-0">
                <div className="px-5 py-3 border-b border-slate-100 bg-slate-50/50 flex justify-between items-center">
                  <div className="flex items-center space-x-2">
                    <Eye className="w-4 h-4 text-emerald-600" />
                    <h3 className="text-xs font-bold text-slate-700 uppercase tracking-wider">Dataset Live Preview</h3>
                  </div>
                  <span className="text-[10px] text-slate-400">Showing top 10 records</span>
                </div>

                <div className="overflow-x-auto">
                  <table className="w-full text-left border-collapse">
                    <thead>
                      <tr className="bg-white text-[10px] font-bold text-slate-400 uppercase tracking-wider border-b border-slate-100">
                        {previewData.columns.map((col) => (
                          <th key={col} className="px-5 py-3 whitespace-nowrap">{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="text-xs text-slate-600 font-mono divide-y divide-slate-50">
                      {previewData.preview_data.map((row, idx) => (
                        <tr key={idx} className="border-b border-slate-50 hover:bg-slate-50/60 transition-colors">
                          {previewData.columns.map((col) => (
                            <td key={col} className="px-5 py-3 whitespace-nowrap">
                              {row[col] !== null && row[col] !== undefined ? String(row[col]) : <span className="text-slate-400 font-normal">null</span>}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

