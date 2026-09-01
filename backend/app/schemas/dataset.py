from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict, Any, Optional

class ColumnDetail(BaseModel):
    name: str
    data_type: str

class DatasetBase(BaseModel):
    filename: str
    original_filename: str
    file_size: int
    file_type: str
    row_count: int
    column_count: int
    status: str

class DatasetResponse(DatasetBase):
    id: str
    upload_timestamp: datetime
    file_hash: Optional[str] = None
    columns: List[str] = []

    class Config:
        from_attributes = True

class DatasetDetailResponse(DatasetResponse):
    storage_path: str
    column_details: List[ColumnDetail] = []

class DatasetPreviewResponse(BaseModel):
    dataset_id: str
    original_filename: str
    row_count: int
    column_count: int
    columns: List[str]
    column_types: Dict[str, str]
    preview_data: List[Dict[str, Any]]

class PipelineStatusResponse(BaseModel):
    dataset_id: str
    status: str
    current_stage: str
    message: str
    last_updated: datetime
