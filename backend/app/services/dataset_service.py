import os
import hashlib
import uuid
import json
import pandas as pd
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime
from fastapi import UploadFile, HTTPException, status
from sqlalchemy.orm import Session
from backend.app.models.dataset import DatasetModel
from backend.app.core.config import settings

class DatasetService:

    @staticmethod
    def calculate_file_hash(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def validate_and_save_csv(file: UploadFile, content: bytes, db: Session) -> Tuple[DatasetModel, bool]:
        # 1. Validate non-empty file
        if not content or len(content) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid dataset: Uploaded file is empty (0 bytes)."
            )

        # 2. Check max size limit (50MB)
        max_size_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024
        if len(content) > max_size_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File exceeds maximum allowed size of {settings.MAX_FILE_SIZE_MB}MB."
            )

        # 3. Check filename extension
        original_filename = file.filename or "uploaded_dataset.csv"
        if not original_filename.strip().lower().endswith(".csv"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid dataset file type. Only CSV files (.csv) are supported."
            )

        # 3b. Idempotency check: if this exact file was already uploaded, return the
        # existing dataset instead of creating a duplicate (matches the SHA-256
        # idempotency hash this service already computes for every dataset).
        file_hash = DatasetService.calculate_file_hash(content)
        existing = db.query(DatasetModel).filter(DatasetModel.file_hash == file_hash).first()
        if existing:
            return existing, True

        # 4. Parse CSV to check validity
        try:
            from io import BytesIO
            # Try reading first few rows with pandas to detect headers and parseability
            df_sample = pd.read_csv(BytesIO(content), nrows=100)
            if df_sample.empty and len(df_sample.columns) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid dataset: required CSV columns could not be detected."
                )
            
            # Count total rows
            df_full = pd.read_csv(BytesIO(content))
            row_count = len(df_full)
            columns = [str(col).strip() for col in df_full.columns]
            column_count = len(columns)

            if column_count == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid dataset: required CSV columns could not be detected."
                )

            # Map data types
            col_types = {col: str(dtype) for col, dtype in df_full.dtypes.items()}

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid dataset: CSV parsing failed. Error: {str(e)}"
            )

        # 5. Generate unique dataset ID & safe storage path
        dataset_id = f"dataset_{uuid.uuid4().hex[:12]}"
        safe_filename = f"{dataset_id}.csv"
        os.makedirs(settings.UPLOAD_DIRECTORY, exist_ok=True)
        storage_path = os.path.join(settings.UPLOAD_DIRECTORY, safe_filename)

        # 6. Write file to storage
        with open(storage_path, "wb") as f:
            f.write(content)

        # 8. Save metadata to database
        column_meta_json = json.dumps({
            "columns": columns,
            "types": col_types
        })

        db_dataset = DatasetModel(
            id=dataset_id,
            filename=safe_filename,
            original_filename=original_filename,
            file_size=len(content),
            file_type="text/csv",
            row_count=row_count,
            column_count=column_count,
            upload_timestamp=datetime.utcnow(),
            storage_path=storage_path,
            status="UPLOADED",
            file_hash=file_hash,
            columns_json=column_meta_json
        )

        db.add(db_dataset)
        db.commit()
        db.refresh(db_dataset)

        return db_dataset, False

    @staticmethod
    def get_dataset(dataset_id: str, db: Session) -> DatasetModel:
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset with ID '{dataset_id}' not found."
            )
        return dataset

    @staticmethod
    def list_datasets(db: Session, limit: int = 50) -> List[DatasetModel]:
        return db.query(DatasetModel).order_by(DatasetModel.upload_timestamp.desc()).limit(limit).all()

    @staticmethod
    def get_dataset_preview(dataset_id: str, db: Session, limit: int = 10) -> Dict[str, Any]:
        dataset = DatasetService.get_dataset(dataset_id, db)
        
        if not os.path.exists(dataset.storage_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset storage file for '{dataset_id}' is missing."
            )

        try:
            df = pd.read_csv(dataset.storage_path, nrows=limit)
            # Fill NaN values with empty string or None for clean JSON serialization
            df = df.where(pd.notnull(df), None)
            
            # Extract column info
            columns = [str(c) for c in df.columns]
            column_types = {str(col): str(dtype) for col, dtype in df.dtypes.items()}
            preview_data = df.to_dict(orient="records")

            return {
                "dataset_id": dataset.id,
                "original_filename": dataset.original_filename,
                "row_count": dataset.row_count,
                "column_count": dataset.column_count,
                "columns": columns,
                "column_types": column_types,
                "preview_data": preview_data
            }
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to read dataset preview: {str(e)}"
            )
