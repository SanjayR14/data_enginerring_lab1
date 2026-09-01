from sqlalchemy import Column, String, Integer, BigInteger, DateTime, Text
from datetime import datetime
import uuid
from backend.app.db.database import Base

class DatasetModel(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True, index=True, default=lambda: f"dataset_{uuid.uuid4().hex[:12]}")
    filename = Column(String, nullable=False)
    original_filename = Column(String, nullable=False)
    file_size = Column(BigInteger, nullable=False)
    file_type = Column(String, nullable=False, default="text/csv")
    row_count = Column(Integer, nullable=False, default=0)
    column_count = Column(Integer, nullable=False, default=0)
    upload_timestamp = Column(DateTime, default=datetime.utcnow)
    storage_path = Column(String, nullable=False)
    status = Column(String, nullable=False, default="UPLOADED")
    file_hash = Column(String, index=True, nullable=True)
    columns_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
