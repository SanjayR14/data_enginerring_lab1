from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from backend.app.core.config import settings

db_url = settings.DATABASE_URL

# Handle connect_args for SQLite if sqlite is used as fallback
connect_args = {}
if db_url.startswith("sqlite"):
    connect_args["check_same_thread"] = False

engine = create_engine(db_url, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    from backend.app.models.dataset import DatasetModel
    from backend.app.models.pipeline import PipelineRunModel, DataQualityResultModel, QuarantineRecordModel
    from backend.app.models.cdc import KafkaEventAuditModel, PipelineEventMetricsModel
    Base.metadata.create_all(bind=engine)
