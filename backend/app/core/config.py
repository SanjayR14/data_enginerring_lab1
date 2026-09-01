import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    PROJECT_NAME: str = "Cloud Cost Intelligence & Data Engineering Platform"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api"
    
    # Environment
    ENV: str = os.getenv("ENV", "development")
    
    # Backend host & port
    BACKEND_HOST: str = os.getenv("BACKEND_HOST", "0.0.0.0")
    BACKEND_PORT: int = int(os.getenv("BACKEND_PORT", "8000"))
    
    # PostgreSQL Database
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "cloud_cost_db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")

    # Airflow Configuration
    AIRFLOW_URL: str = os.getenv("AIRFLOW_URL", "http://localhost:8080")
    AIRFLOW_USERNAME: str = os.getenv("AIRFLOW_USERNAME", "airflow")
    AIRFLOW_PASSWORD: str = os.getenv("AIRFLOW_PASSWORD", "airflow")

    # Databricks Configuration
    DATABRICKS_HOST: str = os.getenv("DATABRICKS_HOST", "")
    DATABRICKS_TOKEN: str = os.getenv("DATABRICKS_TOKEN", "")
    DATABRICKS_WAREHOUSE_ID: str = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    DATABRICKS_CATALOG: str = os.getenv("DATABRICKS_CATALOG", "cloud_cost_catalog")
    DATABRICKS_SCHEMA: str = os.getenv("DATABRICKS_SCHEMA", "cloud_analytics")
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "cloud-cost-events")
    KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "cloud-cost-consumer")
    KAFKA_DLQ_TOPIC: str = os.getenv("KAFKA_DLQ_TOPIC", "cloud-cost-events-dlq")

    # Storage
    UPLOAD_DIRECTORY: str = os.getenv("UPLOAD_DIRECTORY", "./data/uploads")
    MAX_FILE_SIZE_MB: int = 50
    
    # CORS
    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "*")

    @property
    def DATABASE_URL(self) -> str:
        # Strictly use SQLite
        os.makedirs("./data", exist_ok=True)
        return "sqlite:///./data/cloud_cost.db"

settings = Settings()
