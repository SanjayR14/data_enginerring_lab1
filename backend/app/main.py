from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.app.core.config import settings
from backend.app.db.database import init_db
from backend.app.api.endpoints import router as api_router
from backend.app.services.kafka_consumer_service import start_kafka_consumer_daemon

# Initialize database tables
init_db()

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Production-oriented Cloud Cost Intelligence & Data Engineering Platform",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    if settings.KAFKA_ENABLED:
        start_kafka_consumer_daemon()

# Root endpoint
@app.get("/", tags=["Root"])
def read_root():
    return {
        "title": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "phase": "Phase 1: Project Foundation & Dataset Upload",
        "status": "online",
        "docs_url": "/docs"
    }

# Include API Router
app.include_router(api_router, prefix=settings.API_V1_STR)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.app.main:app",
        host=settings.BACKEND_HOST,
        port=settings.BACKEND_PORT,
        reload=True
    )
