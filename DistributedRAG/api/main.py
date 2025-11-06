"""
FastAPI Main Application
Phase 5: API for Data Access
"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
import logging

# Import route modules
from routes import raw_data, processed_data, search

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Distributed RAG-Based Web Scraper API",
    description="API for accessing scraped data with RAG-enhanced search",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key Authentication
API_KEY = "your-secret-api-key-12345"
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Depends(api_key_header)):
    """Verify API key for authentication"""
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    return api_key


# Include routers
app.include_router(
    raw_data.router,
    prefix="/api/v1/raw",
    tags=["Raw Data"],
    dependencies=[Depends(verify_api_key)]
)

app.include_router(
    processed_data.router,
    prefix="/api/v1/processed",
    tags=["Processed Data"],
    dependencies=[Depends(verify_api_key)]
)

app.include_router(
    search.router,
    prefix="/api/v1/search",
    tags=["Search"],
    dependencies=[Depends(verify_api_key)]
)


# Root endpoint (public, no auth required)
@app.get("/", tags=["Root"])
async def root():
    """Welcome endpoint"""
    return {
        "message": "Welcome to Distributed RAG-Based Web Scraper API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "operational"
    }


# Health check endpoint (public, no auth required)
@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Distributed RAG API",
        "version": "1.0.0"
    }


# API Info endpoint (public, no auth required)
@app.get("/api/v1/info", tags=["Info"])
async def api_info():
    """Get API information"""
    return {
        "api_name": "Distributed RAG-Based Web Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "raw_data": "/api/v1/raw",
            "processed_data": "/api/v1/processed",
            "search": "/api/v1/search"
        },
        "authentication": "API Key required (X-API-Key header)",
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc"
        }
    }


# Startup event
@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    logger.info("🚀 API Server Starting...")
    logger.info("📚 Documentation available at /docs")
    logger.info("🔑 API Key authentication enabled")


# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown"""
    logger.info("👋 API Server Shutting Down...")


if __name__ == "__main__":
    import uvicorn

    print("\n" + "=" * 70)
    print("🚀 STARTING FASTAPI SERVER")
    print("=" * 70)
    print("\n📍 API will be available at: http://localhost:8000")
    print("📚 Documentation at: http://localhost:8000/docs")
    print("🔑 API Key: your-secret-api-key-12345")
    print("\n" + "=" * 70 + "\n")

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )