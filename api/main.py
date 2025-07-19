"""
FastAPI application for VALORANT betting predictions
"""

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from contextlib import asynccontextmanager
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from .routers import predictions, matches, odds, teams
from .services.prediction_service import PredictionService
from .services.model_service import ModelService
from .models.api_models import HealthCheck, PredictionResponse
from .core.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Security
security = HTTPBearer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Starting VALORANT Betting API...")
    
    # Initialize services
    model_service = ModelService()
    await model_service.load_production_models()
    
    logger.info("API startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down VALORANT Betting API...")

app = FastAPI(
    title="VALORANT Betting Platform API",
    description="ML-powered VALORANT esports betting predictions and analytics",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://localhost:3000"],  # Streamlit and React
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(predictions.router, prefix="/api/v1", tags=["predictions"])
app.include_router(matches.router, prefix="/api/v1", tags=["matches"])
app.include_router(odds.router, prefix="/api/v1", tags=["odds"])
app.include_router(teams.router, prefix="/api/v1", tags=["teams"])

# Root endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "VALORANT Betting Platform API",
        "version": "0.1.0",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connection
        from ingest.database import db
        db.execute_query("SELECT 1")
        
        # Check model service
        model_service = ModelService()
        models_loaded = await model_service.check_models_loaded()
        
        return HealthCheck(
            status="healthy",
            timestamp=datetime.now(),
            version="0.1.0",
            database_connected=True,
            models_loaded=models_loaded
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service unhealthy: {str(e)}"
        )

# Authentication dependency
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current authenticated user"""
    # For now, simple token validation
    # In production, implement proper JWT validation
    if credentials.credentials != settings.API_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {"user_id": "api_user"}

# Protected route example
@app.get("/api/v1/admin/status")
async def admin_status(current_user: dict = Depends(get_current_user)):
    """Admin status endpoint"""
    return {
        "user": current_user,
        "server_time": datetime.now(),
        "api_version": "0.1.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

def main():
    """Entry point for console script"""
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        workers=settings.API_WORKERS,
        log_level="info"
    )