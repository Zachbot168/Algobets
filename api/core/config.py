"""
Configuration settings for the API
"""

import os
from typing import Optional
from pydantic import BaseSettings

class Settings(BaseSettings):
    """Application settings"""
    
    # API Configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_WORKERS: int = int(os.getenv("API_WORKERS", "4"))
    API_SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-here")
    
    # Database Configuration
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./valorant_betting.db")
    DUCKDB_PATH: str = os.getenv("DUCKDB_PATH", "./data/bronze.db")
    
    # MLflow Configuration
    MLFLOW_TRACKING_URI: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    MLFLOW_EXPERIMENT_NAME: str = os.getenv("MLFLOW_EXPERIMENT_NAME", "valorant_betting")
    
    # External APIs
    RIOT_API_KEY: Optional[str] = os.getenv("RIOT_API_KEY")
    THEODS_API_KEY: Optional[str] = os.getenv("THEODS_API_KEY")
    PINNACLE_API_KEY: Optional[str] = os.getenv("PINNACLE_API_KEY")
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
    
    # Betting Configuration
    MAX_STAKE_PERCENT: float = float(os.getenv("MAX_STAKE_PERCENT", "0.05"))
    KELLY_FRACTION: float = float(os.getenv("KELLY_FRACTION", "0.25"))
    MIN_EDGE_THRESHOLD: float = float(os.getenv("MIN_EDGE_THRESHOLD", "0.02"))
    
    # Application Settings
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()