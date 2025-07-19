"""
Service for loading and managing ML models
"""

import logging
import os
import pickle
from typing import Dict, Any, Optional, List
from datetime import datetime
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd

from ..core.config import settings

logger = logging.getLogger(__name__)

class ModelWrapper:
    """Wrapper for ML models with prediction interface"""
    
    def __init__(self, model, model_type: str, version: str):
        self.model = model
        self.model_type = model_type
        self.version = version
        self.feature_names = getattr(model, 'feature_names_', None)
        
    async def predict(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Make prediction with the model"""
        try:
            # Convert features to DataFrame
            df = pd.DataFrame([features])
            
            # Handle missing features
            if self.feature_names:
                missing_features = set(self.feature_names) - set(df.columns)
                for feature in missing_features:
                    df[feature] = 0.0  # Default value for missing features
                    
                # Reorder columns to match training
                df = df[self.feature_names]
                
            # Make prediction
            if self.model_type in ['match_winner', 'map_winner', 'first_blood']:
                # Classification
                probabilities = self.model.predict_proba(df)[0]
                probability = float(probabilities[1])  # Assuming binary classification
                
                # Calculate confidence based on probability distance from 0.5
                confidence = abs(probability - 0.5) * 2.0
                
            elif self.model_type in ['total_rounds', 'player_kills']:
                # Regression
                prediction = self.model.predict(df)[0]
                
                # For totals, convert to over/under probability
                if self.model_type == 'total_rounds':
                    # Assume we want over 24.5 rounds
                    over_line = 24.5
                    probability = self.calculate_over_probability(prediction, over_line)
                else:
                    # For player props, use prediction directly
                    probability = min(max(prediction / 100.0, 0.01), 0.99)
                    
                confidence = 0.7  # Default confidence for regression
                
            else:
                raise ValueError(f"Unknown model type: {self.model_type}")
                
            return {
                'probability': probability,
                'confidence': confidence,
                'model_version': self.version,
                'raw_prediction': prediction if 'prediction' in locals() else probability
            }
            
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            return {
                'probability': 0.5,
                'confidence': 0.0,
                'model_version': self.version,
                'error': str(e)
            }
            
    def calculate_over_probability(self, predicted_total: float, line: float) -> float:
        """Calculate probability of going over a line for total rounds"""
        # Simple normal distribution assumption
        # In practice, you'd want a more sophisticated approach
        std_dev = 3.0  # Assumed standard deviation for rounds
        
        from scipy.stats import norm
        probability = 1 - norm.cdf(line, predicted_total, std_dev)
        return max(min(probability, 0.99), 0.01)

class ModelService:
    """Service for managing ML models"""
    
    def __init__(self):
        self.models: Dict[str, ModelWrapper] = {}
        self.model_registry = {
            'match_winner': 'match_winner_model',
            'map_winner': 'map_winner_model', 
            'total_rounds': 'total_rounds_model',
            'player_kills': 'player_kills_model',
            'first_blood': 'first_blood_model'
        }
        
        # Set MLflow tracking URI
        mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
        
    async def load_production_models(self) -> Dict[str, str]:
        """Load all production models from MLflow"""
        loaded_models = {}
        
        try:
            for model_type, model_name in self.model_registry.items():
                try:
                    model_wrapper = await self.load_model_from_mlflow(model_name, model_type)
                    if model_wrapper:
                        self.models[model_type] = model_wrapper
                        loaded_models[model_type] = model_wrapper.version
                        logger.info(f"Loaded {model_type} model version {model_wrapper.version}")
                    else:
                        logger.warning(f"Failed to load {model_type} model")
                        
                except Exception as e:
                    logger.error(f"Error loading {model_type} model: {e}")
                    
                    # Try to load fallback model
                    fallback_model = await self.load_fallback_model(model_type)
                    if fallback_model:
                        self.models[model_type] = fallback_model
                        loaded_models[model_type] = "fallback"
                        
            logger.info(f"Loaded {len(loaded_models)} models: {list(loaded_models.keys())}")
            return loaded_models
            
        except Exception as e:
            logger.error(f"Error loading production models: {e}")
            return {}
            
    async def load_model_from_mlflow(self, model_name: str, model_type: str) -> Optional[ModelWrapper]:
        """Load a specific model from MLflow"""
        try:
            # Get latest production version
            client = mlflow.tracking.MlflowClient()
            
            try:
                # Try to get production version
                latest_version = client.get_latest_versions(
                    model_name, stages=["Production"]
                )[0]
            except:
                # Fallback to latest version regardless of stage
                latest_version = client.get_latest_versions(model_name)[0]
                
            # Load model
            model_uri = f"models:/{model_name}/{latest_version.version}"
            model = mlflow.lightgbm.load_model(model_uri)
            
            return ModelWrapper(model, model_type, latest_version.version)
            
        except Exception as e:
            logger.error(f"Error loading model {model_name} from MLflow: {e}")
            return None
            
    async def load_fallback_model(self, model_type: str) -> Optional[ModelWrapper]:
        """Load a simple fallback model"""
        try:
            # Create a simple dummy model for testing
            class DummyModel:
                def __init__(self, model_type):
                    self.model_type = model_type
                    
                def predict_proba(self, X):
                    # Return random probabilities biased towards 50/50
                    n_samples = len(X)
                    probs = np.random.normal(0.5, 0.1, (n_samples, 2))
                    probs = np.clip(probs, 0.1, 0.9)
                    probs = probs / probs.sum(axis=1, keepdims=True)
                    return probs
                    
                def predict(self, X):
                    # For regression models
                    if self.model_type == 'total_rounds':
                        return np.random.normal(24.5, 3.0, len(X))
                    elif self.model_type == 'player_kills':
                        return np.random.normal(15.0, 5.0, len(X))
                    else:
                        return np.random.random(len(X))
                        
            dummy_model = DummyModel(model_type)
            return ModelWrapper(dummy_model, model_type, "fallback_v1")
            
        except Exception as e:
            logger.error(f"Error creating fallback model: {e}")
            return None
            
    async def get_model(self, model_type: str) -> Optional[ModelWrapper]:
        """Get a specific model by type"""
        return self.models.get(model_type)
        
    async def check_models_loaded(self) -> bool:
        """Check if models are loaded and available"""
        return len(self.models) > 0
        
    async def reload_model(self, model_type: str) -> bool:
        """Reload a specific model"""
        try:
            if model_type not in self.model_registry:
                logger.error(f"Unknown model type: {model_type}")
                return False
                
            model_name = self.model_registry[model_type]
            model_wrapper = await self.load_model_from_mlflow(model_name, model_type)
            
            if model_wrapper:
                self.models[model_type] = model_wrapper
                logger.info(f"Reloaded {model_type} model version {model_wrapper.version}")
                return True
            else:
                logger.error(f"Failed to reload {model_type} model")
                return False
                
        except Exception as e:
            logger.error(f"Error reloading model {model_type}: {e}")
            return False
            
    async def get_model_info(self) -> Dict[str, Any]:
        """Get information about loaded models"""
        model_info = {}
        
        for model_type, model_wrapper in self.models.items():
            model_info[model_type] = {
                'version': model_wrapper.version,
                'type': model_wrapper.model_type,
                'feature_count': len(model_wrapper.feature_names) if model_wrapper.feature_names else 0,
                'loaded_at': datetime.now().isoformat()
            }
            
        return model_info
        
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all models"""
        health_status = {
            'models_loaded': len(self.models),
            'models_expected': len(self.model_registry),
            'status': 'healthy' if len(self.models) > 0 else 'unhealthy',
            'models': {}
        }
        
        # Test each model with dummy data
        for model_type, model_wrapper in self.models.items():
            try:
                # Create dummy features
                dummy_features = {
                    'team_a_elo': 1500,
                    'team_b_elo': 1500,
                    'is_lan': False,
                    'best_of': 3
                }
                
                # Test prediction
                result = await model_wrapper.predict(dummy_features)
                
                health_status['models'][model_type] = {
                    'status': 'healthy' if 'error' not in result else 'error',
                    'version': model_wrapper.version,
                    'test_prediction': result.get('probability', 'N/A')
                }
                
            except Exception as e:
                health_status['models'][model_type] = {
                    'status': 'error',
                    'version': model_wrapper.version,
                    'error': str(e)
                }
                
        return health_status