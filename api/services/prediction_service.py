"""
Service for generating and managing predictions
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json

from ..models.api_models import Prediction, Match, BettingRecommendation, MarketType
from .model_service import ModelService
from .odds_service import OddsService
from ..core.config import settings
from ingest.database import db

logger = logging.getLogger(__name__)

class PredictionService:
    """Service for generating ML predictions and betting recommendations"""
    
    def __init__(self):
        self.model_service = ModelService()
        self.odds_service = OddsService()
        
    async def get_predictions_for_match(self, match_id: str, 
                                      market_types: Optional[List[MarketType]] = None) -> List[Prediction]:
        """Get all predictions for a specific match"""
        try:
            if market_types is None:
                market_types = [MarketType.MATCH_WINNER, MarketType.TOTAL_ROUNDS]
                
            predictions = []
            
            # Get match details
            match = await self.get_match_details(match_id)
            if not match:
                logger.error(f"Match not found: {match_id}")
                return []
                
            # Generate predictions for each market type
            for market_type in market_types:
                prediction = await self.generate_prediction(match, market_type)
                if prediction:
                    predictions.append(prediction)
                    
            # Save predictions to database
            await self.save_predictions(predictions)
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error getting predictions for match {match_id}: {e}")
            return []
            
    async def generate_prediction(self, match: Dict[str, Any], 
                                market_type: MarketType) -> Optional[Prediction]:
        """Generate a single prediction for a match and market type"""
        try:
            # Get appropriate model
            model = await self.model_service.get_model(market_type.value)
            if not model:
                logger.error(f"Model not found for {market_type}")
                return None
                
            # Prepare features
            features = await self.prepare_features(match, market_type)
            if not features:
                logger.error(f"Could not prepare features for {match['match_id']}")
                return None
                
            # Make prediction
            prediction_result = await model.predict(features)
            
            # Convert to Prediction object
            prediction = Prediction(
                match_id=match['match_id'],
                market_type=market_type,
                probability=prediction_result['probability'],
                confidence=prediction_result['confidence'],
                fair_odds=1.0 / prediction_result['probability'],
                model_version=prediction_result.get('model_version', 'unknown'),
                created_at=datetime.now()
            )
            
            return prediction
            
        except Exception as e:
            logger.error(f"Error generating prediction: {e}")
            return None
            
    async def prepare_features(self, match: Dict[str, Any], 
                             market_type: MarketType) -> Optional[Dict[str, Any]]:
        """Prepare features for ML model prediction"""
        try:
            features = {}
            
            # Base features
            features.update({
                'team_a_id': match.get('team_a_id'),
                'team_b_id': match.get('team_b_id'),
                'is_lan': match.get('is_lan', False),
                'best_of': match.get('best_of', 3),
                'patch_version': match.get('patch_version'),
            })
            
            # Get team statistics
            team_a_stats = await self.get_team_stats(match['team_a_id'])
            team_b_stats = await self.get_team_stats(match['team_b_id'])
            
            # Add team features
            if team_a_stats:
                features.update({f'team_a_{k}': v for k, v in team_a_stats.items()})
            if team_b_stats:
                features.update({f'team_b_{k}': v for k, v in team_b_stats.items()})
                
            # Market-specific features
            if market_type == MarketType.MATCH_WINNER:
                # Add head-to-head record
                h2h_stats = await self.get_head_to_head_stats(
                    match['team_a_id'], match['team_b_id']
                )
                features.update(h2h_stats)
                
            elif market_type == MarketType.TOTAL_ROUNDS:
                # Add map-specific stats if available
                if match.get('maps'):
                    for i, map_name in enumerate(match['maps']):
                        map_stats = await self.get_map_stats(
                            match['team_a_id'], match['team_b_id'], map_name
                        )
                        features.update({f'map_{i}_{k}': v for k, v in map_stats.items()})
                        
            return features
            
        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            return None
            
    async def get_team_stats(self, team_id: str) -> Dict[str, Any]:
        """Get recent team performance statistics"""
        try:
            # This would typically query a feature store or calculate on-the-fly
            # For now, return mock stats
            return {
                'recent_win_rate': 0.65,
                'avg_rounds_per_map': 21.5,
                'first_blood_rate': 0.52,
                'elo_rating': 1750
            }
            
        except Exception as e:
            logger.error(f"Error getting team stats for {team_id}: {e}")
            return {}
            
    async def get_head_to_head_stats(self, team_a_id: str, team_b_id: str) -> Dict[str, Any]:
        """Get head-to-head statistics between two teams"""
        try:
            query = """
                SELECT 
                    COUNT(*) as total_matches,
                    SUM(CASE WHEN winner_id = ? THEN 1 ELSE 0 END) as team_a_wins,
                    AVG(CASE WHEN team_a_id = ? THEN team_a_score ELSE team_b_score END) as avg_score_a,
                    AVG(CASE WHEN team_b_id = ? THEN team_b_score ELSE team_a_score END) as avg_score_b
                FROM bronze_matches 
                WHERE (team_a_id = ? AND team_b_id = ?) 
                   OR (team_a_id = ? AND team_b_id = ?)
                   AND start_time > ?
            """
            
            cutoff_date = datetime.now() - timedelta(days=365)
            params = [team_a_id, team_a_id, team_b_id, 
                     team_a_id, team_b_id, team_b_id, team_a_id, 
                     cutoff_date.isoformat()]
            
            result = db.execute_query(query, params)
            
            if result and result[0][0] > 0:
                total, team_a_wins, avg_a, avg_b = result[0]
                return {
                    'h2h_total_matches': total,
                    'h2h_team_a_win_rate': team_a_wins / total,
                    'h2h_avg_score_diff': (avg_a or 0) - (avg_b or 0)
                }
            else:
                return {
                    'h2h_total_matches': 0,
                    'h2h_team_a_win_rate': 0.5,
                    'h2h_avg_score_diff': 0.0
                }
                
        except Exception as e:
            logger.error(f"Error getting H2H stats: {e}")
            return {}
            
    async def get_map_stats(self, team_a_id: str, team_b_id: str, 
                          map_name: str) -> Dict[str, Any]:
        """Get map-specific statistics"""
        try:
            # Query map performance for both teams
            query = """
                SELECT 
                    AVG(CASE WHEN winner_id = ? THEN 1.0 ELSE 0.0 END) as team_a_map_win_rate,
                    AVG(team_a_score + team_b_score) as avg_total_rounds
                FROM bronze_maps m
                JOIN bronze_matches ma ON m.match_id = ma.match_id
                WHERE m.map_name = ?
                  AND (ma.team_a_id = ? OR ma.team_b_id = ?)
                  AND ma.start_time > ?
            """
            
            cutoff_date = datetime.now() - timedelta(days=180)
            params = [team_a_id, map_name, team_a_id, team_a_id, cutoff_date.isoformat()]
            
            result = db.execute_query(query, params)
            
            if result and result[0][0] is not None:
                win_rate, avg_rounds = result[0]
                return {
                    'map_win_rate': win_rate,
                    'map_avg_total_rounds': avg_rounds or 24.5
                }
            else:
                return {
                    'map_win_rate': 0.5,
                    'map_avg_total_rounds': 24.5
                }
                
        except Exception as e:
            logger.error(f"Error getting map stats: {e}")
            return {}
            
    async def get_match_details(self, match_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed match information"""
        try:
            query = """
                SELECT 
                    match_id, tournament_name, team_a_id, team_a_name,
                    team_b_id, team_b_name, start_time, best_of,
                    patch_version, is_lan
                FROM bronze_matches 
                WHERE match_id = ?
            """
            
            result = db.execute_query(query, [match_id])
            
            if result:
                match_data = result[0]
                return {
                    'match_id': match_data[0],
                    'tournament_name': match_data[1],
                    'team_a_id': match_data[2],
                    'team_a_name': match_data[3],
                    'team_b_id': match_data[4],
                    'team_b_name': match_data[5],
                    'start_time': match_data[6],
                    'best_of': match_data[7],
                    'patch_version': match_data[8],
                    'is_lan': match_data[9]
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting match details: {e}")
            return None
            
    async def save_predictions(self, predictions: List[Prediction]) -> Dict[str, Any]:
        """Save predictions to database"""
        try:
            if not predictions:
                return {'saved_count': 0}
                
            # Convert predictions to database format
            prediction_records = []
            for pred in predictions:
                record = {
                    'prediction_id': f"{pred.match_id}_{pred.market_type}_{int(pred.created_at.timestamp())}",
                    'match_id': pred.match_id,
                    'market_type': pred.market_type,
                    'selection': pred.selection,
                    'probability': pred.probability,
                    'confidence': pred.confidence,
                    'fair_odds': pred.fair_odds,
                    'model_version': pred.model_version,
                    'created_at': pred.created_at.isoformat()
                }
                prediction_records.append(record)
                
            # Create predictions table if it doesn't exist
            create_table_query = """
                CREATE TABLE IF NOT EXISTS predictions (
                    prediction_id VARCHAR PRIMARY KEY,
                    match_id VARCHAR,
                    market_type VARCHAR,
                    selection VARCHAR,
                    probability DECIMAL(5,4),
                    confidence DECIMAL(5,4),
                    fair_odds DECIMAL(10,3),
                    model_version VARCHAR,
                    created_at TIMESTAMP
                )
            """
            db.execute_query(create_table_query)
            
            # Save predictions
            from ingest.base_ingester import BaseIngester
            ingester = BaseIngester(rate_limit_per_minute=1000)
            saved_count = ingester.save_to_bronze('predictions', prediction_records)
            
            return {'saved_count': saved_count}
            
        except Exception as e:
            logger.error(f"Error saving predictions: {e}")
            return {'saved_count': 0, 'error': str(e)}
            
    async def get_betting_recommendations(self, min_edge: float = None) -> List[BettingRecommendation]:
        """Get current betting recommendations with positive expected value"""
        try:
            if min_edge is None:
                min_edge = settings.MIN_EDGE_THRESHOLD
                
            recommendations = []
            
            # Get recent predictions
            cutoff_time = datetime.now() - timedelta(hours=24)
            query = """
                SELECT p.match_id, p.market_type, p.probability, p.fair_odds, p.confidence
                FROM predictions p
                JOIN bronze_matches m ON p.match_id = m.match_id
                WHERE p.created_at > ? AND m.start_time > ?
            """
            
            predictions = db.execute_query(query, [cutoff_time.isoformat(), datetime.now().isoformat()])
            
            for pred in predictions:
                match_id, market_type, probability, fair_odds, confidence = pred
                
                # Get best available odds
                best_odds = await self.odds_service.get_best_odds(match_id, market_type)
                
                if best_odds:
                    # Calculate edge
                    edge = (best_odds['odds_decimal'] * probability) - 1.0
                    
                    if edge > min_edge:
                        # Calculate Kelly stake
                        kelly_stake = self.calculate_kelly_stake(
                            probability, best_odds['odds_decimal']
                        )
                        
                        recommendation = BettingRecommendation(
                            match_id=match_id,
                            market_type=MarketType(market_type),
                            selection=best_odds['selection'],
                            bookmaker=best_odds['bookmaker'],
                            odds_decimal=best_odds['odds_decimal'],
                            fair_odds=fair_odds,
                            edge_percent=edge * 100,
                            kelly_stake=kelly_stake,
                            confidence=self.get_confidence_level(confidence, edge)
                        )
                        
                        recommendations.append(recommendation)
                        
            # Sort by edge descending
            recommendations.sort(key=lambda x: x.edge_percent, reverse=True)
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting betting recommendations: {e}")
            return []
            
    def calculate_kelly_stake(self, probability: float, odds_decimal: float) -> float:
        """Calculate Kelly criterion stake percentage"""
        try:
            # Kelly formula: f = (bp - q) / b
            # where b = odds - 1, p = probability, q = 1 - p
            b = odds_decimal - 1.0
            p = probability
            q = 1.0 - p
            
            kelly_fraction = (b * p - q) / b
            
            # Apply fractional Kelly for risk management
            fractional_kelly = kelly_fraction * settings.KELLY_FRACTION
            
            # Cap at maximum stake percentage
            return min(fractional_kelly, settings.MAX_STAKE_PERCENT)
            
        except Exception as e:
            logger.error(f"Error calculating Kelly stake: {e}")
            return 0.0
            
    def get_confidence_level(self, model_confidence: float, edge: float) -> str:
        """Determine confidence level based on model confidence and edge"""
        if model_confidence > 0.8 and edge > 0.05:
            return "high"
        elif model_confidence > 0.6 and edge > 0.03:
            return "medium"
        else:
            return "low"