"""
API routes for predictions and betting recommendations
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from datetime import datetime, timedelta

from ..models.api_models import (
    PredictionResponse, BettingRecommendationResponse, 
    MarketType, Prediction, BettingRecommendation
)
from ..services.prediction_service import PredictionService

router = APIRouter()

# Initialize service
prediction_service = PredictionService()

@router.get("/predictions/{match_id}", response_model=PredictionResponse)
async def get_match_predictions(
    match_id: str,
    market_types: Optional[List[MarketType]] = Query(default=None),
    force_refresh: bool = Query(default=False, description="Force regeneration of predictions")
):
    """Get predictions for a specific match"""
    try:
        # Check if we have recent predictions unless force refresh
        if not force_refresh:
            recent_predictions = await get_cached_predictions(match_id, market_types)
            if recent_predictions:
                match_details = await prediction_service.get_match_details(match_id)
                return PredictionResponse(
                    predictions=recent_predictions,
                    match=match_details,
                    total_predictions=len(recent_predictions)
                )
        
        # Generate new predictions
        predictions = await prediction_service.get_predictions_for_match(match_id, market_types)
        
        if not predictions:
            raise HTTPException(status_code=404, detail="No predictions available for this match")
        
        # Get match details
        match_details = await prediction_service.get_match_details(match_id)
        if not match_details:
            raise HTTPException(status_code=404, detail="Match not found")
        
        return PredictionResponse(
            predictions=predictions,
            match=match_details,
            total_predictions=len(predictions)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating predictions: {str(e)}")

@router.get("/predictions", response_model=List[Prediction])
async def get_recent_predictions(
    hours_back: int = Query(default=24, description="Hours to look back for predictions"),
    market_type: Optional[MarketType] = Query(default=None),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0)
):
    """Get recent predictions across all matches"""
    try:
        from ingest.database import db
        
        # Build query
        query = """
            SELECT match_id, market_type, selection, probability, confidence, 
                   fair_odds, model_version, created_at
            FROM predictions 
            WHERE created_at >= ?
        """
        params = [(datetime.now() - timedelta(hours=hours_back)).isoformat()]
        
        if market_type:
            query += " AND market_type = ?"
            params.append(market_type.value)
            
        if min_confidence > 0:
            query += " AND confidence >= ?"
            params.append(min_confidence)
            
        query += " ORDER BY created_at DESC"
        
        results = db.execute_query(query, params)
        
        predictions = []
        for result in results:
            prediction = Prediction(
                match_id=result[0],
                market_type=MarketType(result[1]),
                selection=result[2],
                probability=result[3],
                confidence=result[4],
                fair_odds=result[5],
                model_version=result[6],
                created_at=datetime.fromisoformat(result[7])
            )
            predictions.append(prediction)
            
        return predictions
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving predictions: {str(e)}")

@router.get("/betting-recommendations", response_model=BettingRecommendationResponse)
async def get_betting_recommendations(
    min_edge: float = Query(default=0.02, ge=0.0, description="Minimum edge percentage"),
    max_recommendations: int = Query(default=20, le=100),
    confidence_filter: Optional[str] = Query(default=None, regex="^(high|medium|low)$")
):
    """Get current betting recommendations with positive expected value"""
    try:
        recommendations = await prediction_service.get_betting_recommendations(min_edge=min_edge)
        
        # Apply confidence filter
        if confidence_filter:
            recommendations = [r for r in recommendations if r.confidence == confidence_filter]
        
        # Limit results
        recommendations = recommendations[:max_recommendations]
        
        # Calculate summary stats
        total_recommendations = len(recommendations)
        profitable_bets = len([r for r in recommendations if r.edge_percent > 0])
        total_edge = sum(r.edge_percent for r in recommendations)
        
        return BettingRecommendationResponse(
            recommendations=recommendations,
            total_recommendations=total_recommendations,
            profitable_bets=profitable_bets,
            total_edge=total_edge
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting recommendations: {str(e)}")

@router.post("/predictions/{match_id}/regenerate")
async def regenerate_match_predictions(
    match_id: str,
    market_types: Optional[List[MarketType]] = None
):
    """Force regeneration of predictions for a match"""
    try:
        predictions = await prediction_service.get_predictions_for_match(match_id, market_types)
        
        return {
            "message": f"Generated {len(predictions)} predictions for match {match_id}",
            "predictions": predictions
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error regenerating predictions: {str(e)}")

@router.get("/model-performance")
async def get_model_performance():
    """Get performance metrics for all prediction models"""
    try:
        from ..services.model_service import ModelService
        
        model_service = ModelService()
        
        # Get model info
        model_info = await model_service.get_model_info()
        
        # Get health check
        health_status = await model_service.health_check()
        
        return {
            "model_info": model_info,
            "health_status": health_status,
            "timestamp": datetime.now()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting model performance: {str(e)}")

# Helper functions
async def get_cached_predictions(match_id: str, market_types: Optional[List[MarketType]] = None) -> List[Prediction]:
    """Get cached predictions if they exist and are recent"""
    try:
        from ingest.database import db
        
        # Look for predictions within last 4 hours
        cutoff_time = datetime.now() - timedelta(hours=4)
        
        query = """
            SELECT match_id, market_type, selection, probability, confidence, 
                   fair_odds, model_version, created_at
            FROM predictions 
            WHERE match_id = ? AND created_at >= ?
        """
        params = [match_id, cutoff_time.isoformat()]
        
        if market_types:
            market_type_strs = [mt.value for mt in market_types]
            placeholders = ','.join(['?' for _ in market_type_strs])
            query += f" AND market_type IN ({placeholders})"
            params.extend(market_type_strs)
            
        results = db.execute_query(query, params)
        
        predictions = []
        for result in results:
            prediction = Prediction(
                match_id=result[0],
                market_type=MarketType(result[1]),
                selection=result[2],
                probability=result[3],
                confidence=result[4],
                fair_odds=result[5],
                model_version=result[6],
                created_at=datetime.fromisoformat(result[7])
            )
            predictions.append(prediction)
            
        return predictions
        
    except Exception as e:
        return []