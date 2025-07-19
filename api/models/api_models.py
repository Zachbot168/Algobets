"""
Pydantic models for API requests and responses
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

# Enums
class MarketType(str, Enum):
    MATCH_WINNER = "match_winner"
    MAP_WINNER = "map_winner"
    TOTAL_ROUNDS = "total_rounds"
    PLAYER_KILLS = "player_kills"
    FIRST_BLOOD = "first_blood"

class BetStatus(str, Enum):
    PENDING = "pending"
    PLACED = "placed"
    WON = "won"
    LOST = "lost"
    VOID = "void"

# Base Models
class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    version: str
    database_connected: bool
    models_loaded: bool

class Team(BaseModel):
    team_id: str
    team_name: str
    region: Optional[str] = None
    country: Optional[str] = None
    logo_url: Optional[str] = None
    
class Player(BaseModel):
    player_id: str
    player_name: str
    real_name: Optional[str] = None
    team_id: str
    role: Optional[str] = None
    country: Optional[str] = None

class Match(BaseModel):
    match_id: str
    tournament_name: Optional[str] = None
    team_a: Team
    team_b: Team
    start_time: datetime
    best_of: int
    status: str
    patch_version: Optional[str] = None
    is_lan: bool = False
    
class MatchDetails(Match):
    maps: Optional[List[str]] = None
    team_a_players: Optional[List[Player]] = None
    team_b_players: Optional[List[Player]] = None

# Prediction Models
class PredictionRequest(BaseModel):
    match_id: str
    market_type: MarketType
    selection: Optional[str] = None  # For player props
    
class Prediction(BaseModel):
    match_id: str
    market_type: MarketType
    selection: Optional[str] = None
    probability: float = Field(..., ge=0.0, le=1.0, description="Predicted probability")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Model confidence")
    fair_odds: float = Field(..., gt=1.0, description="Fair odds based on probability")
    model_version: str
    created_at: datetime

class PredictionResponse(BaseModel):
    predictions: List[Prediction]
    match: Match
    total_predictions: int

# Odds Models
class Odds(BaseModel):
    odds_id: str
    match_id: str
    bookmaker: str
    market_type: str
    selection: str
    odds_decimal: float
    odds_american: int
    timestamp: datetime
    is_latest: bool

class OddsComparison(BaseModel):
    market_type: str
    selection: str
    best_odds: float
    best_bookmaker: str
    all_odds: List[Odds]
    implied_probability: float

# Betting Models
class BettingRecommendation(BaseModel):
    match_id: str
    market_type: MarketType
    selection: str
    bookmaker: str
    odds_decimal: float
    fair_odds: float
    edge_percent: float = Field(..., description="Expected edge percentage")
    kelly_stake: float = Field(..., description="Kelly criterion stake")
    confidence: str = Field(..., description="Confidence level (high/medium/low)")
    reasoning: Optional[str] = None

class BetRequest(BaseModel):
    match_id: str
    market_type: MarketType
    selection: str
    bookmaker: str
    odds_decimal: float
    stake_amount: float
    
class Bet(BaseModel):
    bet_id: str
    match_id: str
    market_type: MarketType
    selection: str
    bookmaker: str
    odds_decimal: float
    stake_amount: float
    potential_return: float
    status: BetStatus
    placed_at: datetime
    settled_at: Optional[datetime] = None
    result: Optional[str] = None

# Analytics Models
class PerformanceMetrics(BaseModel):
    total_bets: int
    total_stake: float
    total_return: float
    net_profit: float
    roi_percent: float
    win_rate: float
    average_odds: float
    kelly_adherence: float

class ModelPerformance(BaseModel):
    model_name: str
    accuracy: float
    log_loss: float
    calibration_error: float
    predictions_count: int
    last_updated: datetime

# List Responses
class MatchListResponse(BaseModel):
    matches: List[Match]
    total: int
    page: int
    page_size: int

class PredictionListResponse(BaseModel):
    predictions: List[Prediction]
    total: int
    
class BettingRecommendationResponse(BaseModel):
    recommendations: List[BettingRecommendation]
    total_recommendations: int
    profitable_bets: int
    total_edge: float

class BetHistoryResponse(BaseModel):
    bets: List[Bet]
    performance: PerformanceMetrics
    total: int
    page: int
    page_size: int

# Error Models
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    timestamp: datetime