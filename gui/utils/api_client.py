"""
API client for communicating with the FastAPI backend
"""

import requests
import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class APIClient:
    """Client for interacting with the VALORANT betting API"""
    
    def __init__(self, base_url: str = None):
        self.base_url = base_url or os.getenv('API_BASE_URL', 'http://localhost:8000')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'VALORANT-Betting-GUI/1.0'
        })
        
        # Add API key if available
        api_key = os.getenv('API_SECRET_KEY')
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}'
            })
            
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request to API"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.request(method, url, timeout=30, **kwargs)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {method} {url} - {e}")
            raise
            
    def get(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """Make GET request"""
        return self._make_request('GET', endpoint, params=params)
        
    def post(self, endpoint: str, data: Dict = None) -> Dict[str, Any]:
        """Make POST request"""
        return self._make_request('POST', endpoint, json=data)
        
    def put(self, endpoint: str, data: Dict = None) -> Dict[str, Any]:
        """Make PUT request"""
        return self._make_request('PUT', endpoint, json=data)
        
    def delete(self, endpoint: str) -> Dict[str, Any]:
        """Make DELETE request"""
        return self._make_request('DELETE', endpoint)
        
    # Health and Status
    def get_health(self) -> Dict[str, Any]:
        """Get API health status"""
        return self.get('/health')
        
    # Matches
    def get_matches(self, status: str = None, days_ahead: int = 7, 
                   days_back: int = 7, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """Get matches with filters"""
        params = {
            'days_ahead': days_ahead,
            'days_back': days_back,
            'page': page,
            'page_size': page_size
        }
        
        if status:
            params['status'] = status
            
        return self.get('/api/v1/matches', params=params)
        
    def get_match_details(self, match_id: str) -> Dict[str, Any]:
        """Get detailed match information"""
        return self.get(f'/api/v1/matches/{match_id}')
        
    def get_upcoming_matches(self, hours_ahead: int = 24) -> Dict[str, Any]:
        """Get upcoming matches"""
        return self.get('/api/v1/matches/upcoming', params={'hours_ahead': hours_ahead})
        
    def get_match_stats(self, match_id: str) -> Dict[str, Any]:
        """Get match statistics"""
        return self.get(f'/api/v1/matches/{match_id}/stats')
        
    # Predictions
    def get_match_predictions(self, match_id: str, market_types: List[str] = None,
                            force_refresh: bool = False) -> Dict[str, Any]:
        """Get predictions for a match"""
        params = {'force_refresh': force_refresh}
        if market_types:
            params['market_types'] = market_types
            
        return self.get(f'/api/v1/predictions/{match_id}', params=params)
        
    def get_recent_predictions(self, hours_back: int = 24, market_type: str = None,
                             min_confidence: float = 0.0) -> List[Dict[str, Any]]:
        """Get recent predictions"""
        params = {
            'hours_back': hours_back,
            'min_confidence': min_confidence
        }
        
        if market_type:
            params['market_type'] = market_type
            
        return self.get('/api/v1/predictions', params=params)
        
    def get_betting_recommendations(self, min_edge: float = 0.02, 
                                  max_recommendations: int = 20,
                                  confidence_filter: str = None) -> Dict[str, Any]:
        """Get betting recommendations"""
        params = {
            'min_edge': min_edge,
            'max_recommendations': max_recommendations
        }
        
        if confidence_filter:
            params['confidence_filter'] = confidence_filter
            
        return self.get('/api/v1/betting-recommendations', params=params)
        
    def regenerate_predictions(self, match_id: str, market_types: List[str] = None) -> Dict[str, Any]:
        """Force regeneration of predictions"""
        data = {}
        if market_types:
            data['market_types'] = market_types
            
        return self.post(f'/api/v1/predictions/{match_id}/regenerate', data=data)
        
    def get_model_performance(self) -> Dict[str, Any]:
        """Get model performance metrics"""
        return self.get('/api/v1/model-performance')
        
    # Odds
    def get_match_odds(self, match_id: str, market_type: str = None,
                      bookmaker: str = None, latest_only: bool = True) -> List[Dict[str, Any]]:
        """Get odds for a match"""
        params = {'latest_only': latest_only}
        
        if market_type:
            params['market_type'] = market_type
        if bookmaker:
            params['bookmaker'] = bookmaker
            
        return self.get(f'/api/v1/odds/{match_id}', params=params)
        
    def get_odds_comparison(self, match_id: str, market_type: str) -> Dict[str, Any]:
        """Get odds comparison for a market"""
        return self.get(f'/api/v1/odds/{match_id}/comparison', 
                       params={'market_type': market_type})
        
    def get_odds_movement(self, match_id: str, market_type: str, 
                         hours_back: int = 24) -> Dict[str, Any]:
        """Get odds movement over time"""
        return self.get(f'/api/v1/odds/{match_id}/movement',
                       params={'market_type': market_type, 'hours_back': hours_back})
        
    def get_best_odds(self, match_id: str, market_type: str, 
                     selection: str = None) -> Dict[str, Any]:
        """Get best odds for a market"""
        params = {'market_type': market_type}
        if selection:
            params['selection'] = selection
            
        return self.get(f'/api/v1/odds/{match_id}/best', params=params)
        
    def get_arbitrage_opportunities(self, match_id: str) -> Dict[str, Any]:
        """Get arbitrage opportunities for a match"""
        return self.get(f'/api/v1/odds/{match_id}/arbitrage')
        
    def get_recent_odds(self, hours_back: int = 6, market_type: str = None,
                       bookmaker: str = None, limit: int = 100) -> Dict[str, Any]:
        """Get recent odds updates"""
        params = {
            'hours_back': hours_back,
            'limit': limit
        }
        
        if market_type:
            params['market_type'] = market_type
        if bookmaker:
            params['bookmaker'] = bookmaker
            
        return self.get('/api/v1/odds/recent', params=params)
        
    # Teams
    def get_teams(self, region: str = None, country: str = None,
                 active_only: bool = True, limit: int = 100) -> List[Dict[str, Any]]:
        """Get teams with filters"""
        params = {
            'active_only': active_only,
            'limit': limit
        }
        
        if region:
            params['region'] = region
        if country:
            params['country'] = country
            
        return self.get('/api/v1/teams', params=params)
        
    def get_team_details(self, team_id: str) -> Dict[str, Any]:
        """Get team details"""
        return self.get(f'/api/v1/teams/{team_id}')
        
    def get_team_roster(self, team_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get team roster"""
        return self.get(f'/api/v1/teams/{team_id}/roster',
                       params={'active_only': active_only})
        
    def get_team_matches(self, team_id: str, days_back: int = 90,
                        status: str = None, limit: int = 50) -> Dict[str, Any]:
        """Get team matches"""
        params = {
            'days_back': days_back,
            'limit': limit
        }
        
        if status:
            params['status'] = status
            
        return self.get(f'/api/v1/teams/{team_id}/matches', params=params)
        
    def get_team_stats(self, team_id: str, days_back: int = 90) -> Dict[str, Any]:
        """Get team statistics"""
        return self.get(f'/api/v1/teams/{team_id}/stats',
                       params={'days_back': days_back})
        
    def get_player_details(self, player_id: str) -> Dict[str, Any]:
        """Get player details"""
        return self.get(f'/api/v1/players/{player_id}')
        
    def search_players(self, name: str = None, team_id: str = None,
                      role: str = None, country: str = None,
                      active_only: bool = True, limit: int = 50) -> Dict[str, Any]:
        """Search for players"""
        params = {
            'active_only': active_only,
            'limit': limit
        }
        
        if name:
            params['name'] = name
        if team_id:
            params['team_id'] = team_id
        if role:
            params['role'] = role
        if country:
            params['country'] = country
            
        return self.get('/api/v1/players', params=params)
        
    def get_regions(self) -> Dict[str, Any]:
        """Get all regions"""
        return self.get('/api/v1/regions')
        
    # Market Analysis
    def get_market_analysis(self, market_type: str, days_back: int = 30) -> Dict[str, Any]:
        """Get market efficiency analysis"""
        return self.get(f'/api/v1/market-analysis/{market_type}',
                       params={'days_back': days_back})
        
    def get_bookmaker_limits(self, bookmaker: str) -> Dict[str, Any]:
        """Get bookmaker limits"""
        return self.get(f'/api/v1/bookmakers/{bookmaker}/limits')
        
    def get_closing_line_value(self, match_id: str, market_type: str) -> Dict[str, Any]:
        """Get closing line value analysis"""
        return self.get(f'/api/v1/closing-line-value/{match_id}',
                       params={'market_type': market_type})