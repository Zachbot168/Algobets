"""
Service for managing betting odds and comparisons
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from ingest.database import db

logger = logging.getLogger(__name__)

class OddsService:
    """Service for retrieving and comparing betting odds"""
    
    def __init__(self):
        self.bookmaker_priority = [
            'pinnacle',      # Generally considered sharpest
            'draftkings',
            'fanduel', 
            'betmgm',
            'betrivers',
            'williamhill_us'
        ]
        
    async def get_best_odds(self, match_id: str, market_type: str, 
                          selection: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get the best available odds for a specific market"""
        try:
            # Build query based on parameters
            query = """
                SELECT bookmaker, selection, odds_decimal, odds_american, timestamp
                FROM bronze_odds 
                WHERE match_id = ? AND market_type = ? AND is_latest = true
            """
            params = [match_id, market_type]
            
            if selection:
                query += " AND selection = ?"
                params.append(selection)
                
            query += " ORDER BY odds_decimal DESC"
            
            results = db.execute_query(query, params)
            
            if not results:
                return None
                
            # Find best odds (highest decimal odds = best value)
            best_odds = results[0]
            
            return {
                'bookmaker': best_odds[0],
                'selection': best_odds[1],
                'odds_decimal': best_odds[2],
                'odds_american': best_odds[3],
                'timestamp': best_odds[4],
                'match_id': match_id,
                'market_type': market_type
            }
            
        except Exception as e:
            logger.error(f"Error getting best odds: {e}")
            return None
            
    async def get_odds_comparison(self, match_id: str, market_type: str) -> List[Dict[str, Any]]:
        """Get odds comparison across all bookmakers"""
        try:
            query = """
                SELECT bookmaker, selection, odds_decimal, odds_american, timestamp
                FROM bronze_odds 
                WHERE match_id = ? AND market_type = ? AND is_latest = true
                ORDER BY selection, odds_decimal DESC
            """
            
            results = db.execute_query(query, [match_id, market_type])
            
            odds_comparison = []
            for result in results:
                odds_comparison.append({
                    'bookmaker': result[0],
                    'selection': result[1],
                    'odds_decimal': result[2],
                    'odds_american': result[3],
                    'timestamp': result[4],
                    'implied_probability': 1.0 / result[2] if result[2] > 0 else 0.0
                })
                
            return odds_comparison
            
        except Exception as e:
            logger.error(f"Error getting odds comparison: {e}")
            return []
            
    async def get_odds_movement(self, match_id: str, market_type: str, 
                              hours_back: int = 24) -> List[Dict[str, Any]]:
        """Get odds movement over time for a specific market"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            
            query = """
                SELECT bookmaker, selection, odds_decimal, timestamp
                FROM bronze_odds 
                WHERE match_id = ? AND market_type = ? AND timestamp >= ?
                ORDER BY timestamp ASC
            """
            
            results = db.execute_query(query, [match_id, market_type, cutoff_time.isoformat()])
            
            movement_data = []
            for result in results:
                movement_data.append({
                    'bookmaker': result[0],
                    'selection': result[1],
                    'odds_decimal': result[2],
                    'timestamp': result[3]
                })
                
            return movement_data
            
        except Exception as e:
            logger.error(f"Error getting odds movement: {e}")
            return []
            
    async def calculate_arbitrage_opportunities(self, match_id: str) -> List[Dict[str, Any]]:
        """Find arbitrage opportunities for a match"""
        try:
            # Get all current odds for the match
            query = """
                SELECT DISTINCT market_type 
                FROM bronze_odds 
                WHERE match_id = ? AND is_latest = true
            """
            
            markets = db.execute_query(query, [match_id])
            arbitrage_opportunities = []
            
            for market in markets:
                market_type = market[0]
                
                # Get best odds for each selection in this market
                odds_query = """
                    SELECT selection, MAX(odds_decimal) as best_odds, 
                           MIN(1.0/odds_decimal) as min_implied_prob
                    FROM bronze_odds 
                    WHERE match_id = ? AND market_type = ? AND is_latest = true
                    GROUP BY selection
                """
                
                market_odds = db.execute_query(odds_query, [match_id, market_type])
                
                if len(market_odds) >= 2:
                    # Calculate total implied probability
                    total_implied_prob = sum(1.0 / odds[1] for odds in market_odds)
                    
                    if total_implied_prob < 1.0:  # Arbitrage opportunity
                        profit_margin = (1.0 - total_implied_prob) * 100
                        
                        arbitrage_opportunities.append({
                            'match_id': match_id,
                            'market_type': market_type,
                            'profit_margin_percent': profit_margin,
                            'selections': [
                                {
                                    'selection': odds[0],
                                    'best_odds': odds[1],
                                    'stake_percent': (1.0 / odds[1]) / total_implied_prob * 100
                                }
                                for odds in market_odds
                            ]
                        })
                        
            return arbitrage_opportunities
            
        except Exception as e:
            logger.error(f"Error calculating arbitrage opportunities: {e}")
            return []
            
    async def get_market_efficiency(self, market_type: str, days_back: int = 30) -> Dict[str, Any]:
        """Analyze market efficiency for a specific market type"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            # Get odds data for analysis
            query = """
                SELECT o.odds_decimal, o.selection, m.winner_id, m.team_a_id, m.team_b_id
                FROM bronze_odds o
                JOIN bronze_matches m ON o.match_id = m.match_id
                WHERE o.market_type = ? AND o.timestamp >= ? 
                  AND m.status = 'completed' AND o.bookmaker = 'pinnacle'
            """
            
            results = db.execute_query(query, [market_type, cutoff_date.isoformat()])
            
            if not results:
                return {'error': 'No data available'}
                
            # Calculate market efficiency metrics
            total_predictions = len(results)
            correct_predictions = 0
            average_odds = 0
            
            for result in results:
                odds_decimal, selection, winner_id, team_a_id, team_b_id = result
                average_odds += odds_decimal
                
                # Check if prediction was correct (simplified)
                if market_type == 'h2h':
                    if (selection == team_a_id and winner_id == team_a_id) or \
                       (selection == team_b_id and winner_id == team_b_id):
                        correct_predictions += 1
                        
            if total_predictions > 0:
                accuracy = correct_predictions / total_predictions
                average_odds = average_odds / total_predictions
                
                return {
                    'market_type': market_type,
                    'period_days': days_back,
                    'total_predictions': total_predictions,
                    'accuracy': accuracy,
                    'average_odds': average_odds,
                    'market_efficiency': accuracy * average_odds  # Simple efficiency metric
                }
            else:
                return {'error': 'No predictions found'}
                
        except Exception as e:
            logger.error(f"Error calculating market efficiency: {e}")
            return {'error': str(e)}
            
    async def get_bookmaker_limits(self, bookmaker: str) -> Dict[str, Any]:
        """Get estimated limits for a bookmaker (static data for now)"""
        # This would typically come from a database or external API
        limits_data = {
            'draftkings': {
                'max_stake_percentage': 0.05,
                'typical_limits': {
                    'match_winner': 10000,
                    'total_rounds': 5000,
                    'player_props': 1000
                }
            },
            'fanduel': {
                'max_stake_percentage': 0.04,
                'typical_limits': {
                    'match_winner': 8000,
                    'total_rounds': 4000,
                    'player_props': 800
                }
            },
            'pinnacle': {
                'max_stake_percentage': 0.10,
                'typical_limits': {
                    'match_winner': 50000,
                    'total_rounds': 25000,
                    'player_props': 5000
                }
            }
        }
        
        return limits_data.get(bookmaker, {
            'max_stake_percentage': 0.02,
            'typical_limits': {
                'match_winner': 1000,
                'total_rounds': 500,
                'player_props': 200
            }
        })
        
    async def get_closing_line_value(self, match_id: str, market_type: str) -> Dict[str, Any]:
        """Calculate closing line value for completed matches"""
        try:
            # Get opening and closing odds
            opening_query = """
                SELECT selection, odds_decimal, timestamp
                FROM bronze_odds 
                WHERE match_id = ? AND market_type = ? 
                ORDER BY timestamp ASC 
                LIMIT 2
            """
            
            closing_query = """
                SELECT selection, odds_decimal, timestamp
                FROM bronze_odds 
                WHERE match_id = ? AND market_type = ? 
                ORDER BY timestamp DESC 
                LIMIT 2
            """
            
            opening_odds = db.execute_query(opening_query, [match_id, market_type])
            closing_odds = db.execute_query(closing_query, [match_id, market_type])
            
            if not opening_odds or not closing_odds:
                return {'error': 'Insufficient odds data'}
                
            clv_analysis = []
            
            # Compare opening vs closing for each selection
            for i, opening in enumerate(opening_odds):
                if i < len(closing_odds):
                    closing = closing_odds[i]
                    
                    opening_prob = 1.0 / opening[1]
                    closing_prob = 1.0 / closing[1]
                    
                    clv_percentage = ((closing[1] - opening[1]) / opening[1]) * 100
                    
                    clv_analysis.append({
                        'selection': opening[0],
                        'opening_odds': opening[1],
                        'closing_odds': closing[1],
                        'clv_percentage': clv_percentage,
                        'probability_shift': closing_prob - opening_prob
                    })
                    
            return {
                'match_id': match_id,
                'market_type': market_type,
                'clv_analysis': clv_analysis
            }
            
        except Exception as e:
            logger.error(f"Error calculating closing line value: {e}")
            return {'error': str(e)}