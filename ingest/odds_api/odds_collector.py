import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from ..base_ingester import BaseIngester

logger = logging.getLogger(__name__)

class OddsCollector(BaseIngester):
    """Collects betting odds from various sportsbooks"""
    
    def __init__(self):
        super().__init__(rate_limit_per_minute=500)  # TheOddsAPI allows 500 per month
        self.theodds_api_key = os.getenv('THEODS_API_KEY')
        self.pinnacle_api_key = os.getenv('PINNACLE_API_KEY')
        
        # API endpoints
        self.theodds_base_url = "https://api.the-odds-api.com/v4/sports"
        self.pinnacle_base_url = "https://api.pinnacle.com/v1"
        
        # Illinois legal sportsbooks
        self.target_bookmakers = [
            'draftkings',
            'fanduel', 
            'betrivers',
            'betmgm',
            'pinnacle',
            'williamhill_us'
        ]
        
    def get_default_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': 'VALORANT-Betting-Platform/1.0',
            'Accept': 'application/json'
        }
        
    async def ingest_data(self, **kwargs) -> Dict[str, Any]:
        """Collect odds from all configured sources"""
        start_time = self.get_current_timestamp()
        total_records = 0
        errors = 0
        
        try:
            # Collect from TheOddsAPI
            if self.theodds_api_key:
                theodds_records = await self.collect_from_theodds_api()
                total_records += theodds_records
            else:
                logger.warning("TheOddsAPI key not configured")
                
            # Collect from Pinnacle
            if self.pinnacle_api_key:
                pinnacle_records = await self.collect_from_pinnacle()
                total_records += pinnacle_records
            else:
                logger.warning("Pinnacle API key not configured")
                
            # Mark old odds as not latest
            self.update_odds_latest_flags()
            
            self.log_ingestion_stats("odds_collection", total_records, start_time, errors)
            
            return {
                'records_count': total_records,
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Error in odds collection: {e}")
            self.log_ingestion_stats("odds_collection", total_records, start_time, errors + 1)
            raise
            
    async def collect_from_theodds_api(self) -> int:
        """Collect odds from TheOddsAPI"""
        try:
            # Get upcoming VALORANT matches
            odds_data = await self.fetch_valorant_odds_theodds()
            
            if not odds_data:
                logger.warning("No odds data from TheOddsAPI")
                return 0
                
            # Process and save odds
            processed_odds = self.process_theodds_data(odds_data)
            saved_count = self.save_to_bronze('bronze_odds', processed_odds)
            
            logger.info(f"Collected {saved_count} odds records from TheOddsAPI")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error collecting from TheOddsAPI: {e}")
            return 0
            
    async def fetch_valorant_odds_theodds(self) -> Optional[List[Dict]]:
        """Fetch VALORANT odds from TheOddsAPI"""
        try:
            # Note: VALORANT might not be directly available, check for esports category
            url = f"{self.theodds_base_url}/esports_valorant/odds"
            params = {
                'apiKey': self.theodds_api_key,
                'regions': 'us',
                'markets': 'h2h,totals',  # Head-to-head and totals
                'bookmakers': ','.join(self.target_bookmakers),
                'oddsFormat': 'decimal'
            }
            
            response = await self.make_request(url, params)
            return response if response else []
            
        except Exception as e:
            logger.error(f"Error fetching VALORANT odds from TheOddsAPI: {e}")
            return None
            
    def process_theodds_data(self, odds_data: List[Dict]) -> List[Dict]:
        """Process TheOddsAPI odds data into standardized format"""
        processed_odds = []
        
        for event in odds_data:
            try:
                match_id = self.generate_match_id_from_teams(
                    event.get('home_team'), 
                    event.get('away_team'),
                    event.get('commence_time')
                )
                
                # Process each bookmaker's odds
                for bookmaker in event.get('bookmakers', []):
                    bookmaker_name = bookmaker.get('key')
                    
                    # Process each market (h2h, totals, etc.)
                    for market in bookmaker.get('markets', []):
                        market_type = market.get('key')
                        
                        # Process each outcome
                        for outcome in market.get('outcomes', []):
                            odds_record = {
                                'odds_id': self.generate_id(
                                    match_id, bookmaker_name, market_type, 
                                    outcome.get('name'), event.get('commence_time')
                                ),
                                'match_id': match_id,
                                'bookmaker': bookmaker_name,
                                'market_type': market_type,
                                'selection': outcome.get('name'),
                                'odds_decimal': float(outcome.get('price', 0)),
                                'odds_american': self.decimal_to_american(float(outcome.get('price', 0))),
                                'timestamp': event.get('commence_time'),
                                'is_latest': True,
                                'ingested_at': self.get_current_timestamp()
                            }
                            
                            processed_odds.append(odds_record)
                            
            except Exception as e:
                logger.warning(f"Error processing odds event: {e}")
                continue
                
        return processed_odds
        
    async def collect_from_pinnacle(self) -> int:
        """Collect odds from Pinnacle API"""
        try:
            # Pinnacle has different API structure
            odds_data = await self.fetch_valorant_odds_pinnacle()
            
            if not odds_data:
                logger.warning("No odds data from Pinnacle")
                return 0
                
            processed_odds = self.process_pinnacle_data(odds_data)
            saved_count = self.save_to_bronze('bronze_odds', processed_odds)
            
            logger.info(f"Collected {saved_count} odds records from Pinnacle")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error collecting from Pinnacle: {e}")
            return 0
            
    async def fetch_valorant_odds_pinnacle(self) -> Optional[List[Dict]]:
        """Fetch VALORANT odds from Pinnacle API"""
        try:
            # Get esports events
            url = f"{self.pinnacle_base_url}/fixtures"
            headers = {
                **self.get_default_headers(),
                'Authorization': f'Bearer {self.pinnacle_api_key}'
            }
            params = {
                'sportId': 12,  # Esports (check Pinnacle docs for VALORANT sport ID)
                'since': int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
            }
            
            response = await self.make_request(url, params, headers)
            return response.get('fixtures', []) if response else []
            
        except Exception as e:
            logger.error(f"Error fetching from Pinnacle: {e}")
            return None
            
    def process_pinnacle_data(self, odds_data: List[Dict]) -> List[Dict]:
        """Process Pinnacle odds data"""
        processed_odds = []
        
        for fixture in odds_data:
            try:
                # Filter for VALORANT events
                if 'valorant' not in fixture.get('league', {}).get('name', '').lower():
                    continue
                    
                match_id = self.generate_id(
                    fixture.get('home'), 
                    fixture.get('away'),
                    fixture.get('starts')
                )
                
                # Get odds for this fixture
                odds = fixture.get('odds', {})
                
                # Process moneyline odds
                if 'moneyline' in odds:
                    ml = odds['moneyline']
                    for team, price in ml.items():
                        odds_record = {
                            'odds_id': self.generate_id(match_id, 'pinnacle', 'h2h', team, fixture.get('starts')),
                            'match_id': match_id,
                            'bookmaker': 'pinnacle',
                            'market_type': 'h2h',
                            'selection': team,
                            'odds_decimal': price,
                            'odds_american': self.decimal_to_american(price),
                            'timestamp': fixture.get('starts'),
                            'is_latest': True,
                            'ingested_at': self.get_current_timestamp()
                        }
                        processed_odds.append(odds_record)
                        
            except Exception as e:
                logger.warning(f"Error processing Pinnacle fixture: {e}")
                continue
                
        return processed_odds
        
    def generate_match_id_from_teams(self, team_a: str, team_b: str, start_time: str) -> str:
        """Generate consistent match ID from team names and start time"""
        # Normalize team names
        teams = sorted([team_a.lower().strip(), team_b.lower().strip()])
        return self.generate_id(teams[0], teams[1], start_time)
        
    def decimal_to_american(self, decimal_odds: float) -> int:
        """Convert decimal odds to American format"""
        if decimal_odds >= 2.0:
            return int((decimal_odds - 1) * 100)
        else:
            return int(-100 / (decimal_odds - 1))
            
    def update_odds_latest_flags(self):
        """Mark old odds as not latest"""
        try:
            # Update is_latest flag for older odds
            query = """
                UPDATE bronze_odds 
                SET is_latest = false 
                WHERE timestamp < ? AND is_latest = true
            """
            cutoff_time = datetime.now() - timedelta(hours=2)
            db.execute_query(query, [cutoff_time.isoformat()])
            
        except Exception as e:
            logger.error(f"Error updating odds latest flags: {e}")
            
    async def collect_upcoming_odds(self) -> Dict[str, Any]:
        """Public method to collect odds for upcoming matches"""
        async with self:
            return await self.ingest_data()