"""
Abios API integration for VALORANT esports data
"""

import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from ..base_ingester import BaseIngester

logger = logging.getLogger(__name__)

class AbiosIngester(BaseIngester):
    """Ingests VALORANT match data from Abios API"""
    
    def __init__(self):
        super().__init__(rate_limit_per_minute=600)  # Abios allows 600 requests/hour on free tier
        self.api_key = os.getenv('ABIOS_API_KEY')
        self.base_url = "https://api.abiosgaming.com/v2"
        
    def get_default_headers(self) -> Dict[str, str]:
        headers = {
            'User-Agent': 'VALORANT-Betting-Platform/1.0',
            'Accept': 'application/json'
        }
        
        if self.api_key:
            headers['Abios-Secret'] = self.api_key
            
        return headers
        
    async def ingest_data(self, days_back: int = 7, **kwargs) -> Dict[str, Any]:
        """Ingest recent VALORANT match data from Abios"""
        start_time = self.get_current_timestamp()
        
        try:
            if not self.api_key:
                logger.warning("Abios API key not configured, skipping ingestion")
                return {'matches_count': 0, 'errors': 1, 'source': 'abios'}
                
            # Get VALORANT game ID first
            game_id = await self.get_valorant_game_id()
            if not game_id:
                logger.error("Could not find VALORANT game ID in Abios")
                return {'matches_count': 0, 'errors': 1, 'source': 'abios'}
                
            # Fetch matches and series
            matches_data = await self.fetch_valorant_matches(game_id, days_back)
            series_data = await self.fetch_valorant_series(game_id, days_back)
            
            if not matches_data:
                logger.warning("No matches retrieved from Abios API")
                return {'matches_count': 0, 'errors': 0, 'source': 'abios'}
                
            # Process and save data
            processed_matches = self.process_matches(matches_data)
            saved_matches = self.save_matches_to_bronze(processed_matches)
            
            # Process series data for additional context
            if series_data:
                processed_series = self.process_series(series_data)
                saved_series = self.save_series_to_bronze(processed_series)
            else:
                saved_series = 0
                
            self.log_ingestion_stats("abios_matches", saved_matches, start_time)
            
            return {
                'matches_count': saved_matches,
                'series_count': saved_series,
                'source': 'abios',
                'errors': 0
            }
            
        except Exception as e:
            logger.error(f"Error in Abios ingestion: {e}")
            self.log_ingestion_stats("abios_matches", 0, start_time, errors=1)
            raise
            
    async def get_valorant_game_id(self) -> Optional[int]:
        """Get VALORANT game ID from Abios API"""
        try:
            games_url = f"{self.base_url}/games"
            games = await self.make_request(games_url)
            
            if games and 'data' in games:
                for game in games['data']:
                    if game.get('title', '').lower() == 'valorant':
                        return game.get('id')
                        
            return None
            
        except Exception as e:
            logger.error(f"Error fetching games from Abios: {e}")
            return None
            
    async def fetch_valorant_matches(self, game_id: int, days_back: int) -> Optional[List[Dict]]:
        """Fetch VALORANT matches from Abios API"""
        try:
            since_timestamp = int((datetime.now() - timedelta(days=days_back)).timestamp())
            
            matches_url = f"{self.base_url}/matches"
            params = {
                'game': game_id,
                'starts_after': since_timestamp,
                'limit': 100
            }
            
            all_matches = []
            offset = 0
            
            while offset < 500:  # Limit to 500 matches to avoid excessive API calls
                params['offset'] = offset
                response = await self.make_request(matches_url, params)
                
                if not response or 'data' not in response:
                    break
                    
                matches = response['data']
                if not matches:
                    break
                    
                all_matches.extend(matches)
                
                # If we got less than limit, we're at the end
                if len(matches) < params['limit']:
                    break
                    
                offset += params['limit']
                await asyncio.sleep(0.1)  # Small delay between requests
                
            return all_matches
            
        except Exception as e:
            logger.error(f"Error fetching matches from Abios: {e}")
            return None
            
    async def fetch_valorant_series(self, game_id: int, days_back: int) -> Optional[List[Dict]]:
        """Fetch VALORANT series from Abios API"""
        try:
            since_timestamp = int((datetime.now() - timedelta(days=days_back)).timestamp())
            
            series_url = f"{self.base_url}/series"
            params = {
                'game': game_id,
                'starts_after': since_timestamp,
                'limit': 50
            }
            
            response = await self.make_request(series_url, params)
            
            if response and 'data' in response:
                return response['data']
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error fetching series from Abios: {e}")
            return []
            
    def process_matches(self, matches_data: List[Dict]) -> List[Dict]:
        """Process Abios matches data into standardized format"""
        processed = []
        
        for match in matches_data:
            try:
                # Extract team information
                rosters = match.get('rosters', [])
                team_a = rosters[0] if len(rosters) > 0 else {}
                team_b = rosters[1] if len(rosters) > 1 else {}
                
                # Determine winner
                winner_id = None
                results = match.get('results', [])
                if results:
                    for result in results:
                        if result.get('score', 0) > 0:
                            winner_id = result.get('roster_id')
                            break
                            
                processed_match = {
                    'match_id': f"abios_{match.get('id')}",
                    'tournament_id': match.get('tournament', {}).get('id'),
                    'tournament_name': match.get('tournament', {}).get('title'),
                    'team_a_id': f"abios_{team_a.get('id')}" if team_a.get('id') else None,
                    'team_a_name': team_a.get('team', {}).get('name'),
                    'team_b_id': f"abios_{team_b.get('id')}" if team_b.get('id') else None,
                    'team_b_name': team_b.get('team', {}).get('name'),
                    'start_time': datetime.fromtimestamp(match.get('start', 0)).isoformat() if match.get('start') else None,
                    'end_time': datetime.fromtimestamp(match.get('end', 0)).isoformat() if match.get('end') else None,
                    'status': self.map_abios_status(match.get('deleted', False), match.get('forfeit', False)),
                    'best_of': match.get('best_of', 1),
                    'winner_id': f"abios_{winner_id}" if winner_id else None,
                    'patch_version': None,  # Abios doesn't provide patch info
                    'is_lan': match.get('tournament', {}).get('tier', 0) >= 3,  # Tier 3+ usually LAN
                    'venue': None,
                    'ingested_at': self.get_current_timestamp(),
                    'updated_at': self.get_current_timestamp()
                }
                
                processed.append(processed_match)
                
            except Exception as e:
                logger.warning(f"Error processing Abios match {match.get('id', 'unknown')}: {e}")
                continue
                
        return processed
        
    def process_series(self, series_data: List[Dict]) -> List[Dict]:
        """Process series data for additional context"""
        processed = []
        
        for series in series_data:
            try:
                processed_series = {
                    'series_id': f"abios_{series.get('id')}",
                    'series_title': series.get('title'),
                    'tournament_id': series.get('tournament', {}).get('id'),
                    'tournament_title': series.get('tournament', {}).get('title'),
                    'best_of': series.get('best_of'),
                    'start_time': datetime.fromtimestamp(series.get('start', 0)).isoformat() if series.get('start') else None,
                    'end_time': datetime.fromtimestamp(series.get('end', 0)).isoformat() if series.get('end') else None,
                    'tier': series.get('tier', 0),
                    'postponed': series.get('postponed', False),
                    'ingested_at': self.get_current_timestamp()
                }
                
                processed.append(processed_series)
                
            except Exception as e:
                logger.warning(f"Error processing series {series.get('id', 'unknown')}: {e}")
                continue
                
        return processed
        
    def map_abios_status(self, deleted: bool, forfeit: bool) -> str:
        """Map Abios status flags to standardized status"""
        if deleted:
            return 'cancelled'
        elif forfeit:
            return 'forfeit'
        else:
            return 'completed'
            
    def save_matches_to_bronze(self, matches: List[Dict]) -> int:
        """Save processed matches to bronze layer"""
        if not matches:
            return 0
            
        return self.save_to_bronze('bronze_matches', matches)
        
    def save_series_to_bronze(self, series: List[Dict]) -> int:
        """Save processed series to bronze layer"""
        if not series:
            return 0
            
        # Create series table if it doesn't exist
        from ..database import db
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS bronze_series (
                series_id VARCHAR PRIMARY KEY,
                series_title VARCHAR,
                tournament_id VARCHAR,
                tournament_title VARCHAR,
                best_of INTEGER,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                tier INTEGER,
                postponed BOOLEAN,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        try:
            db.execute_query(create_table_query)
        except Exception as e:
            logger.warning(f"Could not create series table: {e}")
            
        return self.save_to_bronze('bronze_series', series)
        
    async def ingest_recent_matches(self, days_back: int = 7) -> Dict[str, Any]:
        """Public method to ingest recent matches from Abios"""
        async with self:
            return await self.ingest_data(days_back=days_back)