"""
PandaScore API integration for VALORANT esports data
"""

import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from ..base_ingester import BaseIngester

logger = logging.getLogger(__name__)

class PandaScoreIngester(BaseIngester):
    """Ingests VALORANT match data from PandaScore API"""
    
    def __init__(self):
        super().__init__(rate_limit_per_minute=1000)  # PandaScore allows 1000 requests/hour
        self.api_key = os.getenv('PANDASCORE_API_KEY')
        self.base_url = "https://api.pandascore.co"
        
    def get_default_headers(self) -> Dict[str, str]:
        headers = {
            'User-Agent': 'VALORANT-Betting-Platform/1.0',
            'Accept': 'application/json'
        }
        
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
            
        return headers
        
    async def ingest_data(self, days_back: int = 7, **kwargs) -> Dict[str, Any]:
        """Ingest recent VALORANT match data from PandaScore"""
        start_time = self.get_current_timestamp()
        
        try:
            if not self.api_key:
                logger.warning("PandaScore API key not configured, skipping ingestion")
                return {'matches_count': 0, 'errors': 1, 'source': 'pandascore'}
                
            # Get matches and tournaments
            matches_data = await self.fetch_valorant_matches(days_back)
            tournaments_data = await self.fetch_valorant_tournaments()
            
            if not matches_data and not tournaments_data:
                logger.warning("No data retrieved from PandaScore API")
                return {'matches_count': 0, 'errors': 0, 'source': 'pandascore'}
                
            # Process matches
            processed_matches = self.process_matches(matches_data)
            saved_matches = self.save_matches_to_bronze(processed_matches)
            
            # Process tournaments
            processed_tournaments = self.process_tournaments(tournaments_data)
            saved_tournaments = self.save_tournaments_to_bronze(processed_tournaments)
            
            self.log_ingestion_stats("pandascore_matches", saved_matches, start_time)
            
            return {
                'matches_count': saved_matches,
                'tournaments_count': saved_tournaments,
                'source': 'pandascore',
                'errors': 0
            }
            
        except Exception as e:
            logger.error(f"Error in PandaScore ingestion: {e}")
            self.log_ingestion_stats("pandascore_matches", 0, start_time, errors=1)
            raise
            
    async def fetch_valorant_matches(self, days_back: int) -> Optional[List[Dict]]:
        """Fetch VALORANT matches from PandaScore API"""
        try:
            # Get VALORANT videogame ID (game slug: valorant)
            videogame_url = f"{self.base_url}/videogames"
            videogames = await self.make_request(videogame_url)
            
            valorant_id = None
            if videogames:
                for game in videogames:
                    if game.get('slug') == 'valorant':
                        valorant_id = game.get('id')
                        break
                        
            if not valorant_id:
                logger.error("Could not find VALORANT videogame ID in PandaScore")
                return None
                
            # Get matches for VALORANT
            since_date = (datetime.now() - timedelta(days=days_back)).isoformat()
            matches_url = f"{self.base_url}/valorant/matches"
            
            params = {
                'filter[begin_at]': f'>{since_date}',
                'sort': '-begin_at',
                'page[size]': 100
            }
            
            all_matches = []
            page = 1
            
            while page <= 5:  # Limit to 5 pages to avoid excessive API calls
                params['page[number]'] = page
                matches_page = await self.make_request(matches_url, params)
                
                if not matches_page or not isinstance(matches_page, list):
                    break
                    
                all_matches.extend(matches_page)
                
                # If we got less than page size, we're at the end
                if len(matches_page) < params['page[size]']:
                    break
                    
                page += 1
                await asyncio.sleep(0.1)  # Small delay between pages
                
            return all_matches
            
        except Exception as e:
            logger.error(f"Error fetching VALORANT matches from PandaScore: {e}")
            return None
            
    async def fetch_valorant_tournaments(self) -> Optional[List[Dict]]:
        """Fetch VALORANT tournaments from PandaScore API"""
        try:
            tournaments_url = f"{self.base_url}/valorant/tournaments"
            
            params = {
                'filter[tier]': 'a,s',  # Only major tournaments
                'sort': '-begin_at',
                'page[size]': 50
            }
            
            tournaments = await self.make_request(tournaments_url, params)
            return tournaments if tournaments else []
            
        except Exception as e:
            logger.error(f"Error fetching tournaments from PandaScore: {e}")
            return []
            
    def process_matches(self, matches_data: List[Dict]) -> List[Dict]:
        """Process PandaScore matches data into standardized format"""
        processed = []
        
        for match in matches_data:
            try:
                # Extract teams
                opponents = match.get('opponents', [])
                team_a = opponents[0] if len(opponents) > 0 else {}
                team_b = opponents[1] if len(opponents) > 1 else {}
                
                # Determine winner
                winner_id = None
                if match.get('status') == 'finished' and match.get('winner'):
                    winner_id = match['winner'].get('id')
                    
                processed_match = {
                    'match_id': f"pandascore_{match.get('id')}",
                    'tournament_id': match.get('tournament', {}).get('id'),
                    'tournament_name': match.get('tournament', {}).get('name'),
                    'team_a_id': f"pandascore_{team_a.get('opponent', {}).get('id')}" if team_a.get('opponent') else None,
                    'team_a_name': team_a.get('opponent', {}).get('name'),
                    'team_b_id': f"pandascore_{team_b.get('opponent', {}).get('id')}" if team_b.get('opponent') else None,
                    'team_b_name': team_b.get('opponent', {}).get('name'),
                    'start_time': match.get('begin_at'),
                    'end_time': match.get('end_at'),
                    'status': match.get('status'),
                    'best_of': match.get('number_of_games', 3),
                    'winner_id': f"pandascore_{winner_id}" if winner_id else None,
                    'patch_version': None,  # PandaScore doesn't provide patch info
                    'is_lan': match.get('tournament', {}).get('tier') in ['s'],  # S-tier usually LAN
                    'venue': match.get('tournament', {}).get('venue'),
                    'ingested_at': self.get_current_timestamp(),
                    'updated_at': self.get_current_timestamp()
                }
                
                processed.append(processed_match)
                
            except Exception as e:
                logger.warning(f"Error processing PandaScore match {match.get('id', 'unknown')}: {e}")
                continue
                
        return processed
        
    def process_tournaments(self, tournaments_data: List[Dict]) -> List[Dict]:
        """Process tournament data for additional context"""
        processed = []
        
        for tournament in tournaments_data:
            try:
                processed_tournament = {
                    'tournament_id': f"pandascore_{tournament.get('id')}",
                    'tournament_name': tournament.get('name'),
                    'tier': tournament.get('tier'),
                    'begin_at': tournament.get('begin_at'),
                    'end_at': tournament.get('end_at'),
                    'league_name': tournament.get('league', {}).get('name'),
                    'serie_name': tournament.get('serie', {}).get('name'),
                    'location': tournament.get('venue'),
                    'prize_pool': tournament.get('prizepool'),
                    'ingested_at': self.get_current_timestamp()
                }
                
                processed.append(processed_tournament)
                
            except Exception as e:
                logger.warning(f"Error processing tournament {tournament.get('id', 'unknown')}: {e}")
                continue
                
        return processed
        
    def save_matches_to_bronze(self, matches: List[Dict]) -> int:
        """Save processed matches to bronze layer"""
        if not matches:
            return 0
            
        return self.save_to_bronze('bronze_matches', matches)
        
    def save_tournaments_to_bronze(self, tournaments: List[Dict]) -> int:
        """Save processed tournaments to bronze layer"""
        if not tournaments:
            return 0
            
        # Create tournaments table if it doesn't exist
        from ..database import db
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS bronze_tournaments (
                tournament_id VARCHAR PRIMARY KEY,
                tournament_name VARCHAR,
                tier VARCHAR,
                begin_at TIMESTAMP,
                end_at TIMESTAMP,
                league_name VARCHAR,
                serie_name VARCHAR,
                location VARCHAR,
                prize_pool VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        try:
            db.execute_query(create_table_query)
        except Exception as e:
            logger.warning(f"Could not create tournaments table: {e}")
            
        return self.save_to_bronze('bronze_tournaments', tournaments)
        
    async def ingest_recent_matches(self, days_back: int = 7) -> Dict[str, Any]:
        """Public method to ingest recent matches from PandaScore"""
        async with self:
            return await self.ingest_data(days_back=days_back)