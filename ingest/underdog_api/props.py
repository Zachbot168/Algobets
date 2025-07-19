"""
Underdog Fantasy API integration for VALORANT player props
"""

import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from ..base_ingester import BaseIngester

logger = logging.getLogger(__name__)

class UnderdogPropsIngester(BaseIngester):
    """Ingests VALORANT player prop odds from Underdog Fantasy API"""
    
    def __init__(self):
        super().__init__(rate_limit_per_minute=100)  # Conservative rate limiting
        self.api_key = os.getenv('UNDERDOG_API_KEY')
        self.base_url = "https://api.underdogfantasy.com/beta"
        
    def get_default_headers(self) -> Dict[str, str]:
        headers = {
            'User-Agent': 'VALORANT-Betting-Platform/1.0',
            'Accept': 'application/json'
        }
        
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
            
        return headers
        
    async def ingest_data(self, **kwargs) -> Dict[str, Any]:
        """Ingest VALORANT player props from Underdog Fantasy"""
        start_time = self.get_current_timestamp()
        
        try:
            if not self.api_key:
                logger.warning("Underdog Fantasy API key not configured, skipping ingestion")
                return {'props_count': 0, 'errors': 1, 'source': 'underdog'}
                
            # Get VALORANT sport configuration
            sport_config = await self.get_valorant_sport_config()
            if not sport_config:
                logger.error("Could not find VALORANT configuration in Underdog Fantasy")
                return {'props_count': 0, 'errors': 1, 'source': 'underdog'}
                
            # Fetch player props
            props_data = await self.fetch_valorant_props(sport_config)
            
            if not props_data:
                logger.warning("No player props retrieved from Underdog Fantasy API")
                return {'props_count': 0, 'errors': 0, 'source': 'underdog'}
                
            # Process and save props data
            processed_props = self.process_props(props_data)
            saved_props = self.save_props_to_bronze(processed_props)
            
            self.log_ingestion_stats("underdog_props", saved_props, start_time)
            
            return {
                'props_count': saved_props,
                'source': 'underdog',
                'errors': 0
            }
            
        except Exception as e:
            logger.error(f"Error in Underdog Fantasy ingestion: {e}")
            self.log_ingestion_stats("underdog_props", 0, start_time, errors=1)
            raise
            
    async def get_valorant_sport_config(self) -> Optional[Dict]:
        """Get VALORANT sport configuration from Underdog API"""
        try:
            # First, get available games/sports
            games_url = f"{self.base_url}/games"
            games_response = await self.make_request(games_url)
            
            if not games_response:
                return None
                
            # Look for VALORANT in the games list
            valorant_game = None
            for game in games_response.get('games', []):
                if 'valorant' in game.get('name', '').lower() or 'valorant' in game.get('slug', '').lower():
                    valorant_game = game
                    break
                    
            return valorant_game
            
        except Exception as e:
            logger.error(f"Error fetching games from Underdog: {e}")
            return None
            
    async def fetch_valorant_props(self, sport_config: Dict) -> Optional[List[Dict]]:
        """Fetch VALORANT player props from Underdog Fantasy API"""
        try:
            sport_id = sport_config.get('id')
            if not sport_id:
                return None
                
            # Get upcoming slates/contests for VALORANT
            slates_url = f"{self.base_url}/slates"
            params = {
                'game_id': sport_id,
                'status': 'upcoming'
            }
            
            slates_response = await self.make_request(slates_url, params)
            
            if not slates_response or 'slates' not in slates_response:
                return []
                
            all_props = []
            
            # For each slate, get the player props
            for slate in slates_response['slates'][:10]:  # Limit to 10 slates
                slate_id = slate.get('id')
                if not slate_id:
                    continue
                    
                # Get appearances (player props) for this slate
                appearances_url = f"{self.base_url}/slates/{slate_id}/appearances"
                appearances = await self.make_request(appearances_url)
                
                if appearances and 'appearances' in appearances:
                    for appearance in appearances['appearances']:
                        appearance['slate_info'] = slate  # Add slate context
                        all_props.append(appearance)
                        
                await asyncio.sleep(0.1)  # Small delay between requests
                
            return all_props
            
        except Exception as e:
            logger.error(f"Error fetching props from Underdog: {e}")
            return None
            
    def process_props(self, props_data: List[Dict]) -> List[Dict]:
        """Process Underdog Fantasy props data into standardized format"""
        processed = []
        
        for prop in props_data:
            try:
                slate_info = prop.get('slate_info', {})
                player_info = prop.get('player', {})
                
                # Extract match information from slate
                match_info = slate_info.get('match', {})
                
                processed_prop = {
                    'prop_id': f"underdog_{prop.get('id')}",
                    'match_id': f"underdog_{match_info.get('id')}" if match_info.get('id') else None,
                    'slate_id': f"underdog_{slate_info.get('id')}",
                    'player_id': f"underdog_{player_info.get('id')}",
                    'player_name': player_info.get('name'),
                    'team_name': player_info.get('team', {}).get('name'),
                    'stat_type': prop.get('stat_type'),
                    'line': prop.get('line'),
                    'over_odds': prop.get('over_odds'),
                    'under_odds': prop.get('under_odds'),
                    'over_payout': prop.get('over_payout'),
                    'under_payout': prop.get('under_payout'),
                    'start_time': slate_info.get('start_time'),
                    'status': slate_info.get('status'),
                    'bookmaker': 'underdog_fantasy',
                    'market_type': self.map_stat_type_to_market(prop.get('stat_type')),
                    'ingested_at': self.get_current_timestamp(),
                    'is_latest': True
                }
                
                processed.append(processed_prop)
                
            except Exception as e:
                logger.warning(f"Error processing Underdog prop {prop.get('id', 'unknown')}: {e}")
                continue
                
        return processed
        
    def map_stat_type_to_market(self, stat_type: str) -> str:
        """Map Underdog stat types to standardized market types"""
        mapping = {
            'kills': 'player_kills',
            'deaths': 'player_deaths',
            'assists': 'player_assists',
            'headshots': 'player_headshots',
            'first_bloods': 'player_first_bloods',
            'ace': 'player_aces',
            'clutches': 'player_clutches',
            'damage': 'player_damage',
            'rating': 'player_rating'
        }
        
        return mapping.get(stat_type.lower() if stat_type else '', 'player_other')
        
    def save_props_to_bronze(self, props: List[Dict]) -> int:
        """Save processed props to bronze layer"""
        if not props:
            return 0
            
        # Create player props table if it doesn't exist
        from ..database import db
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS bronze_player_props (
                prop_id VARCHAR PRIMARY KEY,
                match_id VARCHAR,
                slate_id VARCHAR,
                player_id VARCHAR,
                player_name VARCHAR,
                team_name VARCHAR,
                stat_type VARCHAR,
                line DECIMAL(10,2),
                over_odds DECIMAL(10,3),
                under_odds DECIMAL(10,3),
                over_payout DECIMAL(10,3),
                under_payout DECIMAL(10,3),
                start_time TIMESTAMP,
                status VARCHAR,
                bookmaker VARCHAR,
                market_type VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_latest BOOLEAN DEFAULT true
            )
        """
        
        try:
            db.execute_query(create_table_query)
        except Exception as e:
            logger.warning(f"Could not create player props table: {e}")
            
        # Mark old props as not latest
        update_query = """
            UPDATE bronze_player_props 
            SET is_latest = false 
            WHERE bookmaker = 'underdog_fantasy'
        """
        
        try:
            db.execute_query(update_query)
        except Exception as e:
            logger.warning(f"Could not update old props: {e}")
            
        return self.save_to_bronze('bronze_player_props', props)
        
    async def collect_player_props(self) -> Dict[str, Any]:
        """Public method to collect player props from Underdog Fantasy"""
        async with self:
            return await self.ingest_data()