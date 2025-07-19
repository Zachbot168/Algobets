import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from ..base_ingester import BaseIngester

logger = logging.getLogger(__name__)

class RiotMatchIngester(BaseIngester):
    """Ingests VALORANT match data from Riot API and unofficial sources"""
    
    def __init__(self):
        super().__init__(rate_limit_per_minute=100)  # Riot API allows 100 requests per 2 minutes
        self.api_key = os.getenv('RIOT_API_KEY')
        self.base_url = "https://americas.api.riotgames.com/val"
        
        # Backup URLs for unofficial APIs
        self.backup_urls = [
            "https://vlrggapi.vercel.app",  # Unofficial VLR API
            "https://api.henrikdev.xyz/valorant"  # Henrik's API
        ]
        
    def get_default_headers(self) -> Dict[str, str]:
        headers = {
            'User-Agent': 'VALORANT-Betting-Platform/1.0',
            'Accept': 'application/json'
        }
        
        if self.api_key:
            headers['X-Riot-Token'] = self.api_key
            
        return headers
        
    async def ingest_data(self, days_back: int = 7, **kwargs) -> Dict[str, Any]:
        """Ingest recent match data"""
        start_time = self.get_current_timestamp()
        
        try:
            # Try official Riot API first, then fallback to unofficial sources
            matches_data = await self.fetch_recent_matches(days_back)
            
            if not matches_data:
                logger.warning("No matches found from Riot API, trying backup sources")
                matches_data = await self.fetch_from_backup_sources(days_back)
                
            if not matches_data:
                logger.error("Failed to fetch match data from all sources")
                return {'matches_count': 0, 'errors': 1}
                
            # Process and save match data
            processed_matches = self.process_matches(matches_data)
            saved_count = self.save_matches_to_bronze(processed_matches)
            
            # Fetch detailed data for each match
            await self.fetch_match_details(processed_matches)
            
            self.log_ingestion_stats("riot_api_matches", saved_count, start_time)
            
            return {
                'matches_count': saved_count,
                'source': 'riot_api',
                'errors': 0
            }
            
        except Exception as e:
            logger.error(f"Error in match ingestion: {e}")
            self.log_ingestion_stats("riot_api_matches", 0, start_time, errors=1)
            raise
            
    async def fetch_recent_matches(self, days_back: int) -> Optional[List[Dict]]:
        """Fetch recent matches from official Riot API"""
        if not self.api_key:
            logger.warning("No Riot API key configured, skipping official API")
            return None
            
        try:
            # Note: Official Riot API for esports data is limited
            # This is a placeholder for when/if it becomes available
            url = f"{self.base_url}/match/v1/recent"
            params = {
                'start_time': (datetime.now() - timedelta(days=days_back)).isoformat(),
                'end_time': datetime.now().isoformat()
            }
            
            response = await self.make_request(url, params)
            return response.get('matches', []) if response else None
            
        except Exception as e:
            logger.error(f"Error fetching from official Riot API: {e}")
            return None
            
    async def fetch_from_backup_sources(self, days_back: int) -> Optional[List[Dict]]:
        """Fetch matches from unofficial API sources"""
        for backup_url in self.backup_urls:
            try:
                matches = await self.fetch_from_vlr_api(backup_url, days_back)
                if matches:
                    return matches
            except Exception as e:
                logger.warning(f"Failed to fetch from {backup_url}: {e}")
                continue
                
        return None
        
    async def fetch_from_vlr_api(self, base_url: str, days_back: int) -> Optional[List[Dict]]:
        """Fetch matches from VLR.gg unofficial API"""
        try:
            # Get recent matches
            url = f"{base_url}/v1/matches"
            params = {
                'status': 'completed',
                'limit': 100
            }
            
            response = await self.make_request(url, params)
            
            if not response or 'data' not in response:
                return None
                
            # Filter matches by date
            cutoff_date = datetime.now() - timedelta(days=days_back)
            recent_matches = []
            
            for match in response['data']:
                match_date = datetime.fromisoformat(match.get('date', '').replace('Z', '+00:00'))
                if match_date >= cutoff_date:
                    recent_matches.append(match)
                    
            return recent_matches
            
        except Exception as e:
            logger.error(f"Error fetching from VLR API: {e}")
            return None
            
    def process_matches(self, matches_data: List[Dict]) -> List[Dict]:
        """Process raw match data into standardized format"""
        processed = []
        
        for match in matches_data:
            try:
                processed_match = {
                    'match_id': self.generate_id(match.get('id', ''), match.get('date', '')),
                    'tournament_id': match.get('tournament', {}).get('id'),
                    'tournament_name': match.get('tournament', {}).get('name'),
                    'team_a_id': match.get('teams', [{}])[0].get('id') if match.get('teams') else None,
                    'team_a_name': match.get('teams', [{}])[0].get('name') if match.get('teams') else None,
                    'team_b_id': match.get('teams', [{}])[1].get('id') if len(match.get('teams', [])) > 1 else None,
                    'team_b_name': match.get('teams', [{}])[1].get('name') if len(match.get('teams', [])) > 1 else None,
                    'start_time': match.get('date'),
                    'end_time': match.get('end_date'),
                    'status': match.get('status', 'completed'),
                    'best_of': match.get('best_of', 3),
                    'winner_id': match.get('winner', {}).get('id'),
                    'patch_version': match.get('patch'),
                    'is_lan': match.get('event', {}).get('is_lan', False),
                    'venue': match.get('event', {}).get('venue'),
                    'ingested_at': self.get_current_timestamp(),
                    'updated_at': self.get_current_timestamp()
                }
                
                processed.append(processed_match)
                
            except Exception as e:
                logger.warning(f"Error processing match {match.get('id', 'unknown')}: {e}")
                continue
                
        return processed
        
    def save_matches_to_bronze(self, matches: List[Dict]) -> int:
        """Save processed matches to bronze layer"""
        if not matches:
            return 0
            
        return self.save_to_bronze('bronze_matches', matches)
        
    async def fetch_match_details(self, matches: List[Dict]) -> None:
        """Fetch detailed statistics for each match"""
        for match in matches:
            try:
                await self.fetch_maps_for_match(match['match_id'])
                await asyncio.sleep(0.1)  # Small delay between requests
            except Exception as e:
                logger.warning(f"Error fetching details for match {match['match_id']}: {e}")
                
    async def fetch_maps_for_match(self, match_id: str) -> None:
        """Fetch map-level data for a specific match"""
        try:
            # This would fetch detailed map/round data
            # Implementation depends on available API endpoints
            url = f"{self.backup_urls[0]}/v1/matches/{match_id}/maps"
            response = await self.make_request(url)
            
            if response and 'data' in response:
                maps_data = self.process_maps_data(match_id, response['data'])
                self.save_to_bronze('bronze_maps', maps_data)
                
                # Fetch player stats for each map
                for map_data in maps_data:
                    await self.fetch_player_stats_for_map(map_data['map_id'])
                    
        except Exception as e:
            logger.warning(f"Error fetching maps for match {match_id}: {e}")
            
    def process_maps_data(self, match_id: str, maps_data: List[Dict]) -> List[Dict]:
        """Process map-level data"""
        processed = []
        
        for i, map_data in enumerate(maps_data):
            processed_map = {
                'map_id': self.generate_id(match_id, i, map_data.get('map')),
                'match_id': match_id,
                'map_name': map_data.get('map'),
                'map_number': i + 1,
                'team_a_score': map_data.get('team_a_score', 0),
                'team_b_score': map_data.get('team_b_score', 0),
                'winner_id': map_data.get('winner_id'),
                'duration': map_data.get('duration'),
                'ingested_at': self.get_current_timestamp()
            }
            processed.append(processed_map)
            
        return processed
        
    async def fetch_player_stats_for_map(self, map_id: str) -> None:
        """Fetch player statistics for a specific map"""
        try:
            # Implementation for fetching detailed player stats
            # This would require map-specific API endpoints
            pass
        except Exception as e:
            logger.warning(f"Error fetching player stats for map {map_id}: {e}")
            
    async def ingest_recent_matches(self, days_back: int = 1) -> Dict[str, Any]:
        """Public method to ingest recent matches"""
        async with self:
            return await self.ingest_data(days_back=days_back)