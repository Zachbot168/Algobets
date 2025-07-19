import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging
from bs4 import BeautifulSoup
import json

from ..base_ingester import BaseIngester

logger = logging.getLogger(__name__)

class RosterScraper(BaseIngester):
    """Scrapes team rosters and player information from VLR.gg and Liquipedia"""
    
    def __init__(self):
        super().__init__(rate_limit_per_minute=30)  # Conservative rate limiting for scraping
        
        # Source URLs
        self.vlr_base_url = "https://www.vlr.gg"
        self.liquipedia_base_url = "https://liquipedia.net/valorant"
        
        # VLR API unofficial endpoints
        self.vlr_api_url = "https://vlrggapi.vercel.app/v1"
        
    def get_default_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
    async def ingest_data(self, **kwargs) -> Dict[str, Any]:
        """Scrape team and player data from multiple sources"""
        start_time = self.get_current_timestamp()
        total_teams = 0
        total_players = 0
        errors = 0
        
        try:
            # Scrape from VLR.gg API
            vlr_teams, vlr_players = await self.scrape_vlr_data()
            total_teams += len(vlr_teams)
            total_players += len(vlr_players)
            
            # Save VLR data
            if vlr_teams:
                self.save_to_bronze('bronze_teams', vlr_teams)
            if vlr_players:
                self.save_to_bronze('bronze_players', vlr_players)
                
            # Scrape from Liquipedia (fallback/additional data)
            try:
                liqui_teams, liqui_players = await self.scrape_liquipedia_data()
                total_teams += len(liqui_teams)
                total_players += len(liqui_players)
                
                if liqui_teams:
                    self.save_to_bronze('bronze_teams', liqui_teams)
                if liqui_players:
                    self.save_to_bronze('bronze_players', liqui_players)
                    
            except Exception as e:
                logger.warning(f"Liquipedia scraping failed: {e}")
                errors += 1
                
            self.log_ingestion_stats("roster_scraper", total_teams + total_players, start_time, errors)
            
            return {
                'teams_count': total_teams,
                'players_count': total_players,
                'changes_count': 0,  # TODO: Implement change detection
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Error in roster scraping: {e}")
            self.log_ingestion_stats("roster_scraper", 0, start_time, errors + 1)
            raise
            
    async def scrape_vlr_data(self) -> tuple[List[Dict], List[Dict]]:
        """Scrape team and player data from VLR.gg API"""
        teams = []
        players = []
        
        try:
            # Get teams from VLR API
            teams_url = f"{self.vlr_api_url}/teams"
            teams_response = await self.make_request(teams_url)
            
            if teams_response and 'data' in teams_response:
                for team_data in teams_response['data']:
                    try:
                        # Process team data
                        team = self.process_vlr_team(team_data)
                        teams.append(team)
                        
                        # Get detailed team info including roster
                        team_id = team_data.get('id')
                        if team_id:
                            team_players = await self.fetch_vlr_team_roster(team_id)
                            players.extend(team_players)
                            
                        # Small delay between requests
                        await asyncio.sleep(0.5)
                        
                    except Exception as e:
                        logger.warning(f"Error processing VLR team {team_data.get('id')}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error scraping VLR teams: {e}")
            
        return teams, players
        
    def process_vlr_team(self, team_data: Dict) -> Dict:
        """Process VLR team data into standardized format"""
        return {
            'team_id': f"vlr_{team_data.get('id')}",
            'team_name': team_data.get('name'),
            'region': team_data.get('region'),
            'country': team_data.get('country'),
            'logo_url': team_data.get('logo'),
            'is_active': True,
            'ingested_at': self.get_current_timestamp(),
            'updated_at': self.get_current_timestamp()
        }
        
    async def fetch_vlr_team_roster(self, team_id: str) -> List[Dict]:
        """Fetch detailed roster for a specific team"""
        try:
            url = f"{self.vlr_api_url}/teams/{team_id}"
            response = await self.make_request(url)
            
            if not response or 'data' not in response:
                return []
                
            team_data = response['data']
            roster = team_data.get('roster', [])
            
            players = []
            for player_data in roster:
                player = {
                    'player_id': f"vlr_{player_data.get('id')}",
                    'player_name': player_data.get('username'),
                    'real_name': player_data.get('real_name'),
                    'team_id': f"vlr_{team_id}",
                    'role': player_data.get('role'),
                    'country': player_data.get('country'),
                    'join_date': None,  # VLR API might not have this
                    'leave_date': None,
                    'is_active': player_data.get('status') == 'active',
                    'ingested_at': self.get_current_timestamp(),
                    'updated_at': self.get_current_timestamp()
                }
                players.append(player)
                
            return players
            
        except Exception as e:
            logger.warning(f"Error fetching VLR team roster {team_id}: {e}")
            return []
            
    async def scrape_liquipedia_data(self) -> tuple[List[Dict], List[Dict]]:
        """Scrape additional data from Liquipedia"""
        teams = []
        players = []
        
        try:
            # Get main teams page
            teams_url = f"{self.liquipedia_base_url}/Portal:Teams"
            response = await self.make_request(teams_url)
            
            if not response:
                return teams, players
                
            # Parse HTML content
            soup = BeautifulSoup(response, 'html.parser')
            
            # Find team links (this is a simplified approach)
            team_links = soup.find_all('a', href=lambda href: href and '/team/' in href)
            
            for link in team_links[:20]:  # Limit to first 20 teams to avoid hitting rate limits
                try:
                    team_url = f"{self.liquipedia_base_url}{link.get('href')}"
                    team_data = await self.scrape_liquipedia_team_page(team_url)
                    
                    if team_data:
                        teams.append(team_data['team'])
                        players.extend(team_data['players'])
                        
                    # Longer delay for web scraping
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.warning(f"Error scraping Liquipedia team: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error scraping Liquipedia: {e}")
            
        return teams, players
        
    async def scrape_liquipedia_team_page(self, team_url: str) -> Optional[Dict]:
        """Scrape individual team page from Liquipedia"""
        try:
            response = await self.make_request(team_url)
            if not response:
                return None
                
            soup = BeautifulSoup(response, 'html.parser')
            
            # Extract team name
            title = soup.find('h1', class_='firstHeading')
            team_name = title.get_text().strip() if title else "Unknown"
            
            # Extract team info from infobox
            infobox = soup.find('table', class_='infobox')
            region = "Unknown"
            country = "Unknown"
            
            if infobox:
                # Look for region/country info
                region_row = infobox.find('th', string='Region')
                if region_row and region_row.find_next_sibling('td'):
                    region = region_row.find_next_sibling('td').get_text().strip()
                    
                country_row = infobox.find('th', string='Country')
                if country_row and country_row.find_next_sibling('td'):
                    country = country_row.find_next_sibling('td').get_text().strip()
                    
            # Extract current roster
            roster_section = soup.find('span', id='Current_Roster')
            players = []
            
            if roster_section:
                roster_table = roster_section.find_next('table')
                if roster_table:
                    rows = roster_table.find_all('tr')[1:]  # Skip header
                    
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            player_name = cells[0].get_text().strip()
                            role = cells[1].get_text().strip() if len(cells) > 1 else "Unknown"
                            
                            player = {
                                'player_id': f"liqui_{self.generate_id(team_name, player_name)}",
                                'player_name': player_name,
                                'real_name': None,
                                'team_id': f"liqui_{self.generate_id(team_name)}",
                                'role': role,
                                'country': country,
                                'join_date': None,
                                'leave_date': None,
                                'is_active': True,
                                'ingested_at': self.get_current_timestamp(),
                                'updated_at': self.get_current_timestamp()
                            }
                            players.append(player)
                            
            team = {
                'team_id': f"liqui_{self.generate_id(team_name)}",
                'team_name': team_name,
                'region': region,
                'country': country,
                'logo_url': None,
                'is_active': True,
                'ingested_at': self.get_current_timestamp(),
                'updated_at': self.get_current_timestamp()
            }
            
            return {
                'team': team,
                'players': players
            }
            
        except Exception as e:
            logger.warning(f"Error scraping team page {team_url}: {e}")
            return None
            
    async def detect_roster_changes(self) -> List[Dict]:
        """Detect recent roster changes by comparing with stored data"""
        # TODO: Implement change detection logic
        # This would compare current scraped data with previously stored data
        # and identify transfers, signings, releases, etc.
        return []
        
    async def scrape_team_rosters(self) -> Dict[str, Any]:
        """Public method to scrape team rosters"""
        async with self:
            return await self.ingest_data()