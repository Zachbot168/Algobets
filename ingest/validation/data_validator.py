import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import pandas as pd

from ..database import db

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates data quality and completeness for ingested data"""
    
    def __init__(self):
        self.validation_rules = {
            'matches': {
                'required_fields': ['match_id', 'team_a_name', 'team_b_name', 'start_time'],
                'min_records_per_day': 5,
                'max_age_hours': 48
            },
            'odds': {
                'required_fields': ['odds_id', 'match_id', 'bookmaker', 'odds_decimal'],
                'min_records_per_day': 10,
                'max_age_hours': 6,
                'min_odds': 1.01,
                'max_odds': 50.0
            },
            'rosters': {
                'required_fields': ['team_id', 'team_name'],
                'min_teams': 50,
                'max_age_days': 7
            }
        }
        
    def validate_matches_data(self) -> bool:
        """Validate matches data quality"""
        try:
            logger.info("Validating matches data...")
            
            # Check for required fields
            if not self._check_required_fields('bronze_matches', self.validation_rules['matches']['required_fields']):
                return False
                
            # Check data freshness
            if not self._check_data_freshness('bronze_matches', 'ingested_at', 
                                            self.validation_rules['matches']['max_age_hours']):
                return False
                
            # Check minimum record count
            if not self._check_minimum_records('bronze_matches', 'start_time', 
                                             self.validation_rules['matches']['min_records_per_day']):
                return False
                
            # Check for duplicate matches
            if not self._check_duplicate_matches():
                return False
                
            # Check team name consistency
            if not self._check_team_name_consistency():
                return False
                
            logger.info("Matches data validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Error validating matches data: {e}")
            return False
            
    def validate_odds_data(self) -> bool:
        """Validate odds data quality"""
        try:
            logger.info("Validating odds data...")
            
            # Check for required fields
            if not self._check_required_fields('bronze_odds', self.validation_rules['odds']['required_fields']):
                return False
                
            # Check data freshness
            if not self._check_data_freshness('bronze_odds', 'ingested_at', 
                                            self.validation_rules['odds']['max_age_hours']):
                return False
                
            # Check odds values are reasonable
            if not self._check_odds_values():
                return False
                
            # Check for bookmaker coverage
            if not self._check_bookmaker_coverage():
                return False
                
            # Check for market type coverage
            if not self._check_market_coverage():
                return False
                
            logger.info("Odds data validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Error validating odds data: {e}")
            return False
            
    def validate_roster_data(self) -> bool:
        """Validate roster data quality"""
        try:
            logger.info("Validating roster data...")
            
            # Check for required fields
            if not self._check_required_fields('bronze_teams', self.validation_rules['rosters']['required_fields']):
                return False
                
            # Check minimum team count
            team_count = self._get_record_count('bronze_teams')
            if team_count < self.validation_rules['rosters']['min_teams']:
                logger.error(f"Insufficient teams: {team_count} < {self.validation_rules['rosters']['min_teams']}")
                return False
                
            # Check for active teams with players
            if not self._check_teams_have_players():
                return False
                
            logger.info("Roster data validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Error validating roster data: {e}")
            return False
            
    def _check_required_fields(self, table: str, required_fields: List[str]) -> bool:
        """Check that required fields are not null"""
        try:
            for field in required_fields:
                query = f"SELECT COUNT(*) FROM {table} WHERE {field} IS NULL"
                result = db.execute_query(query)
                null_count = result[0][0] if result else 0
                
                if null_count > 0:
                    logger.error(f"Found {null_count} null values in {table}.{field}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error checking required fields for {table}: {e}")
            return False
            
    def _check_data_freshness(self, table: str, timestamp_field: str, max_age_hours: int) -> bool:
        """Check that data is fresh enough"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            query = f"SELECT COUNT(*) FROM {table} WHERE {timestamp_field} >= ?"
            result = db.execute_query(query, [cutoff_time.isoformat()])
            recent_count = result[0][0] if result else 0
            
            if recent_count == 0:
                logger.error(f"No recent data in {table} (within {max_age_hours} hours)")
                return False
                
            logger.info(f"Found {recent_count} recent records in {table}")
            return True
            
        except Exception as e:
            logger.error(f"Error checking data freshness for {table}: {e}")
            return False
            
    def _check_minimum_records(self, table: str, date_field: str, min_per_day: int) -> bool:
        """Check minimum record count per day"""
        try:
            yesterday = datetime.now() - timedelta(days=1)
            query = f"SELECT COUNT(*) FROM {table} WHERE DATE({date_field}) >= ?"
            result = db.execute_query(query, [yesterday.date().isoformat()])
            daily_count = result[0][0] if result else 0
            
            if daily_count < min_per_day:
                logger.error(f"Insufficient daily records in {table}: {daily_count} < {min_per_day}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking minimum records for {table}: {e}")
            return False
            
    def _get_record_count(self, table: str) -> int:
        """Get total record count for a table"""
        try:
            query = f"SELECT COUNT(*) FROM {table}"
            result = db.execute_query(query)
            return result[0][0] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting record count for {table}: {e}")
            return 0
            
    def _check_duplicate_matches(self) -> bool:
        """Check for duplicate matches"""
        try:
            query = """
                SELECT team_a_name, team_b_name, start_time, COUNT(*) as count
                FROM bronze_matches 
                GROUP BY team_a_name, team_b_name, start_time
                HAVING COUNT(*) > 1
            """
            result = db.execute_query(query)
            
            if result:
                logger.error(f"Found {len(result)} duplicate matches")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking duplicate matches: {e}")
            return False
            
    def _check_team_name_consistency(self) -> bool:
        """Check for team name variations that might be the same team"""
        try:
            # Get all unique team names
            query = "SELECT DISTINCT team_a_name FROM bronze_matches UNION SELECT DISTINCT team_b_name FROM bronze_matches"
            result = db.execute_query(query)
            
            team_names = [row[0] for row in result if row[0]]
            
            # Look for similar names (basic check)
            similar_teams = []
            for i, name1 in enumerate(team_names):
                for name2 in team_names[i+1:]:
                    if self._names_similar(name1, name2):
                        similar_teams.append((name1, name2))
                        
            if similar_teams:
                logger.warning(f"Found {len(similar_teams)} potentially similar team names")
                # This is a warning, not a failure
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking team name consistency: {e}")
            return False
            
    def _names_similar(self, name1: str, name2: str) -> bool:
        """Check if two team names are similar"""
        # Simple similarity check - could be enhanced
        name1_clean = name1.lower().replace(' ', '').replace('-', '')
        name2_clean = name2.lower().replace(' ', '').replace('-', '')
        
        # Check if one is a substring of the other
        return name1_clean in name2_clean or name2_clean in name1_clean
        
    def _check_odds_values(self) -> bool:
        """Check that odds values are reasonable"""
        try:
            min_odds = self.validation_rules['odds']['min_odds']
            max_odds = self.validation_rules['odds']['max_odds']
            
            query = f"SELECT COUNT(*) FROM bronze_odds WHERE odds_decimal < ? OR odds_decimal > ?"
            result = db.execute_query(query, [min_odds, max_odds])
            invalid_count = result[0][0] if result else 0
            
            if invalid_count > 0:
                logger.error(f"Found {invalid_count} odds with invalid values")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking odds values: {e}")
            return False
            
    def _check_bookmaker_coverage(self) -> bool:
        """Check that we have odds from multiple bookmakers"""
        try:
            query = "SELECT COUNT(DISTINCT bookmaker) FROM bronze_odds WHERE is_latest = true"
            result = db.execute_query(query)
            bookmaker_count = result[0][0] if result else 0
            
            if bookmaker_count < 2:
                logger.error(f"Insufficient bookmaker coverage: {bookmaker_count}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking bookmaker coverage: {e}")
            return False
            
    def _check_market_coverage(self) -> bool:
        """Check that we have odds for different market types"""
        try:
            query = "SELECT COUNT(DISTINCT market_type) FROM bronze_odds WHERE is_latest = true"
            result = db.execute_query(query)
            market_count = result[0][0] if result else 0
            
            if market_count < 1:
                logger.error(f"No market types found in odds data")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking market coverage: {e}")
            return False
            
    def _check_teams_have_players(self) -> bool:
        """Check that active teams have players"""
        try:
            query = """
                SELECT COUNT(*) FROM bronze_teams t
                WHERE t.is_active = true
                AND NOT EXISTS (
                    SELECT 1 FROM bronze_players p 
                    WHERE p.team_id = t.team_id AND p.is_active = true
                )
            """
            result = db.execute_query(query)
            teams_without_players = result[0][0] if result else 0
            
            if teams_without_players > 10:  # Allow some teams without full rosters
                logger.error(f"Too many teams without players: {teams_without_players}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking teams have players: {e}")
            return False
            
    def generate_data_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive data quality report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'matches': self._generate_table_stats('bronze_matches'),
            'odds': self._generate_table_stats('bronze_odds'),
            'teams': self._generate_table_stats('bronze_teams'),
            'players': self._generate_table_stats('bronze_players'),
            'validation_results': {
                'matches_valid': self.validate_matches_data(),
                'odds_valid': self.validate_odds_data(),
                'rosters_valid': self.validate_roster_data()
            }
        }
        
        return report
        
    def _generate_table_stats(self, table: str) -> Dict[str, Any]:
        """Generate statistics for a table"""
        try:
            # Record count
            count_query = f"SELECT COUNT(*) FROM {table}"
            count_result = db.execute_query(count_query)
            record_count = count_result[0][0] if count_result else 0
            
            # Latest record
            if table in ['bronze_matches', 'bronze_odds', 'bronze_teams', 'bronze_players']:
                latest_query = f"SELECT MAX(ingested_at) FROM {table}"
                latest_result = db.execute_query(latest_query)
                latest_record = latest_result[0][0] if latest_result and latest_result[0][0] else None
            else:
                latest_record = None
                
            return {
                'record_count': record_count,
                'latest_record': latest_record
            }
            
        except Exception as e:
            logger.error(f"Error generating stats for {table}: {e}")
            return {'record_count': 0, 'latest_record': None}