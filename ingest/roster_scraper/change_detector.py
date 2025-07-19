"""
Roster change detection via Liquipedia diff and Twitter stream
"""

import os
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set
import logging
import tweepy
import hashlib

from ..base_ingester import BaseIngester

logger = logging.getLogger(__name__)

class RosterChangeDetector(BaseIngester):
    """Detects roster changes via Liquipedia diff monitoring and Twitter stream"""
    
    def __init__(self):
        super().__init__(rate_limit_per_minute=30)
        
        # Twitter API credentials
        self.twitter_bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        self.twitter_api_key = os.getenv('TWITTER_API_KEY')
        self.twitter_api_secret = os.getenv('TWITTER_API_SECRET')
        self.twitter_access_token = os.getenv('TWITTER_ACCESS_TOKEN')
        self.twitter_access_secret = os.getenv('TWITTER_ACCESS_SECRET')
        
        # Initialize Twitter client
        self.twitter_client = None
        if self.twitter_bearer_token:
            self.twitter_client = tweepy.Client(bearer_token=self.twitter_bearer_token)
        
    def get_default_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': 'VALORANT-Betting-Platform/1.0 (RosterChangeDetector)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
    async def ingest_data(self, **kwargs) -> Dict[str, Any]:
        """Detect roster changes from multiple sources"""
        start_time = self.get_current_timestamp()
        
        try:
            # Get current roster state
            current_rosters = await self.get_current_roster_state()
            
            # Compare with stored roster hashes
            liquipedia_changes = await self.detect_liquipedia_changes(current_rosters)
            
            # Scan Twitter for roster announcements
            twitter_changes = await self.detect_twitter_changes()
            
            # Combine and deduplicate changes
            all_changes = self.merge_change_sources(liquipedia_changes, twitter_changes)
            
            # Save changes to database
            saved_changes = self.save_changes_to_bronze(all_changes)
            
            # Update roster change flags on matches
            await self.flag_matches_with_roster_changes(all_changes)
            
            self.log_ingestion_stats("roster_changes", saved_changes, start_time)
            
            return {
                'changes_count': saved_changes,
                'liquipedia_changes': len(liquipedia_changes),
                'twitter_changes': len(twitter_changes),
                'source': 'roster_detection',
                'errors': 0
            }
            
        except Exception as e:
            logger.error(f"Error in roster change detection: {e}")
            self.log_ingestion_stats("roster_changes", 0, start_time, errors=1)
            raise
            
    async def get_current_roster_state(self) -> Dict[str, Dict]:
        """Get current roster state from database"""
        try:
            from ..database import db
            
            query = """
                SELECT team_id, team_name, 
                       GROUP_CONCAT(player_name || ':' || COALESCE(role, 'unknown'), '|') as roster_string
                FROM bronze_players 
                WHERE is_active = true 
                GROUP BY team_id, team_name
            """
            
            results = db.execute_query(query)
            
            rosters = {}
            for result in results:
                team_id, team_name, roster_string = result
                rosters[team_id] = {
                    'team_name': team_name,
                    'roster_string': roster_string,
                    'roster_hash': hashlib.md5(roster_string.encode()).hexdigest()
                }
                
            return rosters
            
        except Exception as e:
            logger.error(f"Error getting current roster state: {e}")
            return {}
            
    async def detect_liquipedia_changes(self, current_rosters: Dict[str, Dict]) -> List[Dict]:
        """Detect changes by comparing with stored roster hashes"""
        changes = []
        
        try:
            from ..database import db
            
            # Get stored roster hashes
            query = """
                SELECT team_id, roster_hash, last_checked 
                FROM bronze_roster_hashes
            """
            
            try:
                stored_hashes = db.execute_query(query)
            except:
                # Table doesn't exist yet, create it
                self.create_roster_hash_table()
                stored_hashes = []
                
            stored_hash_dict = {row[0]: row[1] for row in stored_hashes}
            
            # Compare current vs stored hashes
            for team_id, roster_info in current_rosters.items():
                current_hash = roster_info['roster_hash']
                stored_hash = stored_hash_dict.get(team_id)
                
                if stored_hash and stored_hash != current_hash:
                    # Roster change detected
                    change = {
                        'change_id': f"liquipedia_{team_id}_{int(datetime.now().timestamp())}",
                        'team_id': team_id,
                        'team_name': roster_info['team_name'],
                        'change_type': 'roster_update',
                        'source': 'liquipedia_diff',
                        'detected_at': self.get_current_timestamp(),
                        'description': f"Roster change detected for {roster_info['team_name']}",
                        'confidence': 0.8
                    }
                    changes.append(change)
                    
                # Update stored hash
                upsert_query = """
                    INSERT OR REPLACE INTO bronze_roster_hashes 
                    (team_id, roster_hash, last_checked) 
                    VALUES (?, ?, ?)
                """
                
                db.execute_query(upsert_query, [
                    team_id, 
                    current_hash, 
                    self.get_current_timestamp().isoformat()
                ])
                
            return changes
            
        except Exception as e:
            logger.error(f"Error detecting Liquipedia changes: {e}")
            return []
            
    async def detect_twitter_changes(self) -> List[Dict]:
        """Detect roster changes from Twitter announcements"""
        changes = []
        
        try:
            if not self.twitter_client:
                logger.warning("Twitter client not configured, skipping Twitter detection")
                return []
                
            # Search for recent roster-related tweets
            roster_keywords = [
                "VALORANT roster",
                "signs player",
                "releases player", 
                "benched",
                "trial period",
                "loan deal",
                "free agent",
                "announcement"
            ]
            
            # Get tweets from last 24 hours
            since_time = datetime.now() - timedelta(hours=24)
            
            for keyword in roster_keywords[:3]:  # Limit searches to avoid rate limits
                try:
                    tweets = self.twitter_client.search_recent_tweets(
                        query=f"{keyword} lang:en -is:retweet",
                        max_results=20,
                        start_time=since_time,
                        tweet_fields=['created_at', 'author_id', 'public_metrics']
                    )
                    
                    if tweets.data:
                        for tweet in tweets.data:
                            change = self.analyze_tweet_for_roster_change(tweet, keyword)
                            if change:
                                changes.append(change)
                                
                    await asyncio.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    logger.warning(f"Error searching Twitter for '{keyword}': {e}")
                    continue
                    
            return changes
            
        except Exception as e:
            logger.error(f"Error detecting Twitter changes: {e}")
            return []
            
    def analyze_tweet_for_roster_change(self, tweet, keyword: str) -> Optional[Dict]:
        """Analyze a tweet to extract roster change information"""
        try:
            text = tweet.text.lower()
            
            # Look for team/org mentions
            valorant_orgs = [
                'sentinels', 'fnatic', 'loud', 'nrg', 'cloud9', 'g2', 'liquid',
                'drx', 'paper rex', 'edg', 'navi', 'fut', 'giants', 'kru'
            ]
            
            mentioned_org = None
            for org in valorant_orgs:
                if org in text:
                    mentioned_org = org
                    break
                    
            if not mentioned_org:
                return None
                
            # Determine change type
            change_type = 'roster_announcement'
            confidence = 0.6
            
            if any(word in text for word in ['signs', 'welcomes', 'joins']):
                change_type = 'player_signing'
                confidence = 0.8
            elif any(word in text for word in ['releases', 'parts ways', 'leaves']):
                change_type = 'player_release'
                confidence = 0.8
            elif any(word in text for word in ['benched', 'inactive']):
                change_type = 'player_benched'
                confidence = 0.7
                
            return {
                'change_id': f"twitter_{tweet.id}",
                'team_id': f"twitter_{mentioned_org}",
                'team_name': mentioned_org.title(),
                'change_type': change_type,
                'source': 'twitter',
                'detected_at': tweet.created_at,
                'description': f"Twitter announcement: {tweet.text[:200]}...",
                'confidence': confidence,
                'tweet_id': tweet.id,
                'keyword_matched': keyword
            }
            
        except Exception as e:
            logger.warning(f"Error analyzing tweet: {e}")
            return None
            
    def merge_change_sources(self, liquipedia_changes: List[Dict], 
                           twitter_changes: List[Dict]) -> List[Dict]:
        """Merge and deduplicate changes from different sources"""
        all_changes = []
        seen_teams = set()
        
        # Prioritize Liquipedia changes (higher confidence)
        for change in liquipedia_changes:
            team_key = (change['team_id'], change['detected_at'].date())
            if team_key not in seen_teams:
                all_changes.append(change)
                seen_teams.add(team_key)
                
        # Add Twitter changes if not already covered
        for change in twitter_changes:
            team_key = (change['team_id'], change['detected_at'].date())
            if team_key not in seen_teams:
                all_changes.append(change)
                seen_teams.add(team_key)
                
        return all_changes
        
    def create_roster_hash_table(self):
        """Create table for storing roster hashes"""
        from ..database import db
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS bronze_roster_hashes (
                team_id VARCHAR PRIMARY KEY,
                roster_hash VARCHAR,
                last_checked TIMESTAMP
            )
        """
        
        db.execute_query(create_table_query)
        
    def save_changes_to_bronze(self, changes: List[Dict]) -> int:
        """Save detected changes to bronze layer"""
        if not changes:
            return 0
            
        # Create roster changes table if it doesn't exist
        from ..database import db
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS bronze_roster_changes (
                change_id VARCHAR PRIMARY KEY,
                team_id VARCHAR,
                team_name VARCHAR,
                change_type VARCHAR,
                source VARCHAR,
                detected_at TIMESTAMP,
                description TEXT,
                confidence DECIMAL(3,2),
                tweet_id VARCHAR,
                keyword_matched VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        try:
            db.execute_query(create_table_query)
        except Exception as e:
            logger.warning(f"Could not create roster changes table: {e}")
            
        return self.save_to_bronze('bronze_roster_changes', changes)
        
    async def flag_matches_with_roster_changes(self, changes: List[Dict]):
        """Flag matches that occur soon after roster changes"""
        try:
            from ..database import db
            
            for change in changes:
                change_date = change['detected_at']
                
                # Flag matches within 30 days after the change
                flag_until = change_date + timedelta(days=30)
                
                update_query = """
                    UPDATE bronze_matches 
                    SET has_recent_roster_change = true,
                        roster_change_details = ?
                    WHERE (team_a_id = ? OR team_b_id = ?) 
                      AND start_time BETWEEN ? AND ?
                      AND start_time > ?
                """
                
                change_details = json.dumps({
                    'change_type': change['change_type'],
                    'source': change['source'],
                    'confidence': change['confidence'],
                    'description': change['description']
                })
                
                params = [
                    change_details,
                    change['team_id'],
                    change['team_id'],
                    change_date.isoformat(),
                    flag_until.isoformat(),
                    change_date.isoformat()
                ]
                
                db.execute_query(update_query, params)
                
                # Add columns if they don't exist
                try:
                    db.execute_query("ALTER TABLE bronze_matches ADD COLUMN has_recent_roster_change BOOLEAN DEFAULT false")
                    db.execute_query("ALTER TABLE bronze_matches ADD COLUMN roster_change_details TEXT")
                except:
                    pass  # Columns already exist
                    
        except Exception as e:
            logger.error(f"Error flagging matches with roster changes: {e}")
            
    async def detect_recent_changes(self) -> Dict[str, Any]:
        """Public method to detect recent roster changes"""
        async with self:
            return await self.ingest_data()