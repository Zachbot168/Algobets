import os
import duckdb
from pathlib import Path
from typing import Optional, Dict, Any
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DuckDBConnection:
    """Manages DuckDB connections for bronze layer data storage"""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv('DUCKDB_PATH', './data/bronze.db')
        self.ensure_data_directory()
        
    def ensure_data_directory(self):
        """Create data directory if it doesn't exist"""
        data_dir = Path(self.db_path).parent
        data_dir.mkdir(parents=True, exist_ok=True)
        
    @contextmanager
    def get_connection(self):
        """Get a DuckDB connection with proper cleanup"""
        conn = None
        try:
            conn = duckdb.connect(self.db_path)
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query and return results"""
        with self.get_connection() as conn:
            if params:
                return conn.execute(query, params).fetchall()
            return conn.execute(query).fetchall()
            
    def execute_many(self, query: str, data: list) -> None:
        """Execute query with multiple parameter sets"""
        with self.get_connection() as conn:
            conn.executemany(query, data)
            
    def create_tables(self) -> None:
        """Create bronze layer tables if they don't exist"""
        with self.get_connection() as conn:
            # Matches table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_matches (
                    match_id VARCHAR PRIMARY KEY,
                    tournament_id VARCHAR,
                    tournament_name VARCHAR,
                    team_a_id VARCHAR,
                    team_a_name VARCHAR,
                    team_b_id VARCHAR,
                    team_b_name VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status VARCHAR,
                    best_of INTEGER,
                    winner_id VARCHAR,
                    patch_version VARCHAR,
                    is_lan BOOLEAN,
                    venue VARCHAR,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Maps/Games table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_maps (
                    map_id VARCHAR PRIMARY KEY,
                    match_id VARCHAR,
                    map_name VARCHAR,
                    map_number INTEGER,
                    team_a_score INTEGER,
                    team_b_score INTEGER,
                    winner_id VARCHAR,
                    duration INTEGER,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (match_id) REFERENCES bronze_matches(match_id)
                );
            """)
            
            # Rounds table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_rounds (
                    round_id VARCHAR PRIMARY KEY,
                    map_id VARCHAR,
                    round_number INTEGER,
                    attacking_team_id VARCHAR,
                    defending_team_id VARCHAR,
                    winner_id VARCHAR,
                    reason VARCHAR,
                    duration INTEGER,
                    spike_planted BOOLEAN,
                    spike_defused BOOLEAN,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (map_id) REFERENCES bronze_maps(map_id)
                );
            """)
            
            # Player stats table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_player_stats (
                    stat_id VARCHAR PRIMARY KEY,
                    map_id VARCHAR,
                    player_id VARCHAR,
                    player_name VARCHAR,
                    team_id VARCHAR,
                    agent VARCHAR,
                    kills INTEGER,
                    deaths INTEGER,
                    assists INTEGER,
                    first_bloods INTEGER,
                    first_deaths INTEGER,
                    acs DECIMAL(10,2),
                    adr DECIMAL(10,2),
                    kast DECIMAL(5,2),
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (map_id) REFERENCES bronze_maps(map_id)
                );
            """)
            
            # Odds table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_odds (
                    odds_id VARCHAR PRIMARY KEY,
                    match_id VARCHAR,
                    bookmaker VARCHAR,
                    market_type VARCHAR,
                    selection VARCHAR,
                    odds_decimal DECIMAL(10,3),
                    odds_american INTEGER,
                    timestamp TIMESTAMP,
                    is_latest BOOLEAN DEFAULT true,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Teams/Rosters table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_teams (
                    team_id VARCHAR PRIMARY KEY,
                    team_name VARCHAR,
                    region VARCHAR,
                    country VARCHAR,
                    logo_url VARCHAR,
                    is_active BOOLEAN DEFAULT true,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Players table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_players (
                    player_id VARCHAR PRIMARY KEY,
                    player_name VARCHAR,
                    real_name VARCHAR,
                    team_id VARCHAR,
                    role VARCHAR,
                    country VARCHAR,
                    join_date DATE,
                    leave_date DATE,
                    is_active BOOLEAN DEFAULT true,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (team_id) REFERENCES bronze_teams(team_id)
                );
            """)
            
            # Patch notes table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_patches (
                    patch_id VARCHAR PRIMARY KEY,
                    version VARCHAR,
                    release_date DATE,
                    patch_notes TEXT,
                    agent_changes TEXT,
                    map_changes TEXT,
                    weapon_changes TEXT,
                    llm_summary TEXT,
                    impact_score DECIMAL(3,2),
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            logger.info("Bronze layer tables created successfully")

# Singleton instance
db = DuckDBConnection()