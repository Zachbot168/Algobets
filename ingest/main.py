#!/usr/bin/env python3
"""
Main entry point for data ingestion pipeline
"""

import asyncio
import logging
import sys
import os
from datetime import datetime
from typing import Dict, Any
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest.database import db
from ingest.riot_api.matches import RiotMatchIngester
from ingest.odds_api.odds_collector import OddsCollector
from ingest.roster_scraper.teams import RosterScraper
from ingest.validation.data_validator import DataValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/ingestion.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)

class IngestionOrchestrator:
    """Orchestrates the data ingestion pipeline"""
    
    def __init__(self):
        self.validator = DataValidator()
        self.results = {}
        
    async def run_full_ingestion(self, days_back: int = 1) -> Dict[str, Any]:
        """Run complete data ingestion pipeline"""
        logger.info("Starting full ingestion pipeline...")
        start_time = datetime.now()
        
        try:
            # Initialize database
            await self.initialize_database()
            
            # Run ingestion tasks in parallel
            tasks = [
                self.ingest_matches(days_back),
                self.ingest_odds(),
                self.ingest_rosters()
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            self.results = {
                'matches': results[0] if not isinstance(results[0], Exception) else {'error': str(results[0])},
                'odds': results[1] if not isinstance(results[1], Exception) else {'error': str(results[1])},
                'rosters': results[2] if not isinstance(results[2], Exception) else {'error': str(results[2])}
            }
            
            # Validate ingested data
            validation_results = await self.validate_data()
            self.results['validation'] = validation_results
            
            # Generate summary report
            duration = (datetime.now() - start_time).total_seconds()
            summary = self.generate_summary_report(duration)
            
            logger.info(f"Ingestion pipeline completed in {duration:.2f} seconds")
            return summary
            
        except Exception as e:
            logger.error(f"Error in ingestion pipeline: {e}")
            raise
            
    async def initialize_database(self):
        """Initialize database tables"""
        try:
            logger.info("Initializing database tables...")
            db.create_tables()
            logger.info("Database initialization complete")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
            
    async def ingest_matches(self, days_back: int) -> Dict[str, Any]:
        """Ingest match data"""
        logger.info(f"Starting match ingestion (last {days_back} days)...")
        
        try:
            ingester = RiotMatchIngester()
            result = await ingester.ingest_recent_matches(days_back=days_back)
            
            logger.info(f"Match ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Match ingestion failed: {e}")
            return {'error': str(e), 'matches_count': 0}
            
    async def ingest_odds(self) -> Dict[str, Any]:
        """Ingest odds data"""
        logger.info("Starting odds ingestion...")
        
        try:
            collector = OddsCollector()
            result = await collector.collect_upcoming_odds()
            
            logger.info(f"Odds ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Odds ingestion failed: {e}")
            return {'error': str(e), 'records_count': 0}
            
    async def ingest_rosters(self) -> Dict[str, Any]:
        """Ingest roster data"""
        logger.info("Starting roster ingestion...")
        
        try:
            scraper = RosterScraper()
            result = await scraper.scrape_team_rosters()
            
            logger.info(f"Roster ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Roster ingestion failed: {e}")
            return {'error': str(e), 'teams_count': 0}
            
    async def validate_data(self) -> Dict[str, Any]:
        """Validate all ingested data"""
        logger.info("Starting data validation...")
        
        try:
            validation_results = {
                'matches_valid': self.validator.validate_matches_data(),
                'odds_valid': self.validator.validate_odds_data(),
                'rosters_valid': self.validator.validate_roster_data()
            }
            
            # Generate detailed quality report
            quality_report = self.validator.generate_data_quality_report()
            validation_results['quality_report'] = quality_report
            
            logger.info(f"Data validation completed: {validation_results}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return {'error': str(e)}
            
    def generate_summary_report(self, duration: float) -> Dict[str, Any]:
        """Generate final summary report"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'duration_seconds': duration,
            'pipeline_status': 'success',
            'data_sources': {}
        }
        
        # Summarize results for each data source
        for source, result in self.results.items():
            if source == 'validation':
                continue
                
            if 'error' in result:
                summary['data_sources'][source] = {
                    'status': 'failed',
                    'error': result['error'],
                    'records': 0
                }
                summary['pipeline_status'] = 'partial_success'
            else:
                record_count = (
                    result.get('matches_count', 0) +
                    result.get('records_count', 0) +
                    result.get('teams_count', 0) +
                    result.get('players_count', 0)
                )
                
                summary['data_sources'][source] = {
                    'status': 'success',
                    'records': record_count,
                    'details': result
                }
                
        # Add validation summary
        validation = self.results.get('validation', {})
        if 'error' not in validation:
            summary['validation_status'] = {
                'matches': validation.get('matches_valid', False),
                'odds': validation.get('odds_valid', False),
                'rosters': validation.get('rosters_valid', False)
            }
            
            # Check if any validation failed
            if not all(summary['validation_status'].values()):
                summary['pipeline_status'] = 'validation_failed'
        else:
            summary['validation_status'] = {'error': validation['error']}
            summary['pipeline_status'] = 'validation_failed'
            
        return summary
        
    async def run_incremental_ingestion(self) -> Dict[str, Any]:
        """Run incremental ingestion (faster, for frequent updates)"""
        logger.info("Starting incremental ingestion...")
        
        try:
            # Only update odds and recent matches
            tasks = [
                self.ingest_matches(days_back=1),  # Only today's matches
                self.ingest_odds()  # Fresh odds
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            return {
                'matches': results[0] if not isinstance(results[0], Exception) else {'error': str(results[0])},
                'odds': results[1] if not isinstance(results[1], Exception) else {'error': str(results[1])},
                'type': 'incremental'
            }
            
        except Exception as e:
            logger.error(f"Incremental ingestion failed: {e}")
            raise

def main():
    """Main entry point"""
    orchestrator = IngestionOrchestrator()
    
    # Check command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'incremental':
            result = asyncio.run(orchestrator.run_incremental_ingestion())
        elif command == 'validate':
            result = asyncio.run(orchestrator.validate_data())
        else:
            days = int(command) if command.isdigit() else 7
            result = asyncio.run(orchestrator.run_full_ingestion(days_back=days))
    else:
        # Default: full ingestion for last 7 days
        result = asyncio.run(orchestrator.run_full_ingestion(days_back=7))
    
    # Print results
    print(json.dumps(result, indent=2, default=str))
    
    # Exit with appropriate code
    if result.get('pipeline_status') == 'success':
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()