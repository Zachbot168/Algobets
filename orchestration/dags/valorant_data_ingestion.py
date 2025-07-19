from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
import sys
import os

# Add project root to Python path
sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'valorant-betting-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'valorant_data_ingestion',
    default_args=default_args,
    description='Ingest VALORANT match data, odds, and roster information',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
    max_active_runs=1,
    tags=['valorant', 'ingestion', 'data'],
)

def ingest_riot_api_data(**context):
    """Ingest match data from Riot VALORANT esports API"""
    from ingest.riot_api.matches import RiotMatchIngester
    
    ingester = RiotMatchIngester()
    execution_date = context['ds']
    
    # Ingest recent matches
    result = ingester.ingest_recent_matches(days_back=1)
    
    return {
        'matches_ingested': result.get('matches_count', 0),
        'execution_date': execution_date
    }

def ingest_odds_data(**context):
    """Ingest betting odds from TheOddsAPI and Pinnacle"""
    from ingest.odds_api.odds_collector import OddsCollector
    
    collector = OddsCollector()
    execution_date = context['ds']
    
    # Collect odds for upcoming matches
    result = collector.collect_upcoming_odds()
    
    return {
        'odds_records': result.get('records_count', 0),
        'execution_date': execution_date
    }

def ingest_roster_data(**context):
    """Scrape roster and team information from VLR.gg and Liquipedia"""
    from ingest.roster_scraper.teams import RosterScraper
    
    scraper = RosterScraper()
    execution_date = context['ds']
    
    # Scrape team rosters and recent changes
    result = scraper.scrape_team_rosters()
    
    return {
        'teams_updated': result.get('teams_count', 0),
        'roster_changes': result.get('changes_count', 0),
        'execution_date': execution_date
    }

def validate_ingested_data(**context):
    """Validate data quality and completeness"""
    from ingest.validation.data_validator import DataValidator
    
    validator = DataValidator()
    
    # Get task instance results
    riot_result = context['task_instance'].xcom_pull(task_ids='ingest_riot_data')
    odds_result = context['task_instance'].xcom_pull(task_ids='ingest_odds_data') 
    roster_result = context['task_instance'].xcom_pull(task_ids='ingest_roster_data')
    
    # Validate each data source
    validation_results = {
        'riot_api': validator.validate_matches_data(),
        'odds_api': validator.validate_odds_data(),
        'roster_data': validator.validate_roster_data()
    }
    
    # Raise exception if critical validations fail
    if not all(validation_results.values()):
        raise ValueError(f"Data validation failed: {validation_results}")
    
    return validation_results

# Define tasks
ingest_riot_task = PythonOperator(
    task_id='ingest_riot_data',
    python_callable=ingest_riot_api_data,
    dag=dag,
)

ingest_odds_task = PythonOperator(
    task_id='ingest_odds_data', 
    python_callable=ingest_odds_data,
    dag=dag,
)

ingest_roster_task = PythonOperator(
    task_id='ingest_roster_data',
    python_callable=ingest_roster_data,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_ingested_data',
    python_callable=validate_ingested_data,
    dag=dag,
)

# Create bronze tables if they don't exist
create_tables_task = PostgresOperator(
    task_id='create_bronze_tables',
    postgres_conn_id='valorant_db',
    sql="""
    CREATE TABLE IF NOT EXISTS bronze_matches (
        match_id VARCHAR(255) PRIMARY KEY,
        team_a VARCHAR(255),
        team_b VARCHAR(255),
        start_time TIMESTAMP,
        tournament VARCHAR(255),
        best_of INTEGER,
        patch_version VARCHAR(50),
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS bronze_odds (
        odds_id SERIAL PRIMARY KEY,
        match_id VARCHAR(255),
        bookmaker VARCHAR(100),
        market_type VARCHAR(50),
        selection VARCHAR(255),
        odds_decimal DECIMAL(10,3),
        timestamp TIMESTAMP,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS bronze_rosters (
        roster_id SERIAL PRIMARY KEY,
        team_name VARCHAR(255),
        player_name VARCHAR(255),
        role VARCHAR(50),
        join_date DATE,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

# Trigger feature engineering DAG
trigger_feature_engineering = BashOperator(
    task_id='trigger_feature_engineering',
    bash_command='airflow dags trigger valorant_feature_engineering',
    dag=dag,
)

# Set task dependencies
create_tables_task >> [ingest_riot_task, ingest_odds_task, ingest_roster_task]
[ingest_riot_task, ingest_odds_task, ingest_roster_task] >> validate_data_task
validate_data_task >> trigger_feature_engineering