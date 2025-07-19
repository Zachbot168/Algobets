from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys

# Add project root to Python path
sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'valorant-betting-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
}

dag = DAG(
    'valorant_feature_engineering',
    default_args=default_args,
    description='Engineer features for VALORANT betting models',
    schedule_interval=None,  # Triggered by ingestion DAG
    max_active_runs=1,
    tags=['valorant', 'features', 'ml'],
)

def calculate_team_elo_ratings(**context):
    """Calculate and update Elo/Glicko ratings for teams"""
    from features.engineering.elo_calculator import EloCalculator
    
    calculator = EloCalculator()
    execution_date = context['ds']
    
    # Update ratings based on recent matches
    result = calculator.update_all_team_ratings()
    
    return {
        'teams_updated': result.get('teams_count', 0),
        'matches_processed': result.get('matches_count', 0),
        'execution_date': execution_date
    }

def calculate_rolling_stats(**context):
    """Calculate rolling performance statistics"""
    from features.engineering.rolling_stats import RollingStatsCalculator
    
    calculator = RollingStatsCalculator()
    execution_date = context['ds']
    
    # Calculate various rolling windows (5, 10, 20 matches)
    windows = [5, 10, 20]
    results = {}
    
    for window in windows:
        result = calculator.calculate_team_rolling_stats(window_size=window)
        results[f'window_{window}'] = result
    
    return {
        'rolling_stats': results,
        'execution_date': execution_date
    }

def engineer_map_features(**context):
    """Engineer map-specific features"""
    from features.engineering.map_features import MapFeatureEngineer
    
    engineer = MapFeatureEngineer()
    execution_date = context['ds']
    
    # Calculate map-specific win rates, pick rates, etc.
    result = engineer.calculate_map_features()
    
    return {
        'map_features_count': result.get('features_count', 0),
        'execution_date': execution_date
    }

def engineer_player_features(**context):
    """Engineer player-specific features for props betting"""
    from features.engineering.player_features import PlayerFeatureEngineer
    
    engineer = PlayerFeatureEngineer()
    execution_date = context['ds']
    
    # Calculate player performance metrics
    result = engineer.calculate_player_features()
    
    return {
        'player_features_count': result.get('features_count', 0),
        'players_processed': result.get('players_count', 0),
        'execution_date': execution_date
    }

def calculate_market_features(**context):
    """Calculate market-based features from odds movement"""
    from features.engineering.market_features import MarketFeatureCalculator
    
    calculator = MarketFeatureCalculator()
    execution_date = context['ds']
    
    # Calculate odds movement, implied probabilities, etc.
    result = calculator.calculate_market_features()
    
    return {
        'market_features_count': result.get('features_count', 0),
        'execution_date': execution_date
    }

def create_feature_store(**context):
    """Combine all features into feature store"""
    from features.store.feature_combiner import FeatureCombiner
    
    combiner = FeatureCombiner()
    execution_date = context['ds']
    
    # Get results from previous tasks
    elo_result = context['task_instance'].xcom_pull(task_ids='calculate_elo_ratings')
    rolling_result = context['task_instance'].xcom_pull(task_ids='calculate_rolling_stats')
    map_result = context['task_instance'].xcom_pull(task_ids='engineer_map_features')
    player_result = context['task_instance'].xcom_pull(task_ids='engineer_player_features')
    market_result = context['task_instance'].xcom_pull(task_ids='calculate_market_features')
    
    # Combine all features into final feature store
    result = combiner.create_feature_store()
    
    return {
        'total_features': result.get('feature_count', 0),
        'total_samples': result.get('sample_count', 0),
        'feature_store_path': result.get('store_path'),
        'execution_date': execution_date
    }

def validate_feature_store(**context):
    """Validate feature store quality and completeness"""
    from features.store.feature_validator import FeatureValidator
    
    validator = FeatureValidator()
    
    # Get feature store path from previous task
    store_result = context['task_instance'].xcom_pull(task_ids='create_feature_store')
    store_path = store_result.get('feature_store_path')
    
    # Validate feature store
    validation_results = validator.validate_feature_store(store_path)
    
    if not validation_results['is_valid']:
        raise ValueError(f"Feature store validation failed: {validation_results['errors']}")
    
    return validation_results

# Wait for ingestion DAG to complete
wait_for_ingestion = ExternalTaskSensor(
    task_id='wait_for_data_ingestion',
    external_dag_id='valorant_data_ingestion',
    external_task_id='validate_ingested_data',
    timeout=300,  # 5 minutes
    poke_interval=30,  # Check every 30 seconds
    dag=dag,
)

# Define feature engineering tasks
elo_task = PythonOperator(
    task_id='calculate_elo_ratings',
    python_callable=calculate_team_elo_ratings,
    dag=dag,
)

rolling_stats_task = PythonOperator(
    task_id='calculate_rolling_stats',
    python_callable=calculate_rolling_stats,
    dag=dag,
)

map_features_task = PythonOperator(
    task_id='engineer_map_features',
    python_callable=engineer_map_features,
    dag=dag,
)

player_features_task = PythonOperator(
    task_id='engineer_player_features',
    python_callable=engineer_player_features,
    dag=dag,
)

market_features_task = PythonOperator(
    task_id='calculate_market_features',
    python_callable=calculate_market_features,
    dag=dag,
)

feature_store_task = PythonOperator(
    task_id='create_feature_store',
    python_callable=create_feature_store,
    dag=dag,
)

validate_features_task = PythonOperator(
    task_id='validate_feature_store',
    python_callable=validate_feature_store,
    dag=dag,
)

# Trigger model training
trigger_training = BashOperator(
    task_id='trigger_model_training',
    bash_command='airflow dags trigger valorant_model_training',
    dag=dag,
)

# Set task dependencies
wait_for_ingestion >> [elo_task, rolling_stats_task, map_features_task, player_features_task, market_features_task]
[elo_task, rolling_stats_task, map_features_task, player_features_task, market_features_task] >> feature_store_task
feature_store_task >> validate_features_task
validate_features_task >> trigger_training