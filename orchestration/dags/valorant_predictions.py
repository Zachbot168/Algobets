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
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'valorant_predictions',
    default_args=default_args,
    description='Generate predictions and betting recommendations',
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    max_active_runs=1,
    tags=['valorant', 'predictions', 'betting'],
)

def load_latest_models(**context):
    """Load the latest production models from MLflow"""
    from models.serving.model_loader import ModelLoader
    import mlflow
    
    loader = ModelLoader()
    execution_date = context['ds']
    
    # Load all production models
    models = loader.load_production_models()
    
    return {
        'loaded_models': list(models.keys()),
        'model_versions': {k: v.get('version') for k, v in models.items()},
        'execution_date': execution_date
    }

def get_upcoming_matches(**context):
    """Get upcoming matches that need predictions"""
    from ingest.riot_api.upcoming_matches import UpcomingMatchesFetcher
    
    fetcher = UpcomingMatchesFetcher()
    execution_date = context['ds']
    
    # Get matches in next 24 hours
    matches = fetcher.get_upcoming_matches(hours_ahead=24)
    
    return {
        'upcoming_matches': matches,
        'match_count': len(matches),
        'execution_date': execution_date
    }

def generate_match_predictions(**context):
    """Generate match winner predictions"""
    from models.serving.match_predictor import MatchPredictor
    
    predictor = MatchPredictor()
    
    # Get upcoming matches and loaded models
    matches_result = context['task_instance'].xcom_pull(task_ids='get_upcoming_matches')
    models_result = context['task_instance'].xcom_pull(task_ids='load_models')
    
    upcoming_matches = matches_result['upcoming_matches']
    
    # Generate predictions for each match
    predictions = []
    for match in upcoming_matches:
        prediction = predictor.predict_match_winner(match)
        predictions.append(prediction)
    
    return {
        'match_predictions': predictions,
        'predictions_count': len(predictions),
        'execution_date': context['ds']
    }

def generate_map_predictions(**context):
    """Generate individual map winner predictions"""
    from models.serving.map_predictor import MapPredictor
    
    predictor = MapPredictor()
    
    # Get upcoming matches
    matches_result = context['task_instance'].xcom_pull(task_ids='get_upcoming_matches')
    upcoming_matches = matches_result['upcoming_matches']
    
    # Generate map predictions
    map_predictions = []
    for match in upcoming_matches:
        for map_name in match.get('maps', []):
            prediction = predictor.predict_map_winner(match, map_name)
            map_predictions.append(prediction)
    
    return {
        'map_predictions': map_predictions,
        'predictions_count': len(map_predictions),
        'execution_date': context['ds']
    }

def generate_totals_predictions(**context):
    """Generate total rounds over/under predictions"""
    from models.serving.totals_predictor import TotalsPredictor
    
    predictor = TotalsPredictor()
    
    # Get upcoming matches
    matches_result = context['task_instance'].xcom_pull(task_ids='get_upcoming_matches')
    upcoming_matches = matches_result['upcoming_matches']
    
    # Generate totals predictions
    totals_predictions = []
    for match in upcoming_matches:
        for map_name in match.get('maps', []):
            prediction = predictor.predict_total_rounds(match, map_name)
            totals_predictions.append(prediction)
    
    return {
        'totals_predictions': totals_predictions,
        'predictions_count': len(totals_predictions),
        'execution_date': context['ds']
    }

def generate_player_props_predictions(**context):
    """Generate player prop predictions"""
    from models.serving.player_props_predictor import PlayerPropsPredictor
    
    predictor = PlayerPropsPredictor()
    
    # Get upcoming matches
    matches_result = context['task_instance'].xcom_pull(task_ids='get_upcoming_matches')
    upcoming_matches = matches_result['upcoming_matches']
    
    # Generate player prop predictions
    props_predictions = []
    for match in upcoming_matches:
        for player in match.get('players', []):
            prediction = predictor.predict_player_props(match, player)
            props_predictions.append(prediction)
    
    return {
        'props_predictions': props_predictions,
        'predictions_count': len(props_predictions),
        'execution_date': context['ds']
    }

def calculate_betting_edges(**context):
    """Calculate expected value and betting edges"""
    from models.serving.edge_calculator import EdgeCalculator
    
    calculator = EdgeCalculator()
    
    # Get all predictions
    match_preds = context['task_instance'].xcom_pull(task_ids='predict_match_winners')
    map_preds = context['task_instance'].xcom_pull(task_ids='predict_map_winners')
    totals_preds = context['task_instance'].xcom_pull(task_ids='predict_totals')
    props_preds = context['task_instance'].xcom_pull(task_ids='predict_player_props')
    
    # Calculate edges for each prediction type
    all_predictions = (
        match_preds['match_predictions'] +
        map_preds['map_predictions'] +
        totals_preds['totals_predictions'] +
        props_preds['props_predictions']
    )
    
    # Calculate expected values and recommend bets
    betting_recommendations = calculator.calculate_betting_edges(all_predictions)
    
    return {
        'total_predictions': len(all_predictions),
        'betting_recommendations': betting_recommendations,
        'profitable_bets': len([b for b in betting_recommendations if b['edge'] > 0.02]),
        'execution_date': context['ds']
    }

def save_predictions_to_api(**context):
    """Save predictions to database for API serving"""
    from api.services.prediction_service import PredictionService
    
    service = PredictionService()
    
    # Get betting recommendations
    betting_result = context['task_instance'].xcom_pull(task_ids='calculate_edges')
    recommendations = betting_result['betting_recommendations']
    
    # Save to database
    result = service.save_predictions(recommendations)
    
    return {
        'saved_predictions': result['saved_count'],
        'api_status': 'updated',
        'execution_date': context['ds']
    }

def generate_betting_csv(**context):
    """Generate CSV file for manual betting execution"""
    from models.serving.bet_sheet_generator import BetSheetGenerator
    
    generator = BetSheetGenerator()
    
    # Get betting recommendations
    betting_result = context['task_instance'].xcom_pull(task_ids='calculate_edges')
    recommendations = betting_result['betting_recommendations']
    
    # Generate CSV bet sheet
    csv_path = generator.generate_bet_sheet(recommendations)
    
    return {
        'bet_sheet_path': csv_path,
        'recommended_bets': len(recommendations),
        'execution_date': context['ds']
    }

# Define tasks
load_models_task = PythonOperator(
    task_id='load_models',
    python_callable=load_latest_models,
    dag=dag,
)

get_matches_task = PythonOperator(
    task_id='get_upcoming_matches',
    python_callable=get_upcoming_matches,
    dag=dag,
)

predict_match_winners_task = PythonOperator(
    task_id='predict_match_winners',
    python_callable=generate_match_predictions,
    dag=dag,
)

predict_map_winners_task = PythonOperator(
    task_id='predict_map_winners',
    python_callable=generate_map_predictions,
    dag=dag,
)

predict_totals_task = PythonOperator(
    task_id='predict_totals',
    python_callable=generate_totals_predictions,
    dag=dag,
)

predict_player_props_task = PythonOperator(
    task_id='predict_player_props',
    python_callable=generate_player_props_predictions,
    dag=dag,
)

calculate_edges_task = PythonOperator(
    task_id='calculate_edges',
    python_callable=calculate_betting_edges,
    dag=dag,
)

save_to_api_task = PythonOperator(
    task_id='save_to_api',
    python_callable=save_predictions_to_api,
    dag=dag,
)

generate_csv_task = PythonOperator(
    task_id='generate_bet_sheet',
    python_callable=generate_betting_csv,
    dag=dag,
)

# Set task dependencies
[load_models_task, get_matches_task] >> [predict_match_winners_task, predict_map_winners_task, predict_totals_task, predict_player_props_task]
[predict_match_winners_task, predict_map_winners_task, predict_totals_task, predict_player_props_task] >> calculate_edges_task
calculate_edges_task >> [save_to_api_task, generate_csv_task]