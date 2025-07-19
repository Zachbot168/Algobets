from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.email import EmailOperator
import sys

# Add project root to Python path
sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'valorant-betting-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'catchup': False,
}

dag = DAG(
    'valorant_model_training',
    default_args=default_args,
    description='Train and evaluate VALORANT betting models',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    tags=['valorant', 'ml', 'training'],
)

def train_match_winner_model(**context):
    """Train LightGBM model for match winner prediction"""
    from models.training.match_winner_trainer import MatchWinnerTrainer
    import mlflow
    
    trainer = MatchWinnerTrainer()
    execution_date = context['ds']
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"match_winner_{execution_date}"):
        # Train model with cross-validation
        result = trainer.train_model()
        
        # Log metrics and model
        mlflow.log_metrics(result['metrics'])
        mlflow.lightgbm.log_model(result['model'], "match_winner_model")
    
    return {
        'model_performance': result['metrics'],
        'model_version': result['version'],
        'execution_date': execution_date
    }

def train_map_winner_model(**context):
    """Train model for individual map winner prediction"""
    from models.training.map_winner_trainer import MapWinnerTrainer
    import mlflow
    
    trainer = MapWinnerTrainer()
    execution_date = context['ds']
    
    with mlflow.start_run(run_name=f"map_winner_{execution_date}"):
        result = trainer.train_model()
        mlflow.log_metrics(result['metrics'])
        mlflow.lightgbm.log_model(result['model'], "map_winner_model")
    
    return {
        'model_performance': result['metrics'],
        'model_version': result['version'],
        'execution_date': execution_date
    }

def train_total_rounds_model(**context):
    """Train regression model for total rounds prediction"""
    from models.training.total_rounds_trainer import TotalRoundsTrainer
    import mlflow
    
    trainer = TotalRoundsTrainer()
    execution_date = context['ds']
    
    with mlflow.start_run(run_name=f"total_rounds_{execution_date}"):
        result = trainer.train_model()
        mlflow.log_metrics(result['metrics'])
        mlflow.lightgbm.log_model(result['model'], "total_rounds_model")
    
    return {
        'model_performance': result['metrics'],
        'model_version': result['version'],
        'execution_date': execution_date
    }

def train_player_props_model(**context):
    """Train models for player prop predictions (kills, assists, etc.)"""
    from models.training.player_props_trainer import PlayerPropsTrainer
    import mlflow
    
    trainer = PlayerPropsTrainer()
    execution_date = context['ds']
    
    # Train separate models for different prop types
    prop_types = ['kills', 'assists', 'first_bloods']
    results = {}
    
    for prop_type in prop_types:
        with mlflow.start_run(run_name=f"player_{prop_type}_{execution_date}"):
            result = trainer.train_model(prop_type=prop_type)
            mlflow.log_metrics(result['metrics'])
            mlflow.lightgbm.log_model(result['model'], f"player_{prop_type}_model")
            results[prop_type] = result
    
    return {
        'models_trained': list(results.keys()),
        'model_performances': {k: v['metrics'] for k, v in results.items()},
        'execution_date': execution_date
    }

def evaluate_model_ensemble(**context):
    """Evaluate ensemble of all models and select best performers"""
    from models.evaluation.ensemble_evaluator import EnsembleEvaluator
    
    evaluator = EnsembleEvaluator()
    execution_date = context['ds']
    
    # Get results from all training tasks
    match_result = context['task_instance'].xcom_pull(task_ids='train_match_winner')
    map_result = context['task_instance'].xcom_pull(task_ids='train_map_winner')
    rounds_result = context['task_instance'].xcom_pull(task_ids='train_total_rounds')
    props_result = context['task_instance'].xcom_pull(task_ids='train_player_props')
    
    # Evaluate ensemble performance
    result = evaluator.evaluate_ensemble()
    
    return {
        'ensemble_performance': result['metrics'],
        'best_models': result['best_models'],
        'execution_date': execution_date
    }

def validate_model_performance(**context):
    """Validate model performance against benchmarks"""
    from models.evaluation.performance_validator import PerformanceValidator
    
    validator = PerformanceValidator()
    
    # Get ensemble results
    ensemble_result = context['task_instance'].xcom_pull(task_ids='evaluate_ensemble')
    
    # Validate against historical benchmarks
    validation_results = validator.validate_performance(ensemble_result)
    
    if not validation_results['meets_benchmarks']:
        raise ValueError(f"Model performance below benchmarks: {validation_results['details']}")
    
    return validation_results

def deploy_models_to_production(**context):
    """Deploy validated models to production serving"""
    from models.deployment.model_deployer import ModelDeployer
    import mlflow
    
    deployer = ModelDeployer()
    execution_date = context['ds']
    
    # Get validation results
    validation_result = context['task_instance'].xcom_pull(task_ids='validate_performance')
    
    if validation_result['meets_benchmarks']:
        # Deploy models to production
        result = deployer.deploy_to_production()
        
        # Register models in MLflow model registry
        client = mlflow.tracking.MlflowClient()
        for model_name, model_uri in result['deployed_models'].items():
            client.create_registered_model(model_name)
            client.create_model_version(
                name=model_name,
                source=model_uri,
                run_id=result['run_id']
            )
        
        return {
            'deployed_models': result['deployed_models'],
            'deployment_status': 'success',
            'execution_date': execution_date
        }
    else:
        return {
            'deployment_status': 'skipped',
            'reason': 'Performance validation failed',
            'execution_date': execution_date
        }

# Wait for feature engineering to complete
wait_for_features = ExternalTaskSensor(
    task_id='wait_for_feature_engineering',
    external_dag_id='valorant_feature_engineering',
    external_task_id='validate_feature_store',
    timeout=600,  # 10 minutes
    poke_interval=60,  # Check every minute
    dag=dag,
)

# Model training tasks
train_match_winner_task = PythonOperator(
    task_id='train_match_winner',
    python_callable=train_match_winner_model,
    dag=dag,
)

train_map_winner_task = PythonOperator(
    task_id='train_map_winner',
    python_callable=train_map_winner_model,
    dag=dag,
)

train_total_rounds_task = PythonOperator(
    task_id='train_total_rounds',
    python_callable=train_total_rounds_model,
    dag=dag,
)

train_player_props_task = PythonOperator(
    task_id='train_player_props',
    python_callable=train_player_props_model,
    dag=dag,
)

# Evaluation and deployment tasks
evaluate_ensemble_task = PythonOperator(
    task_id='evaluate_ensemble',
    python_callable=evaluate_model_ensemble,
    dag=dag,
)

validate_performance_task = PythonOperator(
    task_id='validate_performance',
    python_callable=validate_model_performance,
    dag=dag,
)

deploy_models_task = PythonOperator(
    task_id='deploy_models',
    python_callable=deploy_models_to_production,
    dag=dag,
)

# Notification task
notify_completion = EmailOperator(
    task_id='notify_training_completion',
    to=['team@valorantbetting.com'],
    subject='VALORANT Model Training Completed - {{ ds }}',
    html_content="""
    <h3>Model Training Pipeline Completed</h3>
    <p>Date: {{ ds }}</p>
    <p>Status: {{ ti.xcom_pull(task_ids='deploy_models')['deployment_status'] }}</p>
    <p>Check MLflow UI for detailed metrics and model artifacts.</p>
    """,
    dag=dag,
)

# Trigger prediction pipeline
trigger_predictions = BashOperator(
    task_id='trigger_predictions',
    bash_command='airflow dags trigger valorant_predictions',
    dag=dag,
)

# Set task dependencies
wait_for_features >> [train_match_winner_task, train_map_winner_task, train_total_rounds_task, train_player_props_task]
[train_match_winner_task, train_map_winner_task, train_total_rounds_task, train_player_props_task] >> evaluate_ensemble_task
evaluate_ensemble_task >> validate_performance_task
validate_performance_task >> deploy_models_task
deploy_models_task >> [notify_completion, trigger_predictions]