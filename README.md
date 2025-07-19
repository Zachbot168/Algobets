# 🎯 VALORANT Betting Platform

An AI-powered esports betting platform that provides ML-driven predictions, odds analysis, and automated betting recommendations for VALORANT matches.

## 🚀 Features

### Core Functionality
- **ML-Powered Predictions**: LightGBM models for match winners, map winners, total rounds, and player props
- **Real-Time Odds Collection**: Integration with TheOddsAPI, Pinnacle, and major sportsbooks
- **Expected Value Calculation**: Kelly criterion staking with automated edge detection
- **Interactive GUI**: Streamlit-based dashboard for easy betting management
- **Automated Pipeline**: Airflow orchestration for data ingestion, feature engineering, and model training

### Prediction Markets
- **Match Winners**: Binary classification with confidence scores
- **Map Winners**: Individual map predictions for each match
- **Total Rounds**: Over/Under predictions with µ and σ estimates
- **Player Props**: Kills, assists, first bloods with statistical modeling
- **First Blood**: Round-level predictions for opening picks

### Data Sources
- **Match Data**: Riot VALORANT esports API + VLR.gg scraping
- **Odds Data**: TheOddsAPI, Pinnacle API with 30-minute polling
- **Team/Player Info**: Liquipedia GraphQL + VLR.gg roster scraping
- **Patch Notes**: Riot Data Dragon + LLM-powered impact scoring

## 📋 Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Data       │    │  Feature    │    │  ML Models  │
│  Ingestion  │───▶│ Engineering │───▶│  Training   │
│  (Airflow)  │    │  Pipeline   │    │  (MLflow)   │
└─────────────┘    └─────────────┘    └─────────────┘
       │                    │                   │
       ▼                    ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Bronze DB  │    │  Feature    │    │  Model      │
│  (DuckDB)   │    │   Store     │    │ Registry    │
└─────────────┘    └─────────────┘    └─────────────┘
                           │                   │
                           ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐
                   │  Prediction │    │  Streamlit  │
                   │  API        │◀───│     GUI     │
                   │  (FastAPI)  │    │             │
                   └─────────────┘    └─────────────┘
```

## 🛠️ Technology Stack

### Backend
- **Python 3.12**: Core language
- **FastAPI**: Prediction API service
- **Apache Airflow**: Workflow orchestration
- **DuckDB**: Analytics database (Bronze layer)
- **MLflow**: Model lifecycle management
- **LightGBM/XGBoost**: ML models
- **aiohttp/httpx**: Async data ingestion

### Frontend
- **Streamlit**: Interactive web GUI
- **Plotly**: Data visualization
- **Pandas**: Data manipulation

### Infrastructure
- **Docker Compose**: Local development
- **PostgreSQL**: Airflow metadata
- **Redis**: Airflow task queue
- **S3 + Delta Lake**: Feature store (production)

### External APIs
- **Riot Games API**: Official match data
- **TheOddsAPI**: Betting odds collection
- **Pinnacle API**: Sharp odds reference
- **OpenAI GPT-4**: Patch analysis and explanations

## 📦 Installation

### Prerequisites
- Docker & Docker Compose
- Python 3.12+
- Git

### Quick Start

1. **Clone the repository**
```bash
git clone <repository-url>
cd Algobets
```

2. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env with your API keys
```

3. **Build and start services**
```bash
docker-compose up --build
```

4. **Access the applications**
- **Streamlit GUI**: http://localhost:8501
- **FastAPI Docs**: http://localhost:8000/docs
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **MLflow UI**: http://localhost:5000

### Manual Installation

1. **Install Python dependencies**
```bash
pip install -r requirements.txt
```

2. **Initialize database**
```bash
python -m ingest.main
```

3. **Start API service**
```bash
python -m api.main
```

4. **Start GUI**
```bash
streamlit run gui/main.py
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# External API Keys
RIOT_API_KEY=your_riot_api_key_here
THEODS_API_KEY=your_theoddsapi_key_here
PINNACLE_API_KEY=your_pinnacle_api_key_here
OPENAI_API_KEY=your_openai_api_key_here

# Database Configuration
DUCKDB_PATH=./data/bronze.db
DATABASE_URL=sqlite:///./valorant_betting.db

# MLflow Configuration
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT_NAME=valorant_betting

# Betting Configuration
MAX_STAKE_PERCENT=0.05          # Max 5% of bankroll per bet
KELLY_FRACTION=0.25             # Fractional Kelly criterion
MIN_EDGE_THRESHOLD=0.02         # Minimum 2% edge to place bet

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
SECRET_KEY=your_secret_key_here
```

## 🚀 Usage

### Data Pipeline

1. **Start data ingestion**
```bash
# Full historical ingestion
python -m ingest.main 30  # Last 30 days

# Incremental updates  
python -m ingest.main incremental
```

2. **Trigger Airflow workflows**
- Data ingestion: `valorant_data_ingestion`
- Feature engineering: `valorant_feature_engineering`  
- Model training: `valorant_model_training`
- Predictions: `valorant_predictions`

### API Usage

**Get match predictions:**
```bash
curl "http://localhost:8000/api/v1/predictions/match_123?market_types=match_winner,total_rounds"
```

**Get betting recommendations:**
```bash
curl "http://localhost:8000/api/v1/betting-recommendations?min_edge=0.02"
```

**Health check:**
```bash
curl "http://localhost:8000/health"
```

### GUI Features

1. **Dashboard**: Overview of predictions, opportunities, and performance
2. **Matches**: Browse upcoming/completed matches with predictions
3. **Predictions**: Detailed ML model outputs and confidence scores
4. **Bet Builder**: Create betting slips with Kelly sizing
5. **Analytics**: Performance tracking and model metrics
6. **Settings**: Configure preferences and API keys

## 🧠 ML Models

### Model Types

1. **Match Winner Model**
   - **Type**: Binary classification (LightGBM)
   - **Features**: Team Elo, H2H record, recent form, map pool
   - **Output**: Win probability + confidence score

2. **Map Winner Model**  
   - **Type**: Binary classification per map
   - **Features**: Map-specific stats, agent meta, side preference
   - **Output**: Map win probability for each team

3. **Total Rounds Model**
   - **Type**: Regression (Poisson GLM + LightGBM)
   - **Features**: Team playstyles, map characteristics, patch impact
   - **Output**: µ and σ for rounds distribution

4. **Player Props Model**
   - **Type**: Regression for kills/assists/first bloods
   - **Features**: Player form, role, opponent strength, map
   - **Output**: Expected value + probability distributions

## 📊 Project Structure

```
Algobets/
├── ingest/                 # Data ingestion modules
│   ├── riot_api/          # Riot Games API integration
│   ├── odds_api/          # Betting odds collection
│   ├── roster_scraper/    # Team/player data scraping
│   └── validation/        # Data quality validation
├── features/              # Feature engineering
│   ├── engineering/       # Feature calculation
│   └── store/            # Feature store management
├── models/                # ML model training
│   ├── training/         # Model training scripts
│   └── evaluation/       # Model evaluation
├── api/                   # FastAPI prediction service
│   ├── routers/          # API endpoints
│   ├── services/         # Business logic
│   └── models/           # Pydantic schemas
├── gui/                   # Streamlit GUI
│   ├── pages/            # UI pages
│   ├── components/       # Reusable components
│   └── utils/            # GUI utilities
├── orchestration/         # Airflow DAGs
│   └── dags/             # Workflow definitions
├── infra/                 # Infrastructure
│   ├── Dockerfile.*      # Container definitions
│   └── docker-compose.yml
├── tests/                 # Test suite
├── docs/                  # Documentation
└── requirements.txt       # Python dependencies
```

## 🔧 Development

### Adding New Models

1. **Create training script** in `models/training/`
2. **Define feature pipeline** in `features/engineering/`
3. **Add API endpoint** in `api/routers/predictions.py`
4. **Update Airflow DAG** in `orchestration/dags/`
5. **Add GUI integration** in `gui/pages/`

### Testing

```bash
# Run unit tests
pytest tests/unit/

# Run integration tests  
pytest tests/integration/

# Run all tests
pytest
```

### Code Quality

```bash
# Format code
black .

# Lint code
flake8 .

# Type checking
mypy .
```

## 🚨 Compliance & Legal

### Responsible Gambling
- **Stake Limits**: Maximum 5% of bankroll per bet
- **Loss Limits**: Daily/weekly loss tracking
- **Educational Resources**: Links to gambling addiction support

### Legal Compliance
- **Illinois Jurisdiction**: Only integrates with IL-licensed sportsbooks
- **Age Verification**: 21+ requirement enforcement
- **Data Privacy**: GDPR/CCPA compliant data handling

### Supported Sportsbooks
- DraftKings (IL)
- FanDuel (IL)
- BetRivers (IL)
- BetMGM (IL)
- Pinnacle (reference odds only)

## 🛡️ Security

### Data Protection
- **API Keys**: Stored in environment variables or AWS Secrets Manager
- **Database**: Encrypted at rest and in transit
- **Audit Logging**: All betting activity tracked

### Authentication
- **JWT Tokens**: Secure API authentication
- **Rate Limiting**: API abuse prevention
- **Input Validation**: SQL injection and XSS protection

## 📈 Performance

### Scalability
- **Horizontal Scaling**: Stateless API design
- **Caching**: Redis for frequently accessed data
- **Database Optimization**: Indexed queries and partitioning

### Monitoring
- **Application Metrics**: OpenTelemetry integration
- **Model Performance**: MLflow experiment tracking
- **Business Metrics**: Betting ROI and accuracy tracking

## ⚠️ Disclaimer

**This software is for educational and research purposes only.** 

- **No Financial Advice**: Predictions are not guaranteed and should not be considered financial advice
- **Gambling Risks**: Betting involves risk of financial loss
- **Legal Compliance**: Users must comply with local gambling laws
- **Age Restrictions**: Must be 21+ in jurisdictions where sports betting is legal
- **Responsible Use**: Set limits and gamble responsibly

## 🚀 Getting Started Checklist

- [ ] Clone repository and install dependencies
- [ ] Set up API keys in `.env` file
- [ ] Start Docker Compose services
- [ ] Run initial data ingestion
- [ ] Access Streamlit GUI at http://localhost:8501
- [ ] Generate first predictions
- [ ] Review betting recommendations
- [ ] Set up bankroll management
- [ ] Configure risk parameters
- [ ] Start paper trading to validate models

**Happy Betting! 🎯**
