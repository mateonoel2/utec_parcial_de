# Champions League Data Ingestion Pipeline

This project provides an Apache Airflow-based data pipeline to ingest Champions League data from the [Live Score API](https://live-score-api.com) and store it in Azure Data Lake Storage (ADLS).

## Architecture Overview

The pipeline consists of the following DAGs that run in sequence:

1. **dag_live_fixtures** - Fetches Champions League fixtures (scheduled matches)
2. **dag_live_teams** - Fetches Champions League teams information
3. **dag_live_score** - Fetches live match scores (runs frequently)

## Prerequisites

1. Live Score API credentials (key and secret)
2. Azure Data Lake Storage account
3. Docker and Docker Compose
4. Python 3.8+

## Setup Instructions

### 1. Get API Credentials

1. Sign up at [Live Score API](https://live-score-api.com)
2. Obtain your API key and secret

### 2. Test API Connection

Before setting up the pipeline, test your API credentials:

```bash
# Set environment variables
export LIVE_SCORE_API_KEY="your_api_key_here"
export LIVE_SCORE_API_SECRET="your_api_secret_here"

# Run the test script
python scripts/test_api_connection.py
```

This script will:
- Test your API connection
- Find the Champions League competition ID
- Test all relevant endpoints
- Display available data

### 3. Update Competition ID

After running the test script, update the `CHAMPIONS_LEAGUE_ID` variable in each DAG file with the correct competition ID for UEFA Champions League.

### 4. Configure Environment

Create a `.env` file in the project root:

```bash
# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Live Score API Credentials
LIVE_SCORE_API_KEY=your_api_key_here
LIVE_SCORE_API_SECRET=your_api_secret_here

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Admin User
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### 5. Start the Pipeline

```bash
# Build and start Airflow
docker-compose up -d --build

# Wait for services to be ready (check logs)
docker-compose logs -f
```

### 6. Configure Airflow

1. Access Airflow UI at `http://localhost:8080`
2. Login with credentials from `.env` file
3. Configure Azure WASB connection:
   - Go to Admin > Connections
   - Create new connection with ID: `utec_blob_storage`
   - Connection Type: Azure Blob Storage
   - Add your Azure storage credentials

## DAG Details

### 1. dag_live_fixtures

- **Schedule**: Daily at 6:00 AM
- **Purpose**: Fetch scheduled Champions League matches
- **Output**: Parquet file with fixture information
- **Location**: `raw/airflow/G30/champions_league_fixtures/`

**Data Schema:**
- fixture_id, home_team_id, home_team_name, away_team_id, away_team_name
- date, time, location, status, competition_info, country_info
- extracted_at (timestamp)

### 2. dag_live_teams

- **Schedule**: Daily at 7:00 AM (after fixtures)
- **Purpose**: Fetch Champions League team information
- **Output**: Parquet file with team details
- **Location**: `raw/airflow/G30/champions_league_teams/`

**Data Schema:**
- team_id, team_name, logo_url, stadium
- country_id, country_name, fifa_code, flag
- extracted_at (timestamp)

### 3. dag_live_score

- **Schedule**: Every 5 minutes
- **Purpose**: Fetch live match scores and status
- **Output**: JSON file with live match data
- **Location**: `raw/airflow/G30/champions_league_live_scores/`

**Data Structure:**
```json
{
  "extraction_timestamp": "2025-01-01T10:00:00",
  "competition_id": "1",
  "competition_name": "UEFA Champions League",
  "total_matches": 2,
  "matches": [...],
  "api_response_success": true,
  "raw_api_data": {...}
}
```

## File Structure

```
├── dags/
│   ├── dag_live_fixtures.py    # Fixtures ingestion
│   ├── dag_live_teams.py       # Teams ingestion
│   └── dag_live_score.py       # Live scores ingestion
├── scripts/
│   ├── test_api_connection.py  # API testing utility
│   ├── azure_upload.py         # ADLS upload functions
│   └── helpers.py              # Utility functions
├── docker-compose.yaml         # Airflow setup
├── Dockerfile                  # Custom Airflow image
├── requirements.txt            # Python dependencies
└── README_CHAMPIONS_LEAGUE_API.md
```

## Data Flow

1. **Fixtures DAG** runs first (daily) to get scheduled matches
2. **Teams DAG** runs after fixtures to get team information
3. **Live Scores DAG** runs continuously (every 5 minutes) during match days

## Monitoring and Troubleshooting

### Check DAG Status
- Access Airflow UI at `http://localhost:8080`
- Monitor DAG runs and task status
- Check logs for any errors

### Common Issues

1. **API Rate Limits**: The API has rate limits. Adjust schedule intervals if needed.
2. **No Data**: Champions League matches are seasonal. Outside the season, you may get empty responses.
3. **Authentication Errors**: Verify your API credentials in the `.env` file.

### Log Locations
- Airflow logs: Available in the UI under each task
- Container logs: `docker-compose logs [service_name]`

## API Reference

- **Live Scores**: [Documentation](https://live-score-api.com/documentation/reference/6/getting_livescores)
- **Fixtures**: [Documentation](https://live-score-api.com/documentation/reference/13/getting-scheduled-games)
- **Competitions**: [List](https://live-score-api.com/competitions)

## Data Usage

The ingested data can be used for:
- Match analytics and statistics
- Team performance analysis
- Real-time score tracking
- Historical match data analysis
- Building sports applications

## Contributing

1. Test any changes with the API test script
2. Ensure proper error handling in DAGs
3. Update documentation for any new features
4. Follow the existing code structure and naming conventions

## License

This project is for educational and development purposes. Please ensure compliance with the Live Score API terms of service. 