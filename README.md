# Champions League Data Pipeline 🏆

## Overview
This project implements an automated data pipeline using Apache Airflow to collect and process Champions League data from the Live Score API. The data is stored in Azure Data Lake Storage (ADLS) for further analysis and processing.

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- Live Score API credentials
- Azure Data Lake Storage account

### Initial Setup
1. Clone the repository
2. Create required directories:
```bash
mkdir ./dags ./logs ./config ./plugins ./data
```

3. Set up environment variables:
```bash
# For Linux/Mac
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# For Windows
# Create .env file with:
# AIRFLOW_UID=50000
```

4. Initialize Airflow:
```bash
docker compose up airflow-init
```

5. Start the services:
```bash
docker compose up -d
```

## 📊 Pipeline Architecture

### DAGs Overview
The pipeline consists of three main DAGs that run in sequence:

1. **Fixtures DAG** (`dag_live_fixtures.py`)
   - Schedule: Daily at 6:00 AM
   - Purpose: Fetches scheduled Champions League matches
   - Output: Parquet files in `raw/airflow/G30/champions_league_fixtures/`

2. **Teams DAG** (`dag_live_teams.py`)
   - Schedule: Daily at 7:00 AM
   - Purpose: Collects team information
   - Output: Parquet files in `raw/airflow/G30/champions_league_teams/`

3. **Live Scores DAG** (`dag_live_score.py`)
   - Schedule: Every 5 minutes
   - Purpose: Real-time match score updates
   - Output: JSON files in `raw/airflow/G30/champions_league_live_scores/`

## 🔧 Configuration

### Environment Variables
Create a `.env` file with the following variables:
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

### Azure Blob Storage Setup
1. Access Airflow UI at `http://localhost:8080`
2. Navigate to Admin > Connections
3. Create new connection:
   - Connection ID: `utec_blob_storage`
   - Connection Type: Azure Blob Storage
   - Add your Azure storage credentials

## 📁 Project Structure
```
├── dags/                    # Airflow DAG definitions
│   ├── dag_live_fixtures.py
│   ├── dag_live_teams.py
│   └── dag_live_score.py
├── scripts/                 # Utility scripts
│   ├── test_api_connection.py
│   ├── azure_upload.py
│   └── helpers.py
├── config/                  # Configuration files
├── plugins/                 # Custom Airflow plugins
├── data/                    # Data storage
├── docker-compose.yaml      # Docker configuration
├── Dockerfile              # Custom Airflow image
└── requirements.txt        # Python dependencies
```

## 🔍 Monitoring & Troubleshooting

### Common Issues
1. **API Rate Limits**
   - Adjust schedule intervals if hitting rate limits
   - Monitor API response headers for limit information

2. **No Data**
   - Champions League is seasonal
   - Empty responses are normal outside the season

3. **Authentication Errors**
   - Verify API credentials in `.env`
   - Check Azure storage connection settings

### Logs
- Airflow UI: Task-specific logs
- Container logs: `docker compose logs [service_name]`

## 🛠️ Development

### Testing API Connection
```bash
# Set environment variables
export LIVE_SCORE_API_KEY="your_api_key_here"
export LIVE_SCORE_API_SECRET="your_api_secret_here"

# Run test script
python scripts/test_api_connection.py
```

### Adding Dependencies
1. Add to `requirements.txt`
2. Rebuild containers:
```bash
docker compose down && docker compose up --build -d
```
## 📚 API Documentation
- [Live Scores API](https://live-score-api.com/documentation/reference/6/getting_livescores)
- [Fixtures API](https://live-score-api.com/documentation/reference/13/getting-scheduled-games)
- [Competitions List](https://live-score-api.com/competitions)

## 📊 ETLs & Analytics
This project includes ETLs that fetch and process football match data to power analytics pipelines and insights.

## ☁️ Connecting to Azure Blob Storage

To connect Azure Blob Storage in Airflow:

🔗 Tutorial: [Astronomer Guide](https://www.astronomer.io/docs/learn/connections/azure-blob-storage?tab=shared-access-key#azure-blob-storage)  
🔐 How to get Storage Keys: [Microsoft Docs](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys)

### Steps:
1. In Airflow, go to **Admin > Connections**
2. Click on **"Add Connection"**
3. Set the following fields:
   - **Connection ID**: `utec_blob_storage`
   - **Connection Type**: `wasb`
   - **Extra (JSON)**:
     ```json
     {
       "connection_string": "<paste_your_connection_string_here>"
     }
     ```
4. 💡 *Ensure there are no duplicate Azure connections — they can trigger warnings.*

---

## 🤝 Contributing
1. Test changes with the API test script
2. Implement proper error handling
3. Update documentation as needed
4. Follow existing code structure and naming conventions

---

## 🔄 Useful Commands
```bash
# Start services
docker compose up -d

# View logs
docker compose logs -f

# Restart services
docker compose down && docker compose up -d

# Rebuild with changes
docker compose down && docker compose up --build -d