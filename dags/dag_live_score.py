from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import pytz
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
import requests
import json
import pandas as pd
import logging
import os

# Configure logging
logger = logging.getLogger(__name__)

# File and Azure configuration
TODAY_UTC = datetime.now(timezone.utc).strftime('%Y-%m-%d')
SUMMARY_FILE_PATH = f"/opt/airflow/data/summary_{TODAY_UTC}.json"
MATCHES_FILE_PATH = f"/opt/airflow/data/matches_{TODAY_UTC}.parquet"
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"
BLOB_NAME = "raw/airflow/G30/api/live_scores/champions_league/"

# Champions League competition ID
CHAMPIONS_LEAGUE_ID = 244

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="dag_live_score",
    description="Fetch Champions League live scores and upload to ADLS as JSON and Parquet",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=pytz.timezone("America/Bogota")),
    schedule="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    tags=["champions_league", "live_scores", "json", "parquet", "adls_upload"],
)
def live_scores_dag():

    @task
    def fetch_and_save_live_scores():
        """
        Fetch Champions League live scores from API and save as JSON summary and Parquet matches
        """
        try:
            # Get API credentials
            api_key = Variable.get('live_score_api_key')
            api_secret = Variable.get('live_score_api_secret')
            
            # API endpoint for live scores
            base_url = "https://livescore-api.com/api-client/matches/live.json"
            
            params = {
                'key': api_key,
                'secret': api_secret,
                'competition_id': CHAMPIONS_LEAGUE_ID
            }
            
            logger.info(f"Fetching Champions League live scores (competition_id: {CHAMPIONS_LEAGUE_ID})")
            
            # Make API request
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('success'):
                raise Exception(f"API returned error: {data.get('error', 'Unknown error')}")
            
            matches = data.get('data', {}).get('match', [])
            logger.info(f"Successfully fetched {len(matches)} live matches")
            
            # Create summary data
            summary_data = {
                'extraction_timestamp': datetime.now().isoformat(),
                'competition_id': CHAMPIONS_LEAGUE_ID,
                'competition_name': 'UEFA Champions League',
                'total_matches': len(matches),
                'api_response_success': data.get('success')
            }
            logger.info(matches[0])
            
            # Create matches data for parquet
            matches_list = []
            if matches:
                for match in matches:
                    match_data = {
                        'match_id': match.get('id', None),
                        'fixture_id': match.get('fixture_id', None),
                        'home_team_id': match.get('home', {}).get('id', None),
                        'home_team_name': match.get('home', {}).get('name', None),
                        'home_team_logo': match.get('home', {}).get('logo', None),
                        'away_team_id': match.get('away', {}).get('id', None),
                        'away_team_name': match.get('away', {}).get('name', None),
                        'away_team_logo': match.get('away', {}).get('logo', None),
                        'score': match.get('scores', {}).get('score', None),
                        'status': match.get('status', None),
                        'time': match.get('time', None),
                        'location': match.get('location', None),
                        'scheduled': match.get('scheduled', None),
                        'extracted_at': datetime.now().isoformat()
                    }
                    matches_list.append(match_data)
            else:
                # Return empty dataframe with column names
                matches_list = [{
                    'match_id': None,
                    'fixture_id': None,
                    'home_team_id': None,
                    'home_team_name': None,
                    'home_team_logo': None,
                    'away_team_id': None,
                    'away_team_name': None,
                    'away_team_logo': None,
                    'score': None,
                    'ht_score': None,
                    'ft_score': None,
                    'et_score': None,
                    'ps_score': None,
                    'status': None,
                    'time': None,
                    'location': None,
                    'scheduled': None,
                    'competition_id': None,
                    'competition_name': None,
                    'country_id': None,
                    'country_name': None,
                    'federation_name': None,
                    'odds_home_win': None,
                    'odds_draw': None,
                    'odds_away_win': None,
                    'extracted_at': None
                }]
            
            # Ensure directories exist
            os.makedirs(os.path.dirname(SUMMARY_FILE_PATH), exist_ok=True)
            os.makedirs(os.path.dirname(MATCHES_FILE_PATH), exist_ok=True)
            
            # Save summary as JSON
            with open(SUMMARY_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
            
            # Save matches as Parquet
            df = pd.DataFrame(matches_list)
            df.to_parquet(MATCHES_FILE_PATH, index=False)
            
            logger.info(f"Saved summary to {SUMMARY_FILE_PATH}")
            logger.info(f"Saved {len(matches_list)} matches to {MATCHES_FILE_PATH}")
            
        except Exception as e:
            logger.error(f"Error fetching live scores: {str(e)}")
            raise

    @task
    def upload_live_scores_to_adls():
        """
        Upload live scores files to Azure Data Lake Storage
        """
        try:
            # Upload summary JSON
            if os.path.exists(SUMMARY_FILE_PATH):
                summary_blob_name = add_date_suffix(BLOB_NAME) + ".json"
                upload_to_adls(
                    local_file_path=SUMMARY_FILE_PATH,
                    container_name=CONTAINER_NAME,
                    blob_name=summary_blob_name,
                    wasb_conn_id=WASB_CONN_ID
                )
                logger.info(f"Successfully uploaded summary to ADLS: {summary_blob_name}")
            else:
                logger.warning("No summary file found to upload")
            
            # Upload matches Parquet
            if os.path.exists(MATCHES_FILE_PATH):
                matches_blob_name = add_date_suffix(BLOB_NAME) + ".parquet"
                upload_to_adls(
                    local_file_path=MATCHES_FILE_PATH,
                    container_name=CONTAINER_NAME,
                    blob_name=matches_blob_name,
                    wasb_conn_id=WASB_CONN_ID
                )
                logger.info(f"Successfully uploaded matches to ADLS: {matches_blob_name}")
            else:
                logger.warning("No matches file found to upload")
            
        except Exception as e:
            logger.error(f"Error uploading live scores to ADLS: {str(e)}")
            raise

    # Define task dependencies
    fetch_scores = fetch_and_save_live_scores()
    upload_scores = upload_live_scores_to_adls()
    
    fetch_scores >> upload_scores

dag = live_scores_dag()
