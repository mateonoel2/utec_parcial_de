from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import pytz
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
import requests
import pandas as pd
import logging
import os

# Configure logging
logger = logging.getLogger(__name__)

# File and Azure configuration
TODAY_UTC = datetime.now(timezone.utc).strftime('%Y-%m-%d')
LOCAL_FILE_PATH = f"/opt/airflow/data/{TODAY_UTC}.parquet"
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"
BLOB_NAME = "raw/airflow/G30/api/fixtures/champions_league/"

# Champions League competition ID
CHAMPIONS_LEAGUE_ID = 244

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="dag_live_fixtures",
    description="Fetch Champions League fixtures and upload to ADLS",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=pytz.timezone("America/Bogota")),
    schedule="0 6 * * *",  # Daily at 6 AM
    catchup=False,
    tags=["champions_league", "fixtures", "adls_upload"],
)
def fixtures_dag():

    @task
    def fetch_and_save_fixtures():
        """
        Fetch Champions League fixtures from API and save as parquet
        """
        try:
            # Get API credentials
            api_key = Variable.get('live_score_api_key')
            api_secret = Variable.get('live_score_api_secret')
            
            # API endpoint for fixtures
            base_url = "https://livescore-api.com/api-client/fixtures/matches.json"
            
            params = {
                'key': api_key,
                'secret': api_secret,
                'competition_id': CHAMPIONS_LEAGUE_ID
            }
            
            logger.info(f"Fetching Champions League fixtures (competition_id: {CHAMPIONS_LEAGUE_ID})")
            
            # Make API request
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('success'):
                raise Exception(f"API returned error: {data.get('error', 'Unknown error')}")
            
            fixtures = data.get('data', {}).get('fixtures', [])
            logger.info(f"Successfully fetched {len(fixtures)} fixtures")
            
            if not fixtures:
                logger.warning("No fixtures found for Champions League")
                return
            
            # Transform data for parquet
            fixtures_list = []
            for fixture in fixtures:
                fixture_data = {
                    'id': fixture.get('id'),
                    'home_id': fixture.get('home_id'),
                    'home_name': fixture.get('home_name'),
                    'away_id': fixture.get('away_id'),
                    'away_name': fixture.get('away_name'),
                    'date': fixture.get('date'),
                    'time': fixture.get('time'),
                    'location': fixture.get('location'),
                    'round': fixture.get('round'),
                    'competition_id': fixture.get('competition_id'),
                    'group_id': fixture.get('group_id'),
                    'home_win_odds': fixture.get('odds', {}).get('pre', {}).get('1'),
                    'draw_odds': fixture.get('odds', {}).get('pre', {}).get('X'),
                    'away_win_odds': fixture.get('odds', {}).get('pre', {}).get('2'),
                    'extracted_at': datetime.now().isoformat()
                }
                fixtures_list.append(fixture_data)
            
            # Create DataFrame and save as parquet
            df = pd.DataFrame(fixtures_list)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(LOCAL_FILE_PATH), exist_ok=True)
            
            # Save as parquet
            df.to_parquet(LOCAL_FILE_PATH, index=False)
            logger.info(f"Saved {len(fixtures_list)} fixtures to {LOCAL_FILE_PATH}")
            
        except Exception as e:
            logger.error(f"Error fetching fixtures: {str(e)}")
            raise

    @task
    def upload_fixtures_to_adls():
        """
        Upload fixtures parquet file to Azure Data Lake Storage
        """
        try:
            new_blob_name = add_date_suffix(BLOB_NAME)
            new_blob_name = new_blob_name + ".parquet"
            
            upload_to_adls(
                local_file_path=LOCAL_FILE_PATH,
                container_name=CONTAINER_NAME,
                blob_name=new_blob_name,
                wasb_conn_id=WASB_CONN_ID
            )
            
            logger.info(f"Successfully uploaded fixtures to ADLS: {new_blob_name}")
            
        except Exception as e:
            logger.error(f"Error uploading fixtures to ADLS: {str(e)}")
            raise

    # Define task dependencies
    fetch_fixtures = fetch_and_save_fixtures()
    upload_fixtures = upload_fixtures_to_adls()
    
    fetch_fixtures >> upload_fixtures

dag = fixtures_dag() 