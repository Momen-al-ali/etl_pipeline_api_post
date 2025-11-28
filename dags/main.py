from airflow import DAG 
import pendulum 
from datetime import timedelta, datetime
from dags.video_status import get_uploads_playlist_id, get_video_id, extract_video_data,save_to_json
from datawarehouse.dw import staging_table, core_table

timezone = pendulum.timezone("Asia/Amman")



default_args = {
    'owner': 'momenalali',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    'retry_delay': timedelta(minutes=180), # retry after 3 hours
    'start_date': datetime(2025, 11, 29, tzinfo=timezone),
    'catchup': False
}



with DAG (
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to produce JSON file of video IDs from a YouTube channel',
    schedule = "0 13 * * *",
    catchup=False
    ) as dag:
    
    # Define the tasks
    playlist_id = get_uploads_playlist_id()
    video_ids = get_video_id(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)
    
    
    
    # dependencies
    
    playlist_id >> video_ids >> extracted_data >> save_to_json_task



with DAG (
    dag_id='db_updated',
    default_args=default_args,
    description='A DAG to process the json file and insert into staging and core',
    schedule = "0 15 * * *",
    catchup=False
    ) as dag:
    
    # Define the tasks
    staging = staging_table()
    core = core_table()
    
    # dependencies
    
    staging >> core


