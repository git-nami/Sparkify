from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    #'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    aws_region="us-west-2",
    json_copy_mode="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    aws_region="us-west-2",
    json_copy_mode="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_table_statement=SqlQueries.songplay_table_insert,
    table="public.songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_table_statement=SqlQueries.user_table_insert,
    table="public.users",
    truncate_ind="Y"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_table_statement=SqlQueries.song_table_insert,
    table="public.songs",
    truncate_ind="Y"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_table_statement=SqlQueries.artist_table_insert,
    table="public.artists",
    truncate_ind="Y"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_table_statement=SqlQueries.time_table_insert,
    table="public.time",
    truncate_ind="Y"
)

test_cases_dict={"select count(*) from public.staging_events" : 1, 
            "select count(*) from public.staging_songs" : 1,
            "select count(*) from public.songplays" : 1,
            "select count(*) from public.users" : 1,
            "select count(*) from public.songs" : 1,
            "select count(*) from public.artists" : 1, 
            "select count(*) from public.time" : 1
           }

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    test_cases=test_cases_dict)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Run individual tasks in the order below:

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
