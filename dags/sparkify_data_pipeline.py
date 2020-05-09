from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
				PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Gaurav',
    'start_date': datetime(2019, 1, 12),
	'depends_on_past': False
}

with DAG(dag_id='sparkify_data_pipeline', default_args=args, schedule_interval='0 0 * * *', schedule_interval=None, description='Load and transform data in Redshift with Airflow') as dag:

	start_operator = DummyOperator(task_id='Begin_execution')
	
	create_tables = PostgresOperator(
		task_id="create_tables",
		postgres_conn_id="redshift",
		sql="create_tables.sql"
	)

	stage_events_to_redshift = StageToRedshiftOperator(
		task_id='Stage_events',
		redshift_conn_id='redshift',
		aws_credentials_id='aws_credentials',
		table='staging_events',
		s3_bucket='udacity-dend',
		s3_key='log_data',
		copy_json_option='s3://udacity-dend/log_json_path.json',
		region='us-west-2'
	)
	
	stage_songs_to_redshift = StageToRedshiftOperator(
		task_id='Stage_songs',
		redshift_conn_id='redshift',
		aws_credentials_id='aws_credentials',
		table='staging_songs',
		s3_bucket='udacity-dend',
		s3_key='song_data',
		copy_json_option='auto',
		region='us-west-2'
	)

	load_songplays_table = LoadFactOperator(
		task_id='Load_songplays_fact_table',
		redshift_conn_id='redshift',
		table='songplays',
		select_sql=SqlQueries.songplay_table_insert
	)
	
	
	load_user_dimension_table = LoadDimensionOperator(
		task_id='Load_user_dimension_table',
		redshift_conn_id='redshift',
		table='users',
		select_sql=SqlQueries.user_table_insert
	)
	
	load_song_dimension_table = LoadDimensionOperator(
		task_id='Load_song_dimension_table',
		redshift_conn_id='redshift',
		table='songs',
		select_sql=SqlQueries.song_table_insert
	)

	load_artist_dimension_table = LoadDimensionOperator(
		task_id='Load_artist_dimension_table',
		redshift_conn_id='redshift',
		table='artists',
		select_sql=SqlQueries.artist_table_insert,
		append_insert=True,
		primary_key="artistid"
	)

	load_time_dimension_table = LoadDimensionOperator(
		task_id='Load_time_dimension_table',
		redshift_conn_id='redshift',
		table='time',
		select_sql=SqlQueries.time_table_insert
    	)

	run_quality_checks = DataQualityOperator(
		task_id='Run_data_quality_checks',
		redshift_conn_id='redshift',
		test_query='select count(*) from songs where songid is null;',
		expected_result=0
    	)

	end_operator = DummyOperator(task_id='Stop_execution')
	
	start_operator >> create_tables
	create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
	load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
	run_quality_checks >> end_operator
