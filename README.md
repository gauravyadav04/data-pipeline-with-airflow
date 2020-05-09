# Data Pipeline with Airflow
Data Engineering Nanodegree Program - Project 5


The objective of this project is to build high grade dynamic data pipeline allowing for easy monitoring and data backfills. The ETL pipeline extracts song and events log data from S3 bucket, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights about user activity on their app.


## Data
* Songs metadata: collection of JSON files that describes the songs such as title, artist name, duration, year, etc.
* Logs data: collection of JSON files where each file covers the users activities over a given day.


## Methodology
We'll build the database by optimizing the tables around efficient reads for complex queries. To do that, Star schema will be used utilizing dimensional modeling as follows:

* Fact table: songplays
* Dimensions tables: songs, artist, users, time

![StarSchema](https://user-images.githubusercontent.com/6285945/76783209-75c9a400-67d7-11ea-8594-9710cae17048.PNG)

The three most important advantages of using Star schema are:

* Denormalized tables
* Simplified queries
* Fast aggregation


## Pipeline Components

* **create_tables.sql**: SQL commands for building required tables
* **Dags**: sparkify_data_pipeline.py - DAG defining pipeline tasks and their execution relationships
* **Plugins**:
  * **helpers**: sql_queries.py contains SQL queries used in ETL process
  * **operators**:
    * stage_redshift.py contains StageToRedshiftOperator, which loads JSON data from S3 to staging tables in the Redshift 
    * load_dimension.py contains LoadDimensionOperator, which loads a dimension table from data in the staging table(s)
    * load_fact.py contains LoadFactOperator, which loads a fact table from data in the staging table(s)
    * data_quality.py contains DataQualityOperator, which runs a data quality check by passing an SQL query and expected result as arguments, failing if the results don't match
