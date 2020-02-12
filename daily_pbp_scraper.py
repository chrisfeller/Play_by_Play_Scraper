# Project: Query Play-by-Play Data from Postgres Database
# Description: Query play-by-play data from postgres database into
# a pandas dataframe to use in subsecquent analysis and models.
# Data Sources: Play-by-play data
# Last Updated: 2/4/20

import pandas as pd
import os
import nba_scraper.nba_scraper as ns
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def scrape_previous_day():
    """
    Scrape play-by-play data for games on the previous day. Save resulting .csv file as
    temporary staging table in local directory before moving to postgres database.

    Args:
        None
    Returns:
        None: Saves resulting .csv file to temporary local directory /data.
    """
    # Create string variable with date of previous day
    previous_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    # Scrape play-by-play data for previous day's games
    ns.scrape_date_range(previous_date, previous_date,
                        data_format='csv',
                        data_dir='/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/{0}.csv'.format(previous_date.replace('-', '')))

def connect():
    """
    Connect to play_by_play postgres database using psycopg2 library.

    Args:
        None

    Returns:
        conn: postgres connection to play_by_play database
    """
    # Define connection parameters
    cons = "dbname='play_by_play' user='chrisfeller' host='localhost' password='postgres_password'"
    # Try connection
    try:
        conn = psycopg2.connect(cons)
        print("Connected")
    # Print error if connection fails
    except:
        print("Unable to connect to the database")
    return conn

def create_table(table, df):
    """
    Create table in play_by_play database in postgres from pandas dataframe.

    Args:
        table (str): Name of table to create in play_by_play database.
        df (pandas DataFrame): Pandas DataFrame from output of `scrape_previous_day`
        function.

    Returns:
        None
    """
    # Create sqlalchemy engine to connect to postgres database
    engine = create_engine('postgresql+psycopg2://chrisfeller:postgres_password@localhost:5432/play_by_play')
    # Insert pandas dataframe into postgres table
    df.to_sql(table, engine, if_exists='append')

def update_pbp_postgres():
    """
    Update postgres table with data. Calls helper functions `connect` and `create_table`
    to create a table in play_by_play database in postgres and insert data
    into the resulting table.

    Args:
        None

    Returns:
        None
    """
    # Create string variable with date of previous day
    previous_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    # Read in temporary staging table
    df = pd.read_csv('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/{0}.csv'.format(previous_date.replace('-', '')))
    # Connect to play_by_play database in postgres
    connect()
    # Create table for input season
    create_table('pbp_2020', df)

def remove_staging_table():
    """
    Remove temporary staging table with .csv file of input season's play-by-play
    data.

    Args:
        season (int): 'YYYY' format of season to scrape. Input the ending 'YYYY'
        of the season (example: '2019' for the 2018-2019 season).

    Returns:
        None
    """
    # Create string variable with date of previous day
    previous_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    os.remove('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/{0}.csv'.format(previous_date.replace('-', '')))

# Define dags
dag = DAG('scrape_previous_day', description='Scrape pbp from previous day',
          schedule_interval='@once',
          start_date=datetime(2020, 2, 11), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

scrape_operator = PythonOperator(task_id='scrape_task', python_callable=scrape_previous_day, dag=dag)

to_postgres_operator = PythonOperator(task_id='to_postgres_task', python_callable=update_pbp_postgres, dag=dag)

remove_staging_table_operator = PythonOperator(task_id='remove_staging_table_task', python_callable=remove_staging_table, dag=dag)

# Define dag flow
dummy_operator >> scrape_operator >> to_postgres_operator >> remove_staging_table_operator
