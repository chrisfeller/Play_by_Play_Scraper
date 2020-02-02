import pandas as pd
import nba_scraper.nba_scraper as ns
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def scrape_previous_day():
    previous_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    ns.scrape_date_range(previous_date, previous_date,
                        data_format='csv',
                        data_dir='/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/{0}.csv'.format(previous_date.replace('-', '')))

def connect():
    cons = "dbname='play_by_play' user='chrisfeller' host='localhost' password='postgres_password'"
    try:
        conn = psycopg2.connect(cons)
        print("Connected")
    except:
        print("Unable to connect to the database")
    return conn


def create_table(table, df):
    engine = create_engine('postgresql+psycopg2://chrisfeller:postgres_password@localhost:5432/play_by_play')
    df.to_sql(table, engine, if_exists='append')

def update_pbp_postgres():
    previous_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    df = pd.read_csv('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/{0}.csv'.format(previous_date.replace('-', '')))
    connect()
    create_table('play_by_play', df)

dag = DAG('scrape_previous_day', description='Scrape pbp from previous day',
          schedule_interval='@once',
          start_date=datetime(2020, 1, 27), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

scrape_operator = PythonOperator(task_id='scrape_task', python_callable=scrape_previous_day, dag=dag)

to_postgres_operator = PythonOperator(task_id='to_postgres_task', python_callable=update_pbp_postgres, dag=dag)

dummy_operator >> scrape_operator >> to_postgres_operator



##########################
# Query Data or reset play_by_play table with first day (20200127)
#########################
import pandas as pd
import nba_scraper.nba_scraper as ns
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import getpass

def connect():
    cons = "dbname='play_by_play' user='chrisfeller' host='localhost' password='postgres_password'"
    try:
        conn = psycopg2.connect(cons)
        print("Connected")
    except:
        print("Unable to connect to the database")
    return conn


def create_table(table, df):
    engine = create_engine('postgresql+psycopg2://chrisfeller:postgres_password@localhost:5432/play_by_play')
    df.to_sql(table, engine, if_exists='append')

def update_pbp_postgres():
    df = pd.read_csv('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/20200127.csv')
    connect()
    create_table('play_by_play', df)

def query_table(table):
    engine = create_engine('postgresql+psycopg2://chrisfeller:postgres_password@localhost:5432/play_by_play')
    df = pd.read_sql_table(table, engine)
    return df
