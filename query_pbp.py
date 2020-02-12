# Project: Query Play-by-Play Data from Postgres Database
# Description: Query play-by-play data from postgres database into
# a pandas dataframe to use in subsecquent analysis and models.
# Data Sources: Play-by-play data
# Last Updated: 2/4/20

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

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

def query_table(table):
    """
    Queries table in play_by_play database in postgres and returns a pandas
    dataframe for analysis.

    Args:
        table (str): name of table in play_by_play database in postgres .

    Returns:
        df (pandas DataFrame): pandas DataFrame with play-by-play data
        from specified table.
    """
    # Connect to play_by_play database in postgres
    connect()
    # Create sqlalchemy engine to connect to postgres database
    engine = create_engine('postgresql+psycopg2://chrisfeller:postgres_password@localhost:5432/play_by_play')
    # Query table
    df = pd.read_sql_table(table, engine)
    return df


if __name__=='__main__':
    # Query 2019 play-by-play data
    pbp_2020 = query_table('pbp_2020')
