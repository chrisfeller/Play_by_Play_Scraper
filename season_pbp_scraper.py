# Project: Scrape NBA Play-by-Play Data
# Description: Scrape NBA's play-by-play data for completed seasons using
# the nba_scraper library (https://github.com/mcbarlowe/nba_scraper)
# Data Sources: Play-by-play data
# Last Updated: 2/4/20

import pandas as pd
import os
import nba_scraper.nba_scraper as ns
import psycopg2
from sqlalchemy import create_engine


def scrape_complete_season(season):
    """
    Scrape complete season of play-by-play data. Save resulting .csv file as
    temporary staging table in local directory before moving to postgres database.

    Args:
        season (int): 'YYYY' format of season to scrape. Input the ending 'YYYY'
        of the season (example: '2019' for the 2018-2019 season).
    Returns:
        None: Saves resulting .csv file to temporary local directory /data.
    """
    # scrape full season and save to
    ns.scrape_season(season,
                    data_format='csv',
                    data_dir='/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/')

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
        df (pandas DataFrame): Pandas DataFrame from output of `scrape_complete_season`
        function.

    Returns:
        None
    """
    # Create sqlalchemy engine to connect to postgres database
    engine = create_engine('postgresql+psycopg2://chrisfeller:postgres_password@localhost:5432/play_by_play')
    # Insert pandas dataframe into postgres table
    df.to_sql(table, engine, if_exists='append')

def update_pbp_postgres(season):
    """
    Update postgres table with data. Calls helper functions `connect` and `create_table`
    to create a table in play_by_play database in postgres and insert data
    into the resulting table.

    Args:
        season (int): 'YYYY' format of season to scrape. Input the ending 'YYYY'
        of the season (example: '2019' for the 2018-2019 season).

    Returns:
        None
    """
    # Read in temporary staging table
    df = pd.read_csv('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/nba{0}.csv'.format(season))
    # Connect to play_by_play database in postgres
    connect()
    # Create table for input season
    create_table('pbp_{0}'.format(season), df)
    # Remove temporary staging table
    remove_staging_table(season)

def remove_staging_table(season):
    """
    Remove temporary staging table with .csv file of input season's play-by-play
    data.

    Args:
        season (int): 'YYYY' format of season to scrape. Input the ending 'YYYY'
        of the season (example: '2019' for the 2018-2019 season).

    Returns:
        None
    """
    os.remove('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/nba{0}.csv'.format(season))


if __name__=='__main__':
    # Scrape 2018-2019 season
    scrape_complete_season(2019)
    # Insert 2018-2019 data into play_by_play table in postgres
    update_pbp_postgres(2019)
