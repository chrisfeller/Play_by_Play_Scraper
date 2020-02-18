# Project: Create box score metrics
# Description: Create box score metrics from play-by-play data
# Data Sources: Play-by-play data
# Last Updated: 2/17/20

import numpy as np
import pandas as pd
from query_pbp import query_table



if __name__=='__main__':
    # Query 2019 play-by-play data
    # pbp_2020 = query_table('pbp_2020')

    # Create toy dataset and write to temp table
    # pbp_toy = pbp_2020[pbp_2020['game_id'].isin([21900750, 21900763])]
    # pbp_toy.to_csv('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/tmp/pbp_toy.csv', index=False)

    # Read in toy dataset
    pbp_df = (pd.read_csv('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/tmp/pbp_toy.csv')\
                # isolate one game as example
                .query("game_id == 21900750")\
                # create column with seconds remaining in the quarter
                .assign(qrt_time_remaining = lambda x: [int(m) * 60 + int(s) for m, s in  x['pctimestring'].str.split(':')])\
                # sort dataframe by game id, period, time remaining in quarter, and event number
                .sort_values(by=['game_id', 'period', 'qrt_time_remaining', 'eventnum'], ascending=[True, True, False, True])\
                [['game_date',
                'game_id',
                'eventnum',
                'period',
                'pctimestring',
                'homedescription',
                'visitordescription',
                'score',
                'scoremargin',
                'home_team_abbrev',
                'away_team_abbrev',
                'event_type_de',
                'shot_made',
                'is_block',
                'shot_type',
                'seconds_elapsed',
                'qrt_time_remaining',
                'event_length',
                'is_three',
                'points_made',
                'is_o_rebound',
                'is_d_rebound',
                'is_turnover',
                'is_steal',
                'foul_type',
                'is_putback']])

    # Define possessions
    # Filter to event types that end possessions and remove technicals
    poss_df = pbp_df.query("event_type_de in ['rebound', 'shot', 'missed_shot', 'turnover', 'free-throw']")\
                    .query("homedescription.str.contains('Technical')==False or visitordescription.str.contains('Technical')==False")

    # Define heaves
    # Possessions that start with 4 or fewer seconds on the game clock at the
    # end of one of the first three quarters.
    # pbp_df['heave'] = np.where((pbp_df['period'].isin([1, 3])) & (pbp_df['pctimestring']<=0:04)
