# Project: Create box score metrics
# Description: Create box score metrics from play-by-play data
# Data Sources: Play-by-play data
# Last Updated: 2/15/20

import numpy as np
import pandas as pd
from query_pbp import query_table



if __name__=='__main__':
    # Query 2019 play-by-play data
    # pbp_2020 = query_table('pbp_2020')

    # Create toy dataset and write to temp table
    # pbp_toy = pbp_2020[pbp_2020['game_id'].isin([21900750, 21900763])]
    # pbp_toy.to_csv('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/tmp/pbp_toy', index=False)

    # Read in toy dataset
    pbp_df = pd.read_csv('/Users/chrisfeller/Desktop/Play_by_Play_Scraper/data/tmp/pbp_toy')
