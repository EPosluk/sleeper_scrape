import requests
import pandas as pd
import psycopg
import config
from sqlalchemy import create_engine, Date
from sqlalchemy.types import INTEGER, VARCHAR, BIGINT, VARCHAR, FLOAT

def main():
    ######################
    #       SET UP       #
    ######################
    engine = create_engine(config.connection_string)
    # Subsets of keys and child keys to extract
    keys_subset = ['league_id', 'players', 'reserve', 'roster_id', 'starters', 'taxi'] 
    metadata_subset = ['record', 'streak']
    settings_subset = ['division', 'fpts', 'fpts_against', 'losses', 'ppts', 'ties', 'waiver_budget_used', 'waiver_position', 'wins']
    rosters_df = pd.DataFrame(columns=keys_subset + metadata_subset + settings_subset) # Create empty dataframe with columns of interest
    rosters_dict = {}

    ######################
    #       EXTRACT      #
    ######################
    url = 'https://api.sleeper.app/v1/league/964039299760967680/rosters'
    response = requests.get(url)

    ######################
    #     TRANSFORM      #
    ######################
    # Flatten nested dictionary to a dataframe
    for roster in response.json():
        keys_dict = {k: roster[k] for k in keys_subset}
        metadata_dict = {k: roster['metadata'][k] for k in metadata_subset}
        settings_dict = {k: roster['settings'][k] for k in settings_subset}
        rosters_dict = keys_dict | metadata_dict | settings_dict
        row_df = pd.DataFrame([rosters_dict])
        rosters_df = pd.concat([rosters_df, row_df], axis=0, ignore_index = True)
    


    ######################
    #        LOAD        #
    ######################
    # Write rosters dataframe to postgres db


