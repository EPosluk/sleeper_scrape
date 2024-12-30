import requests
import pandas as pd
import json

def main():
    ######################
    #       SET UP       #
    ######################
    

    ######################
    #       EXTRACT      #
    ######################
    # Get Users Data via Sleeper API
    url = 'https://api.sleeper.app/v1/state/nfl'
    response = requests.get(url)


    ######################
    #     TRANSFORM      #
    ######################
    # Extract data of interest using subset of key/value pairs
    keys_subset = ['week', # week
                    'season', # current season
                    'season_type', # pre, post, regular
                    'season_start_date', # regular season start
                    'leg', # week of regular season
                    'league_season', # active season for leagues
                    'league_create_season'] # flips in December
    state_df = pd.DataFrame([{k: response.json()[k] for k in keys_subset}])
    
    


    ######################
    #        LOAD        #
    ######################
    # Write state dataframe to postgres db



