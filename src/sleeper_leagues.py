import requests
import pandas as pd

def main():
    ######################
    #       SET UP       #
    ######################
    # Place most recent year in league_id parameter
    league_id = 1048244634536857600
    keys_subset = ['season', 'name', 'league_id', 'draft_id', 'bracket_id', 'loser_bracket_id'] 
    leagues_df = pd.DataFrame(columns=keys_subset) # Create empty dataframe with columns of interest

    while league_id is not None:
        ######################
        #       EXTRACT      #
        ######################
        # Get League Data via Sleeper API
        url = 'https://api.sleeper.app/v1/league/' + str(league_id)
        response = requests.get(url)

        ######################
        #     TRANSFORM      #
        ######################
        # Get new row of data
        league_id_row = pd.DataFrame([{k: response.json()[k] for k in keys_subset}])
        # Append new row to dataframe
        leagues_df = pd.concat([leagues_df, league_id_row], axis=0, ignore_index = True)

        # Loop to previous year's league_id
        league_id = response.json()['previous_league_id']

        ######################
        #        LOAD        #
        ######################
        # Write leagues dataframe to postgres db

