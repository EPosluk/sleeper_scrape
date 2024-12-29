import requests
import pandas as pd
import json

def main():
    ######################
    #       SET UP       #
    ######################

    url = 'https://api.sleeper.app/v1/league/1048244634536857600/matchups/1'
    response = requests.get(url)
    response.json()

    response = requests.get(url)
    keys_subset = ['points', 'players', 'roster_id', 'starters', 'starters_points'] 
    matchups_df = pd.DataFrame(columns=keys_subset) # Create empty dataframe with columns of interest

    ######################
    #     TRANSFORM      #
    ######################
    # Flatten nested dictionary to a dataframe
    json_data = json.loads(response.content)
    for matchup in json_data:
        keys_dict = {k: matchup[k] for k in keys_subset}
        row_df = pd.DataFrame([keys_dict])
        matchups_df = pd.concat([matchups_df, row_df], axis=0, ignore_index = True)
    


    ######################
    #        LOAD        #
    ######################
    # Write players dataframe to postgres db


