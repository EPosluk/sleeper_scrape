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
    url = 'https://api.sleeper.app/v1/league/1048244634536857600/users'
    response = requests.get(url)


    ######################
    #     TRANSFORM      #
    ######################
    # Extract data of interest using subset of key/value pairs
    keys_subset = ['user_id', 'display_name'] 
    users_df = pd.DataFrame([{k: v for k, v in d.items() if k in keys_subset} for d in response.json()])
    
    


    ######################
    #        LOAD        #
    ######################
    # Write players dataframe to postgres db


url = 'https://api.sleeper.app/v1/players/nfl'
response = requests.get(url)

