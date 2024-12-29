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
    # Get Players Data via Sleeper API
    url = 'https://api.sleeper.app/v1/players/nfl'
    response = requests.get(url)


    ######################
    #     TRANSFORM      #
    ######################
    # Flatten nested dictionary to a dataframe
    json_data = json.loads(response.content)
    players = [{**{'player_id':k1}, **v1} for k1, v1 in json_data.items()]
    players_df = pd.DataFrame(players)


    ######################
    #        LOAD        #
    ######################
    # Write players dataframe to postgres db



