import requests
import pandas as pd
import psycopg
import config
from sqlalchemy import create_engine
from sqlalchemy.types import INTEGER, VARCHAR, BIGINT

def main():
    ######################
    #       SET UP       #
    ######################
    # SQL Alchemy engine creation
    engine = create_engine(config.connection_string)
    original_league_id = 964039300688011264 # Original league ID by which to reference
    previous_league_id = None # Initialize previous years league ID to none
    start_year = 2023 # Year of the original league
    latest_year = pd.read_sql_query('SELECT season FROM sleeper.sleeper_state', engine)['season'][0] # Query to get current NFL season
    keys_subset = {'season': INTEGER(), 
                    'name':  VARCHAR(length=255),
                    'league_id': BIGINT(),
                    'draft_id': BIGINT(),
                    'bracket_id': BIGINT(),
                    'loser_bracket_id': BIGINT()} # Subset of keys to extract
    leagues_df = pd.DataFrame(columns=keys_subset) # Create empty dataframe with columns of interest

    for year in range(start_year, latest_year+1):
        ######################
        #       EXTRACT      #
        ######################
        # Get League Data via Sleeper API
        url = 'https://api.sleeper.app/v1/user/964304712390467584/leagues/nfl/' + str(year)
        response = requests.get(url)

        for league_year in response.json():
            if league_year['previous_league_id'] == previous_league_id or league_year['league_id'] == original_league_id:
                ######################
                #     TRANSFORM      #
                ######################
                # Get new row of data
                league_id_row = pd.DataFrame([{k: league_year[k] for k in keys_subset.keys()}])
                # Append new row to dataframe
                leagues_df = pd.concat([leagues_df, league_id_row], axis=0, ignore_index = True)

                # Loop to previous year's league_id
                previous_league_id = league_year['league_id']

        ######################
        #        LOAD        #
        ######################
        # Write leagues dataframe to postgres db
        leagues_df.to_sql(name='sleeper_leagues', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                          dtype=keys_subset)
        engine.dispose()


if __name__ == "__main__":
    main()
