import requests
import pandas as pd
import psycopg
import config
from sqlalchemy import create_engine, Date
from sqlalchemy.types import INTEGER, Float, VARCHAR


def main():
    ######################
    #       SET UP       #
    ######################
    # SQL Alchemy engine creation
    engine = create_engine(config.connection_string)


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
    keys_subset = {'week': INTEGER(), # week
                    'season': INTEGER(), # current season
                    'season_type': VARCHAR(length=255), # pre, post, regular
                    'season_start_date': Date, # regular season start
                    'leg': INTEGER(), # week of regular season
                    'league_season': INTEGER(), # active season for leagues
                    'league_create_season': INTEGER()} # flips in December
    state_df = pd.DataFrame([{k: response.json()[k] for k in keys_subset.keys()}])


    ######################
    #        LOAD        #
    ######################
    # Write state dataframe to postgres db
    state_df.to_sql(name='sleeper_state', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                    dtype=keys_subset)
    engine.dispose()


if __name__ == "__main__":
    main()