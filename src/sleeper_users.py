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
    leagues = pd.read_sql_query('SELECT league_id FROM sleeper.sleeper_leagues', engine)
    keys_subset = {'league_id': BIGINT(),
                   'user_id': BIGINT(), 
                   'display_name': VARCHAR(length = 255)
                   } # Keys of interest from users API call
    users_df = pd.DataFrame(columns=keys_subset.keys()) # Create empty dataframe with columns of interest

    
    for league in leagues['league_id']:
        ######################
        #       EXTRACT      #
        ######################
        url = 'https://api.sleeper.app/v1/league/' + str(league) + '/users'
        response = requests.get(url)


        ######################
        #     TRANSFORM      #
        ######################
        temp_df = pd.DataFrame([{k: v for k, v in d.items() if k in keys_subset.keys()} for d in response.json()])
        temp_df['league_id'] = league
        users_df = pd.concat([users_df, temp_df], axis=0, ignore_index = True)
    
    


    ######################
    #        LOAD        #
    ######################
    # Write users dataframe to postgres db
    users_df.to_sql(name='sleeper_users', con=engine, schema = 'sleeper', if_exists = 'replace', index=False, dtype = keys_subset)
    engine.dispose()


if __name__ == "__main__":
    main()

