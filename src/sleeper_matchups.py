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
    keys_subset = {'roster_id': BIGINT(), 
                   'points': FLOAT(), 
                   'players': VARCHAR(), 
                   'starters': VARCHAR(), 
                   'starters_points': VARCHAR(), 
                   'matchup_id': INTEGER()}
    matchups_df = pd.DataFrame(columns=keys_subset.keys()) # Create empty dataframe with columns of interest


    for week in range(1,19):
        ######################
        #       EXTRACT      #
        ######################
        url = 'https://api.sleeper.app/v1/league/1048244634536857600/matchups/' + str(week)
        response = requests.get(url)
        response.json()

        ######################
        #     TRANSFORM      #
        ######################
        # Flatten list of dictionaries to a dataframe
        for roster in response.json():
            row_df = pd.DataFrame([{k: roster[k] for k in keys_subset.keys()}])
            row_df['week'] = week
            matchups_df = pd.concat([matchups_df, row_df], axis=0, ignore_index = True)
    

    ######################
    #        LOAD        #
    ######################
    # Write matchups dataframe to postgres db
    matchups_df.to_sql(name='sleeper_matchups', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                          dtype=keys_subset)
    engine.dispose()


if __name__ == "__main__":
    main()




