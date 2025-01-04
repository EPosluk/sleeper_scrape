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
    matchups_keys_subset = {'roster_id': BIGINT(), 
                   'points': FLOAT(), 
                   'players': VARCHAR(), 
                   'starters': VARCHAR(), 
                   'starters_points': VARCHAR(), 
                   'matchup_id': INTEGER(),
                   'week': INTEGER()}
    starters_keys_subset = {'roster_id': INTEGER(),
                            'week': INTEGER(),
                            'QB': INTEGER(),
                            'RB1': INTEGER(),
                            'RB2': INTEGER(),
                            'WR1': INTEGER(),
                            'WR2': INTEGER(),
                            'WR3': INTEGER(),
                            'TE': INTEGER(),
                            'FLEX1': INTEGER(),
                            'FLEX2': INTEGER(),
                            'DEF': VARCHAR()}
    matchups_df = pd.DataFrame(columns=matchups_keys_subset.keys()) # Create empty dataframe for matchup data
    starters_df = pd.DataFrame(columns=starters_keys_subset.keys()) # Create empty dataframe for starters data


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
            row_matchups_df = pd.DataFrame([{k: roster[k] for k in matchups_keys_subset.keys() if k in roster.keys()}])
            row_matchups_df['week'] = week
            row_starters_df = pd.DataFrame(data = [[roster['roster_id']] + [week] + roster['starters']], index = None, columns = starters_keys_subset.keys())
            matchups_df = pd.concat([matchups_df, row_matchups_df], axis=0, ignore_index = True)
            starters_df = pd.concat([starters_df, row_starters_df], axis=0, ignore_index = True)
    

    ######################
    #        LOAD        #
    ######################
    # Write matchups dataframe to postgres db
    matchups_df.to_sql(name='sleeper_matchups', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                          dtype=matchups_keys_subset)
    starters_df.to_sql(name='sleeper_starters', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                          dtype=starters_keys_subset)
    engine.dispose()


if __name__ == "__main__":
    main()




