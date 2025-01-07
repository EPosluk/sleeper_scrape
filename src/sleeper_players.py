import pandas as pd
import requests 
import psycopg
import config
from sqlalchemy import create_engine, Date
from sqlalchemy.types import INTEGER, VARCHAR, BOOLEAN

def main():
    ######################
    #       SET UP       #
    ######################
    # SQL Alchemy engine creation
    engine = create_engine(config.connection_string)
    keys_subset = {'search_full_name': VARCHAR(),
        'weight': VARCHAR(),
        'years_exp': INTEGER(),
        'search_last_name': VARCHAR(),
        'active': BOOLEAN(),
        'last_name': VARCHAR(),
        'injury_body_part': VARCHAR(),
        'first_name': VARCHAR(),
        'depth_chart_position': VARCHAR(),
        'depth_chart_order': INTEGER(),
        'status': VARCHAR(),
        'age': INTEGER(),
        'full_name': VARCHAR(),
        'injury_status': VARCHAR(),
        'search_rank': INTEGER(),
        'search_first_name': VARCHAR(),
        'birth_date': Date(),
        'college': VARCHAR(),
        'high_school': VARCHAR(),
        'height': VARCHAR(),
        'position': VARCHAR(),
        'stats_id': INTEGER(),
        'fantasy_positions': VARCHAR(),
        'player_id': VARCHAR(),
        'team': VARCHAR(),
        'number': INTEGER(),
        'rookie_year': INTEGER()}
    players_df = pd.DataFrame(columns=keys_subset.keys()) # Create empty dataframe with columns of interest
    


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
    for key, value in response.json().items():
        temp_df = pd.DataFrame({k: v for k, v in value.items() if k in keys_subset.keys()})
        if 'rookie_year' in value['metadata'].keys():
            temp_df['rookie_year'] = value['metadata']['rookie_year']
        players_df = pd.concat([players_df, temp_df], axis=0, ignore_index = True)

    players_df = pd.DataFrame([{k: v for k, v in e.items() if k in keys_subset.keys()} for d, e in response.json().items()])
    rookie_year_df = pd.DataFrame([{'rookie_year'}])


    ######################
    #        LOAD        #
    ######################
    # Write players dataframe to postgres db
    players_df.to_sql(name='sleeper_players', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                dtype=keys_subset)
    engine.dispose()


if __name__ == "__main__":
    main()
