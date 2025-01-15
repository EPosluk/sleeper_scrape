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
    players_df = pd.DataFrame([
        {
            **(value if isinstance(value, dict) else {}),  # Ensure value is a dict
            'rookie_year': value.get('metadata', {}).get('rookie_year') if isinstance(value, dict) and isinstance(value.get('metadata'), dict) else None  # Fully safe extraction
        }
        for key, value in response.json().items()
    ])
    players_df = players_df[keys_subset.keys()]
    players_df['rookie_year'] = pd.to_numeric(players_df['rookie_year'])


    ######################
    #        LOAD        #
    ######################
    # Write players dataframe to postgres db
    players_df.to_sql(name='sleeper_players', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                dtype=keys_subset)
    engine.dispose()


if __name__ == "__main__":
    main()
