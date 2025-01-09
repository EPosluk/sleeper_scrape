import pandas as pd
import requests 
import psycopg
import config
from sqlalchemy import create_engine, Date
from sqlalchemy.types import INTEGER, VARCHAR, BOOLEAN, FLOAT
import time

def main():
    ######################
    #       SET UP       #
    ######################
    # SQL Alchemy engine creation
    engine = create_engine(config.connection_string)
    keys_subset = {'week': INTEGER(),
        'date': Date(),
        'bonus_rec_yd_100': INTEGER(),
        "pos_rank_std": INTEGER(),
        "gp": INTEGER(),
        "tm_def_snp": INTEGER(),
        "gms_active": INTEGER(),
        "rec_td": INTEGER(),
        "rec_ypt": FLOAT(),
        "rec_20_29": INTEGER(),
        "pos_rank_half_ppr": INTEGER(),
        "bonus_fd_wr": INTEGER(),
        "pts_std": FLOAT(),
        "rec_lng": INTEGER(),
        "tm_st_snp": INTEGER(),
        "bonus_rush_rec_yd_100": INTEGER(),
        "rec": INTEGER(),
        "rec_yar": INTEGER(),
        "rec_fd": INTEGER(),
        "rec_5_9": INTEGER(),
        "bonus_rec_wr": INTEGER(),
        "gs": INTEGER(),
        "tm_off_snp": INTEGER(),
        "pos_rank_ppr": INTEGER(),
        "rec_rz_tgt": INTEGER(),
        "rec_40p": INTEGER(),
        "off_snp": INTEGER(),
        "pts_half_ppr": FLOAT(),
        "rec_air_yd": INTEGER(),
        "rec_ypr": FLOAT(),
        "rec_10_19": INTEGER(),
        "rec_td_40p": INTEGER(),
        "rec_td_lng": INTEGER(),
        "rec_td_50p": INTEGER(),
        "rush_rec_yd": INTEGER(),
        "anytime_tds": INTEGER(),
        "rec_tgt": INTEGER(),
        "pts_ppr": FLOAT(),
        "rec_yd": FLOAT(),
        "category": "stat",
        "week": INTEGER(),
        "season": VARCHAR(),
        "season_type": VARCHAR(),
        "player_id": VARCHAR(),
        "game_id": VARCHAR(),
        "team": VARCHAR(),
        "opponent": VARCHAR()}
    player_id = 5859; # Placeholder for player_id, this will be looped through from query of active players
    season = 2024; #Placeholder for season, this will be looped through from rookie year to current season
    player_id_query = '''
        select a.player_id, sp.rookie_year
        from 
            (select player_id
            from sleeper.sleeper_players
            where team is not null 
            and position in ('QB','RB','WR','TE','DEF') and 
            depth_chart_position is not null
            union 
            select player_id
            from sleeper.sleeper_player_points) a
        left join
        (select player_id, rookie_year
        from sleeper.sleeper_players) sp
        ON a.player_id = sp.player_id
        '''
    player_id_info = pd.read_sql_query(player_id_query, engine) # Query to get current NFL season
    latest_year = pd.read_sql_query('SELECT MAX(season) as season FROM sleeper.sleeper_state', engine)['season'][0] # Query to get current NFL season
    


    ######################
    #       EXTRACT      #
    ######################
    # Get Players Data via Sleeper API
    for player in player_id_info.iterrows():
        for season in range(player['rookie_year'], latest_year+1):
            print(player_id, season)
        break
    url = 'https://api.sleeper.com/stats/nfl/player/' + str(player_id) '?season_type=regular&season=' + str(season) + '&grouping=week'
    response = requests.get(url)
    time.sleep()

    ######################
    #     TRANSFORM      #
    ######################
    # Flatten nested dictionary to a dataframe
    players_df = pd.DataFrame([{k: v for k, v in e.items() if k in keys_subset.keys()} for d, e in response.json().items()])


    ######################
    #        LOAD        #
    ######################
    # Write players dataframe to postgres db
    players_df.to_sql(name='sleeper_players', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                dtype=keys_subset)
    engine.dispose()


if __name__ == "__main__":
    main()
