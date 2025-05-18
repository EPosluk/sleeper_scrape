import pandas as pd
import requests 
import psycopg
import config
from sqlalchemy import create_engine, Date, text
from sqlalchemy.types import INTEGER, VARCHAR, BOOLEAN, FLOAT
import time

def main():
    ######################
    #       SET UP       #
    ######################
    # SQL Alchemy engine creation
    engine = create_engine(config.connection_string)
    keys_subset = {
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
        "category": VARCHAR(),
        "week": INTEGER(),
        "season": VARCHAR(),
        "season_type": VARCHAR(),
        "player_id": INTEGER(),
        "game_id": VARCHAR(),
        "team": VARCHAR(),
        "opponent": VARCHAR()}
    stats_df = pd.DataFrame(columns=keys_subset.keys()) # Create empty dataframe for matchup data

    player_id_query = '''
        select a.player_id, COALESCE(sp.years_exp,10)
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
        (select player_id, years_exp
        from sleeper.sleeper_players) sp
        ON a.player_id = sp.player_id
        '''
    with engine.connect() as con:
        player_id_info = con.execute(text(player_id_query))
        latest_year = list(con.execute(text('SELECT MAX(season) as season FROM sleeper.sleeper_state')))[0][0]
    
    player_id_df = pd.DataFrame([d[0], year] for d in list(player_id_info) for year in range(d[1],int(latest_year)+1))

    
    # Check for existence of stats table
    stats_table_check_query = '''SELECT EXISTS (
                                SELECT 1
                                FROM information_schema.tables
                                WHERE table_name = 'sleeper_stats'
                                ) AS table_existence;'''
    stats_table_flag = pd.read_sql_query(stats_table_check_query, engine)['table_existence'][0]
    
    if stats_table_flag == True:
        # Check for existence of stats table
        stats_table_query = '''SELECT DISTINCT player_id, year
                                FROM sleeper.sleeper_stats;'''
        
        


    ######################
    #       EXTRACT      #
    ######################
    # Get Players Data via Sleeper API
    for player in player_id_df.iterrows():
        print(player)
        years_exp = 0
        for season in range(latest_year, 2015, -1):
            url = 'https://api.sleeper.com/stats/nfl/player/' + str(player[1][0]) + '?season_type=regular&season=' + str(season) + '&grouping=week'
            response = requests.get(url)
            #print(response.json())
            if not all(response.json().values()):
                years_exp += 1
            if years_exp >= player_id_df[1][1]:
                break
            time.sleep(1)

        ######################
        #     TRANSFORM      #
        ######################
        # Flatten nested dictionary to a dataframe
        #players_df = pd.DataFrame([{k: v for k, v in e.items() if k in keys_subset.keys()} for d, e in response.json().items()])
        #row_player_points_df = pd.DataFrame({'week': week, 'player_id': roster['players_points'].keys(), 'player_points': roster['players_points'].values()})
            for week in response.json().keys():
                if response.json()[week] != None:
                    metadata_dict = {k: response.json()[week][k] for k in keys_subset.keys() if k in response.json()[week].keys()}
                    stats_dict = response.json()[week]['stats']
                    row_df = pd.DataFrame([metadata_dict | stats_dict])
                    stats_df = pd.concat([stats_df, row_df], axis=0, ignore_index = True)



    ######################
    #        LOAD        #
    ######################
    # Write players dataframe to postgres db
    stats_df.to_sql(name='sleeper_stats', con=engine, schema = 'sleeper', if_exists = 'replace', index=False,
                dtype=keys_subset)
    engine.dispose()


if __name__ == "__main__":
    main()
