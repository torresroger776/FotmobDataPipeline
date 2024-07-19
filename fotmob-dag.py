import pandas as pd
import pandas_gbq
import time
import requests
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromiumService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.oauth2 import service_account

default_args = {
    'owner': 'me',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_match_ids_for_round(browser, round):
    # open page for matches for the current round
    browser.get(f'https://www.fotmob.com/leagues/47/matches/premier-league/by-round?season=2023-2024&round={round}')
    time.sleep(5)

    # extract the match ids of each match in the round
    match_ids = []
    links = browser.find_elements(By.CLASS_NAME, 'css-hvo6tv-MatchWrapper')
    for link in links:
        match_id = link.get_attribute('href').split('#')[1]
        if (match_id not in match_ids):
            match_ids.append(match_id)
    return match_ids

def get_match_data(match_id):
    params = {
        "matchId": match_id
    }
    response = requests.get('https://www.fotmob.com/api/matchDetails', params=params)
    return response.json()

def extract_fotmob_data(ti):
    season_shot_data = []
    
    # run Chrome in headless mode
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    # create chrome driver context manager for scraping
    with webdriver.Chrome(service=ChromiumService(ChromeDriverManager(driver_version="125.0.6422.78", chrome_type=ChromeType.CHROMIUM).install()), options=options) as browser:
        # open premier league latest season matches on Fotmob
        browser.get('https://www.fotmob.com/leagues/47/matches/premier-league/by-round?season=2023-2024')
       
        # extract available rounds
        select_element = browser.find_element(By.XPATH, "html/body/div/main/main/section/div/div[2]/section/div/div[1]/div/select")
        select = Select(select_element)
        options = select.options
        available_rounds = [option.get_attribute('value') for option in options]

        # populate shot data array
        for round in available_rounds:
            # get match ids for current round
            match_ids = get_match_ids_for_round(browser, round)

            # get match data for each match in the round
            for match_id in match_ids:
                # retrieve match data
                match_data = get_match_data(match_id)

                # get home and away teams
                home_team_name = match_data['general']['homeTeam']['name']
                home_team_id = match_data['general']['homeTeam']['id']
                away_team_name = match_data['general']['awayTeam']['name']
                away_team_id = match_data['general']['awayTeam']['id']

                # get shot data
                shotData = match_data['content']['shotmap']['shots']

                # add team data to shots
                for shot in shotData:
                    shot['home_team_name'] = home_team_name
                    shot['home_team_id'] = home_team_id
                    shot['away_team_name'] = away_team_name
                    shot['away_team_id'] = away_team_id
                    shot['matchId'] = match_id

                # populate season shot data
                season_shot_data.append(shotData)

    ti.xcom_push(key='season_shot_data', value=season_shot_data)

def transform_fotmob_data(ti):
    # get season shot data from extract step
    season_shot_data = ti.xcom_pull(task_ids='extract_fotmob_data', key='season_shot_data')

    # create pandas dataframe of shot data
    df = pd.DataFrame([shot for shot_data in season_shot_data for shot in shot_data])

    # rename data frame columns
    rename_dict = {
        'id': 'shot_id',
        'eventType': 'event_type',
        'playerName': 'player_name',
        'shotType': 'shot_type',
        'x': 'shot_from_x',
        'y': 'shot_from_y',
        'isBlocked': 'is_blocked',
        'blockedX': 'blocked_x',
        'blockedY': 'blocked_y',
        'goalCrossedY': 'goal_crossed_y',
        'goalCrossedZ': 'goal_crossed_z',
        'expectedGoals': 'xG',
        'expectedGoalsOnTarget': 'xGOT'
    }
    df.rename(columns=rename_dict, inplace=True)

    # clean data
    df.loc[df['home_team_name']=='Tottenham', 'home_team_name'] = 'Tottenham Hotspur'

    # create match dimension table
    match_dim = df[['matchId']].drop_duplicates().reset_index(drop=True)
    match_dim['match_id'] = match_dim.index

    # create team dimension table
    team_dim = pd.concat([df[['home_team_name', 'home_team_id']].drop_duplicates().rename(columns={'home_team_name': 'team_name', 'home_team_id': 'teamId'}), df[['away_team_name', 'away_team_id']].drop_duplicates().rename(columns={'away_team_name': 'team_name', 'away_team_id': 'teamId'})], ignore_index=True).drop_duplicates()
    team_dim['team_id'] = team_dim.index

    # create player dimension table
    player_dim = df[['player_name']].drop_duplicates().reset_index(drop=True)
    player_dim['player_id'] = player_dim.index

    # create shot type dimension table
    shot_type_dim = df[['shot_type']].drop_duplicates().reset_index(drop=True)
    shot_type_dim['shot_type_id'] = shot_type_dim.index

    # create event type dimension table
    event_type_dim = df[['event_type']].drop_duplicates().reset_index(drop=True)
    event_type_dim['event_type_id'] = event_type_dim.index

    # create fact table
    fact_table = df.merge(match_dim, on='matchId') \
            .merge(team_dim, on='teamId') \
            .merge(player_dim, on='player_name') \
            .merge(shot_type_dim, on='shot_type') \
            .merge(event_type_dim, on='event_type') \
            [['shot_id', 'match_id', 'team_id', 'player_id', 
              'shot_type_id', 'event_type_id', 'xG', 'xGOT', 
              'shot_from_x', 'shot_from_y', 'is_blocked', 
              'blocked_x', 'blocked_y', 'goal_crossed_y', 
              'goal_crossed_z']]
    
    # output dictionary
    output = {
        'match_dim': match_dim.to_dict(orient='dict'),
        'team_dim': team_dim.to_dict(orient='dict'),
        'player_dim': player_dim.to_dict(orient='dict'),
        'shot_type_dim': shot_type_dim.to_dict(orient='dict'),
        'event_type_dim': event_type_dim.to_dict(orient='dict'),
        'fact_table': fact_table.to_dict(orient='dict')
    }

    ti.xcom_push(key='tables', value=output)

def load_fotmob_data(ti):
    # get tables from transform step
    tables = ti.xcom_pull(task_ids='transform_fotmob_data', key='tables')

    # service account credentials
    credentials_path = Variable.get('service_account_credentials_path')
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # project id
    project_id = "epl-data-project"

    # load each table into BigQuery
    for table_key, table in tables.items():
        # table id
        table_id = f"fotmob_data.{table_key}"
        # write df to BigQuery table
        pandas_gbq.to_gbq(pd.DataFrame(table), table_id, project_id, credentials=credentials)

with DAG(
    dag_id='fotmob_dag',
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2024, 7, 19)
) as dag:
    extract = PythonOperator(
        task_id='extract_fotmob_data',
        python_callable=extract_fotmob_data
    )

    transform = PythonOperator(
        task_id='transform_fotmob_data',
        python_callable=transform_fotmob_data
    )

    load = PythonOperator(
        task_id='load_fotmob_data',
        python_callable=load_fotmob_data
    )

    extract >> transform >> load