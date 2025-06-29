import pandas as pd
import json
import logging
import time
import numpy as np
from data_fetchers.fetch_nba_games import fetch
from bq_util.bq_select import select_last_game
from bq_util.insert import insert_games

def main():
	# logging tool configuration
	logging.basicConfig(
        filename='fetch_current_seasons_daemon.log',
        level=logging.INFO,
        format='%(asctime)s - %(message)s'
    )

	# daemon loop keeps running in the background
	while True:
		try:
			logging.info('Starting fetch_current_season daemon')
			
			# retrieving daemon config 
			with open('fetch-current-season-config.json', 'r') as file:
				config = json.load(file)

			# retrieving last fetched game, that will help to cut the later retrieved data and remove entries that are already saved in the database
			last_fetched_game = select_last_game()
			print('last_fetched_game', last_fetched_game)

			if last_fetched_game.empty:
				logging.error('Error during select last_played_game in fetch_current_season daemon')
				return False

			# retrieving the datetime of the last fetched game, to use in comparison with later retrieved data
			last_fetched_game_datetime = last_fetched_game['datetime'].values[0]
			last_fetched_game_datetime = np.datetime64('2024-11-21')
			
			# extracting the year value, so we can determine the year parameter that should be passed to the api
			year = last_fetched_game_datetime.astype('datetime64[Y]').astype(int) + 1970
			
			season_to_fetch = year
			# using july 1st as the value, to compare the escaped year, so we can determine which season the game was played
			jul_1 = np.datetime64(f'{year}-08-01')

			if last_fetched_game_datetime < jul_1:
				season_to_fetch -= 1

			print(f'Season to fetch: {season_to_fetch}')

			# fetching all the games for the season that was determined 
			current_season_games = fetch(season_to_fetch)
			print(f'Number of current season games: {len(current_season_games)}')

			# ensuring the datetime value is of the correct data type, so it can be later used in the comparison
			current_season_games['datetime'] = pd.to_datetime(current_season_games['datetime'])
			current_season_games['datetime'] = current_season_games['datetime'].dt.tz_localize(None)

			# converting the last fetched game datetime value to be a correct data type for comparison
			last_fetched_game_datetime = pd.to_datetime(last_fetched_game_datetime)
			# cutting the dataframe, to remove the records that were already fetched and are present in the database
			new_current_season_games = current_season_games[current_season_games['datetime'] >= last_fetched_game_datetime]
			
			# saving the dataframe to the csv, so it can be examined in case of any errors during the daemon working process
			new_current_season_games.to_csv(f'data/games_{season_to_fetch}_{season_to_fetch + 1}.csv', index=False, encoding='utf-8')
			
			# uploading the cut dataframe with new records to bigquery
			games_uploaded_successfully = insert_games(new_current_season_games)
			games_uploaded_successfully = True

			if games_uploaded_successfully:
				logging.info(f'fetch_current_season daemon for season {season_to_fetch} completed')

			logging.error('Error during upload in fetch_current_season daemon')
			

		except Exception as e:
			logging.error(f'Error during fetch_current_season daemon {e}')

		#retrieveing the interval value from config and putting the daemon to "sleep" (the interval is 1 day, so the daemon checks every day for the new games that were played and inserts them into bigquery)
		interval = config['fetchCurrentSeasonInterval']

		time.sleep(interval)

main()