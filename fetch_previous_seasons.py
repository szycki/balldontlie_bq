import pandas as pd
import json
import logging
import time
from data_fetchers.fetch_nba_games import fetch
from bq_util.insert import insert_games

# this dameon is used to retrieve the historic data, it checks the config for the last fetched season, than it retrieves the data for the previous season and so on

def main():
	logging.basicConfig(
        filename='fetch_previous_seasons_daemon.log',
        level=logging.INFO,
        format='%(asctime)s - %(message)s'
    )


	while True:
		try:
			logging.info('Starting fetch_previous_seasons daemon')
			with open('fetch-current-season-config.json', 'r') as file:
				config = json.load(file)

			# checking which historic season was lastly fetched
			last_fetched_season = config['last_fetched_season']
			#determing the next season to fetch
			season_to_fetch = last_fetched_season - 1

			games = fetch(season_to_fetch)

			# saving results to a local file, to prevent data loss
			games.to_csv(f'data/games_{season_to_fetch}_{season_to_fetch + 1}.csv', index=False, encoding='utf-8')

			# uploading data to bigquery
			gamesUploadedSuccessfully = insert_games(games)

			if gamesUploadedSuccessfully:
				config['last_fetched_season'] = season_to_fetch
				logging.info(f'fetch_previous_seasons daemon for season {season_to_fetch} completed')

			logging.error('Error during upload in fetch_previous_seasons daemon')
			

		except Exception as e:
			logging.error(f'Error during fetch_previous_seasons daemon {e}')

		interval = config['fetchPreviousSeasonsInterval']

		time.sleep(interval)
