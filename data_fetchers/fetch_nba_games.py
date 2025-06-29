import os
import time
from dotenv import load_dotenv
import pandas as pd
import requests



def fetch(season):
	load_dotenv()

	API_KEY = os.environ.get('BALLDONTLIE_API_KEY')
	API_URL = os.environ.get('BALLDONTLIE_API_URL')
	
	headers = {
		'Authorization': API_KEY
	}

	# defining parameters to be used by the api

	params = {
		'seasons[]': season,
		'per_page': 100
	}

	games_url = f'{API_URL}/games'
	number_of_games = 0 
	games = []

	data_fetched = False

	# loop that will run until all the games from the given season are fetched

	while not data_fetched:
		try:
			response = requests.get(games_url, params=params, headers=headers)

			# games returned by the request
			data = response.json()['data']

			# request meta data, used to determine next request parameters
			meta = response.json()['meta']
			print('meta', meta)

			# defining next request parameters
			next_cursor = meta.get('next_cursor')
			print('next_cursor', next_cursor)

			per_page = params['per_page']
			
			# checking if all the data has been fetched
			if not next_cursor and per_page == 1:
				data_fetched = True
				continue
			
			# checking if another set of params will return data
			if not next_cursor and per_page != 1:
				params['cursor'] = meta.get('prev_cursor')
				params['per_page'] = int(per_page / 2)
				time.sleep(12)
				continue


			params['cursor'] = next_cursor

			# appending data from the request to the data from previous requests
			games.extend(data)

			number_of_games += len(data)
			print(f'{number_of_games} retrieved')

			# saving data to csv files every 500 records to prevent data loss			
			if len(games) > 500:	
				df = pd.json_normalize(games, sep='_')
				df.to_csv(f'../data/games_{number_of_games}.csv', index=False, encoding='utf-8')
				games = []

			# api has a request limit so the interval is used, to not trigger errors
			time.sleep(12)

		except Exception as ex:
			print(ex)
			data_fetched = True

	# returning flattened json converted to pandas dataframe
	return pd.json_normalize(games, sep='_') 
