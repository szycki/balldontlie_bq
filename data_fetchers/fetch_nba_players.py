import os
import time
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
import requests
import seaborn as sns

# one time script to fetch all the available players from the api

load_dotenv()

API_KEY = os.environ.get('BALLDONTLIE_API_KEY')
API_URL = os.environ.get('BALLDONTLIE_API_URL')

headers = {
	'Authorization': API_KEY
}

params = {
	'per_page': 100
}

players_url = f'{API_URL}/players'
number_of_players = 4000
players = []

x = 0

while x < number_of_players:
	response = requests.get(players_url, params=params, headers=headers)
	data = response.json()['data']
	meta = response.json()['meta']
	next_cursor = meta['next_cursor']
	params['cursor'] = next_cursor
	x = next_cursor
	print(f'{x}/{number_of_players} completed')
	players.extend(data)
	time.sleep(1)

df = pd.json_normalize(players, sep='_')
df.to_csv('nba_players.csv', index=False, encoding='utf-8')