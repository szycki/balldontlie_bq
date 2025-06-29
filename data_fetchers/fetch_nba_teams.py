import os
import time
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
import requests
import seaborn as sns

# one time script to fetch all the available teams from the api

load_dotenv()

API_KEY = os.environ.get('BALLDONTLIE_API_KEY')
API_URL = os.environ.get('BALLDONTLIE_API_URL')

headers = {
	'Authorization': API_KEY
}

teams_url = f'{API_URL}/teams'


response = requests.get(teams_url, headers=headers)
data = response.json()['data']

df = pd.json_normalize(data, sep='_')
df.to_csv('C:\\Users\\simon\\programming\\balldontlieapi\\data\\nba_teams.csv', index=False, encoding='utf-8')