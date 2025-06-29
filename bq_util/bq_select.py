from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# google cloud authorization using IAM keys
credentials = service_account.Credentials.from_service_account_file('key.json')
client = bigquery.Client(credentials=credentials)

# this is a util for fetching data from bigquery

# this method returns the game with the highest datetime, which is then used to update the database with new records from the api
def select_last_game():
	query = 'SELECT * FROM `balldontlie-463416.balldontlie.game` ORDER BY datetime DESC LIMIT 1'

	query_job = client.query(query)
	rows = query_job.result()

	# checking if the result is of desired size, so that it can be properly processed later
	if rows.total_rows == 1:
		return rows.to_dataframe()
	else:
		data = []
		return pd.DataFrame(data)