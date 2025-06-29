from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# this is a util used to upload data into existing bigquery tables

# method used to format the value used in pandas dataframe, so that it can be later read by bigquery
def format_value(v):
    if pd.isna(v):
        return 'NULL'          
    elif isinstance(v, str):
        return f"'{v}'"        
    else:
        return str(v)

# this method is used to upload new records into game table
def insert_games(data):
	credentials = service_account.Credentials.from_service_account_file('C:\\Users\\simon\\programming\\balldontlieapi\\bq_util\\key.json')
	client = bigquery.Client(credentials=credentials)

	# dynamically determing the columns for the insert query
	column_names = ','.join(data.columns)
	
	# preperation of VALUES () fields
	value_tuples = [
    	"(" + ",".join(format_value(val) for val in row) + ")"
    	for row in data.itertuples(index=False, name=None)
	]

	# combining value fields
	value_string = ','.join(value_tuples)

	query = (
		'INSERT INTO `balldontlie-463416.balldontlie.game` '
		f'({column_names}) '
		'VALUES '
		f'{value_string} '
	)
	
	query_job = client.query(query)
	rows = query_job.result()
    
	return True