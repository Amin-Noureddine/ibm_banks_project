import pandas as pd 
import numpy as np 
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

logfile = 'code_log.txt'

def log_progress(msg):
    timeformat = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timeformat)

    with open(logfile, 'a') as f:
        f.write(timestamp + ' : ' + msg + '\n')

def extract(url, table_attribs):
    log_progress('Extracting data from the webpage...')
    df = pd.DataFrame(columns=table_attribs)

    try:
        page = requests.get(url).text
        data = BeautifulSoup(page, 'html.parser')

        # Find the table under the specified heading
        table = data.find('span', {'id': 'By_market_capitalization'}).find_next('table')
        
        rows = table.find_all('tr')

        for row in rows:
            col = row.find_all('td')
            if len(col) != 0:
                anchor_data = col[1].find_all('a')[1]
                if anchor_data is not None:
                    data_dict = {
                        'Name': anchor_data.contents[0],
                        'MC_USD_Billion': col[2].contents[0].replace('\n', '')
                    }
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)

        # Convert the Market Cap column to float
        df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

        log_progress('Extraction complete.')
        return df
    except Exception as e:
        log_progress(f'Error during extraction: {str(e)}')
        return pd.DataFrame()

def transform(df, csv_path):
    log_progress('Transforming data...')
    # Read the exchange rate CSV file and convert to a dictionary
    exchange_rate = pd.read_csv(csv_path).set_index('Currency').to_dict()['Rate']

    # Add transformed columns to the DataFrame
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress('Transformation complete.')
    return df

def load_to_db(df, sql_connection, table_name):
    log_progress('Loading data to the database...')
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Data loaded to the database.')

def run_queries(query_statements, sql_connection):
    for query in query_statements:
        print(f"Query statement: {query}")
        result = pd.read_sql(query, sql_connection)
        print(result)
        print('\n')

# URL and table attributes
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']

# Call the extract function
df = extract(url, table_attribs)

# Path to exchange rate CSV
exchange_rate_path = 'exchange_rate.csv'

# Call the transform function
df = transform(df, exchange_rate_path)

# SQLite database connection
db_name = 'Banks.db'
table_name = 'Largest_banks'
conn = sqlite3.connect(db_name)

# Call the load_to_db function
load_to_db(df, conn, table_name)

# Query statements
query_statements = [
    'SELECT * FROM Largest_banks',
    'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
    'SELECT Name FROM Largest_banks LIMIT 5'
]

# Call the run_queries function
run_queries(query_statements, conn)

# Close the SQLite connection
conn.close()

# Print the contents of the returning data frame
print(df)

# Print the market capitalization of the 5th largest bank in billion EUR
print(f"Market Cap of the 5th largest bank in billion EUR: {df['MC_EUR_Billion'][4]}")

# Save the updated DataFrame to CSV
output_csv_path = 'transformed_data.csv'
df.to_csv(output_csv_path)

# Make the relevant log entry
log_progress('Data transformed. Output saved to CSV.')
