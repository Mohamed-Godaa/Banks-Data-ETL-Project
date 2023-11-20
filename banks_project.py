'''
The following code can be used to compile the list of the top 10 largest banks 
in the world ranked by market capitalization in billion USD. The data needs to be transformed 
and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that 
has been made available in a CSV file.
'''
# imports

import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import warnings

warnings.simplefilter(action = "ignore", category=FutureWarning)

# Code for ETL operations on banks data

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('./code_log.txt', 'a') as file:
        file.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    data = BeautifulSoup(requests.get(url).text, 'html.parser')
    table = data.find_all('tbody')[0] # First table in page
    df = pd.DataFrame(columns=table_attribs)
    rows = table.find_all('tr')[1:] # to get all rows excluding first one

    for row in rows:
        col = row.find_all('td')
        data_dict = {
                    'Name': col[1].find_all('a')[1].contents,
                    'MC_USD_Billion': float(col[2].contents[0])
                    }
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path).set_index('Currency')['Rate'].to_dict()

    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index = 0)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, we define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion'] # final: Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
csv_path = 'exchange_rate.csv'
output_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file_path = 'code_log.txt'

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)


log_progress('Data extraction complete. Initiating Transformation process')
transform(df, csv_path)

log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df, output_path)

log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')
load_to_db(df, sql_connection, table_name)

# query_statement = 'SELECT * FROM Largest_banks'
# query_statement = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_statement = 'SELECT Name from Largest_banks LIMIT 5'

log_progress('Data loaded to Database as a table, Executing queries')
run_query(query_statement, sql_connection)

log_progress('Process Complete')
sql_connection.close()

log_progress('Server Connection closed')