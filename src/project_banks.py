import requests
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

load_dotenv()
conn_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attri = ['Name', 'MC_USD_Billion']
exchange_rate_path = 'exchange_rate.csv'
output_csv = 'Largest_banks_data.csv'
table_name = 'Largest_banks'


#Log process
def log_process(message):
    stamptime_format = '%Y/%m/%d %H:%M:%S'
    now = datetime.now()
    stamp_time = now.strftime(stamptime_format)

    with open('log_code.txt', 'a') as f:
        f.write(stamp_time + ':' + message + '\n')
    

#Extract process
def extract(url, table_attri):
    df = pd.DataFrame(columns=table_attri)
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if (len(cols)!= 0):
            row_dict = {'Name': cols[1].get_text(strip=True), 'MC_USD_Billion': float(cols[2].text)}
            df = pd.concat([df, pd.DataFrame(row_dict, index=[0])], ignore_index=True)

    return df

#Transformation process
def transform(df, exchange_rate_path):
    exchange_data = pd.read_csv(exchange_rate_path)
    exchange_rate = dict(zip(exchange_data['Currency'], exchange_data['Rate']))

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_csv):
    df.to_csv(output_csv, index=False)

def load_to_db(df, table_name, conn_string): 
    
    engine = create_engine(conn_string)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    engine.dispose()

def run_queries(query):
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    
    cursor = conn.cursor()
    cursor.execute(query)

    if query.strip().upper().startswith('SELECT'):
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    conn.commit()
    cursor.close()
    conn.close()

print('helloo')
log_process('ETL pipeline started')
log_process('Extraction start')
df = extract(url, table_attri)
log_process('Extraction completed. Trasformation started')
df = transform(df, exchange_rate_path)
log_process('Transformation completed. Loading started')
load_to_csv(df, output_csv)
log_process('Data loading to the CSV file completed successfully.')
load_to_db(df, table_name, conn_string)
log_process('Data loading to the Database completed successfully.')
print("Enter SQL query (Press Enter to pass):")
query = input().strip()
if query:
    log_process(f"Received SQL query: {query}")
    result = run_queries(query)
    if result is not None:
        log_process(f"Query result: {result}")
        print("Kết quả truy vấn:", result)
    else:
        log_process("Query executed but returned no results.")
else:
    log_process("No query was provided.")
log_process("Pipeline ETL ended")