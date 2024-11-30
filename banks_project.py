from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import numpy as np
import sqlite3
import pandas as pd

def log_progress(message):
    time_stampf= "%d-%h-%Y-%H:%M:%S"
    now = datetime.now()
    time = now.strftime(time_stampf)
    with open("./code_log.txt", "a") as file:
        file.write(time + '->' + message + '\n')

data_url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"

def extract(url, table_attributes):
    response = requests.get(url)  # Get the response object
    log_progress(f"Successfully got HTML response with status code {response.status_code}")  # Log the status code
    html = response.text 
    soup = BeautifulSoup(html,'html.parser')
    df = pd.DataFrame(columns=table_attributes)
    all_tables = soup.find_all('tbody')
    rows = all_tables[0].find_all('tr')
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) < 3:
            continue
        name = cells[1].find("a")["title"] 
        mc = cells[2].get_text(strip=True)
        data_dict = {"Name": name, "MC_USD_Billion": mc}
        df1 = pd.DataFrame([data_dict])
        df = pd.concat([df, df1], ignore_index=True)
    log_progress("data extracted")
    return df

def transform(data_df, rates_csv):
    rates_df = pd.read_csv(rates_csv)
    #d = rates_df.set_index('Bank_name').to_dict()['Market_cap']
    rates = rates_df.set_index('Currency')['Rate'].to_dict()
    data_df['MC_EUR_Billion'] = [np.round(float(x)*rates['EUR'],2) for x in data_df['MC_USD_Billion']] 
    data_df['MC_GBP_Billion'] = [np.round(float(x)*rates['GBP'],2) for x in data_df['MC_USD_Billion']] 
    data_df['MC_INR_Billion'] = [np.round(float(x)*rates['INR'],2) for x in data_df['MC_USD_Billion']] 
    log_progress("data transformed to EUR, GBR, INR")
    return data_df


def load_to_csv(data_df,output_file):
    data_df.to_csv(output_file, index=False)
    log_progress(f"Transformed data saved to {output_file}")
    

csv_file_path = './Largest_banks_data.csv'

def load_to_db(csv_file, db_name, table_name):
    data = pd.read_csv(csv_file)
    conn = sqlite3.connect(db_name)
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    log_progress(f"data loaded to sqlite-db {db_name}")

db_name = 'Banks.db' 
table_name = 'bank_data'

def all_job():
    columns = ["Name", "MC_USD_Billion"]
    df = extract(data_url , columns)
    df = transform(df, "./exchange_rate.csv")
    print(df)
    

all_job()  