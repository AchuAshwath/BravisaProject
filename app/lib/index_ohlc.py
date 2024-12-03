# Script for fetching and inserting OHLC data
import datetime
import requests
import os.path
import os
from utils.db_helper import DB_Helper
from utils.check_helper import Check_Helper
from zipfile import ZipFile
import csv
import psycopg2
import pandas as pd
import calendar
import numpy as np
import pandas.io.sql as sqlio
import utils.date_set as date_set
import rootpath


# headers = {}
# cookies = {}


if os.name =='nt':
    my_path =os.getcwd()  # working directory
    filepath = os.path.join(my_path, "IndexOHLCFiles\\")
    print("IndexOHLC File path : ",filepath)
else:
   my_path = rootpath.detect()
   filepath = os.path.join(my_path, "index-ohlc-files/") 



# """ get current date"""
# def get_date():
#     download_date = run_date
#     return download_date


""" Fetch index nse data for a date """
def fetch_nse_index_ohlc(date):

    download_date = date.strftime("%d%m%Y")
    
    # csv_url = 'https://www1.nseindia.com/content/indices/ind_close_all_' + download_date + '.csv'
    # csv_file_name = 'ind_close_all_' + download_date + '.csv'

    # req = requests.get(csv_url)

    # if req.status_code == 200:

    #     path_to_store_csv = open(filepath + csv_file_name, "wb")
    #     for chunk in req.iter_content(100000):
    #         path_to_store_csv.write(chunk)
    #     path_to_store_csv.close()
    #     print("Completed")

    # else:

    #     print("Data doesnt exist for this date")
    #     raise ValueError('Index OHLC could not be fetched for '+download_date)

    csv_file = filepath+'ind_close_all_' + download_date + '.csv'
   
    if os.path.isfile(csv_file):

        index_ohlc = pd.read_csv(csv_file)

        # os.remove(csv_file)

        return index_ohlc
    
    else:
        print("Index OHLC data is not there for",download_date)
        return None



""" Merge with index btt mapping"""
def merge_btt_index_code(index_ohlc, conn):

    index_btt_mapping_sql = 'SELECT * FROM public.index_btt_mapping ;'
    index_btt_mapping = sqlio.read_sql_query(index_btt_mapping_sql, con=conn)

    index_ohlc = pd.merge(index_ohlc, index_btt_mapping,
                          left_on='Index Name', right_on='IndexName', how='left')

    return index_ohlc


""" Merge with index btt mapping for history data insert """


def merge_index_name(index_ohlc, conn):

    index_btt_mapping_sql = 'SELECT * FROM public.index_btt_mapping ;'
    index_btt_mapping = sqlio.read_sql_query(index_btt_mapping_sql, con=conn)

    index_ohlc = pd.merge(index_ohlc, index_btt_mapping,
                          left_on='TICKER', right_on='BTTCode', how='left')

    return index_ohlc


""" Fill empty open, high, low with close value"""


def fill_empty_values(index_ohlc):

    for index, row in index_ohlc.iterrows():

        open_val_list = index_ohlc.loc[(
            index_ohlc['Index Name'] == row['Index Name'])]['Open Index Value']
        open_val = open_val_list.item() if len(open_val_list.index) == 1 else np.nan

        high_val_list = index_ohlc.loc[(
            index_ohlc['Index Name'] == row['Index Name'])]['High Index Value']
        high_val = high_val_list.item() if len(high_val_list.index) == 1 else np.nan

        low_val_list = index_ohlc.loc[(
            index_ohlc['Index Name'] == row['Index Name'])]['Low Index Value']
        low_val = low_val_list.item() if len(low_val_list.index) == 1 else np.nan

        close_val_list = index_ohlc.loc[(
            index_ohlc['Index Name'] == row['Index Name'])]['Closing Index Value']
        close_val = close_val_list.item() if len(close_val_list.index) == 1 else np.nan

        if(open_val == '-' and high_val == '-' and low_val == '-'):

            open_val = close_val
            high_val = close_val
            low_val = close_val

            index_ohlc.loc[index, 'Open Index Value'] = open_val
            index_ohlc.loc[index, 'High Index Value'] = high_val
            index_ohlc.loc[index, 'Low Index Value'] = low_val

        else:

            None

    return index_ohlc


""" Insert into Index OHLC"""


def insert_index_ohlc(index_ohlc, conn, cur, date):

    index_ohlc.rename(columns={'BTTCode': 'NSECode', 'Open Index Value': 'Open', 'High Index Value': 'High',
                               'Low Index Value': 'Low', 'Closing Index Value': 'Close', 'Change(%)': 'Change', 'Turnover (Rs. Cr.)': 'Turnover',
                               'P/E': 'PE', 'P/B': 'PB'}, inplace=True)

    index_ohlc['Date'] = pd.to_datetime(date).strftime("%Y-%m-%d")
    
    index_ohlc = index_ohlc[['IndexName', 'NSECode', 'Date', 'Open', 'High', 'Low', 'Close', 'Points Change', 'Change',
                                                     'Volume', 'Turnover', 'PE', 'PB', 'Div Yield']]

    index_ohlc[index_ohlc.columns[7:]] = index_ohlc[index_ohlc.columns[7:]].replace(r'\-', '-1', regex=True).astype(float)
    index_ohlc[index_ohlc.columns[7:]] = index_ohlc[index_ohlc.columns[7:]].replace(-1, np.nan)

    exportfilename = "exportIndexOHLC.csv"
    exportfile = open(exportfilename, "w")
    index_ohlc.to_csv(exportfile, header=True,
                      index=False, lineterminator='\r')
    exportfile.close()

    copy_sql = """
		   COPY public."IndexOHLC" FROM stdin WITH CSV HEADER
		   DELIMITER as ','
		   """
    with open(exportfilename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        f.close()

    print("Index OHLC Insert Completed")
    os.remove(exportfilename)

    return index_ohlc


""" Merge with NSE"""


def merge_indexohlc_nse(index_ohlc, conn, cur):

    # nse_cols = [['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'TOTALTRDQTY', 'TOTTRDVAL', \
            #  'TIMESTAMP', 'TOTALTRADES', 'ISIN']]

    index_ohlc.rename(columns={'NSECode': 'SYMBOL', 'Open': 'OPEN', 'High': 'HIGH',
                               'Low': 'LOW', 'Close': 'CLOSE', 'Date': 'TIMESTAMP'}, inplace=True)
    # index_ohlc_cols = list(index_ohlc.columns)

    # cols_to_add = [x for x in index_ohlc_cols if x not in nse_cols]
    # index_ohlc.add(cols_to_add, axis='columns')

    index_ohlc = index_ohlc[['SYMBOL', 'OPEN',
                             'HIGH', 'LOW', 'CLOSE', 'TIMESTAMP']]
    cols_to_add = pd.DataFrame(columns=[
                               'SERIES', 'LAST', 'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TOTALTRADES', 'ISIN'])
    index_ohlc_nse = pd.concat([index_ohlc, cols_to_add], axis='columns')

    index_ohlc_nse = index_ohlc_nse[['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE',
                                     'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES', 'ISIN']]

    exportfilename = "exportNSE.csv"
    exportfile = open(exportfilename, "w")
    index_ohlc_nse.to_csv(exportfile, header=True,
                          index=False, lineterminator='\r')
    exportfile.close()

    copy_sql = """
		   COPY public."NSE" FROM stdin WITH CSV HEADER
		   DELIMITER as ','
		   """
    with open(exportfilename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        f.close()

    print("Merged with NSE")
    os.remove(exportfilename)


""" History insert for index OHLC"""


def insert_history_index(conn, cur):

    index_history = filepath + 'IndexOHLCHistory.csv'
    index_ohlc_history = pd.read_csv(index_history, encoding='latin1')

    # Insert into Index OHLC
    index_ohlc_history = merge_index_name(index_ohlc_history, conn)

    index_ohlc_history.rename(columns={'TICKER': 'NSECode', 'OPEN': 'Open', 'HIGH': 'High',
                                       'LOW': 'Low', 'CLOSE': 'Close', 'VOL': 'Volume', 'DATE': 'Date'}, inplace=True)

    cols_to_add = pd.DataFrame(
        columns=['Points Change', 'Change', 'Turnover', 'PE', 'PB', 'Div Yield'])
    index_ohlc_history = pd.concat(
        [index_ohlc_history, cols_to_add], axis='columns')

    index_ohlc_history = index_ohlc_history[['IndexName', 'NSECode', 'Date', 'Open', 'High', 'Low', 'Close', 'Points Change', 'Change',
                                                                     'Volume', 'Turnover', 'PE', 'PB', 'Div Yield']]

    exportfilename = "exportIndexHistory.csv"
    exportfile = open(exportfilename, "w")
    index_ohlc_history.to_csv(exportfile, header=True,
                              index=False, lineterminator='\r')
    exportfile.close()

    copy_sql = """
		   COPY public."IndexOHLC" FROM stdin WITH CSV HEADER
		   DELIMITER as ','
		   """
    with open(exportfilename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        f.close()

    print("Inserted History index OHLC")
    os.remove(exportfilename)

    # Insert into NSE table
    merge_indexohlc_nse(index_ohlc_history, conn, cur)


def main(curr_date):

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("\n\t\t Index OHLC Fetch Service")
    Check_Helper().check_path(filepath)

    date = curr_date

    print("index OHLC date\n",date)
    
    print("Fetching IndexOHLC from NSE")
    index_ohlc = fetch_nse_index_ohlc(date)

    if(not(index_ohlc is None)):

        print("Merging with index BTTCode")
        index_ohlc = merge_btt_index_code(index_ohlc, conn)

        print("Filling empty values")
        index_ohlc = fill_empty_values(index_ohlc)

        print("Inserting into index OHLC table")
        index_ohlc = insert_index_ohlc(index_ohlc, conn, cur, date)

        print("Merging with OHLC")
        merge_indexohlc_nse(index_ohlc, conn, cur)

    else:
        print("index OHLC table is Null")
        raise ValueError('Index OHLC could not be inserted due to null error')

    print("\n\t\t  Index OHLC Fetch Completed.")
    conn.close()


def history_insert():

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("\n\t\t History insert for Index OHLC")
    Check_Helper().check_path(filepath)

    insert_history_index(conn, cur)

    conn.close()
