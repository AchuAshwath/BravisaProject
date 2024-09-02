
# Script for fetching and inserting OHLC data
import datetime
import requests
import os.path
import os
from utils.db_helper import DB_Helper
from utils.check_helper import Check_Helper
from zipfile import ZipFile
import csv
import glob
import os.path
import psycopg2
import pandas as pd
import calendar
import numpy as np
import pandas.io.sql as sqlio
import utils.date_set as date_set
import rootpath

headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8', 'Connection': 'keep-alive', 'Content-Type': 'application/zip', 'Referer': 'https://www.nseindia.com/products/content/derivatives/equities/archieve_fo.htm',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36', 'X-frame-options': 'SAMEORIGIN'}
cookies = {'pointer': '1', 'sym1': 'KPIT',
           'NSE-TEST-1': '1826627594.20480.0000'}
           
if os.name == 'nt':
    my_path = os.path.abspath(os.path.dirname('C:\\Users\\dsram\\OneDrive\\Desktop\\Braviza\\app\\'))
    filepath = os.path.join(my_path, "OHLCFiles\\")
else:
  my_path = os.path.abspath(os.path.dirname('C:\\Users\\dsram\\OneDrive\\Desktop\\Braviza\\app\\'))
  filepath = os.path.join(my_path, "ohlc-files/")


def get_date():
    download_date = date_set.get_run_date()
    return download_date

# Function to fetch BSE OHLC daily file and store it in provided file path
def fetch_bse(conn, cur, download_date=None):

    if download_date is None:
        download_date = get_date()
    download_date = download_date.strftime("%d%m%y")
    print("download date\t",download_date)

    print("\n\nBSE Fetch invoked ....")

    print("Download date:"+download_date)

    csv_file = filepath+ 'EQ_ISINCODE_' + download_date + '.CSV'
    
    if os.path.isfile(csv_file):
       
        table = pd.read_csv(csv_file)
        
        table_columns = ['SC_CODE', 'SC_NAME', 'SC_GROUP', 'SC_TYPE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                        'PREVCLOSE', 'NO_TRADES', 'NO_OF_SHRS', 'NET_TURNOV', 'TDCLOINDI', 'ISIN_CODE', 'TRADING_DATE']
        csv_columns = list(table.columns.values)
        columns_to_remove = [x for x in csv_columns if x not in table_columns]
        table = table.drop(columns_to_remove, axis=1)
        table = table[['SC_CODE', 'SC_NAME', 'SC_GROUP', 'SC_TYPE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                    'PREVCLOSE', 'NO_TRADES', 'NO_OF_SHRS', 'NET_TURNOV', 'TDCLOINDI', 'ISIN_CODE', 'TRADING_DATE']]

        # os.remove(csv_file)

        return table
        
    else:
        print("BSE Data is not there for",download_date)
        return None


def insert_bse(table, conn, cur):

    exportfilename = "exportBSE.csv"
    exportfile = open(exportfilename, "w")

    table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
    exportfile.close()

    print("Attempting to insert BSE data into DB")

    copy_sql = """
           COPY public."BSE" FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """
    with open(exportfilename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)

    exportfile.close()
    print("BSE Insert Completed")

    os.remove(exportfilename)

# Function to fetch NSE OHLC daily file and store it in provided file path
def fetch_nse(conn, cur,	download_date=None):

    if download_date is None:
        download_date = get_date()

    print("\n\nNSE Fetch invoked ....")

    download_date = download_date.strftime("%d%b%Y").upper()
    print("download date\n",download_date)

    csv_file = filepath+"cm"+download_date+"bhav.csv"
    
    if os.path.isfile(csv_file):

        table = pd.read_csv(csv_file)

        table_columns = ['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                        'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES', 'ISIN']
        csv_columns = list(table.columns.values)
        columns_to_remove = [x for x in csv_columns if x not in table_columns]

        table = table.drop(columns_to_remove, axis=1)
        table = table[table.SERIES.isin(["EQ", "BZ", "BE", "RR"])]
        table = table[['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                    'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES', 'ISIN']]
        
        # os.remove(csv_file)

        return table
        
    else:
        print("NSE data is not there for",download_date)
        return None


def insert_nse(table, conn, cur):

    exportfilename = "exportNSE.csv"
    exportfile = open(exportfilename, "w")
    table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
    exportfile.close()

    print("Attempting to insert NSE data into DB")

    copy_sql = """
           COPY public."NSE" FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """
    with open(exportfilename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)

    exportfile.close()

    print("NSE Insert Completed")
    os.remove(exportfilename)


def ohlc_join(ohlc_nse, ohlc_bse, conn, cur):

    table_ohlc = pd.merge(ohlc_nse, ohlc_bse, left_on='ISIN',
                          right_on='ISIN_CODE', how='outer')
    table_ohlc["ISIN"].fillna(table_ohlc["ISIN_CODE"], inplace=True)
    table_ohlc["OPEN_x"].fillna(table_ohlc["OPEN_y"], inplace=True)
    table_ohlc["HIGH_x"].fillna(table_ohlc["HIGH_y"], inplace=True)
    table_ohlc["LOW_x"].fillna(table_ohlc["LOW_y"], inplace=True)
    table_ohlc["CLOSE_x"].fillna(table_ohlc["CLOSE_y"], inplace=True)
    table_ohlc["LAST_x"].fillna(table_ohlc["LAST_y"], inplace=True)
    table_ohlc["PREVCLOSE_x"].fillna(table_ohlc["PREVCLOSE_y"], inplace=True)
    table_ohlc["TIMESTAMP"].fillna(table_ohlc["TRADING_DATE"], inplace=True)
    table_ohlc["TOTTRDVAL"].fillna(table_ohlc["NET_TURNOV"], inplace=True)
    table_ohlc["TOTTRDQTY"].fillna(table_ohlc["NO_OF_SHRS"], inplace=True)

    table_ohlc.rename(columns={'SC_NAME': 'Company',  'SC_CODE': 'BSECode', 'SYMBOL': 'NSECode',
                               'OPEN_x': 'Open', 'HIGH_x': 'High', 'LOW_x': 'Low', 'CLOSE_x': 'Close', 'LAST_x': 'Last',
                               'PREVCLOSE_x': 'PrevClose', 'TIMESTAMP': 'Date', 'TOTTRDVAL': 'Value', 'TOTTRDQTY': 'Volume'}, inplace=True)

    table_columns = ['Company', 'BSECode', 'NSECode', 'ISIN',
                     'Open', 'High', 'Low', 'Close', 'Date', 'Value', 'Volume']
    csv_columns = list(table_ohlc.columns.values)
    columns_to_remove = [x for x in csv_columns if x not in table_columns]
    table_ohlc = table_ohlc.drop(columns_to_remove, axis=1)
    table_ohlc = table_ohlc[["Company",  "NSECode", "BSECode", "ISIN",
                             "Open", "High", "Low", "Close", "Date", "Value", "Volume"]]

    table_ohlc["BSECode"].fillna(-1, inplace=True)
    table_ohlc = table_ohlc.astype({"BSECode": int})
    table_ohlc = table_ohlc.astype({"BSECode": str})
    table_ohlc["BSECode"] = table_ohlc["BSECode"].replace('-1', np.nan)
    table_ohlc = table_ohlc.astype({"Volume": int})

    return table_ohlc


def merge_background(table_ohlc, conn):

    sql_background = 'SELECT * FROM public."BackgroundInfo" ;'
    background_info = sqlio.read_sql_query(sql_background, con=conn)

    table_ohlc["BSECode"].fillna(0, inplace=True)
    table_ohlc = table_ohlc.astype({"BSECode": int})

    table_ohlc = pd.merge(table_ohlc, background_info[['CompanyCode', 'ISINCode']], left_on='ISIN', right_on='ISINCode',
                          how='left')

    table_ohlc = table_ohlc.astype({"BSECode": str})
    table_ohlc["BSECode"] = table_ohlc["BSECode"].replace('0', np.nan)

    return table_ohlc


def insert_ohlc(table_ohlc, conn, cur):

    table_ohlc = table_ohlc[['Company', 'NSECode', 'BSECode', 'ISIN', 'Open', 'High', 'Low', 'Close',
                             'Date', 'Value', 'Volume', 'CompanyCode']]

    exportfilename = "exportOHLC.csv"
    exportfile = open(exportfilename, "w")
    table_ohlc.to_csv(exportfile, header=True,
                      index=False, lineterminator='\r')
    exportfile.close()

    copy_sql = """
           COPY public."OHLC" FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """
    with open(exportfilename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)
        f.close()

    print("OHLC Insert Completed")
    os.remove(exportfilename)


# NSE table = table[['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES', 'ISIN']]
# BSE table = table[['SC_CODE', 'SC_NAME', 'SC_GROUP', 'SC_TYPE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'NO_TRADES', 'NO_OF_SHRS', 'NET_TURNOV', 'TDCLOINDI', 'ISIN_CODE', 'TRADING_DATE']]

def ohlc_date_join(dates=None):

    if dates is None:
        return

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    for date in dates:

        ohlc_bse = fetch_bse(conn, cur, date)
        ohlc_nse = fetch_nse(conn, cur, date)
        if(not(ohlc_nse is None or ohlc_bse is None)):
            ohlc_join(ohlc_nse, ohlc_bse, conn, cur)
        else:
            print("Cannot insert for date: "+str(date))

    conn.commit()
    conn.close()


def main():

    conn = DB_Helper().db_connect()
    cur = conn.cursor()
    print("\n\t\t OHLC Fetch Service Started..........")
    Check_Helper().check_path(filepath)

    ohlc_nse = fetch_nse(conn, cur)
    ohlc_bse = fetch_bse(conn, cur)

    if(not(ohlc_nse is None or ohlc_bse is None)):  
        
        insert_nse(ohlc_nse, conn, cur)
        insert_bse(ohlc_bse, conn, cur)

        ohlc_full = ohlc_join(ohlc_nse, ohlc_bse, conn, cur)
        ohlc_full = merge_background(ohlc_full, conn)
        insert_ohlc(ohlc_full, conn, cur)

    else:
        print("BSE/NSE table is Null")
        raise ValueError('OHLC could not be inserted due to null error')

    conn.commit()
    print("\n\t\t OHLC Fetch Completed.")
    conn.close()
    
