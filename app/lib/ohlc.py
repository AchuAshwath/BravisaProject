# Script for fetching and inserting OHLC data
import datetime
import os.path
import os
from utils.db_helper import DB_Helper
from utils.check_helper import Check_Helper
import pandas as pd
import numpy as np
import pandas.io.sql as sqlio
import time
from utils.logs import insert_logs

LOGS = {
    "log_date": None,
    "log_time": None,
    "BTT_count": None,
    "ISIN_matches" : None,
    "BTT_fix": None,
    "OHLC_with_CompanyCode": None,
    "OHLC_count": None,
    "nse_file": None,
    "bse_file": None,
    "runtime": None
}

if os.name == 'nt':
    cwd = os.getcwd()
    filepath = os.path.join(cwd, "OHLCFiles\\")
    print("File Path :",filepath)

# Function to fetch BSE OHLC daily file and store it in provided file path
def fetch_bse(curr_date):
    
    download_date = curr_date.strftime("%Y%m%d")         

    print("\n\nBSE Fetch invoked for date : ", download_date)
    csv_file = filepath+ 'BhavCopy_BSE_CM_0_0_0_' + download_date + '_F_0000.csv'  #"BhavCopy_BSE_CM_0_0_0_20240709_F_0000"

    download_date_bse = curr_date.strftime("%d%m%y")
    old_csv_file = filepath+'EQ_ISINCODE_' + download_date_bse  + '.CSV'
    
    
    try:
        table = pd.read_csv(csv_file)
        print("bse file : ", csv_file)
    except FileNotFoundError:
        table = pd.read_csv(old_csv_file)
        print("bse file : ", csv_file)
    
    LOGS["bse_file"] = csv_file
                            
    bse_changed_format_dictionary = {'FinInstrmId' : 'SC_CODE', 'TckrSymb': 'SC_NAME','SctySrs': 'SC_GROUP', 'OpnPric':'OPEN',	'HghPric' :'HIGH',	
                                    'LwPric' : 'LOW', 'ClsPric' : 'CLOSE','LastPric' : 'LAST',	'PrvsClsgPric' : 'PREVCLOSE', 'TtlNbOfTxsExctd' : 'NO_TRADES',
                                    'TtlTradgVol' : 'NO_OF_SHRS','TtlTrfVal':'NET_TURNOV',  'ISIN':'ISIN_CODE', 'TradDt':'TRADING_DATE'}

    table.rename(columns = bse_changed_format_dictionary, inplace= True)  

    table['SC_TYPE'] = np.nan  
    table['TDCLOINDI'] = np.nan  

    table_columns = ['SC_CODE', 'SC_NAME', 'SC_GROUP', 'SC_TYPE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                     'PREVCLOSE', 'NO_TRADES', 'NO_OF_SHRS', 'NET_TURNOV', 'TDCLOINDI', 'ISIN_CODE', 'TRADING_DATE']
    csv_columns = list(table.columns.values)
    columns_to_remove = [x for x in csv_columns if x not in table_columns]
    table = table.drop(columns_to_remove, axis=1)
    table = table[['SC_CODE', 'SC_NAME', 'SC_GROUP', 'SC_TYPE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                   'PREVCLOSE', 'NO_TRADES', 'NO_OF_SHRS', 'NET_TURNOV', 'TDCLOINDI', 'ISIN_CODE', 'TRADING_DATE']]

    return table


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

# # Function to fetch NSE OHLC daily file and store it in provided file path
def fetch_nse( curr_date):
    print("\n\nNSE Fetch invoked for date : ", curr_date)

    download_date = curr_date.strftime("%Y%m%d").upper()

    csv_file = filepath+"BhavCopy_NSE_CM_0_0_0_"+download_date+"_F_0000.csv" #+"\\cm"+download_date+"bhav.csv" "BhavCopy_NSE_CM_0_0_0_20240711_F_0000.csv"
    
    download_date_nse = curr_date.strftime("%d%b%Y").upper()
    old_csv_file = filepath+"CM"+download_date_nse+"BHAV.CSV"
    
    try:
        table = pd.read_csv(csv_file)
        print("nse file : ", csv_file)
    except FileNotFoundError:
        table = pd.read_csv(old_csv_file)
        print("nse file : ", csv_file)
    
    LOGS["nse_file"] = csv_file
        
    nse_changed_format_dictionary = {'TckrSymb' : 'SYMBOL', 'SctySrs': 'SERIES','OpnPric': 'OPEN', 	'HghPric' :'HIGH',	'LwPric':'LOW',	'ClsPric' :'CLOSE',	
                                     'LastPric' : 'LAST', 'PrvsClsgPric' : 'PREVCLOSE','TtlTradgVol' : 'TOTTRDQTY',	'TtlTrfVal' : 'TOTTRDVAL', 'TradDt' : 'TIMESTAMP',
                                     'TtlNbOfTxsExctd' : 'TOTALTRADES','ISIN':'ISIN'}

    table.rename(columns = nse_changed_format_dictionary, inplace= True)

    table_columns = ['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                     'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES', 'ISIN']
    csv_columns = list(table.columns.values)
    columns_to_remove = [x for x in csv_columns if x not in table_columns]

    table = table.drop(columns_to_remove, axis=1)
    table = table[table.SERIES.isin(["EQ", "BZ", "BE", "RR", "IV"])]
    table = table[['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                   'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES', 'ISIN']]

    return table



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


def ohlc_join(ohlc_nse, ohlc_bse, conn, curr):

    ohlc_bse['SC_NAME'] = ohlc_bse['SC_NAME'].str.strip().str.replace(' ', '')

    table_ohlc = pd.merge(ohlc_nse, ohlc_bse, left_on='ISIN',right_on='ISIN_CODE', how='outer')
    
    # table_ohlc = pd.merge(ohlc_nse, ohlc_bse, left_on='SYMBOL', right_on='SC_NAME', how='outer')

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
    # get the null values in the CompanyCode column
    null_company_codes = table_ohlc.loc[table_ohlc['CompanyCode'].isnull()]
    print("nulls while merging", len(null_company_codes))
    # print(null_company_codes['ISIN'])
    table_ohlc = table_ohlc.astype({"BSECode": str})
    table_ohlc["BSECode"] = table_ohlc["BSECode"].replace('0', np.nan)

        # Replace "nan" values in the BSECode column with an empty string
    table_ohlc['BSECode'] = table_ohlc['BSECode'].fillna('')

    # Convert empty strings to NaN
    table_ohlc['BSECode'] = table_ohlc['BSECode'].replace('', np.nan)

    # Convert BSECode column to integer type
    # table_ohlc['BSECode'] = table_ohlc['BSECode'].astype(float).astype('Int64')

    return table_ohlc


def btt_fix(ohlc_full, curr_date, conn):
    # Define date range for querying current month data
    BTT_back = datetime.date(curr_date.year, curr_date.month, 1).strftime("%Y-%m-%d")
    next_month = curr_date.month + 1 if curr_date.month + 1 <= 12 else 1
    next_year = curr_date.year if curr_date.month + 1 <= 12 else curr_date.year + 1
    BTT_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")

    # Fetch BTT list data for the current month
    btt_sql = """
    SELECT *
    FROM "BTTList"
    WHERE "BTTDate" >= %s AND "BTTDate" < %s
    """
    bttlist = pd.read_sql_query(btt_sql, con=conn, params=(BTT_back, BTT_next))
    
    # Logging count of BTT records
    LOGS["BTT_count"] = len(bttlist)
    print(f"Total BTT records fetched: {len(bttlist)}")    
    # Initialize counter to track the number of rows updated
    btt_fix_count = 0

    # Filter bttlist to only rows with matching ISINs in ohlc_full
    btt_ohlc = bttlist[bttlist['ISIN'].isin(ohlc_full['ISIN'])]
    LOGS['ISIN_matches'] = len(btt_ohlc)
    print(f"Total BTT records matching ISINs in ohlc_full: {len(btt_ohlc)}")
    
    # Iterate over ohlc_full to check for missing CompanyCode and fill values if ISIN matches
    for index, row in ohlc_full.iterrows():
        if row['ISIN'] in btt_ohlc['ISIN'].values:
            matching_btt_row = btt_ohlc[btt_ohlc['ISIN'] == row['ISIN']].iloc[0]
            
            # Check if CompanyCode is missing in ohlc_full
            if pd.isnull(row['CompanyCode']):
                # Fill in missing CompanyCode, NSECode, BSECode from matching bttlist row
                ohlc_full.at[index, 'CompanyCode'] = matching_btt_row['CompanyCode']
                ohlc_full.at[index, 'NSECode'] = matching_btt_row['NSECode']
                ohlc_full.at[index, 'BSECode'] = matching_btt_row['BSECode']
                
                # Increment the counter since we've updated a row
                btt_fix_count += 1

    # Log the count of updated rows
    LOGS["BTT_fix"] = btt_fix_count
    print(f"Total rows updated in btt_fix: {btt_fix_count}")
    

    # Remove duplicates based on CompanyCode to ensure uniqueness
    ohlc_full = ohlc_full.drop_duplicates(subset=['CompanyCode'])

    # Create ohlc_clean DataFrame by filtering rows with no nulls in critical columns
    ohlc_clean = ohlc_full.dropna(subset=['CompanyCode', 'NSECode', 'BSECode', 'ISIN'])
    
    # Logging final count of cleaned OHLC data
    LOGS["OHLC_count"] = len(ohlc_full)
    LOGS["OHLC_with_CompanyCode"] = len(ohlc_clean)
    print(f"OHLC count with CompanyCode: {len(ohlc_clean)}")
    
    print("Final length of ohlc_full:", len(ohlc_full))
    
    return ohlc_full

def insert_ohlc(table_ohlc, conn, cur):
    table_ohlc = table_ohlc[['Company', 'NSECode', 'BSECode', 'ISIN', 'Open', 'High', 'Low', 'Close',
                                'Date', 'Value', 'Volume', 'CompanyCode']]

    # Replace "nan" values in the BSECode column with an empty string
    table_ohlc['BSECode'] = table_ohlc['BSECode'].fillna('')

    # Convert empty strings to NaN
    table_ohlc['BSECode'] = table_ohlc['BSECode'].replace('', np.nan)

    # Convert BSECode column to integer type
    table_ohlc['BSECode'] = table_ohlc['BSECode'].astype(float).astype('Int64')

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

    conn.commit()  # Commit the transaction
    print("OHLC Insert Completed")
    os.remove(exportfilename)

def ohlc_date_join(dates=None):

    if dates is None:
        return

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    for date in dates:
        ohlc_bse = fetch_bse(date)
        ohlc_nse = fetch_nse(date)
        if(not(ohlc_nse is None or ohlc_bse is None)):
            ohlc_join(ohlc_nse, ohlc_bse, conn, cur)
        else:
            print("Cannot insert for date: "+str(date))

    conn.commit()
    conn.close()


def main(curr_date):
    start_time = time.time()
    
    cwd = os.getcwd()

    conn = DB_Helper().db_connect()
    cur = conn.cursor()
    print("\n\t\t OHLC Fetch Service Started..........\n")
    Check_Helper().check_path(filepath)
    
    #LOGS initialization
    LOGS["log_date"] = curr_date
    LOGS["log_time"] = datetime.datetime.now().strftime("%H:%M:%S")

    ohlc_nse = fetch_nse(curr_date)
    # print(ohlc_nse)
    ohlc_bse = fetch_bse(curr_date)
    # print(ohlc_bse)

    insert_nse(ohlc_nse, conn, cur)
    insert_bse(ohlc_bse, conn, cur)

    if(not(ohlc_nse is None or ohlc_bse is None)):
        ohlc_joined = ohlc_join(ohlc_nse, ohlc_bse, conn, cur)
        ohlc_bg = merge_background(ohlc_joined, conn)
        ohlc_full = btt_fix(ohlc_bg, curr_date,conn)
        insert_ohlc(ohlc_full, conn, cur)
        # print(ohlc_full)

    else:
        print("BSE/NSE table is Null")
        # raise ValueError('OHLC could not be inserted due to null error')

    conn.commit()
    print("\n\t\t OHLC Fetch Completed.")
    
    end_time = time.time() 
    LOGS["runtime"] = end_time - start_time
    
    insert_logs("OHLC", [LOGS], conn, cur)
    print("Inserted logs")
    
    conn.close()
    

    return "OHLC generation Completed"
