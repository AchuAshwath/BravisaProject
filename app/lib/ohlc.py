
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
import utils.date_set as date_see
import time
import rootpath

# headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8', 'Connection': 'keep-alive', 'Content-Type': 'application/zip', 'Referer': 'https://www.nseindia.com/products/content/derivatives/equities/archieve_fo.htm',
#            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36', 'X-frame-options': 'SAMEORIGIN'}
# cookies = {'pointer': '1', 'sym1': 'KPIT',
#            'NSE-TEST-1': '1826627594.20480.0000'}

if os.name == 'nt':
    my_path = os.path.abspath(os.path.dirname('D:\\Desktop Copy\\Braviza\\app\\'))
    filepath = os.path.join(my_path, "OHLCFiles\\")
    print("File Path :",filepath)
else:
    my_path = rootpath.detect()
    filepath = os.path.join(my_path, "ohlc-files/")


# Function to fetch BSE OHLC daily file and store it in provided file path
def fetch_bse(conn, cur, curr_date):
    
    download_date = curr_date.strftime("%Y%m%d")         
    # print("download date\t",download_date)

    print("\n\nBSE Fetch invoked ....")

    csv_file = filepath+ 'BhavCopy_BSE_CM_0_0_0_' + download_date + '_F_0000.csv'  #"BhavCopy_BSE_CM_0_0_0_20240709_F_0000"
    ''' 
    Uncomment the next line to run it from a new file.
    File format example : EQ_ISINCODE_150722_New.csv                                                                      
    '''
    # csv_file = filepath+'EQ_ISINCODE_' + download_date + '_New' + '.CSV'

    print("bse file : ", csv_file)

    table = pd.read_csv(csv_file)

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

    # os.remove(csv_file)

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


def fetch_nse(conn, cur, curr_date):
    print("\n\nNSE Fetch invoked ....")

    download_date = curr_date.strftime("%Y%m%d").upper()

    csv_file = filepath+"BhavCopy_NSE_CM_0_0_0_"+download_date+"_F_0000.csv" #+"\\cm"+download_date+"bhav.csv" "BhavCopy_NSE_CM_0_0_0_20240711_F_0000.csv"
    print("nse file : ", csv_file)
    table = pd.read_csv(csv_file)

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
    # os.remove(csv_file)

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


# def ohlc_join(ohlc_nse, ohlc_bse, conn, cur):

#     table_ohlc = pd.merge(ohlc_nse, ohlc_bse, left_on='ISIN',
#                           right_on='ISIN_CODE', how='outer')
#     table_ohlc["ISIN"].fillna(table_ohlc["ISIN_CODE"], inplace=True)
#     table_ohlc["OPEN_x"].fillna(table_ohlc["OPEN_y"], inplace=True)
#     table_ohlc["HIGH_x"].fillna(table_ohlc["HIGH_y"], inplace=True)
#     table_ohlc["LOW_x"].fillna(table_ohlc["LOW_y"], inplace=True)
#     table_ohlc["CLOSE_x"].fillna(table_ohlc["CLOSE_y"], inplace=True)
#     table_ohlc["LAST_x"].fillna(table_ohlc["LAST_y"], inplace=True)
#     table_ohlc["PREVCLOSE_x"].fillna(table_ohlc["PREVCLOSE_y"], inplace=True)
#     table_ohlc["TIMESTAMP"].fillna(table_ohlc["TRADING_DATE"], inplace=True)
#     table_ohlc["TOTTRDVAL"].fillna(table_ohlc["NET_TURNOV"], inplace=True)
#     table_ohlc["TOTTRDQTY"].fillna(table_ohlc["NO_OF_SHRS"], inplace=True)

#     table_ohlc.rename(columns={'SC_NAME': 'Company',  'SC_CODE': 'BSECode', 'SYMBOL': 'NSECode',
#                                'OPEN_x': 'Open', 'HIGH_x': 'High', 'LOW_x': 'Low', 'CLOSE_x': 'Close', 'LAST_x': 'Last',
#                                'PREVCLOSE_x': 'PrevClose', 'TIMESTAMP': 'Date', 'TOTTRDVAL': 'Value', 'TOTTRDQTY': 'Volume'}, inplace=True)

#     table_columns = ['Company', 'BSECode', 'NSECode', 'ISIN',
#                      'Open', 'High', 'Low', 'Close', 'Date', 'Value', 'Volume']
#     csv_columns = list(table_ohlc.columns.values)
#     columns_to_remove = [x for x in csv_columns if x not in table_columns]
#     table_ohlc = table_ohlc.drop(columns_to_remove, axis=1)
#     table_ohlc = table_ohlc[["Company",  "NSECode", "BSECode", "ISIN",
#                              "Open", "High", "Low", "Close", "Date", "Value", "Volume"]]

#     table_ohlc["BSECode"].fillna(-1, inplace=True)
#     table_ohlc = table_ohlc.astype({"BSECode": int})
#     table_ohlc = table_ohlc.astype({"BSECode": str})
#     table_ohlc["BSECode"] = table_ohlc["BSECode"].replace('-1', np.nan)
#     table_ohlc = table_ohlc.astype({"Volume": int})

#     return table_ohlc


# def merge_background(table_ohlc, conn):
#     # Assuming you have necessary imports and connections set up
    
#     # Fetching background info from the database
#     sql_background = 'SELECT * FROM public."BackgroundInfo" ;'
#     background_info = pd.read_sql_query(sql_background, con=conn)
    
#     # Merging the dataframes
#     table_ohlc = pd.merge(table_ohlc, background_info[['CompanyCode', 'ISINCode']], 
#                            left_on='ISIN', right_on='ISINCode', how='left')
    
#     # Checking for null CompanyCode in table_ohlc after merging
#     null_company_codes = table_ohlc.loc[table_ohlc['CompanyCode'].isnull(), ['BSECode']]
    
#     # Mapping CompanyCode if BSECode matches
#     for index, row in null_company_codes.iterrows():
#         bse_code = row['BSECode']
#         if not pd.isnull(bse_code):
#             matching_row = background_info[background_info['BSECode'] == bse_code]
#             if not matching_row.empty:
#                 table_ohlc.at[index, 'CompanyCode'] = matching_row.iloc[0]['CompanyCode']

    
#     return table_ohlc


# def merge_background(table_ohlc, conn):

#     sql_background = 'SELECT * FROM public."BackgroundInfo" ;'
#     background_info = sqlio.read_sql_query(sql_background, con=conn)

#     table_ohlc["BSECode"].fillna(0, inplace=True)
#     table_ohlc = table_ohlc.astype({"BSECode": int})

#     table_ohlc = pd.merge(table_ohlc, background_info[['CompanyCode', 'ISINCode']], left_on='ISIN', right_on='ISINCode',
#                           how='left')

#     table_ohlc = table_ohlc.astype({"BSECode": str})
#     table_ohlc["BSECode"] = table_ohlc["BSECode"].replace('0', np.nan)

#     return table_ohlc


# def btt_fix(ohlc_full, curr_date, conn):
#     today = curr_date
#     BTT_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
#     count = 0
#     next_month = today.month + 1 if today.month + 1 <= 12 else 1
#     next_year = today.year if today.month + 1 <= 12 else today.year + 1
#     BTT_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")

#     btt_sql = """
#     SELECT *
#     FROM "BTTList"
#     WHERE "BTTDate" >= %s AND "BTTDate" < %s
#     """
#     bttlist = pd.read_sql_query(btt_sql, con=conn, params=(BTT_back, BTT_next))

#     ohlc_full["BSECode"].fillna(0, inplace=True)
#     ohlc_full = ohlc_full.astype({"BSECode": int})

#     coco_null_ohlc_full = ohlc_full[ohlc_full["CompanyCode"].isnull()]

#     for index, row in coco_null_ohlc_full.iterrows():
#         nsecode = row['NSECode']
#         bsecode = row['BSECode']
#         isin = row['ISIN']

#         if nsecode is not None:
#             btt_data = bttlist[bttlist["NSECode"] == nsecode]
#             if not btt_data.empty:
#                 coco_null_ohlc_full.loc[index, 'CompanyCode'] = btt_data['CompanyCode'].values[0]
#                 coco_null_ohlc_full.loc[index, 'BSECode'] = btt_data['BSECode'].values[0]
#                 if pd.isnull(coco_null_ohlc_full.loc[index, 'ISIN']):
#                     coco_null_ohlc_full.loc[index, 'ISIN'] = btt_data['ISIN'].values[0]
#                 count += 1
#         elif bsecode is not None:
#             btt_data = bttlist[bttlist["BSECode"] == bsecode]
#             coco_null_ohlc_full.loc[index, 'CompanyCode'] = btt_data['CompanyCode'].values[0]
#             coco_null_ohlc_full.loc[index, 'NSECode'] = btt_data['NSECode'].values[0]
#             if pd.isnull(coco_null_ohlc_full.loc[index, 'ISIN']):
#                 coco_null_ohlc_full.loc[index, 'ISIN'] = btt_data['ISIN'].values[0]

#     ohlc_full.update(coco_null_ohlc_full)

#     null_btt_ohlc = coco_null_ohlc_full[coco_null_ohlc_full["CompanyCode"].isnull()]

#     for index, row in null_btt_ohlc.iterrows():
#         bsecode = row['BSECode']

#         background_info_sql = """
#         SELECT * FROM public."BackgroundInfo"
#         WHERE "BSECode" = %s
#         """
#         background_info = pd.read_sql_query(background_info_sql, con=conn, params=(bsecode,))

#         if not background_info.empty:
#             ohlc_full.at[index, 'CompanyCode'] = background_info['CompanyCode'].values[0]
#             ohlc_full.at[index, 'BSECode'] = background_info['BSECode'].values[0]  # Fill BSECode as well
            
#     ohlc_full = ohlc_full.astype({"BSECode": str})
#     ohlc_full["BSECode"] = ohlc_full["BSECode"].replace('0', np.nan)

#     print("Count:", count)

#     return ohlc_full



# def btt_fix(ohlc_full, curr_date,conn):
#     today = curr_date
#     BTT_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
#     count =0
#     next_month = today.month + 1 if today.month + 1 <= 12 else 1
#     next_year = today.year if today.month + 1 <= 12 else today.year + 1

#     BTT_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")

#     # print(BTT_back, BTT_next)

#     btt_sql = """
#     SELECT *
#     FROM "BTTList"
#     WHERE "BTTDate" >= %s AND "BTTDate" < %s
#     """
#     bttlist = pd.read_sql_query(btt_sql, con=conn, params=(BTT_back, BTT_next))

#     ohlc_full["BSECode"].fillna(0, inplace=True)
#     ohlc_full = ohlc_full.astype({"BSECode": int})

#     # filter ohlc_full where CompanyCode is null
#     coco_null_ohlc_full = ohlc_full[ohlc_full["CompanyCode"].isnull()]
#     # print(type(coco_null_ohlc_full))

#     # nse_null_ohlc_full = ohlc_full[ohlc_full["NSECode"].isnull()]
#     # bse_null_ohlc_full = ohlc_full[ohlc_full["BSECode"].isnull()]
#     for index, row in coco_null_ohlc_full.iterrows():  

#         nsecode = row['NSECode']
#         bsecode = row['BSECode']
#         isin = row['ISIN']
#         # print("nsecode: ", nsecode)

#         if nsecode is not None:
#             btt_data = bttlist[bttlist["NSECode"] == nsecode]
#             # print(len(btt_data))
#             if not btt_data.empty:
#                 # print("BTT Data: ", btt_data)
#                 # replace the CompanyCode with the CompanyCode from btt_data
#                 coco_null_ohlc_full.loc[index, 'CompanyCode'] = btt_data['CompanyCode'].values[0]
#                 # print("companycode has been replaced for ", nsecode, 'as ', btt_data['CompanyCode'].values[0])
#                 # replace the bsecode with the BSECode from btt_data 
#                 coco_null_ohlc_full.loc[index, 'BSECode'] = btt_data['BSECode'].values[0]
#                 # # replace the isi with the ISIN from btt_data
#                 coco_null_ohlc_full.loc[index, 'ISIN'] = btt_data['ISIN'].values[0]
#                 count = count + 1
#         elif bsecode is not None:
#             btt_data = bttlist[bttlist["BSECode"] == bsecode]
#             # replace the CompanyCode with the CompanyCode from btt_data
#             coco_null_ohlc_full.loc[index, 'CompanyCode'] = btt_data['CompanyCode'].values[0]
#             # replace the nsecode with the NSECode from btt_data
#             coco_null_ohlc_full.loc[index, 'NSECode'] = btt_data['NSECode'].values[0]
#             # replace the isi with the ISIN from btt_data
#             coco_null_ohlc_full.loc[index, 'ISIN'] = btt_data['ISIN'].values[0]
#         # repalce coco_null_ohlc_full with the updated values in the ohlc_full
#         ohlc_full.update(coco_null_ohlc_full)
#     # filter the ohlc_full where CompanyCode matches with the CompanyCode in bttlist
#     btt_ohlc = ohlc_full[ohlc_full['NSECode'].isin(bttlist['NSECode']) | 
#                           ohlc_full['BSECode'].isin(bttlist['BSECode']) |
#                           ohlc_full['ISIN'].isin(bttlist['ISIN'])]

#     null_btt_ohlc = btt_ohlc[btt_ohlc["CompanyCode"].isnull()]

#     for index, row in null_btt_ohlc.iterrows():
#         nsecode = row['NSECode']
#         bsecode = row['BSECode']
#         isin = row['ISIN']

#         background_info_sql = """
#         SELECT * FROM public."BackgroundInfo"
#         WHERE "NSECode" = %s
#         OR "BSECode" = %s
#         OR "ISINCode" = %s
#         """
#         background_info = pd.read_sql_query(background_info_sql, con=conn, params=(nsecode, bsecode, isin))
#         # print(len(background_info))
#         if not background_info.empty:
#             # assing the values from background_info to the null_btt_ohlc
#             null_btt_ohlc.loc[index, 'CompanyCode'] = background_info['CompanyCode'].values[0]
#             # null_btt_ohlc.loc[index, 'Company'] = background_info['CompanyName'].values[0]
#             null_btt_ohlc.loc[index, 'ISIN'] = background_info['ISINCode'].values[0]
#             null_btt_ohlc.loc[index, 'NSECode'] = background_info['NSECode'].values[0]
#             null_btt_ohlc.loc[index, 'BSECode'] = background_info['BSECode'].values[0]

#             # print("CompanyCode has been replaced for ", nsecode, 'as ', background_info['CompanyCode'].values[0])
#         # repalce coco_null_ohlc_full with the updated values in the ohlc_full
#         ohlc_full.update(null_btt_ohlc)
    
        
    
#     ohlc_full = ohlc_full.astype({"BSECode": str})
#     ohlc_full["BSECode"] = ohlc_full["BSECode"].replace('0', np.nan)
#     # print("count: ", count)
    
#     return ohlc_full



# def insert_ohlc(table_ohlc, conn, cur):

#     table_ohlc = table_ohlc[['Company', 'NSECode', 'BSECode', 'ISIN', 'Open', 'High', 'Low', 'Close',
#                              'Date', 'Value', 'Volume', 'CompanyCode']]

#     exportfilename = "exportOHLC.csv"
#     exportfile = open(exportfilename, "w")
#     table_ohlc.to_csv(exportfile, header=True,
#                       index=False, lineterminator='\r')
#     exportfile.close()

#     copy_sql = """
#            COPY public."OHLC" FROM stdin WITH CSV HEADER
#            DELIMITER as ','
#            """
#     with open(exportfilename, 'r') as f:
#         cur.copy_expert(sql=copy_sql, file=f)
#         f.close()

#     print("OHLC Insert Completed")
#     os.remove(exportfilename)

def ohlc_join(ohlc_nse, ohlc_bse, conn, curr):

    ohlc_bse['SC_NAME'] = ohlc_bse['SC_NAME'].str.strip().str.replace(' ', '')

    table_ohlc = pd.merge(ohlc_nse, ohlc_bse, left_on='SYMBOL',
                          right_on='SC_NAME', how='outer')

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

        # Replace "nan" values in the BSECode column with an empty string
    table_ohlc['BSECode'] = table_ohlc['BSECode'].fillna('')

    # Convert empty strings to NaN
    table_ohlc['BSECode'] = table_ohlc['BSECode'].replace('', np.nan)

    # Convert BSECode column to integer type
    # table_ohlc['BSECode'] = table_ohlc['BSECode'].astype(float).astype('Int64')

    return table_ohlc

# NSE table = table[['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES', 'ISIN']]
# BSE table = table[['SC_CODE', 'SC_NAME', 'SC_GROUP', 'SC_TYPE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'NO_TRADES', 'NO_OF_SHRS', 'NET_TURNOV', 'TDCLOINDI', 'ISIN_CODE', 'TRADING_DATE']
def btt_fix(ohlc_full, curr_date,conn):
    today = curr_date
    BTT_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
    count = 0
    next_month = today.month + 1 if today.month + 1 <= 12 else 1
    next_year = today.year if today.month + 1 <= 12 else today.year + 1
    print("initial ohlc_full : ",len(ohlc_full))
    BTT_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")

    # print(BTT_back, BTT_next)

    btt_sql = """
    SELECT *
    FROM "BTTList"
    WHERE "BTTDate" >= %s AND "BTTDate" < %s
    """
    bttlist = pd.read_sql_query(btt_sql, con=conn, params=(BTT_back, BTT_next))
    print("bttlist : ",len(bttlist))
    # print(len(ohlc_full[ohlc_full["CompanyCode"].isnull()]))
    # filter ohlc_full where CompanyCode is null
    coco_null_ohlc_full = ohlc_full[ohlc_full[["CompanyCode", "NSECode", "BSECode"]].isnull().any(axis=1)]
    coco_null_ohlc_full = pd.DataFrame(coco_null_ohlc_full)  # Convert to DataFrame
    print("initial companycode nulls in ohlc : ",len(coco_null_ohlc_full))

    # nse_null_ohlc_full = ohlc_full[ohlc_full["NSECode"].isnull()]
    # bse_null_ohlc_full = ohlc_full[ohlc_full["BSECode"].isnull()]
    for index, row in coco_null_ohlc_full.iterrows():

        nsecode = row['NSECode']
        bsecode = row['BSECode']
        isin = row['ISIN']
        # print("nsecode: ", nsecode)

        if nsecode is not None:
            btt_data = bttlist[bttlist["NSECode"] == nsecode]
            # print(len(btt_data))
            if not btt_data.empty:
                # print("BTT Data: ", btt_data)
                # replace the CompanyCode with the CompanyCode from btt_data
                coco_null_ohlc_full.loc[index, 'CompanyCode'] = btt_data['CompanyCode'].values[0]
                # print("companycode has been replaced for ", nsecode, 'as ', btt_data['CompanyCode'].values[0])
                # replace the bsecode with the BSECode from btt_data 
                coco_null_ohlc_full.loc[index, 'BSECode'] = btt_data['BSECode'].values[0]
                # # replace the isi with the ISIN from btt_data
                # if coco_null_ohlc_full.loc[index, 'ISIN'] is  null
                # if coco_null_ohlc_full.loc[index, 'ISIN'].isnull():
                if pd.isnull(coco_null_ohlc_full.loc[index, 'ISIN']):

                    coco_null_ohlc_full.loc[index, 'ISIN'] = btt_data['ISIN'].values[0]
                count = count + 1
        elif bsecode is not None:
            btt_data = bttlist[bttlist["BSECode"] == bsecode]
            # replace the CompanyCode with the CompanyCode from btt_data
            coco_null_ohlc_full.loc[index, 'CompanyCode'] = btt_data['CompanyCode'].values[0]
            # replace the nsecode with the NSECode from btt_data
            # coco_null_ohlc_full.loc[index, 'NSECode'] = btt_data['NSECode'].values[0]
            # # replace the isi with the ISIN from btt_data
            # # if coco_null_ohlc_full.loc[index, 'ISIN'].isnull():
            # if pd.isnull(coco_null_ohlc_full.loc[index, 'ISIN']):
            #     coco_null_ohlc_full.loc[index, 'ISIN'] = btt_data['ISIN'].values[0]
            count = count + 1   

        # repalce coco_null_ohlc_full with the updated values in the ohlc_full
        ohlc_full.update(coco_null_ohlc_full)
    print("ohlc_full : ",len(ohlc_full))
    print("count count replaced in bttlist : ", count)
    
    count = 0

    # filter the ohlc_full where CompanyCode matches with the CompanyCode in bttlist
    # btt_ohlc = ohlc_full[ohlc_full['NSECode'].isin(bttlist['NSECode']) | 
    #                       ohlc_full['BSECode'].isin(bttlist['BSECode']) |
    #                       ohlc_full['ISIN'].isin(bttlist['ISIN'])]
    # print("btt_ohlc : ",len(btt_ohlc))

    null_btt_ohlc = ohlc_full[ohlc_full[["CompanyCode", "NSECode", "BSECode"]].isnull().any(axis=1)]
    print("null_btt_ohlc : ",len(null_btt_ohlc))
    for index, row in null_btt_ohlc.iterrows():
        nsecode = row['NSECode']
        bsecode = row['BSECode']
        isin = row['ISIN']



        background_info_sql = """
        SELECT * FROM public."BackgroundInfo"
        WHERE "NSECode" = %s::varchar
        OR "BSECode" = %s
        OR "ISINCode" = %s::varchar
        """
        background_info = pd.read_sql_query(background_info_sql, con=conn, params=(nsecode, bsecode, isin))
        # if bsecode == 506879:
        #     print(nsecode, bsecode, isin)
        #     print(background_info)
        # print(len(background_info))
        if not background_info.empty:
            # assing the values from background_info to the null_btt_ohlc
            null_btt_ohlc.loc[index, 'CompanyCode'] = background_info['CompanyCode'].values[0]
            # null_btt_ohlc.loc[index, 'Company'] = background_info['CompanyName'].values[0]
            # null_btt_ohlc.loc[index, 'ISIN'] = background_info['ISINCode'].values[0]
            # null_btt_ohlc.loc[index, 'NSECode'] = background_info['NSECode'].values[0]
            # null_btt_ohlc.loc[index, 'BSECode'] = background_info['BSECode'].values[0]
            count = count + 1
            # print("CompanyCode has been replaced for ", nsecode, 'as ', background_info['CompanyCode'].values[0])
        # repalce coco_null_ohlc_full with the updated values in the ohlc_full
        ohlc_full.update(null_btt_ohlc)

    ohlc_full = ohlc_full.drop_duplicates(subset=['CompanyCode'])
    print("ohlc_full : ",len(ohlc_full))
    print("count count replaced in backgrounndinfo : ", count)

    return ohlc_full

# def btt_fix(ohlc_full, curr_date, conn):
#     today = curr_date
#     BTT_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
#     count = 0
#     next_month = today.month + 1 if today.month + 1 <= 12 else 1
#     next_year = today.year if today.month + 1 <= 12 else today.year + 1

#     BTT_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")

#     # print(BTT_back, BTT_next)

#     btt_sql = """
#     SELECT *
#     FROM "BTTList"
#     WHERE "BTTDate" >= %s AND "BTTDate" < %s
#     """
#     bttlist = pd.read_sql_query(btt_sql, con=conn, params=(BTT_back, BTT_next))
#     # print(len(bttlist))
#     # print(len(ohlc_full[ohlc_full["CompanyCode"].isnull()]))
#     # filter ohlc_full where CompanyCode is null
#     coco_null_ohlc_full = ohlc_full[ohlc_full[["CompanyCode", "BSECode", "NSECode"]].isnull().any(axis = 1)]
#     coco_null_ohlc_full = pd.DataFrame(coco_null_ohlc_full)  # Convert to DataFrame
#     # print(type(coco_null_ohlc_full))

#     # nse_null_ohlc_full = ohlc_full[ohlc_full["NSECode"].isnull()]
#     # bse_null_ohlc_full = ohlc_full[ohlc_full["BSECode"].isnull()]
#     for index, row in coco_null_ohlc_full.iterrows():

#         nsecode = row['NSECode']
#         bsecode = row['BSECode']
#         isin = row['ISIN']
#         # print("nsecode: ", nsecode)

#         if nsecode is not None:
#             btt_data = bttlist[bttlist["NSECode"] == nsecode]
#             # print(len(btt_data))
#             if not btt_data.empty:
#                 # print("BTT Data: ", btt_data)
#                 # replace the CompanyCode with the CompanyCode from btt_data
#                 coco_null_ohlc_full.loc[index, 'CompanyCode'] = btt_data['CompanyCode'].values[0]
#                 # print("companycode has been replaced for ", nsecode, 'as ', btt_data['CompanyCode'].values[0])
#                 # replace the bsecode with the BSECode from btt_data 
#                 coco_null_ohlc_full.loc[index, 'BSECode'] = btt_data['BSECode'].values[0]
#                 # # replace the isi with the ISIN from btt_data
#                 # if coco_null_ohlc_full.loc[index, 'ISIN'] is  null
#                 # if coco_null_ohlc_full.loc[index, 'ISIN'].isnull():
#                 if pd.isnull(coco_null_ohlc_full.loc[index, 'ISIN']):
#                     coco_null_ohlc_full.loc[index, 'ISIN'] = btt_data['ISIN'].values[0]
#                 count = count + 1
#         elif bsecode is not None:
#             btt_data = bttlist[bttlist["BSECode"] == bsecode]
#             # replace the CompanyCode with the CompanyCode from btt_data
#             coco_null_ohlc_full.loc[index, 'CompanyCode'] = btt_data['CompanyCode'].values[0]
#             # replace the nsecode with the NSECode from btt_data
#             coco_null_ohlc_full.loc[index, 'NSECode'] = btt_data['NSECode'].values[0]
#             # replace the isi with the ISIN from btt_data
#             # if coco_null_ohlc_full.loc[index, 'ISIN'].isnull():
#             if pd.isnull(coco_null_ohlc_full.loc[index, 'ISIN']):
#                 coco_null_ohlc_full.loc[index, 'ISIN'] = btt_data['ISIN'].values[0]
#             count = count + 1   

#         # repalce coco_null_ohlc_full with the updated values in the ohlc_full
#         ohlc_full.update(coco_null_ohlc_full)
#     # print("count: ", count)
    
#     count = 0

#     # filter the ohlc_full where CompanyCode matches with the CompanyCode in bttlist
#     # btt_ohlc = ohlc_full[ohlc_full['NSECode'].isin(bttlist['NSECode']) | 
#     #                       ohlc_full['BSECode'].isin(bttlist['BSECode']) |
#     #                       ohlc_full['ISIN'].isin(bttlist['ISIN'])]

#     null_btt_ohlc = ohlc_full[ohlc_full[["CompanyCode", "BSECode", "NSECode"]].isnull().any(axis = 1)]

#     for index, row in null_btt_ohlc.iterrows():
#         nsecode = row['NSECode']
#         bsecode = row['BSECode']
#         isin = row['ISIN']

#         # print(nsecode, bsecode, isin)

#         background_info_sql = """
#         SELECT * FROM public."BackgroundInfo"
#         WHERE "NSECode" = %s::varchar
#         OR "BSECode" = %s
#         OR "ISINCode" = %s::varchar
#         """
#         background_info = pd.read_sql_query(background_info_sql, con=conn, params=(nsecode, bsecode, isin))
#         # print(len(background_info))
#         if not background_info.empty:
#             # assing the values from background_info to the null_btt_ohlc
#             null_btt_ohlc.loc[index, 'CompanyCode'] = background_info['CompanyCode'].values[0]
#             # null_btt_ohlc.loc[index, 'Company'] = background_info['CompanyName'].values[0]
#             # null_btt_ohlc.loc[index, 'ISIN'] = background_info['ISINCode'].values[0]
#             null_btt_ohlc.loc[index, 'NSECode'] = background_info['NSECode'].values[0]
#             null_btt_ohlc.loc[index, 'BSECode'] = background_info['BSECode'].values[0]
#             count = count + 1
#             # print("CompanyCode has been replaced for ", nsecode, 'as ', background_info['CompanyCode'].values[0])
#         # repalce coco_null_ohlc_full with the updated values in the ohlc_full
#         ohlc_full.update(null_btt_ohlc)
#     # print("count: ", count)

#     # print(len(ohlc_full[ohlc_full["CompanyCode"].isnull()]))

#     return ohlc_full

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
        ohlc_bse = fetch_bse(conn, cur, date)
        ohlc_nse = fetch_nse(conn, cur, date)
        if(not(ohlc_nse is None or ohlc_bse is None)):
            ohlc_join(ohlc_nse, ohlc_bse, conn, cur)
        else:
            print("Cannot insert for date: "+str(date))

    conn.commit()
    conn.close()


def main(curr_date):
    cwd = os.getcwd()

    conn = DB_Helper().db_connect()
    cur = conn.cursor()
    print("\n\t\t OHLC Fetch Service Started..........\n")
    Check_Helper().check_path(filepath)

    ohlc_nse = fetch_nse(conn, cur, curr_date)
    # print(ohlc_nse)
    ohlc_bse = fetch_bse(conn, cur, curr_date)
    # print(ohlc_bse)

    insert_nse(ohlc_nse, conn, cur)
    insert_bse(ohlc_bse, conn, cur)

    if(not(ohlc_nse is None or ohlc_bse is None)):
        ohlc_full = ohlc_join(ohlc_nse, ohlc_bse, conn, cur)
        ohlc_full = merge_background(ohlc_full, conn)
        ohlc_full = btt_fix(ohlc_full, curr_date,conn)
        insert_ohlc(ohlc_full, conn, cur)
        # print(ohlc_full)

    else:
        print("BSE/NSE table is Null")
        # raise ValueError('OHLC could not be inserted due to null error')

    conn.commit()
    print("\n\t\t OHLC Fetch Completed.")
    conn.close()
    return "OHLC"
