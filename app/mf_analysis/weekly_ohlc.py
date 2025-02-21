# Python script to get weekly OHLC and process it
import datetime
import requests
import os.path
import os
import csv
import psycopg2
import pandas as pd
import calendar
import numpy as np
import pandas.io.sql as sqlio
import time
import math
from datetime import timedelta


class WeeklyOHLC():
    """ Contains methods which fetch BTT, OHLC data for the week and process it to 
        get consolidated WeeklyOHLC data.
    """

    def __init__(self):
        pass

    def __del__(self):
        pass

    def get_btt(self, conn, date):
        """ Function to fetch BTTList for current month. 

            Args: 
                conn: database connection

            Returns: 
                btt: dataframe of BTTList for current month

            Raises: 
                No errors/exceptions. 
        """

        month_back = date + datetime.timedelta(-30)
    
        btt_sql = 'SELECT "CompanyCode", "CompanyName" FROM public."BTTList" \
                   WHERE "BTTDate" >= \''+str(month_back)+'\' AND "BTTDate" <= \''+str(date)+'\' \
                   AND "CompanyCode" is NOT NULL;'
        btt = sqlio.read_sql_query(btt_sql, con = conn)

        return btt

    def get_ohlc_week(self, conn, date):
        """ Function to fetch OHLC data for the current week. 
            Date passed in here should be every friday's date. 

            Args: 
                conn: database connection. 
                date: current day's date. 

            Returns: 
                ohlc_list: dataframe of OHLC list for the entire week.  

            Raises: 
                No errors/exceptions.

        """

        week_start_date = date + datetime.timedelta(-4)

        ohlc_sql = 'SELECT * FROM public."OHLC" WHERE "Date" >= \''+str(week_start_date)+'\' AND "Date" <= \''+str(date)+'\' \
                    AND "CompanyCode" IS NOT NULL;'
        print(ohlc_sql)
        ohlc_list = sqlio.read_sql_query(ohlc_sql, con=conn)

        return ohlc_list

    def merge_btt_ohlc(self, btt, ohlc):
        """ Merge OHLC and BTT to get BTT specific stocks. 

            Args: 
                btt: BTT list returned from get_btt function. 
                ohlc: OHLC list returned from get_ohlc function. 

            Returns: 
                ohlc_list: dataframe resulting in pandas merge(left join) of btt and ohlc. 

            Raises: 
                No errors/exceptions. 
        """

        ohlc_list = pd.merge(btt, ohlc, on='CompanyCode', how='left')

        ohlc_list.sort_values(by=['Date'], inplace=True, ascending=True)
        ohlc_list = ohlc_list[np.isfinite(ohlc_list['Close'])]

        return ohlc_list

    def process_week_ohlc(self, ohlc_list, date):
        """ Process weekly OHLC data to get Week's Open/High/Low/Close for every stock. 

            Week High: Highest 'High' value for a particular stock in the week.
            Week Low: Lowest 'Low' value for a particular stock in the week.
            Week Open: 'Open' value for week start date for a particular stock. 
            Week Close: 'Close' value for week end date for a particular stock.

            Args: 
                ohlc_list: ohlc data for the entire week(returned from merge_btt_ohlc function).

            Returns: 
                ohlc_weekly: dataframe containing weekly OHLC data for all the BTT stocks. 

            Raises: 
                No errors/exceptions.   
        """

        ohlc_week_list = ohlc_list
        stock_list = ohlc_week_list['CompanyCode'].drop_duplicates().tolist()

        ohlc_weekly = pd.DataFrame()

        # print("stock_list\n",stock_list)

        for stock in stock_list:

            stock_data = ohlc_week_list.loc[ohlc_week_list['CompanyCode'] == stock]

            start_date = stock_data['Date'].min()
            end_date = stock_data['Date'].max()

            # Get Week's start and end date for every stock
            start_day_ = stock_data.loc[stock_data['Date'] == start_date]
            end_day_ = stock_data.loc[stock_data['Date'] == end_date]

            # print("start_day_\n",start_day_)
            # print("end_day_\n",end_day_)

            high_val_list = stock_data['High']
            high_val = high_val_list.max()

            low_val_list = stock_data['Low']
            low_val = low_val_list.min()

            open_val = start_day_['Open'].values
            close_val = end_day_['Close'].values
            volume = end_day_['Volume'].values

            # Ensure all arrays are of equal length by checking and padding if necessary
            max_len = max(len(open_val), len(close_val), len(volume))
            open_val = np.pad(open_val, (0, max_len - len(open_val)), 'constant', constant_values=np.nan)
            close_val = np.pad(close_val, (0, max_len - len(close_val)), 'constant', constant_values=np.nan)
            volume = np.pad(volume, (0, max_len - len(volume)), 'constant', constant_values=np.nan)

            # print(stock)
            df = pd.DataFrame(data={'company_code': stock, 'open': open_val, 'high': high_val,
                                    'low': low_val, 'close': close_val, 'volume': volume, 'date': date})

            # print("df\n",df)

            ohlc_weekly = pd.concat([ohlc_weekly, df])
            # ohlc_weekly = ohlc_weekly.append(df)

            ohlc_weekly = ohlc_weekly.reset_index(drop=True)
            # print("ohlc_weekly\n",ohlc_weekly)

        return ohlc_weekly

    def insert_weekly_ohlc(self, ohlc_weekly, conn, cur):
        """ Function to insert week's OHLC in table. 

            Args: 
                ohlc_weekly: processed OHLC for the week(data returned from process_week_ohlc)

            Returns: 
                None

            Raises: 
                No errors/exceptions. 
        """

        # print(ohlc_weekly)
        #check if ohlc_weekly is not empty
        if not ohlc_weekly.empty:
            # Fill null values as -1 to cast volume as integer and replace by it by NaN
            ohlc_weekly["volume"].fillna(-1, inplace=True)
            ohlc_weekly = ohlc_weekly.astype({"volume": int})
            ohlc_weekly = ohlc_weekly.astype({"volume": str})
            ohlc_weekly["volume"] = ohlc_weekly["volume"].replace('-1', np.nan)

            exportfilename = "ohlc_weekly.csv"
            exportfile = open(exportfilename, "w")
            ohlc_weekly.to_csv(exportfile, header=True, index=False,
                            float_format="%.2f", lineterminator='\r')
            exportfile.close()

            copy_sql = """
            COPY public.ohlc_weekly FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

            with open(exportfilename, 'r') as f:
                cur.copy_expert(sql=copy_sql, file=f)
                conn.commit()
                f.close()
                os.remove(exportfilename)
        else:
            print("ohlc_weekly is empty")