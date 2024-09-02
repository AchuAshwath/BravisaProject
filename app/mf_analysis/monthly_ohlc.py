# Python script to get Monthly OHLC and process it
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



class MonthlyOHLC():
    """ Contains methods which fetch BTT, OHLC data for the month and process it to
        get consolidated MonthlyOHLC data.
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

        # month_back = date + datetime.timedelta(-30)
    
        btt_sql = 'SELECT "CompanyCode", "CompanyName" FROM public."BTTList" \
                   WHERE "BTTDate" = (SELECT MAX("BTTDate") FROM public."BTTList") \
                   AND "CompanyCode" IS NOT NULL;'
        btt = sqlio.read_sql_query(btt_sql, con = conn)

        return btt

    def get_ohlc_month(self, conn, date):
        """ Function to fetch OHLC data for the current month.
            Date passed in here should be every month end date.

            Args:
                conn: database connection.
                date: current day's date.

            Returns:
                ohlc_list: dataframe of OHLC list for the entire month.

            Raises:
                No errors/exceptions.

        """

        # month_start_date = date + datetime.timedelta(-4)
        month_first_day = date.replace(day=1)

        ohlc_sql = 'SELECT * FROM public."OHLC" WHERE "Date" >= \
                    \'' + str(month_first_day) + '\' AND "Date" <= \''+str(date) + '\' \
                    AND "CompanyCode" IS NOT NULL;'
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

    def process_month_ohlc(self, ohlc_list, date):
        """ Process monthly OHLC data to get Week's Open/High/Low/Close for every stock.

            Week High: Highest 'High' value for a particular stock in the month.
            Week Low: Lowest 'Low' value for a particular stock in the month.
            Week Open: 'Open' value for month start date for a particular stock.
            Week Close: 'Close' value for month end date for a particular stock.

            Args:
                ohlc_list: ohlc data for the entire month(returned from merge_btt_ohlc function).

            Returns:
                ohlc_monthly: dataframe containing monthly OHLC data for all the BTT stocks.

            Raises:
                No errors/exceptions.
        """

        ohlc_month_list = ohlc_list
        stock_list = ohlc_month_list['CompanyCode'].drop_duplicates().tolist()

        ohlc_monthly = pd.DataFrame()

        # print("stock_list\n",stock_list)

        for stock in stock_list:

            stock_data = ohlc_month_list.loc[ohlc_month_list['CompanyCode'] == stock]

            stock_name = ohlc_month_list.loc[ohlc_month_list['CompanyCode'] == stock]['Company'].head(1)
            stock_NSE = ohlc_month_list.loc[ohlc_month_list['CompanyCode'] == stock]['NSECode'].head(1)
            stock_BSE = ohlc_month_list.loc[ohlc_month_list['CompanyCode'] == stock]['BSECode'].head(1)
            # print(stock_name)
            # print(stock_NSE)
            # print(stock_BSE)


            start_date = stock_data['Date'].min()
            end_date = stock_data['Date'].max()

            # Get Week's start and end date for every stock
            start_day_ = stock_data.loc[stock_data['Date'] == start_date]
            end_day_ = stock_data.loc[stock_data['Date'] == end_date]

            high_val_list = stock_data['High']
            high_val = high_val_list.max()

            low_val_list = stock_data['Low']
            low_val = low_val_list.min()

            open_val = np.unique(start_day_['Open'].values,axis=0)
            close_val = np.unique(end_day_['Close'].values, axis=0)
            volume = np.unique(end_day_['Volume'].values,axis=0)

            # d={'company_code': stock, 'company_name': stock_name, \
            #      'nse_code': stock_NSE, 'bse_code': stock_BSE, 'open': open_val, \
            #      'high': high_val, 'low': low_val, 'close': close_val, 'volume': volume, \
            #      'date': date}
            # print(d)

            df = pd.DataFrame(data={'company_code': stock, 'company_name': stock_name, \
                 'nse_code': stock_NSE, 'bse_code': stock_BSE, 'open': open_val, \
                 'high': high_val, 'low': low_val, 'close': close_val, 'volume': volume, \
                 'date': date})
                    
            # ohlc_monthly = ohlc_monthly.append(df)
            ohlc_monthly = pd.concat([ohlc_monthly, df])

            ohlc_monthly = ohlc_monthly.reset_index(drop=True)
        
        # print("VALUE OHLC MONTH\n", ohlc_monthly.head())
       
        return ohlc_monthly

    def insert_monthly_ohlc(self, ohlc_monthly, conn, cur):
        """ Function to insert month's OHLC in table.

            Args:
                ohlc_monthly: processed OHLC for the month(data returned from process_month_ohlc)

            Returns:
                None.

            Raises:
                No errors/exceptions.
        """

        # print("ohlc_monthly\n",ohlc_monthly.head())
        # Fill null values as -1 to cast bse_code as integer and replace by it by NaN
        ohlc_monthly["bse_code"].fillna(-1, inplace=True)
        ohlc_monthly = ohlc_monthly.astype({"bse_code": int})
        ohlc_monthly = ohlc_monthly.astype({"bse_code": str})
        ohlc_monthly["bse_code"] = ohlc_monthly["bse_code"].replace('-1', np.nan)

        # Fill null values as -1 to cast volume as integer and replace by it by NaN
        ohlc_monthly["volume"].fillna(-1, inplace=True)
        ohlc_monthly = ohlc_monthly.astype({"volume": int})
        ohlc_monthly = ohlc_monthly.astype({"volume": str})
        ohlc_monthly["volume"] = ohlc_monthly["volume"].replace('-1', np.nan)

        exportfilename = "ohlc_monthly.csv"
        exportfile = open(exportfilename, "w")
        ohlc_monthly.to_csv(exportfile, header=True, index=False,  float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
        COPY public.ohlc_monthly FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
