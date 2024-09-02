# Python script to Get daily OHLC for MF Trends calculation
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

class DailyOHLC():
    """ Contains methods which fetch BTT, OHLC data for current day.
    """

    def __init__(self):
        pass

    def __del__(self):
        pass

    def get_btt(self, conn, date):
        """ Function to fetch BTTList for current month. 

            Args:
                conn: database connection
                date: date for which the process is run 

            Returns:
                btt: dataframe of BTTList for current month

            Raises:
                No errors/exceptions. 
        """

        month_back_date = date + datetime.timedelta(-30)

        btt_sql = 'SELECT "CompanyCode", "CompanyName" FROM public."BTTList" \
                   WHERE "BTTDate" = (SELECT MAX("BTTDate") FROM public."BTTList" \
                                     WHERE "BTTDate" <= \''+str(date)+'\' \
                                     AND "BTTDate" >= \''+str(month_back_date)+'\');'
        btt = sqlio.read_sql_query(btt_sql, con=conn)

        return btt

    def get_ohlc(self, conn, date):
        """ Function to fetch OHLC data for given date. 

            Args: 
                conn: database connection. 
                date: current day's date. 

            Returns: 
                ohlc_list: dataframe of OHLC list for provided date. 

            Raises: 
                No errors/exceptions. 
        """

        # Uncomment this in case of running history calculation
        # ohlc_sql = 'SELECT * FROM public."OHLC" WHERE "Date" >= \''+back_date+'\' AND "Date" <= \''+date+'\' AND "CompanyCode" IS NOT NULL;'
        # ohlc_list = sqlio.read_sql_query(ohlc_sql, con = conn)

        ohlc_sql = 'SELECT * FROM public."OHLC" WHERE "Date" = \'' + \
            str(date)+'\' AND "CompanyCode" is not null;'
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
