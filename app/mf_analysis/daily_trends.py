# Python script to generate trends data for daily OHLC
import datetime
import requests
import os.path
import os
from zipfile import ZipFile
import csv
import psycopg2
import pandas as pd
import calendar
import numpy as np
import pandas.io.sql as sqlio
import time
import math
from datetime import timedelta
from mf_analysis.calc_trends_helper import Trends
from ta import trend, volatility
import numpy as np


class DailyTrends():
    """ Contains methods to calculate trends data for stocks based on daily indicator data.
    """

    def __init__(self):

        self.calc_trends = Trends()

    def __del__(self):
        pass

    def get_indicator_daily(self, conn, date):
        """ Function to get current day's indicator data.

            Args: 
                conn: database connection. 
                date: current day's date. 

            Returns: 
                indicator_daily: dataframe of current day's indicator data. 

            Raises:
                No errors/exceptions. 
        """

        sql = 'SELECT * FROM mf_analysis.indicators where "gen_date" = \'' + \
            str(date)+'\';'
        indicator_daily = sqlio.read_sql_query(sql, con=conn)

        return indicator_daily

    def get_indicator_daily_back(self, conn, date):
        """ Function to get previous day's indicator data.

            Args: 
                conn: database connection. 
                date: current day's date. 

            Returns: 
                indicator_daily_back: dataframe of previous day's indicator data. 

            Raises: 
                No errors/exceptions. 
        """

        # sql = 'SELECT DISTINCT ON("company_code") * FROM mf_analysis.indicators \
        #        WHERE "gen_date" < \''+str(self.date)+'\' \
        #        ORDER BY "company_code", "gen_date" DESC;'

        sql = 'SELECT DISTINCT ON("company_code") * FROM mf_analysis.indicators \
               WHERE "gen_date" < \''+str(date)+'\' \
               ORDER BY "company_code", "gen_date" DESC;'
        indicator_daily_back = sqlio.read_sql_query(sql, con=conn)

        return indicator_daily_back

    def get_trends_daily(self, indicator_daily, indicator_daily_back):
        """ Function to get trends data for current day. 

            Args:
                indicator_daily: current day's indicator data. 
                indicator_daily_back: previous day's indicator data. 

            Returns: 
                trends_daily: dataframe containing daily trends data. 

            Raises:
                No errors/exceptions.  
        """

        trends_daily = self.calc_trends.calc_trends(
            indicator_daily, indicator_daily_back)

        return trends_daily

    def insert_daily_trends(self, conn, cur, trends_data):
        """ Function to insert Daily trends data into the DB.

            Args:
                conn: database connection.
                cur: cursor using the connection. 
                trends_data: trends data returned from get_trends_daily function. 

            Returns: 
                None

            Raises: 
                No errors/exceptions. 
        """

        # casting BSE as integer
        trends_data["bse_code"].fillna(-1, inplace=True)
        trends_data = trends_data.astype({"bse_code": int})
        trends_data = trends_data.astype({"bse_code": str})
        trends_data["bse_code"] = trends_data["bse_code"].replace('-1', np.nan)

        trends_data = trends_data.astype({"bullish_trending": int})
        trends_data = trends_data.astype({"bearish_trending": int})
        trends_data = trends_data.astype({"bullish_non_trending": int})
        trends_data = trends_data.astype({"bearish_non_trending": int})
        trends_data = trends_data.astype({"long": int})
        trends_data = trends_data.astype({"short": int})
        trends_data = trends_data.astype({"long_sideways": int})
        trends_data = trends_data.astype({"short_sideways": int})
        trends_data = trends_data.astype({"buy": int})
        trends_data = trends_data.astype({"sell": int})

        trends_data = trends_data[['company_code', 'company_name', 'nse_code', 'bse_code', 'bullish_trending', 'bearish_trending', 'bullish_non_trending',
                                   'bearish_non_trending', 'long', 'short', 'long_sideways', 'short_sideways', 'buy', 'sell', 'gen_date']]

        exportfilename = "trends_data.csv"
        exportfile = open(exportfilename, "w")
        trends_data.to_csv(exportfile, header=True, index=False,
                           float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY mf_analysis.trends FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
        os.remove(exportfilename)
