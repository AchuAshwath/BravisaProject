# Python script to generate trends data for monthly OHLC
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


class MonthlyTrends():
    """ Contains methods to calculate trends data for stocks based on monthly
        indicator data.
    """

    def __init__(self):
        self.calc_trends = Trends()

    def __del__(self):
        pass

    def get_indicator_monthly(self, conn, date):
        """ Function to get monthly indicator data for latest date.

            Args:
                conn: database connection.
                date: month's end date, i.e. every friday's date.

            Returns:
                indicator_monthly: dataframe containing indicator data for given date.
            
            Raises:
                No errors/exceptions.
        """

        sql = 'SELECT * FROM mf_analysis.indicators_monthly \
               WHERE "gen_date" = \''+str(date)+'\';'
        indicator_monthly = sqlio.read_sql_query(sql, con=conn)

        return indicator_monthly

    def get_indicator_monthly_back(self, conn, date):
        """ Function to get monthly indicator date for previous date.

            Args:
                conn: database connection.
                date: month's end date, i.e. every friday's date.

            Returns: 
                indicator_monthly_back: dataframe containing indicator data 
                for previous date.
            
            Raises:
                No errors/exceptions.
        """

        sql = 'SELECT DISTINCT ON("company_code") * FROM mf_analysis.indicators_monthly \
               WHERE "gen_date" < \''+str(date)+'\' \
               ORDER BY "company_code", "gen_date" DESC;'
        indicator_monthly_back = sqlio.read_sql_query(sql, con=conn)

        return indicator_monthly_back

    def get_trends_monthly(self, indicator_monthly, indicator_monthly_back):
        """ Function to get monthly trends data for latest date.
            
            Args:
                indicator_monthly: current month's indicator data.
                indicator_monthly_back: previous month's indicator data.
            
            Returns:
                trends_monthly: dataframe containing monthly trends data.
            
            Raises:
                No errors/exceptions.
        """

        trends_monthly = self.calc_trends.calc_trends(indicator_monthly, indicator_monthly_back)

        return trends_monthly

    def insert_monthly_trends(self, conn, cur, trends_data):
        """ Function to insert Monthly trends data into the DB.

            Args:
                conn: database connection.
                cur: cursor using the connection. 
                trends_data: trends data returned from get_trends_monthly function.

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

        trends_data = trends_data[['company_code', 'company_name', 'nse_code', \
                                    'bse_code', 'bullish_trending', 'bearish_trending', \
                                    'bullish_non_trending', 'bearish_non_trending', \
                                    'long', 'short', 'long_sideways', 'short_sideways', \
                                    'buy', 'sell', 'gen_date']]
    
        exportfilename = "trends_data.csv"
        exportfile = open(exportfilename, "w")
        trends_data.to_csv(exportfile, header = True, index = False, float_format = "%.2f", lineterminator = '\r')
        exportfile.close()  

        copy_sql = """
            COPY mf_analysis.trends_monthly FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
        os.remove(exportfilename)