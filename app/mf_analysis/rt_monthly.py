# Script to generate trends data for OHLC monthly

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
import datetime
import time
from dateutil.relativedelta import relativedelta
import math
from datetime import timedelta
from ta import trend, volatility
import numpy as np

from mf_analysis.monthly_ohlc import MonthlyOHLC
from mf_analysis.monthly_indicator import MonthlyIndicator
from mf_analysis.monthly_trends import MonthlyTrends

from mf_analysis.common_trend_weightage import TrendWeightageCommon
import utils.date_set as date_set


class MonthlyRTProcess():
    """ Class containing methods which are run in order to generate RT data for everyday.
    """

    def __init__(self):
    
        self.date = datetime.date.today()
        self.back_date = datetime.date(2010, 1, 1)
        self.back1yr = datetime.date(self.date.year -1, self.date.month, self.date.day)

        self.monthly_ohlc_class = MonthlyOHLC()
        self.monthly_indicator_class = MonthlyIndicator()
        self.monthly_trends_class = MonthlyTrends()
        
        self.trend_weightage_common = TrendWeightageCommon()

    def __del__(self):
        pass

    def monthly_ohlc(self, conn, cur, date):
        """ Function to call methods in MonthlyOHLC class in order to generate
            monthly OHLC data. 

            Args: 
                conn: database connection.
                cur: cursor object using the connection. 
                date: month's end date
            
            Returns: 
                None
            
            Raises:
                No errors/exceptions. 
        """

        print("Getting current BTT list")
        btt_list = self.monthly_ohlc_class.get_btt(conn, date)

        print("Getting OHLC data for current month")
        ohlc_list = self.monthly_ohlc_class.get_ohlc_month(conn, date)

        print("Getting BTT stocks from OHLC list")
        ohlc_list = self.monthly_ohlc_class.merge_btt_ohlc(btt_list, ohlc_list)
        # print(ohlc_list)

        # print("COLUMN ohlc_list\n",ohlc_list.columns)
        # ['CompanyCode', 'CompanyName', 'Company', 'NSECode', 'BSECode',
        #  'ISIN','Open', 'High', 'Low', 'Close', 'Date', 'Value', 'Volume']

        print("Processing monthly OHLC list")
        monthly_ohlc_list = self.monthly_ohlc_class.process_month_ohlc(ohlc_list, date)

        # print("COLUMN ohlc_list\n", monthly_ohlc_list.columns)
        # ['company_code', 'company_name', 'nse_code', 'bse_code', 'open',
        #  'high','low', 'close', 'volume', 'date']

        print("Inserting monthly OHLC data")
        self.monthly_ohlc_class.insert_monthly_ohlc(monthly_ohlc_list, conn, cur)

    def monthly_indicator(self, conn, cur, date):
        """ Function to call methods in MonthlyIndicators class in order to generate 
            indicator data for monthly OHLC. 

            Args: 
                conn: database connection. 
                cur: cursor object using the connection. 
                date: month's end date, i.e. every friday's date.
            
            Returns: 
                None
            
            Raises:
                No errors/exceptions. 
        """

        print("Calculating indicator values for monthly OHLC")
        monthly_indicator_data = self.monthly_indicator_class.technical_indicators_monthly(conn, date)

        # print("monthly_indicator_data \n", monthly_indicator_data.head())

        print("Inserting into table")
        self.monthly_indicator_class.insert_monthly_indicators(conn, cur, monthly_indicator_data)

    def monthly_trends(self, conn, cur, date):
        """ Function to call methods in MonthlyTrends in order to generate trends data for monthly OHLC. 

            Args: 
                conn: database connection. 
                cur: cursor object using the connection. 
                date: month's end date, i.e. every friday's date.
            
            Returns: 
                None
            
            Raises:
                No errors/exceptions.
        """

        print("Getting current month indicator data")
        monthly_indicator = self.monthly_trends_class.get_indicator_monthly(conn, date)

        print("Getting previous indicator data")
        monthly_indicator_back = self.monthly_trends_class.get_indicator_monthly_back(conn, date)

        print("Calculating trends for the month")
        monthly_trends_data = self.monthly_trends_class.get_trends_monthly(monthly_indicator, monthly_indicator_back)

        print("Inserting into table")
        self.monthly_trends_class.insert_monthly_trends(conn, cur, monthly_trends_data)
    
    def gen_rt_monthly(self, curr_date, conn, cur):
        """ Function to generate Trends data for the month.

            Args: 
                conn: database connection. 
                cur: cursor object using the connection. 

            Returns: 
                None
            
            Raises: 
                No errors/exceptions. 
        """


        print("Generating trends data for the month\n")
        self.monthly_ohlc(conn, cur, curr_date)
        self.monthly_indicator(conn, cur, curr_date)
        self.monthly_trends(conn, cur, curr_date)

    def gen_rt_monthly_history(self, conn, cur):
        """ Function to generate Trends data for the month.

            Args: 
                conn: database connection. 
                cur: cursor object using the connection. 

            Returns: 
                None
            
            Raises: 
                No errors/exceptions. 
        """

        start_date = self.back_date
        end_date = self.date

        while(end_date >= start_date):

            start_date = self.last_day_of_month(start_date)

            print("start_date\n", start_date)
            print("Generating Monthly OHLC for the month:\n", start_date)
            # self.monthly_ohlc(conn, cur, start_date)
            # self.monthly_indicator(conn, cur, start_date)
            self.monthly_trends(conn, cur, start_date)

            start_date = start_date + relativedelta(months=1)
            print("Moving to the next date:", start_date)


    def last_day_of_month(self, date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)
    
    
    
    ## Trend Weightage Process for monthly and Back months for RT Trends Data ##    
    def get_rt_monthly(self, curr_date, conn):
        
        """ Fetch the data for monthly RT trends
        
            Args: 
                conn : database connection.
            
            Returns: 
                rt_monthly_df : RT trends data for Monthly.
        
        """
        rt_monthly_query = 'SELECT "gen_date", "rt_bullish_trending", "rt_bearish_trending", \
                            "rt_bullish_non_trending", rt_bearish_non_trending  FROM \
                            mf_analysis.trends_monthly where "gen_date" = \''+str(curr_date)+'\'; '
        
        rt_monthly_df = sqlio.read_sql_query(rt_monthly_query, con=conn)
        
        return rt_monthly_df
    
        
    def get_rt_monthly_backdate_df(self, curr_date, conn):
        
        """ Fetch the data for 1 year back from the latest month ending date
            of RT trends monthly table.
            
            Args: 
                conn: database connection.
            
            Returns: 
                rt_monthly_1yr_back_df : One year back RT monthly data.     
            
        """ 
        back1yr= datetime.date(curr_date.year -1, curr_date.month, curr_date.day)
        rt_monthly_1yr_back_query = 'SELECT "gen_date", "rt_bullish_trending", "rt_bearish_trending", \
                                    "rt_bullish_non_trending", rt_bearish_non_trending \
                                     FROM mf_analysis.trends_monthly where "gen_date" between \
                                    \''+str(back1yr)+'\' and \''+str(curr_date)+'\' \
                                    order by "gen_date" desc;'
        rt_monthly_1yr_back_df = sqlio.read_sql_query(rt_monthly_1yr_back_query, con =conn)
        
        return rt_monthly_1yr_back_df
    
    
    def trend_weightage_monthly(self, rt_monthly_df):
        
        """ Calculate the trend weightage values for current date or back dates.

            Args: 
                rt_monthly_df : current date RT monthly data.
            
            Returns: 
                monthly_trend_weightage_df : date, weightage of the trend weightage
                                            for monthly. 
        
        """
        monthly_trend_weightage_df = self.trend_weightage_common.\
                                    cal_trend_weightage_rt_data(rt_monthly_df)

        return monthly_trend_weightage_df
    
    
    def insert_trend_weightage_df(self, monthly_trend_weightage_df, conn, cur):
        
        """ Inserting those calculated Trend Weightage values into DB for
            latest date or back date.

            Args: 
                monthly_trend_weightage_df : date, weightage of the trend weightage
                                            for monthly and monthly back dates.
                conn : database connection.
                cur : cursor object using the connection.   
        
        """

        monthly_trend_weightage_df["weightage"].fillna(-1, inplace=True)
        monthly_trend_weightage_df = monthly_trend_weightage_df.astype({"weightage": int})
        weekly_trend_weightage_df = monthly_trend_weightage_df.astype({"weightage": str})
        monthly_trend_weightage_df["weightage"] = monthly_trend_weightage_df["weightage"].replace('-1', np.nan)

        exportfilename = "monthly_trend_weightage_df.csv"
        exportfile = open(exportfilename,"w")
        monthly_trend_weightage_df.to_csv(exportfile, header=True, \
            index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        
        copy_sql = """
        COPY mf_analysis."trend_weightage_monthly" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
        with open(exportfilename, 'r') as f:

            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename) 
    
    
    #Process for Trend weightage Back dates           
    def gen_trend_weightage_monthly_history(self, conn, cur):
        
        """ Function to generate Monthly Trend weightage
            for back dates of RT monthly data.
            
        """
        print("RT monthly Data for Back Dates")
        rt_monthly_backdate_df = self.get_rt_monthly_backdate_df(conn)
        
        print("Calculate Trend weightage values for 1 year.")
        monthly_trend_weightage_backdate_df = self.trend_weightage_monthly(rt_monthly_backdate_df)
        
        print("Inserting Trend weightage Into the DB")
        self.insert_trend_weightage_df(monthly_trend_weightage_backdate_df, conn, cur)
        
        
    #Process for Trend weightage monthly dates      
    def gen_trend_weightage_monthly_data(self, curr_date, conn, cur):
        
        """ Function to generate monthly Trend weightage
            for RT monthly data.
            
        """        
        print("RT monthly Data for Current Date.")
        rt_monthly_df = self.get_rt_monthly(curr_date, conn)
        
        if not(rt_monthly_df.empty):
        
            print("Calculate Trend weightage values for Current Date.")
            monthly_trend_weightage_df = self.trend_weightage_monthly(rt_monthly_df)
            
            print("Inserting Trend weightage Into the DB")
            self.insert_trend_weightage_df(monthly_trend_weightage_df, conn, cur)
            
        else:
            print("RT monthly Data is not found for this date:", curr_date)
            raise ValueError("RT monthly Data not found for current date: "+str(curr_date))