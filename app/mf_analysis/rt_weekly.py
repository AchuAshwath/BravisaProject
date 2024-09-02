# Script to generate trends data for OHLC weekly
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
from ta import trend, volatility
import numpy as np
from mf_analysis.weekly_ohlc import WeeklyOHLC
from mf_analysis.weekly_indicator import WeeklyIndicator
from mf_analysis.weekly_trends import WeeklyTrends

from mf_analysis.common_trend_weightage import TrendWeightageCommon
import utils.date_set as date_set



class WeeklyRTProcess():
    """ Class containing methods which are run in order to generate RT data for everyday. 
    """

    def __init__(self):

        self.date = datetime.date.today()
        self.back_date = datetime.date.today() + datetime.timedelta(-66)
        self.back1yr = datetime.date(self.date.year -1, self.date.month, self.date.day)

        self.weekly_ohlc_class = WeeklyOHLC()
        self.weekly_indicator_class = WeeklyIndicator()
        self.weekly_trends_class = WeeklyTrends()
        
        self.trend_weightage_common = TrendWeightageCommon()

    def __del__(self):
        pass

    def weekly_ohlc(self, conn, cur, date):
        """ Function to call methods in WeeklyOHLC class in order to generate 
            weekly OHLC data. 

            Args: 
                conn: database connection.
                cur: cursor object using the connection. 
                date: week's end date, i.e. every friday's date.

            Returns: 
                None

            Raises:
                No errors/exceptions. 
        """

        print("Getting current BTT list")
        btt_list = self.weekly_ohlc_class.get_btt(conn, date)

        print("Getting OHLC data for current week")
        ohlc_list = self.weekly_ohlc_class.get_ohlc_week(conn, date)

        print("Getting BTT stocks from OHLC list")
        ohlc_list = self.weekly_ohlc_class.merge_btt_ohlc(btt_list, ohlc_list)

        print("Processing weekly OHLC list")
        weekly_ohlc_list = self.weekly_ohlc_class.process_week_ohlc(
            ohlc_list, date)

        print("Inserting weekly OHLC data")
        self.weekly_ohlc_class.insert_weekly_ohlc(weekly_ohlc_list, conn, cur)

    def weekly_indicator(self, conn, cur, date):
        """ Function to call methods in WeeklyIndicators class in order to generate 
            indicator data for weekly OHLC. 

            Args: 
                conn: database connection. 
                cur: cursor object using the connection. 
                date: week's end date, i.e. every friday's date.

            Returns: 
                None

            Raises:
                No errors/exceptions. 
        """

        print("Calculating indicator values for weekly OHLC")
        weekly_indicator_data = self.weekly_indicator_class.technical_indicators_weekly(
            conn, date)

        print("Inserting into table")
        self.weekly_indicator_class.insert_weekly_indicators(
            conn, cur, weekly_indicator_data)

    def weekly_trends(self, conn, cur, date):
        """ Function to call methods in WeeklyTrends in order to generate trends data for weekly OHLC. 

            Args: 
                conn: database connection. 
                cur: cursor object using the connection. 
                date: week's end date, i.e. every friday's date.

            Returns: 
                None

            Raises:
                No errors/exceptions.
        """

        print("Getting current week indicator data")
        weekly_indicator = self.weekly_trends_class.get_indicator_weekly(
            conn, date)

        print("Getting previous indicator data")
        weekly_indicator_back = self.weekly_trends_class.get_indicator_weekly_back(
            conn, date)

        print("Calculating trends for the week")
        weekly_trends_data = self.weekly_trends_class.get_trends_weekly(
            weekly_indicator, weekly_indicator_back)

        print("Inserting into table")
        self.weekly_trends_class.insert_weekly_trends(
            conn, cur, weekly_trends_data)

    def gen_rt_weekly(self, curr_date,conn, cur):
        """ Function to generate Trends data for the week.

            Args: 
                conn: database connection. 
                cur: cursor object using the connection. 
        """

        print("Generating trends data for the week\n")
        self.weekly_ohlc(conn, cur, curr_date)
        self.weekly_indicator(conn, cur, curr_date)
        self.weekly_trends(conn, cur, curr_date)

    def gen_rt_weekly_history(self, conn, cur):
        """ Function to generate Weekly trends data for backdate. 

            Args:
                conn: database connection. 
                cur: cursor object using the connection. 
        """

        end_date = self.date
        start_date = self.back_date

        while(end_date >= start_date):

            print("Generating Weekly Trends data for date:", str(start_date))
            self.weekly_ohlc(conn, cur, start_date)
            self.weekly_indicator(conn, cur, start_date)
            self.weekly_trends(conn, cur, start_date)

            start_date = start_date + datetime.timedelta(7)
            
    
    
    ## Function for Trend Weightage Process for weekly and Back weeks for RT Trends Data ##  
    def get_rt_weekly(self, curr_date,conn):
        
        """ Fetch the data for weekly RT trends
        
            Args: 
                conn : database connection.
            
            Returns: 
                rt_weekly_df : RT trends data for Weekly.
        
        """
        rt_weekly_query = 'SELECT "gen_date", "rt_bullish_trending", "rt_bearish_trending", \
                            "rt_bullish_non_trending", rt_bearish_non_trending  FROM \
                            mf_analysis.trends_weekly where "gen_date" = \''+str(curr_date)+'\';'
        
        rt_weekly_df = sqlio.read_sql_query(rt_weekly_query, con=conn)
        
        return rt_weekly_df
    
          
    def get_rt_weekly_backdate_df(self, curr_date,conn):
        
        """ Fetch the data for 1 year back from the current date week
            of RT trends weekly.
            
            Args: 
                conn: database connection.
            
            Returns: 
                rt_weekly_1yr_back_df : One year back RT weekly data.     
            
        """ 
        back1yr=datetime.date(curr_date.year -1, curr_date.month, curr_date.day)
        rt_weekly_1yr_back_query = 'SELECT "gen_date", "rt_bullish_trending", "rt_bearish_trending", \
                                    "rt_bullish_non_trending", rt_bearish_non_trending \
                                     FROM mf_analysis.trends_weekly where "gen_date" between \
                                    \''+str(back1yr)+'\' and \''+str(curr_date)+'\' \
                                    order by "gen_date" desc;'
        rt_weekly_1yr_back_df = sqlio.read_sql_query(rt_weekly_1yr_back_query, con =conn)
        
        return rt_weekly_1yr_back_df
    
    
    def trend_weightage_weekly(self, rt_weekly_df):
        
        """ Calculate the trend weightage values for current date.

            Args: 
                rt_weekly_df : current date RT weekly data.
            
            Returns: 
                weekly_trend_weightage_df : date, weightage of the trend weightage
                                            for weekly 
        
        """
        weekly_trend_weightage_df = self.trend_weightage_common.\
                                    cal_trend_weightage_rt_data(rt_weekly_df)

        return weekly_trend_weightage_df
    
    
    def insert_trend_weightage_df(self, weekly_trend_weightage_df, conn, cur):
        
        """ Inserting those calculated Trend Weightage values into DB for 
            latest date or back date.

            Args: 
                weekly_trend_weightage_df : date, weightage of the trend weightage
                                            for weekly and weekly back dates.
                conn : database connection.
                cur : cursor object using the connection.   
        
        """

        weekly_trend_weightage_df["weightage"].fillna(-1, inplace=True)
        weekly_trend_weightage_df = weekly_trend_weightage_df.astype({"weightage": int})
        weekly_trend_weightage_df = weekly_trend_weightage_df.astype({"weightage": str})
        weekly_trend_weightage_df["weightage"] = weekly_trend_weightage_df["weightage"].replace('-1', np.nan)

        exportfilename = "weekly_trend_weightage_df.csv"
        exportfile = open(exportfilename,"w")
        weekly_trend_weightage_df.to_csv(exportfile, header=True, \
            index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        
        copy_sql = """
        COPY mf_analysis."trend_weightage_weekly" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
        with open(exportfilename, 'r') as f:

            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename) 
        
    
    #Process for Trend weightage weekly dates      
    def gen_trend_weightage_weekly_data(self, curr_date, conn, cur):
        
        """ Function to generate weekly Trend weightage
            for RT weekly data.
            
        """ 
        print("RT weekly Data for Current Date.", curr_date)
        rt_weekly_df = self.get_rt_weekly(curr_date,conn)
    
        if not(rt_weekly_df.empty):
        
            print("Calculate Trend weightage values for Current Date.")
            weekly_trend_weightage_df = self.trend_weightage_weekly(rt_weekly_df)
            
            print("Inserting Trend weightage Into the DB")
            self.insert_trend_weightage_df(weekly_trend_weightage_df, conn, cur)
            
        else:
            print("RT weekly Data is not found for this date:", curr_date)
            # raise ValueError("RT weekly Data not found for current date: "+str(curr_date))
        
        
        
        
