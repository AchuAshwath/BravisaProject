# Script to generate trends data for OHLC daily
import datetime
import requests
import os.path
import os
from zipfile import ZipFile
import csv
import psycopg2
import pandas as pd
import calendar
import pandas.io.sql as sqlio
import time
import math
from datetime import timedelta
import numpy as np

from mf_analysis.daily_ohlc import DailyOHLC
from mf_analysis.daily_indicator import DailyIndicator
from mf_analysis.daily_trends import DailyTrends

from mf_analysis.common_trend_weightage import TrendWeightageCommon
import utils.date_set as date_set


class DailyRTProcess():
    """ Class containing methods which are run in order to generate RT data for everyday. 
    """

    def __init__(self):

        self.date = datetime.date.today()
        self.back_date = datetime.date.today() + datetime.timedelta(-63)
        self.back1yr = datetime.date(self.date.year -1, self.date.month, self.date.day)

        self.ohlc_class = DailyOHLC()
        self.daily_indicator_class = DailyIndicator()
        self.daily_trends_class = DailyTrends()
        
        self.trend_weightage_common = TrendWeightageCommon()

    def __del__(self):
        
        pass

    def daily_indicator(self, conn, cur, date):
        """ Function to call methods in daily ohlc & daily indicators in order to 
            generate indicator data. 

            Args: 
                conn: database connection.
                cur: cursor object using the connection.
                date: date for which the process is run. 

            Returns: 
                Returns 1 if ohlc is present for date for which process is run. 
                Else returns 0.
        """

        print("\nGetting BTT List")
        btt = self.ohlc_class.get_btt(conn, date)

        print("Getting ohlc data")
        ohlc = self.ohlc_class.get_ohlc(conn, date)

        if not(ohlc.empty):

            print("Merging BTT and OHLC")
            ohlc_list = self.ohlc_class.merge_btt_ohlc(btt, ohlc)

            print("Calculating indicator data")
            indicator_data = self.daily_indicator_class.technical_indicators_daily(
                conn, ohlc_list)

            print("Inserting into table")
            self.daily_indicator_class.insert_daily_indicators(
                conn, cur, indicator_data)

            return 1

        else:

            print("OHLC empty for today")
            return 0

    def daily_trends(self, conn, cur, date):
        """ Function to call methods in daily trends in order to generate different 
            Trends metrics for stocks. 

            Args: 
                conn: database connection. 
                cur: cursor object using the connection.
                date: date for which the process is run.  

            Returns: 
                No return value. 
        """

        print("Getting indicator data for today")
        daily_indicator_data = self.daily_trends_class.get_indicator_daily(
            conn, date)

        print("Getting indicator data for backdate")
        daily_indicator_back = self.daily_trends_class.get_indicator_daily_back(
            conn, date)

        print("Calculating trends for today")
        trends_data = self.daily_trends_class.get_trends_daily(
            daily_indicator_data, daily_indicator_back)

        print("Inserting into table")
        self.daily_trends_class.insert_daily_trends(conn, cur, trends_data)
    

    def gen_rt_daily(self, curr_date, conn, cur):
        """ Function to generate daily indicator data for stocks. 

            Args:
                conn: database connection.
                cur: cursor object using the connection. 

            Returns: 
                No return value. 

            Raises: 
                Raises a value error if OHLC is not present for current date. 
        """

        print("Calculating indicator data for current day:", curr_date)
        check_val = self.daily_indicator(conn, cur, curr_date)

        
        if(check_val == 1):
           self.daily_trends(conn, cur, curr_date)
            
        else:
            print("OHLC not found for date:", curr_date)
            # raise ValueError('OHLC data not found for date: '+str(curr_date))
        

    def gen_rt_daily_history(self, conn, cur):
        """ Function to generate history RT data. 

            Args: 
                conn: database connection.
                cur: cursor object using the connection. 

            Returns: 
                No return value.  
        """

        start_date = self.back_date
        end_date = self.date

        while(end_date >= start_date):

            print("Generating RT data for date:", start_date)
            check_val = self.daily_indicator(conn, cur, start_date)

            if (check_val == 1):
                self.daily_trends(conn, cur, start_date)

            else:
                print("OHLC empty for date:", str(start_date))

            start_date = start_date + datetime.timedelta(1)
            
            
    ## Trend Weightage Process for Daily and Back Dates ##    
    def get_rt_daily(self, curr_date, conn):
        
        """ Fetch the data for daily RT trends
        
            Args: 
                conn : database connection.
            
            Returns: 
                rt_daily_df : RT trends data for Daily.
        
        """
        rt_daily_query = 'SELECT "gen_date", "rt_bullish_trending", "rt_bearish_trending", \
                            "rt_bullish_non_trending", rt_bearish_non_trending  FROM \
                            mf_analysis.trends where "gen_date" = \''+str(curr_date)+'\';'
        
        rt_daily_df = sqlio.read_sql_query(rt_daily_query, con=conn)
        
        return rt_daily_df
    
        
    def get_rt_daily_backdate_df(self, curr_date, conn):
        
        """ Fetch the data for 1 year back from the current date
            of RT trends daily.
            
            Args: 
                conn: database connection.
            
            Returns: 
                rt_daily_yr_back_df : One year back RT daily data.     
            
        """ 
        back1yr= datetime.date(curr_date.year -1, curr_date.month, curr_date.day)
        rt_daily_1yr_back_query = 'SELECT "gen_date", "rt_bullish_trending", "rt_bearish_trending", \
                                    "rt_bullish_non_trending", rt_bearish_non_trending \
                                     FROM mf_analysis.trends where "gen_date" between \
                                    \''+str(back1yr)+'\' and \''+str(curr_date)+'\' \
                                    order by "gen_date" desc;'
        rt_daily_1yr_back_df = sqlio.read_sql_query(rt_daily_1yr_back_query, con =conn)
        
        return rt_daily_1yr_back_df
    
    
    def trend_weightage_daily(self, rt_daily_df):
        
        """ Calculate the trend weightage values for current date or back date.

            Args: 
                rt_daily_df : current date RT daily data.
            
            Returns: 
                daily_trend_weightage_df : date, weightage of the trend weightage
                                            for daily 
        
        """
        daily_trend_weightage_df = self.trend_weightage_common.\
                                    cal_trend_weightage_rt_data(rt_daily_df)

        return daily_trend_weightage_df
    
    
    def insert_trend_weightage_df(self, daily_trend_weightage_df, conn, cur):
        
        """ Inserting those calculated Trend Weightage values into DB for
            current date or back dates.

            Args: 
                daily_trend_weightage_df : date, weightage of the trend weightage
                                            for daily and daily back dates.
                conn : database connection.
                cur : cursor object using the connection.   
        
        """

        daily_trend_weightage_df["weightage"].fillna(-1, inplace=True)
        daily_trend_weightage_df = daily_trend_weightage_df.astype({"weightage": int})
        daily_trend_weightage_df = daily_trend_weightage_df.astype({"weightage": str})
        daily_trend_weightage_df["weightage"] = daily_trend_weightage_df["weightage"].replace('-1', np.nan)

        exportfilename = "daily_trend_weightage_df.csv"
        exportfile = open(exportfilename,"w")
        daily_trend_weightage_df.to_csv(exportfile, header=True, \
            index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        
        copy_sql = """
        COPY mf_analysis."trend_weightage_daily" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
        with open(exportfilename, 'r') as f:

            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename) 
        
    
    #Process for Trend weightage Daily dates      
    def gen_trend_weightage_daily_data(self, curr_date,conn, cur):
        
        """ Function to generate daily Trend weightage
            for RT daily data of Current Date.
            
        """
 
        print("RT Daily Data for Current Date.", curr_date)
        rt_daily_df = self.get_rt_daily(curr_date,conn)
        
        if not(rt_daily_df.empty):
        
            print("Calculate Trend weightage values for Current Date.")
            daily_trend_weightage_df = self.trend_weightage_daily(rt_daily_df)
            
            print("Inserting Trend weightage Into the DB")
            self.insert_trend_weightage_df(daily_trend_weightage_df, conn, cur)
            
        else:
            print("RT Daily Data is not found for this date:", curr_date)
            # raise ValueError("RT weekly Data not found for current date: "+str(curr_date))

         
        
        
    #Process for Trend weightage Back dates           
    def gen_trend_weightage_daily_history(self, conn, cur):
        
        """ Function to generate daily Trend weightage
            for back dates of RT daily data.
            
        """
        print("RT Daily Data for Back Dates")
        rt_daily_backdate_df = self.get_rt_daily_backdate_df(conn)
        
        print("Calculate Trend weightage values for 1 year.")
        daily_trend_weightage_backdate_df = self.trend_weightage_daily(rt_daily_backdate_df)
        
        print("Inserting Trend weightage Into the DB")
        self.insert_trend_weightage_df(daily_trend_weightage_backdate_df, conn, cur)
        
        
    
        
        
