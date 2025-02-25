# script to run EMA50 Daily Process
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

from mf_analysis.ema50.cal_common_ema50 import Cal_EMA50
import utils.date_set as date_set

class EMA50_daily():
    """ contains the function which we calculate the EMA50 Process Daily

    """

    def __init__(self):

        self.start_date = datetime.date(2016,1,1)
        self.end_date = datetime.date.today() + datetime.timedelta(-1)
        
        
        self.cal_ema50 = Cal_EMA50()

    def get_indicator_daily(self, conn, date):
        """ fetching the daily data of the indicator from the indicator table

            Args: 
                conn: database connection. 
                date: current day's date. 

            Returns: 
                indicator_daily: dataframe of current day's indicator data. 

            Raises:
                No errors/exceptions. 
        """

        daily_indicator_sql = 'SELECT "close", "ema50", "gen_date" FROM mf_analysis.indicators \
                                 where gen_date = \''+str(date)+'\';' 
        daily_indicator_df = sqlio.read_sql_query(daily_indicator_sql, con=conn)
        
        # if any one wants to run for histroy data they can use this query

        # daily_indicator_sql = 'SELECT "close", "ema50", "gen_date" FROM mf_analysis.indicators \
        #                         where "gen_date" between \''+str(self.start)+'\' and \''+str(self.end)+'\';'
        # daily_indicator_df = sqlio.read_sql_query(daily_indicator_sql, con=conn)

        daily_indicator_df = daily_indicator_df.fillna(0)
    
        return daily_indicator_df

    def ema50_above_close(self, indicator_daily):
        """ passing the daily data of the indicator from the indicator_dialy function

            Args: 
                indicator_daily

            Returns: 
                ema50_above_df: dataframe of ema50_above calculated percentage data \. 

            Raises:
                No errors/exceptions. 

        """

        ema50_above_df = self.cal_ema50.cal_EMA50Above(indicator_daily)

        return ema50_above_df

    def insert_ema50_daily(self, ema50_above_df, conn, cur):
        """insert the ema50_daily data to the DB

        """
                    # Extract the date from the tuple if it is in tuple format
        if ema50_above_df['date'].apply(lambda x: isinstance(x, tuple)).any():
            ema50_above_df['date'] = ema50_above_df['date'].apply(lambda x: x[0] if isinstance(x, tuple) else x)
            
        ema50_above_df['date'] = pd.to_datetime(ema50_above_df['date'], errors='coerce')
    
        ema50_above_df['date'] = ema50_above_df['date'].dt.strftime('%Y-%m-%d')
        print(ema50_above_df)
        exportfilename = "ema50_above_df.csv"
        exportfile = open(exportfilename, "w")
        ema50_above_df.to_csv(exportfile, header=True, index=False,
                              float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
                    COPY  mf_analysis.ema50_daily FROM stdin WITH CSV HEADER
                    DELIMITER as ','
                 """
        with open(exportfilename, 'r') as f:

            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)

    def generating_EMA50_daily(self, curr_date,conn, cur):
        """calling all the function that are used to generate EMA50_daily 
            process 

        """

        indicator_daily = self.get_indicator_daily(conn, curr_date)
        if not(indicator_daily.empty):
            
            print("EMA50 calculated Data")
            ema50_Above_df = self.ema50_above_close(indicator_daily)

            print("inserting the EMA50Above_percentage df to the DB")
            self.insert_ema50_daily(ema50_Above_df, conn, cur)  

        else:
            print("Indicator daily data not found for date:", curr_date)



    def gen_ema50daily_history(self, conn, cur):
        """ Function to generate history data for above 50 ema percentage.
        """

        start_date = self.start_date
        end_date = self.end_date

        while(end_date>=start_date):

            print("Starting process for date:\n", start_date)

            print("Getting indicator data for current day")
            indicator_daily = self.get_indicator_daily(conn, start_date)

            if not(indicator_daily.empty):

                print("Calculating above50 ema percentage")
                above50ema = self.ema50_above_close(indicator_daily)

                print("Inserting into the DB")
                self.insert_ema50_daily(above50ema, conn, cur)

            else:

                print("Indicator data empty for current date. Moving to the next date", start_date)

            start_date = start_date + datetime.timedelta(1)