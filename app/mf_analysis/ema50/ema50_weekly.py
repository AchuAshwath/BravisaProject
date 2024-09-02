#script to run EMA50 Weekly Process
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

class EMA50_weekly():
    
    """ contains the function which we calculate the EMA50 Process weekly

    """

    def __init__(self):
        
        self.start_date = datetime.date(2019,9,6)
        self.end_date = datetime.date.today() + datetime.timedelta(-1)
                
        self.cal_ema50 = Cal_EMA50()
    
    def get_indicator_weekly(self, conn, date):
        """ fetching the weekly data of the indicator from the indicator table
        
            Args: 
                conn: database connection. 
                date: current week day's date. 
                
            Returns: 
                indicator_weekly: dataframe of current week day's indicator data. 
                
            Raises:
                No errors/exceptions. 
        """
        
        # weekly_indicator_sql = 'SELECT "close", "ema50", "gen_date" FROM mf_analysis.indicators \
        #                          where gen_date = \''+str(self.today_date)+'\';' 
        # weekly_indicator_df = sqlio.read_sql_query(weekly_indicator_sql, con=conn)
        
        # if any one wants to run for histroy data they can use this query
        
        weekly_indicator_sql = 'SELECT "close", "ema50", "gen_date" FROM mf_analysis.indicators_weekly \
                                where "gen_date" = \''+str(date)+'\';' 
        weekly_indicator_df = sqlio.read_sql_query(weekly_indicator_sql, con=conn)
        
        weekly_indicator_df = weekly_indicator_df.fillna(0)
        
        return weekly_indicator_df
    
    
    def ema50_above_close(self, weekly_indicator_df):
        
        """ passing the weekly data of the indicator from the indicator_weekly function
        
            Args: 
                indicator_weekly
                
            Returns: 
                ema50_above_df: dataframe of ema50_above calculated percentage data \. 
                
            Raises:
                No errors/exceptions. 
        """
        
        ema50_above_df = self.cal_ema50.cal_EMA50Above(weekly_indicator_df)
        
        return ema50_above_df
    
    def insert_ema50_weekly(self, ema50_above_df, conn, cur):
        
        """insert the ema50_weekly data to the DB
        
        """
        ema50_above_df['date'] = pd.to_datetime(ema50_above_df['date'], errors='coerce')
    
        ema50_above_df['date'] = ema50_above_df['date'].dt.strftime('%Y-%m-%d')
        exportfilename = "ema50_above_df.csv"
        exportfile = open(exportfilename, "w")
        ema50_above_df.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        
        copy_sql = """
                    COPY  mf_analysis.ema50_weekly FROM stdin WITH CSV HEADER
                    DELIMITER as ','
                 """
        with open(exportfilename, 'r') as f:
    
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)
    
    
    def generating_EMA50_weekly(self, curr_date, conn, cur):
        
        """calling all the function that are used to generate EMA50_weekly 
            process 
            
        """

        indicator_weekly = self.get_indicator_weekly(conn, curr_date)
        if not(indicator_weekly.empty):
        
            print("EMA50 calculated Data")
            ema50_Above_df = self.ema50_above_close(indicator_weekly)
        
            print("inserting the EMA50Above_percentage df to the DB")
            self.insert_ema50_weekly(ema50_Above_df, conn, cur)      
             
        else :
            print("Indicator weekly data not found for date:", curr_date)
            raise ValueError("Indicator weekly data not found for date: "+str(curr_date))
            



    def gen_ema50weekly_history(self, conn, cur):
        """ Function to generate history data for above 50 ema percentage.
        """

        start_date = self.start_date
        end_date = self.end_date

        while(end_date>=start_date):

            print("Starting process for date:\n", start_date)

            print("Getting indicator data for current day")
            indicator_weekly = self.get_indicator_weekly(conn, start_date)

            if not(indicator_weekly.empty):

                print("Calculating above50 ema percentage")
                above50ema = self.ema50_above_close(indicator_weekly)

                print("Inserting into the DB")
                self.insert_ema50_weekly(above50ema, conn, cur)

            else:

                print("Indicator data empty for current date. Moving to the next date", start_date)

            start_date = start_date + datetime.timedelta(7)
        
        
    
    
        
        
