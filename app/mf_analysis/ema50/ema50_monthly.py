#script to run EMA50 Monthly Process
import datetime
import requests
import os.path
import os
import csv
import io
import psycopg2
import pandas as pd
import calendar
import numpy as np
import pandas.io.sql as sqlio
import time
import math
from datetime import timedelta
from dateutil.relativedelta import relativedelta 

from mf_analysis.ema50.cal_common_ema50 import Cal_EMA50
import utils.date_set as date_set

class EMA50_monthly():
    
    """ contains the function which we calculate the EMA50 Process Monthly
    
    """  
    def __init__(self):
        
        self.start_date = datetime.date(2019,9,6)
        self.end_date = datetime.date.today() + datetime.timedelta(-3)
        
        
        self.cal_ema50 = Cal_EMA50()


    def last_day_of_month(self, date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)
        
    
    def get_indicator_monthly(self, conn, date):
        
        """ fetching the Monthly data of the indicator from the indicators_monthly table
        
            Args: 
                conn: database connection. 
                date: current Month day's date. 
                
            Returns: 
                indicator_monthly: dataframe of current month day's indicator data. 
                
            Raises:
                No errors/exceptions. 
        """
        
        # monthly_indicator_sql = 'SELECT "close", "ema50", "gen_date" FROM mf_analysis.indicators_monthly \
        #                          where gen_date = \''+str(self.today_date)+'\';' 
        # monthly_indicator_df = sqlio.read_sql_query(monthly_indicator_sql, con=conn)
        
        # if any one wants to run for histroy data they can use this query
        
        monthly_indicator_sql = 'SELECT "close", "ema50", "gen_date" FROM mf_analysis.indicators_monthly \
                                where "gen_date" = \''+str(date)+'\';' 
        monthly_indicator_df = sqlio.read_sql_query(monthly_indicator_sql, con=conn)
        
        monthly_indicator_df = monthly_indicator_df.fillna(0)
        
        return monthly_indicator_df
    
    
    def ema50_above_close(self, monthly_indicator_df):
        
        """ passing the monthly data of the indicator from the indicator_monthly function
        
            Args: 
                indicator_monhtly
                
            Returns: 
                ema50_above_df: dataframe of ema50_above calculated percentage data \. 
                
            Raises:
                No errors/exceptions. 
        """
        
        ema50_above_df = self.cal_ema50.cal_EMA50Above(monthly_indicator_df)
        
        return ema50_above_df
    

    def insert_ema50_monthly(self, ema50_above_df, conn, cur):
        """Insert the ema50_monthly data into the DB."""
                            # Extract the date from the tuple if it is in tuple format
        if ema50_above_df['date'].apply(lambda x: isinstance(x, tuple)).any():
            ema50_above_df['date'] = ema50_above_df['date'].apply(lambda x: x[0] if isinstance(x, tuple) else x)
            
        ema50_above_df['date'] = pd.to_datetime(ema50_above_df['date'], errors='coerce')
    
        ema50_above_df['date'] = ema50_above_df['date'].dt.strftime('%Y-%m-%d')
        
        # Create an in-memory CSV file
        csv_buffer = io.StringIO()
        ema50_above_df.to_csv(csv_buffer, header=True, index=False, float_format="%.2f", lineterminator='\r')
        
        # Reset the buffer position to the beginning to read it
        csv_buffer.seek(0)

        copy_sql = """
                COPY mf_analysis.ema50_monthly FROM stdin WITH CSV HEADER
                DELIMITER as ','
            """
        
        try:
            # Use copy_expert with the in-memory CSV buffer
            cur.copy_expert(sql=copy_sql, file=csv_buffer)
            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"Error: {e}")
        
        csv_buffer.close()  # Close the in-memory CSV buffer
    
    '''def insert_ema50_monthly(self, ema50_above_df, conn, cur):
        
        """ insert the ema50_monthly data to the DB
        
        """
        exportfilename = "ema50_above_df.csv"
        exportfile = open(exportfilename, "w")
        ema50_above_df.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        
        copy_sql = """
                    COPY  mf_analysis.ema50_monthly FROM stdin WITH CSV HEADER
                    DELIMITER as ','
                 """
        with open(exportfilename, 'r') as f:
    
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)'''
    
    
    def generating_EMA50_monthly(self, curr_date,conn, cur):
        
        """ calling all the function that are used to generate EMA50_monthly 
            process 
            
        """

        print("Indicator monthly data") 
        indicator_monthly_df = self.get_indicator_monthly(conn, curr_date)
        
        if not(indicator_monthly_df.empty):
            
            print("EMA50 calculated Data")
            ema50_Above_df = self.ema50_above_close(indicator_monthly_df)
            
            print("inserting the EMA50Above_percentage df to the DB")
            self.insert_ema50_monthly(ema50_Above_df, conn, cur)        
        else :
            print("Indicator monthly data not found for date:", curr_date)
            # raise ValueError("Indicator monthly data not found for date: "+str(curr_date))


    def gen_ema50monthly_history(self, conn, cur):
        """ Function to generate history data for above 50 ema percentage.
        """

        start_date = self.start_date
        end_date = self.end_date

        while(end_date>=start_date):

            start_date = self.last_day_of_month(start_date)

            print("Starting process for date:\n", start_date)

            print("Getting indicator data for current day")
            indicator_monthly = self.get_indicator_monthly(conn, start_date)

            if not(indicator_monthly.empty):

                print("Calculating above50 ema percentage")
                above50ema = self.ema50_above_close(indicator_monthly)

                print("Inserting into the DB")
                self.insert_ema50_monthly(above50ema, conn, cur)

            else:

                print("Indicator data empty for current date. Moving to the next date", start_date)

            start_date = start_date + relativedelta(months=1)


            
        
        
        
    
    
        
        
