# Python script to Generate Market Quality Number for BTT_index data daily and back dates.
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
from datetime import timedelta
import sys

from mf_analysis.market_quality_number.mqn_common_calculation import Cal_Common_MQN
import utils.date_set as date_set

class MarketQualityBTT_Index():
    
    """ Contains the function which will calculate the Market  
        Quality Number for BTT_index data daily and History(back date).
        
    """
    
    def __init__(self):
        
        self.BTT_index = 'BTTIndex'
        
        self.start_date = datetime.date(2017,1,1)
        self.end_date = self.date + datetime.timedelta(-417)

        
        self.mqn_common_cal = Cal_Common_MQN()
    
    
    def get_btt_index_df_history(self, conn):
        
        """ Fetching the data of BTT_index from IRS table.
        
            Args : 
                conn : database connection. 
            
            Returns : 
                btt_index_back_df : dataframe of back day BTT_index. 
                
        """
        # Query for to take previous date history data of BTT_index.
        btt_index_back_query = 'SELECT "IndexName", "Open", "High", "Low", "Close", "GenDate"  \
                                FROM "Reports"."IRS" where  \
                                "IndexName" = \''+str(self.BTT_index)+'\' \
                                and "GenDate" between \''+str(self.start_date)+'\' \
                                and \''+str(self.end_date)+'\' order by "GenDate" asc;'
        btt_index_back_df = sqlio.read_sql_query(btt_index_back_query, con = conn)
        
        btt_index_back_df = btt_index_back_df.rename(columns = \
                            {"IndexName":"NSECode", "GenDate": "Date"})
        
        return btt_index_back_df
    
    
    def get_btt_index_df_daily(self, conn, date):
        
        """ Fetching the data of BTT_index from IRS table.
        
            Args : 
                conn : database connection. 
                date : current day's date.
            
            Returns : 
                btt_index_daily_df : dataframe of current day BTT_index
                                     with latest 42 days back data.
                
        """
        # Query for to take current date data of BTT_index. 
        btt_index_daily_query = 'Select "IndexName", "Open", "High", "Low", "Close", \
                                "GenDate" FROM "Reports"."IRS" where \
                                "IndexName" = \''+str(self.BTT_index)+'\' \
                                and "GenDate" between \''+str(self.end_date)+'\' \
                                and \''+str(date)+'\' order by "GenDate" desc limit 42 ;'
        btt_index_daily_df = sqlio.read_sql_query(btt_index_daily_query, con = conn)

        btt_index_daily_df = btt_index_daily_df.rename(columns = \
                            {"IndexName":"NSECode", "GenDate": "Date"})
        
        return btt_index_daily_df
    
        
    def atr21days_btt_index(self, btt_index_df):
        
        """ Calculating 21 days ATR values for BTT_index data
            using Open, High, Low, Close Values.
            
            Args :
                btt_index_df : BTT_index data.
                
            Returns : 
                atr21days_close_val : It contains 21days ATR values
                                      of BTT_index dataframe.
                                      
        """
        atr21days_close_val = self.mqn_common_cal.atr21days_close_common(btt_index_df)
        
        return atr21days_close_val
    
    
    def atr_average_btt_index(self, atr21days_close_val, btt_index_df):
        
        """ Calculating ATR Average values by using formula(21days_atr / close)
            for BTT_index data.
            
            Args : 
                btt_index_df : BTT_index data.
                
                atr21days_close_val : It contains 21 days ATR values
                                      of BTT_index dataframe.
            Returns :
                atr_avrg_btt_index_df : data with atr_avrg of BTT_index
                                        data.
                                        
        """
        atr_avrg_btt_index_df = self.mqn_common_cal.atr_average_common\
                                    (atr21days_close_val, btt_index_df)
        
        return atr_avrg_btt_index_df
    
    
    def latest42days_btt_index_mqn_df(self, atr_avrg_btt_index_df):
        
        """ Calculating the Market Quality Condition part 1 that is
            (Very Volatile, Volatile, Normal, Quiet) for BTT_index data.
            
            Args :
                atr_avrg_btt_index_df : Data with atr_avrg of BTT_index
                                        data frame.
                                        
            Returns :    
                latest42_days_btt_index_df : Data with "NSECode", "Date", "atr_avg", "Very Volatile",
                                             Volatile, Normal, Quiet for BTT_index dataframes).    
        """
        
        latest42_days_btt_index_df = self.mqn_common_cal.latest_42_days_data(atr_avrg_btt_index_df)
        
        return latest42_days_btt_index_df
    
  
    def get_btt_index_changeValue_backdate(self, conn, date, back_date):
        
        """ Fetching Change and date data from IRS table.
        
            Args : end_date
                conn : database connection. 
            
            Returns : 
                    btt_index_backdays_df : Data of Change, GenDate columns for Index BTT_index.
                    
        """
        btt_index_backdays_query = ('SELECT "date","change" FROM "public"."nse_index_change" where "symbol" = \
                                    \''+str(self.BTT_index)+'\'  and "date" between \''+str(back_date)+'\' \
                                    and \''+str(date)+'\' order by "date" desc limit 100;')
        btt_index_backdays_df = sqlio.read_sql_query(btt_index_backdays_query, con = conn)

        # print(btt_index_backdays_df)
        
        return btt_index_backdays_df
    
    
    def btt_index_100daysBack_mqn_condtion_value(self,latest42_days_btt_index_df, btt_index_backdays_df):
        
        """ Calculation Market Quality Number Condition and Values for daily data of 
            BTT_index dataframe


            Args : 
                btt_index_backdays_df : Data of Change, Date columns for Index BTT_index.
                
                latest42_days_btt_index_df : Data with NSECode, Date, atr_avg, Very Volatile,
                                            Volatile, Normal, Quiet for BTT_index dataframes).
                        
            Returns :
                btt_index_mqn_df : Data with NSECode, Date, atr_avg, Very Volatile, Volatile,
                               Normal, Quiet, mqn_condtion, mqn_val for daily BTT_index data.
            
        """
        btt_index_mqn_df = self.mqn_common_cal.latest_100daysback_mqn_condtion_value\
                            (latest42_days_btt_index_df, btt_index_backdays_df)
        
        return btt_index_mqn_df
    
    
    def insert_btt_index_mqn_df(self, btt_index_mqn_df, conn, cur):
        
        """ Inserting BTT_index Market Quality Number Conditions and Values
            into the Data base.
            
            Args : 
                btt_index_mqn_df : Data with NSECode, Date, atr_avg, Very Volatile,
                                    Volatile, Normal, Quiet, mqn_condtion, mqn_val for daily 
                                    BTT_index data.
                                    
        """

        # print(btt_index_mqn_df['date'])

        btt_index_mqn_df = btt_index_mqn_df[["IndexName", "Normal", "Quiet", "Very Volatile",\
                            "Volatile", "atr_avg", "atr21","date", "mqn_condtion", "mqn_val", \
                                "very_volatile_val", "volatile_val", "normal_val"]]
        
        exportfilename = "btt_index_mqn_df.csv"
        exportfile = open(exportfilename,"w")
        btt_index_mqn_df.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        
        copy_sql = """
        COPY mf_analysis."market_quality_number" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
        with open(exportfilename, 'r') as f:

            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename) 
        
    
    def generate_btt_index_mqn_df_daily(self, conn, cur, date, back_date):
        
        """ Generating BTT_index Market Quality Number for
            History Data (back date Data).
        
        """
        curr_date = date
        
        print("BTT_index daily data")
        btt_index_df = self.get_btt_index_df_daily(conn, curr_date)
        
        if not(btt_index_df.empty):
            
            # print("BTT_index data history for calculating ATR value")
            # btt_index_df_history = self.get_btt_index_df_history(conn)
            
            print("ATR 21 days close values for BTT_index data")
            atr21days_closeVal = self.atr21days_btt_index(btt_index_df)
            
            atr21days_closeVal = atr21days_closeVal.tail(42)
            atr21days_closeVal = atr21days_closeVal.sort_index(ascending=False)
            atr21days_closeVal = atr21days_closeVal.reset_index(drop=True)
            
            print("ATR Average values for BTT_index data")
            atr_average_btt_index_df = self.atr_average_btt_index(atr21days_closeVal, btt_index_df)
              
            print("Latest 42 days BTT_index data with Market Quality Condition")
            latest42days_btt_index_mqn_df = self.latest42days_btt_index_mqn_df(atr_average_btt_index_df)
            
            print("BTT_index Change Value for back dates")
            btt_index_change_df = self.get_btt_index_changeValue_backdate(conn, date, back_date)
            # btt_index_change_df = btt_index_change_df.head(100)
            
            print("Market Quality Number with Conditions and Value df")
            btt_index_mqn_df = self.btt_index_100daysBack_mqn_condtion_value(latest42days_btt_index_mqn_df,\
                                btt_index_change_df)
            btt_index_mqn_df = btt_index_mqn_df.head(1)
            
            print("Insert BTT_index Market Quality Number Condition and Values into the DB")
            self.insert_btt_index_mqn_df(btt_index_mqn_df, conn, cur)
            
        else :
            
            print("BTT_index daily data not found for date:", curr_date)
            # raise ValueError("Indicator monthly data not found for date: "+str(curr_date))
        

    def daily_mqn_btt(self, conn, cur):
        start_date = datetime.date(2019,11,6)
        end_date = datetime.date.today() + datetime.timedelta(-2)

        while end_date>=start_date:
            back_date = start_date + datetime.timedelta(-200)
            self.generate_btt_index_mqn_df_daily(conn, cur, start_date, back_date)
            start_date = start_date+datetime.timedelta(1)
    
    def mqn_btt(self, conn, cur):
        date = self.date
        back_date = date + datetime.timedelta(-200)
        self.generate_btt_index_mqn_df_daily(conn, cur, date, back_date)


    def generate_btt_index_mqn_df_history(self, conn, cur):
        
        """ Generating BTT_index Market Quality Number for
            History Data (back date Data).
        
        """
        print("BTT_index History data")
        btt_index_df = self.get_btt_index_df_history(conn)

        print("ATR 21 days close values for BTT_index data")
        atr21days_closeVal = self.atr21days_btt_index(btt_index_df)
        
        print("ATR Average values for BTT_index data")
        atr_average_btt_index_df = self.atr_average_btt_index(atr21days_closeVal, btt_index_df)
        
        atr_average_btt_index_df = atr_average_btt_index_df.sort_values(by = ['Date'],\
                                        ascending=False)
        atr_average_btt_index_df = atr_average_btt_index_df.reset_index(drop=True)
          
        print("Latest 42 days BTT_index data with Market Quality Condition")
        latest42days_btt_index_mqn_df = self.latest42days_btt_index_mqn_df(atr_average_btt_index_df)
    
        print("BTT_index Change Value for back date")
        btt_index_change_df = self.get_btt_index_changeValue_backdate(conn)
        
        print("Market Quality Number with Conditions and Value DataFrame")
        btt_index_mqn_df = self.btt_index_100daysBack_mqn_condtion_value(latest42days_btt_index_mqn_df,\
                             btt_index_change_df)
        
        print("Insert BTT_index Market Quality Number Condition and Values into the DB")
        self.insert_btt_index_mqn_df(btt_index_mqn_df, conn, cur)
        
        

        
        
        
    
    
        
        