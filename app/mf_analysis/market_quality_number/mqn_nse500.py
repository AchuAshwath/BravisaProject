# Python script to Generate Market Quality Number for NSE500 data daily and back date.
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
from ta import trend, volatility

from mf_analysis.market_quality_number.mqn_common_calculation import Cal_Common_MQN
import utils.date_set as date_set


class MarketQualityNSE500():
    
    """ Contains the function which will calculate the 
        Market Quality Number for NSE500 data daily.
        
    """
    def __init__(self):
        
        self.NSE500 = 'NSE500'
        
        # self.start_date = datetime.date(2017,1,1)
        # self.end_date = self.date + datetime.timedelta(0)

        self.mqn_common_cal = Cal_Common_MQN()
    
    
    # def get_nse500_df_history(self, conn):
        
    #     """ Fetching the data of NSE500 from IndexOHLC table.
        
    #         Args : 
    #             conn : database connection.
            
    #         Returns : 
    #             nse500_backdate_df : dataframe of current day NSE500. 
                
    #     """
    #     # Query for to take previous date history data of NSE500.
    #     nse500_backdate_query = 'Select "NSECode", "Open", "High", "Low", "Close", \
    #                             "Date"FROM public."IndexOHLC" where \
    #                             "NSECode" = \''+str(self.NSE500)+'\' \
    #                             and "Date" between \''+str(self.start_date)+'\' \
    #                             and \''+str(self.end_date)+'\' order by "Date" asc;'
    #     nse500_backdate_df = sqlio.read_sql_query(nse500_backdate_query, con =conn)
        
    #     return nse500_backdate_df
    
    
    def get_nse500_df_daily(self, conn, date, back_date):
        
        """ Fetching the data of NSE500 from IndexOHLC table.
        
            Args : 
                conn : database connection. 
                date : current day's date.
            
            Returns : 
                nse500_daily_df : dataframe of current day NSE500. 
                
        """
        # Query for to take current date data of NSE500. 
        nse500_daily_query = 'Select "NSECode", "Open", "High", "Low", "Close", \
                             "Date"FROM public."IndexOHLC" where \
                             "NSECode" = \''+str(self.NSE500)+'\' \
                             and "Date" between \''+str(back_date)+'\' \
                             and \''+str(date)+'\' order by "Date" desc;'
        nse500_daily_df = sqlio.read_sql_query(nse500_daily_query, con =conn)
        
        return nse500_daily_df
    
        
    def atr21days_nse500(self, nse500_df):
        
        """ Calculating 21 days ATR values for NSE500 data
            using Open, High, Low, Close Values.
            
            Args :
                nse500_df : NSE500 data.
                
            Returns : 
                atr21days_close_val : It contains 21days atr values
                                        of NSE500 dataframe.    
                                         
        """
        atr21days_close_val = self.mqn_common_cal.atr21days_close_common(nse500_df)
        
        return atr21days_close_val
    
    
    def atr_average_nse500(self,atr21days_close_val, nse500_df):
        
        """ Calculating ATR Average values by using formula(close / 21days_atr)
            for NSE500 data
            
            Args : 
                nse500_df :  NSE500 data.
                
                atr21days_close_val : It contains 21days atr values
                                        of NSE500 dataframe.   
            Returns :
                    atr_avrg_nsee500_df : returns the data with atr_avrg of NSE500
                                           data.      
                                                  
        """
        atr_avrg_nsee500_df = self.mqn_common_cal.atr_average_common(atr21days_close_val, nse500_df)
        
        return atr_avrg_nsee500_df
    
    
    def latest42days_nse500_mqn_df(self, atr_avrg_nsee500_df):
        
        """ Calculating the Market Quality Condition part 1 that is
            (Very Volatile, Volatile, Normal, Quiet) for NSE 500 data.
            
            Args :
                atr_avrg_nsee500_df : Data with atr_avrg of NSE500
                                        data frame.
                                        
            Returns : Data with NSECode, Date, atr_avg, Very Volatile,
                        Volatile, Normal, Quiet for NSE500 dataframes).    
                        
        """
        latest42_days_nse500df = self.mqn_common_cal.latest_42_days_data(atr_avrg_nsee500_df)
        
        return latest42_days_nse500df
    
  
    def get_nse500_changeValue_backdate(self, conn, date, back_date):
        
        """ Fetching Change and date data from nse_index_change table.
        
            Args : 
                conn : database connection. 
            
            Returns : 
                    nse500_backdays_df : Data of "Change", "Date" columns forIndex  NSE500.   
                      
        """
        nse500_backdays_query = 'SELECT "date","change" FROM public.nse_index_change where "symbol" = \
                                    \''+str(self.NSE500)+'\'  and "date" between \''+str(back_date)+'\' \
                                    and \''+str(date)+'\' order by "date" desc limit 100;'
        nse500_backdays_df = sqlio.read_sql_query(nse500_backdays_query, con = conn)
        
        return nse500_backdays_df
    
    
    def nse500_100daysBack_mqn_condtion_value(self,latest42_days_nse500df, nse500_backdays_df):
        
        """ Calculation Market Quality Number Condition and Values for daily data of 
            NSE500 dataframe.

            Args : 
                nse500_backdays_df : Data of Change, Date columns forIndex NSE500.
                
                latest42_days_nse500df : Data with NSECode, Date, atr_avg, Very Volatile,
                                        Volatile, Normal, Quiet for NSE500 dataframes.
                        
            Returns :
                nse500_mqn_df : Data with NSECode, Date, atr_avg, Very Volatile, Volatile,
                                Normal, Quiet, mqn_condtion, mqn_val for daily NSE500 data.
            
        """ 
        nse500_mqn_df = self.mqn_common_cal.latest_100daysback_mqn_condtion_value\
                            (latest42_days_nse500df, nse500_backdays_df)
        
        return nse500_mqn_df
    
    
    def insert_nse500_mqn_df(self, nse500_mqn_df, conn, cur):
        
        """ Inserting NSE500 Market Quality Number Conditions and Values
            into the Data base.
            
            Args : 
                nse500_mqn_df : Data with "NSECode", "Date", "atr_avg", "Very Volatile",
                                Volatile, Normal, Quiet, mqn_condtion, mqn_val for daily 
                                NSE500 data.
                                
        """
        nse500_mqn_df = nse500_mqn_df[["IndexName", "Normal", "Quiet", "Very Volatile",\
                        "Volatile", "atr_avg", "atr21","date", "mqn_condtion", "mqn_val", \
                            "very_volatile_val", "volatile_val", "normal_val"]]
        
        exportfilename = "nse500_mqn_df.csv"
        exportfile = open(exportfilename,"w")
        nse500_mqn_df.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
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
        
    
    def generate_nse500_mqn_df_daily(self, conn, cur, date, back_date):
        
        """ Generating NSE500 Market Quality Number for
            History Data (back date Data).
        
        """ 
        curr_date = date
        
        print("NSE500 daily data ")
        nse500_df = self.get_nse500_df_daily(conn, curr_date, back_date)
        
        if not(nse500_df.empty):
            
            # print("NSE500 data history for calculating ATR value")
            # nse500_df_history = self.get_nse500_df_history(conn)
            
            print("ATR 21 days close values for NSE500 data")
            atr21days_closeVal = self.atr21days_nse500(nse500_df)
            
            atr21days_closeVal = atr21days_closeVal.tail(42)
            atr21days_closeVal = atr21days_closeVal.sort_index(ascending=False)
            atr21days_closeVal = atr21days_closeVal.reset_index(drop=True)
            
            print("ATR Average values for NSE500 data")
            atr_average_nse500_df = self.atr_average_nse500(atr21days_closeVal, nse500_df)
              
            print("Latest 42 days NSE500 data with Market Quality Condition")
            latest42days_nse500_mqn_df = self.latest42days_nse500_mqn_df(atr_average_nse500_df)
            
            print("NSE500 Change Value for back dates")
            nse500_index_change_df = self.get_nse500_changeValue_backdate(conn, date, back_date)
            # nse500_index_change_df = nse500_index_change_df.head(100)
            
            print("Market Quality Number with Conditions and Value df")
            nse500_mqn_df = self.nse500_100daysBack_mqn_condtion_value(latest42days_nse500_mqn_df,\
                                nse500_index_change_df)
            nse500_mqn_df = nse500_mqn_df.head(1)
            
            print("Insert NSE500 Market Quality Number Condition and Values into the DB")
            self.insert_nse500_mqn_df(nse500_mqn_df, conn, cur)
            
        else :
            
            print("NSE500 daily data not found for date:", curr_date)
            # raise ValueError("Indicator monthly data not found for date: "+str(curr_date))
        

    def daily_mqn_nse(self, conn, cur):
        start_date = datetime.date(2019,11,6)
        end_date = datetime.date.today() + datetime.timedelta(-2)

        while end_date>=start_date:
            back_date = start_date + datetime.timedelta(-200)
            self.generate_nse500_mqn_df_daily(conn, cur, start_date, back_date)
            start_date = start_date+datetime.timedelta(1)


    def mqn_nse(self, curr_date, conn, cur):
        back_date = curr_date + datetime.timedelta(-400)
        self.generate_nse500_mqn_df_daily(conn, cur, curr_date, back_date)
            
 
    def generate_nse500_mqn_df_history(self, conn, cur):
        
        """ Generating NSE500 Market Quality Number for
            History Data (back date Data).
        
        """
        print("NSE500 History data")
        nse500_df = self.get_nse500_df_history(conn)
        
        print("ATR 21 days close values for NSE500 data")
        atr21days_closeVal = self.atr21days_nse500(nse500_df)
        
        print("ATR Average values for NSE500 data")
        atr_average_nse500_df = self.atr_average_nse500(atr21days_closeVal, nse500_df)
        
        atr_average_nse500_df = atr_average_nse500_df.sort_values(by = ['Date'],\
                                        ascending=False)
        atr_average_nse500_df = atr_average_nse500_df.reset_index(drop=True)
          
        print("Latest 42 days NSE500 data with Market Quality Condition")
        latest42days_nse500_mqn_df = self.latest42days_nse500_mqn_df(atr_average_nse500_df)
    
        print("NSE500 Change Value for back dates")
        nse500_index_change_df = self.get_nse500_changeValue_backdate(conn)
        
        print("Market Quality Number with Conditions and Value DataFrame")
        nse500_mqn_df = self.nse500_100daysBack_mqn_condtion_value(latest42days_nse500_mqn_df,\
                             nse500_index_change_df)
        
        print("Insert NSE500 Market Quality Number Condition and Values into the DB")
        self.insert_nse500_mqn_df(nse500_mqn_df, conn, cur)
        
        

        
        
        
    
    
        
        