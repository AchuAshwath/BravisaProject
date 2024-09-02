# Python script to Generate Market Quality Number for NIFTY data daily and back dates.
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


class MarketQualityNIFTY():
    
    """ Contains the function which will calculate the Market  
        Quality Number for NIFTY data daily and History(back date).
        
    """
    
    def __init__(self):
        
        self.NIFTY = 'NSENIFTY'
        
        self.start_date = datetime.date(2017,1,1)
        self.end_date =datetime.date(2017,1,1)
        
        self.mqn_common_cal = Cal_Common_MQN()
    
    
    def get_nifty_df_history(self, conn):
        
        """ Fetching the data of NIFTY from IndexOHLC table.
        
            Args : 
                conn : database connection. 
            
            Returns : 
                nifty_backdate_df : dataframe of back day NIFTY. 
                
        """
        # Query for to take previous date history data of NIFTY.
        nifty_backdate_query = 'Select "NSECode", "Open", "High", "Low", "Close", \
                                "Date"FROM public."IndexOHLC" where \
                                "NSECode" = \''+str(self.NIFTY)+'\' \
                                and "Date" between \''+str(self.start_date)+'\' \
                                and \''+str(self.end_date)+'\' order by "Date" asc;'
        nifty_backdate_df = sqlio.read_sql_query(nifty_backdate_query, con = conn)
        
        return nifty_backdate_df
    
    
    def get_nifty_df_daily(self, conn, date, back_date):
        
        """ Fetching the data of NIFTY from IndexOHLC table.
        
            Args : 
                conn : database connection. 
                date : current day's date.
            
            Returns : 
                nifty_daily_df : dataframe of current day NIFTY
                                with latest 42 days back data.
                
        """
        # Query for to take current date data of NIFTY. 
        nifty_daily_query = 'Select "NSECode", "Open", "High", "Low", "Close", \
                            "Date"FROM public."IndexOHLC" where \
                            "NSECode" = \''+str(self.NIFTY)+'\' \
                            and "Date" between \''+str(back_date)+'\' \
                            and \''+str(date)+'\' order by "Date" desc;'
        nifty_daily_df = sqlio.read_sql_query(nifty_daily_query, con = conn)
        # print(nifty_daily_df)

        return nifty_daily_df
    
        
    def atr21days_nifty(self, nifty_df):
        
        """ Calculating 21 days ATR values for NIFTY data
            using Open, High, Low, Close Values.
            
            Args :
                nifty_df : NIFTY data.
                
            Returns : 
                atr21days_close_val : It contains 21days ATR values
                                      of NIFTY dataframe.
        """
        
        atr21days_close_val = self.mqn_common_cal.atr21days_close_common(nifty_df)
        
        return atr21days_close_val
    
    
    def atr_average_nifty(self,atr21days_close_val, nifty_df):
        
        """ Calculating ATR Average values by using formula(close / 21days_atr)
            for NIFTY data.
            
            Args : 
                nifty_df : NIFTY data.
                
                atr21days_close_val : It contains 21 days ATR values
                                      of NIFTY dataframe.
            Returns :
                    atr_avrg_nifty_df : data with atr_avrg of NIFTY
                                        data.
                                        
        """
        atr_avrg_nifty_df = self.mqn_common_cal.atr_average_common(atr21days_close_val, nifty_df)
        # print(atr_avrg_nifty_df)
        
        return atr_avrg_nifty_df
    
    
    def latest42days_nifty_mqn_df(self, atr_avrg_nifty_df):
        
        """ Calculating the Market Quality Condition part 1 that is
            (Very Volatile, Volatile, Normal, Quiet) for NIFTY data.
            
            Args :
                atr_avrg_nifty_df : Data with atr_avrg of NIFTY
                                    data frame.
                                        
            Returns : Data with "NSECode", "Date", "atr_avg", "Very Volatile",
                      Volatile, Normal, Quiet for NIFTY dataframes).   
                       
        """
        latest42_days_niftydf = self.mqn_common_cal.latest_42_days_data(atr_avrg_nifty_df)
        
        return latest42_days_niftydf
    
  
    def get_nifty_changeValue_backdate(self, conn, date, back_date):
        
        """ Fetching Change and date data from nse_index_change table.
        
            Args : 
                conn : database connection. 
            
            Returns : 
                    nifty_backdays_df : Data of Change, Date columns for Index NIFTY.
                    
        """
        nifty_backdays_query = ('SELECT "date","change" FROM public.nse_index_change where "symbol" = \
                                \''+str(self.NIFTY)+'\'  and "date" between \''+str(back_date)+'\' \
                                and \''+str(date)+'\' order by "date" desc limit 100;')
        nifty_backdays_df = sqlio.read_sql_query(nifty_backdays_query, con = conn)
        
        return nifty_backdays_df
    
    
    def nifty_100daysBack_mqn_condtion_value(self,latest42_days_niftydf, nifty_backdays_df):
        
        """ Calculation Market Quality Number Condition and Values for daily data of 
            NIFTY dataframe.

            Args : 
                nifty_backdays_df : Data of Change, Date columns for Index NIFTY.
                
                latest42_days_niftydf : Data with NSECode, Date, atr_avg, Very Volatile,
                                        Volatile, Normal, Quiet for NIFTY dataframes).
                        
            Returns :
                nifty_mqn_df : Data with NSECode, Date, atr_avg, Very Volatile, Volatile,
                               Normal, Quiet, mqn_condtion, mqn_val for daily NIFTY data.
            
        """
        nifty_mqn_df = self.mqn_common_cal.latest_100daysback_mqn_condtion_value\
                            (latest42_days_niftydf, nifty_backdays_df)
        
        return nifty_mqn_df
    
    
    def insert_nifty_mqn_df(self, nifty_mqn_df, conn, cur):
        
        """ Inserting NIFTY Market Quality Number Conditions and Values
            into the Data base.
            
            Args : 
                nifty_mqn_df : Data with NSECode, Date, atr_avg, Very Volatile,
                                Volatile, Normal, Quiet, mqn_condtion, mqn_val for daily 
                                NIFTY data.
                                
        """
        nifty_mqn_df = nifty_mqn_df[["IndexName", "Normal", "Quiet", "Very Volatile",\
                            "Volatile", "atr_avg", "atr21","date", "mqn_condtion", "mqn_val", \
                            "very_volatile_val", "volatile_val", "normal_val"]]
        
        exportfilename = "nifty_mqn_df.csv"
        exportfile = open(exportfilename,"w")
        nifty_mqn_df.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
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
        
    
    def generate_nifty_mqn_df_daily(self, conn, cur, date, back_date):
        
        """ Generating NIFTY Market Quality Number for
            History Data (back date Data).
        
        """
        curr_date = date
        
        print("NIFTY daily data")
        nifty_df = self.get_nifty_df_daily(conn, curr_date, back_date)
        
        if not(nifty_df.empty):
            
            # print("NIFTY data history for calculating ATR value")
            # nifty_df_history = self.get_nifty_df_history(conn)
            
            print("ATR 21 days close values for NIFTY data")
            atr21days_closeVal = self.atr21days_nifty(nifty_df)
        
            atr21days_closeVal = atr21days_closeVal.tail(42)
            atr21days_closeVal = atr21days_closeVal.sort_index(ascending=False)
            atr21days_closeVal = atr21days_closeVal.reset_index(drop=True)
            
            print("ATR Average values for NIFTY data")
            atr_average_nifty_df = self.atr_average_nifty(atr21days_closeVal, nifty_df)
              
            print("Latest 42 days NIFTY data with Market Quality Condition")
            latest42days_nifty_mqn_df = self.latest42days_nifty_mqn_df(atr_average_nifty_df)
            
            print("NIFTY Change Value for back dates")
            nifty_index_change_df = self.get_nifty_changeValue_backdate(conn, date, back_date)
            # nifty_index_change_df = nifty_index_change_df.head(100)
            
            print("Market Quality Number with Conditions and Value df")
            nifty_mqn_df = self.nifty_100daysBack_mqn_condtion_value(latest42days_nifty_mqn_df,\
                                nifty_index_change_df)
            nifty_mqn_df = nifty_mqn_df.head(1)
            
            print("Insert NIFTY Market Quality Number Condition and Values into the DB")
            self.insert_nifty_mqn_df(nifty_mqn_df, conn, cur)
            
        else :
            
            print("NIFTY daily data not found for date:", curr_date)
            # raise ValueError("Indicator monthly data not found for date: "+str(curr_date))
        

    def daily_mqn_nifty(self, conn, cur):
        start_date = datetime.date(2019,11,6)
        end_date = datetime.date.today() + datetime.timedelta(-2)

        while end_date>=start_date:
            back_date = start_date + datetime.timedelta(-400)
            self.generate_nifty_mqn_df_daily(conn, cur, start_date, back_date)
            start_date = start_date+datetime.timedelta(1)

    
    def mqn_nifty(self, curr_date, conn, cur):
        back_date = curr_date + datetime.timedelta(-400)
        self.generate_nifty_mqn_df_daily(conn, cur, curr_date, back_date) 


              
    # def generate_nifty_mqn_df_history(self, conn, cur):
        
    #     """ Generating NIFTY Market Quality Number for
    #         History Data (back date Data).
        
    #     """
    #     print("NIFTY History data")
    #     nifty_df = self.get_nifty_df_history(conn)

    #     print("ATR 21 days close values for NIFTY data")
    #     atr21days_closeVal = self.atr21days_nifty(nifty_df)
        
    #     print("ATR Average values for NIFTY data")
    #     atr_average_nifty_df = self.atr_average_nifty(atr21days_closeVal, nifty_df)
        
    #     atr_average_nifty_df = atr_average_nifty_df.sort_values(by = ['Date'],\
    #                                     ascending=False)
    #     atr_average_nifty_df = atr_average_nifty_df.reset_index(drop=True)
          
    #     print("Latest 42 days NIFTY data with Market Quality Condition")
    #     latest42days_nifty_mqn_df = self.latest42days_nifty_mqn_df(atr_average_nifty_df)
    
    #     print("NIFTY Change Value for back date")
    #     nifty_index_change_df = self.get_nifty_changeValue_backdate(conn)
        
    #     print("Market Quality Number with Conditions and Value DataFrame")
    #     nifty_mqn_df = self.nifty_100daysBack_mqn_condtion_value(latest42days_nifty_mqn_df,\
    #                          nifty_index_change_df)
        
    #     print("Insert NIFTY Market Quality Number Condition and Values into the DB")
    #     self.insert_nifty_mqn_df(nifty_mqn_df, conn, cur)
        
        

        
        
        
    
    
        
        