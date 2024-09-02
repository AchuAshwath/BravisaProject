# Python script to calculate Index Change values of NIFTY, NSE500 and store in DB
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
import sys
import utils.date_set as date_set


class IndexCloseChange():
    
    """ Contains the functions which will calculate the change of close values
        for NIFTY, NSE500 for current date and History date too.
        
    """
    def __init__(self):
        
        self.NSE500 = 'NSE500'
        self.NIFTY = 'NSENIFTY'
        
        self.start_date = datetime.date(2017,1,1)
        self.end_date = datetime.date.today() + datetime.timedelta(-2)
        
    
    def get_nifty_nse500_df(self, conn, date):
        
        """ Fetching the data of NSE500 and NIFTY from IndexOHLC table
        
            Args: 
                conn, date
            
            Returns: 
                nse_index: dataframe of current day's NIFTY and NSE500 data. 
    
        """
        # Query to take current date data of NSE500 and NIFTY
        nse500_nifty_query = 'Select "NSECode", "Close", "Date" FROM public."IndexOHLC" where \
                                 ("NSECode" = \''+str(self.NSE500)+'\' or \
                                 "NSECode" = \''+str(self.NIFTY)+'\') and "Date" \
                                 <= \''+str(date)+'\' order by "Date" desc limit 4;'
        nse500_nifty_df = sqlio.read_sql_query(nse500_nifty_query, con =conn)
        return nse500_nifty_df
    
    
    # def get_history_nse500_nifty_df(self):
        
    #     """ Fetching the NSEIndex history data form the text file, the file
    #         contains data seens 2000-01-02 to 2019-09-05. Now we have to calculate 
    #         the change value of close for those dates
            
    #         Returns : 
    #             Change value of close for all the back dates, with 
    #             NSECode, and Date.
                    
    #     """
    #     # Query to take previous date history data of NSE500 and NIFTY
    #     nse500_nifty_query = 'Select "NSECode", "Close", "Date" FROM public."IndexOHLC" where \
    #                             ("NSECode" = \''+str(self.NSE500)+'\' or \
    #                             "NSECode" = \''+str(self.NIFTY)+'\') and "Date" \
    #                             between \''+str(self.start_date)+'\' and \
    #                             \''+str(self.end_date)+'\' order by "Date" desc;'
    #     history_nseindex_df = sqlio.read_sql_query(nse500_nifty_query, con =conn)
    #     return history_nseindex_df
    
        
    def nse500_nifty_df(self, nse500_nifty_df):
        
        """ In this function we split data frame according to NSECode
            that is NSE500 and NIFTY.
            
            Returns : 
                nse500_df : contains data of NSE500.
                nifty_df : contains data of Nifty.
            
        """
        nse500_df = nse500_nifty_df.loc[nse500_nifty_df["NSECode"] == 'NSE500']
        
        nifty_df = nse500_nifty_df.loc[nse500_nifty_df["NSECode"] == 'NSENIFTY']
        
        return nse500_df, nifty_df
    
    
    def cal_index_changeValues(self, index_Change_df):
        
        """ In this function we will do calculation of NIFTY, NSE500  change values of close,
            the formula will be (current close - prevous close / prevous close * 100).
            
            Args: 
                index_change_df : It is Dataframe which contain data of NIFTY( NSENIFTY ),
                                     NSE500 data.
            
            Returns: 
                index_change_close_df : Dataframe of Calculated Change of close values of
                                            NIFTY, NSE500 .  
                                         
        """
        
        index_Change_df = index_Change_df.reset_index(drop=True)
    
        index_change_close_df = pd.DataFrame()
        
        for i in range(0, len(index_Change_df)):

            curr_index_df = index_Change_df.iloc[[i]]
            i = i + 1
            
            if (i != len(index_Change_df)):
                
                curr_close = curr_index_df["Close"].item() 
                    
                prev_index_df = index_Change_df.iloc[[i]]
                prev_close = prev_index_df["Close"].item() 
                
                change = ((curr_close - prev_close) / abs(prev_close)) * 100
                
            else : 
                change = np.nan
                
            index_change_df = pd.DataFrame({"index_name" : curr_index_df["NSECode"] \
                                , "change" : [change], "date" : curr_index_df["Date"], })
            
            # index_change_close_df = index_change_close_df.append(index_change_df, ignore_index=True)
            index_change_close_df = pd.concat([index_change_close_df, index_change_df], ignore_index=True)

        
        #Comment this out in case of running history calculation 
        index_change_close_df = index_change_close_df.iloc[[0]]
        # print(index_change_close_df)
        
        return index_change_close_df
    
    
    def insert_index_Change_df_toDB(self, index_change_close_df, conn, cur):
        
        """ In this function we will insert the calculated nse_change_close_df Data to the 
            Data Base
            
        """

        exportfilename = "nse_index_change.csv"
        exportfile = open(exportfilename, "w")
        index_change_close_df.to_csv(exportfile, header=True, index=False,
                       float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
                COPY public.nse_index_change FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)
        
    
    def generating_daily_nse_index_changeValues_df(self, curr_date, conn, cur):
        
        """ Calling all the function, that are used to generate the NSE Index
            change Values for Daily data.
            
        """
        print("NSE index data")
        nse500_nifty_df = self.get_nifty_nse500_df(conn, curr_date)
        
        if not(nse500_nifty_df.empty):
            
            print("NSE500 and NIFTY Index Daily data")
            nse500_df , nifty_df =  self.nse500_nifty_df(nse500_nifty_df)
            
            print("NSE500 Change calculated Data")
            nse500_change_close_df = self.cal_index_changeValues(nse500_df)
            
            print("NIFTY Index Change calculated Data")
            nifty_change_close_df = self.cal_index_changeValues(nifty_df)

            print("Inserting the NSE500 Change df to the DB")
            self.insert_index_Change_df_toDB(nse500_change_close_df, conn, cur)  

            print("Inserting the NIFTY index Change df to the DB")
            self.insert_index_Change_df_toDB(nifty_change_close_df, conn, cur)  

        else:
            print("NSE index Change data not found for date:", curr_date)
            # raise ValueError("NSE index Change data not found for date: "+str(curr_date))
        
        
    # def generating_history_nse_index_changeValues_df(self, conn, cur):
        
    #     """ Calling those function that are used to generate 
    #         the NSE Index Change value of close for the back(history) date.
            
    #     """
    #     print("NSE index History Data")
    #     history_nseindex_df = self.get_nifty_nse500_df(conn, cur)
        
    #     print("NSE500  and NIFTY Data")
    #     nse500_df , nifty_df = self.nse500_nifty_df(history_nseindex_df)
        
    #     print("NSE500 History close Change calculated Data")
    #     nse500_history_df = self.cal_index_changeValues(nse500_df)
        
    #     print("NIFTY History close Change calculated Data")
    #     nifty_history_df = self.cal_index_changeValues(nifty_df)

    #     print("Inserting the NSE500 index History close Change value df to the DB")
    #     self.insert_index_Change_df_toDB(nse500_history_df, conn, cur) 
        
    #     print("Inserting the NIFTY index History close Change value df to the DB")
    #     self.insert_index_Change_df_toDB(nifty_history_df, conn, cur) 
