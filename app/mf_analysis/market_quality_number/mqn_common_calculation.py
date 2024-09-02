# Python script to Generate Market Quality Number for NSE500, NIFITY and BTTIndex.
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

class Cal_Common_MQN():
    
    """ All the common function that are used in Market Quality Number
        Process.
        
    """
    def __init__(self):
        
        pass
    
       
    def atr21days_close_common(self, index_df):
        
        """ Calculating the ATR 21 days values for the
            close values of NSE500, NIFTY and BTTIndex.

            Args:
                index_df : NSE500, NIFTY and BTTIndex dataframes.
                
            Returns:
                atr21_close_val : It contains 21days atr values
                                of (NSE500, NIFTY and BTTIndex dataframes).

        """
        timeperiod = 21
        
        index_df['H_L'] = index_df["High"] - index_df["Low"]
        index_df['H_Cp'] = abs(index_df["High"] - index_df["Close"].shift(1))
        index_df['L_Cp'] = abs(index_df["Low"] - index_df["Close"].shift(1))
        index_df['TR'] = index_df[["H_L", "H_Cp", "L_Cp"]].max(axis=1)
        index_df['ATR'] = index_df['TR'].rolling(timeperiod).mean()
        
        for i in range(timeperiod , len(index_df)):
            
            index_df.loc[i, 'ATR'] = (index_df.loc[i - 1, 'ATR'] * \
            (timeperiod -1) + index_df.loc[i, 'TR']) / timeperiod

        atr21_close_val = index_df['ATR']
        
        return atr21_close_val
    
    
    def atr_average_common(self, atr21days_close_val, index_df):
        
        """ Calculating ATR Average values by using formula
            (21days_atr / close).
            
            Args :
                atr21days_close_val : 21days atr values of 
                                    (NSE500, NIFTY and BTTIndex).
                
                index_df : It contians all the data of (NSE500,
                                 NIFTY and BTTIndex).
                         
            Returns :
                    index_atr_avrg_df :  data with "NSECode", "Date", "atr_avg"
                                         for (NSE500, NIFTY and BTTIndex).
                        
        """
        index_df["atr_avg"] = atr21days_close_val.div(index_df["Close"], axis=0)
        index_df["atr_avg"] = index_df["atr_avg"].mul(100, axis=0)
        index_df["atr21"] = atr21days_close_val
        
        index_atr_avrg_df = index_df[["NSECode", "Date", "atr_avg", "atr21"]]
        
        return index_atr_avrg_df
    
    
    def latest_42_days_data(self, index_atr_avrg_df):
        
        """ Calculating the four parameter (very volatile, volatile, normal, quiet) 
            Condition for the Market Quality Number for daily data of NSE500, NIFTY and BTTIndex .

            Args :
                index_atr_avrg_df : data with NSECode, Date, atr_avg
                        for (NSE500, NIFTY and BTTIndex dataframes).
                        
            Returns :
                mqn_df_condition : Data with NSECode, Date, atr_avg, Very Volatile,
                        Volatile, Normal, Quiet for (NSE500, NIFTY and BTTIndex dataframes).
            
        """
        mqn_df_condition = pd.DataFrame()
        
        for i in range(0, len(index_atr_avrg_df)):
            
            latest42_days_df = index_atr_avrg_df.iloc[i : i + 42]
    
            i_stop = 42 - (len(index_atr_avrg_df)-1)
            
            if abs(i_stop) >= i :
                
                atr_avrg_sd = latest42_days_df["atr_avg"].std()
                atr_avrg_mean = latest42_days_df["atr_avg"].mean()
                
                very_volatile_val = atr_avrg_mean + (atr_avrg_sd * 3)
                volatile_val = atr_avrg_mean + (atr_avrg_sd * 0.5)
                normal_val = atr_avrg_mean - (atr_avrg_sd * 0.5)
                
                latest_day_df = index_atr_avrg_df.loc[[i]]
                
                latest_day_df['very_volatile_val'] = very_volatile_val
                latest_day_df['volatile_val'] = volatile_val
                latest_day_df['normal_val'] = normal_val
                
                latest_day_df.loc[(latest_day_df["atr_avg"] >= very_volatile_val),\
                                                     "Very Volatile"] = "very volatile"
                
                latest_day_df.loc[(latest_day_df["atr_avg"] < very_volatile_val) & \
                (latest_day_df["atr_avg"] >= volatile_val), "Volatile"] = "volatile"
                
                latest_day_df.loc[(latest_day_df["atr_avg"] < volatile_val) & \
                (latest_day_df["atr_avg"] >= normal_val), "Normal"] = "normal"
                
                latest_day_df.loc[(latest_day_df["atr_avg"] < normal_val), "Quiet"] = "quiet"
                
                # mqn_df_condition = mqn_df_condition.append(latest_day_df, ignore_index=True)
                mqn_df_condition = pd.concat([mqn_df_condition, latest_day_df], ignore_index=True)
                
            if abs(i_stop) < i:
                      
                latest_day_df = index_atr_avrg_df.loc[[i]]
                mqn_df_condition = pd.concat([mqn_df_condition, latest_day_df], ignore_index=True)
                # mqn_df_condition = mqn_df_condition.append(latest_day_df, ignore_index=True)
              
        mqn_df_condition = mqn_df_condition.fillna(0)     
    
        return  mqn_df_condition    
    
    
    def latest_100daysback_mqn_condtion_value(self, mqn_df_condition, nse_index_backdays_df):
        
        """ Calculation Market Quality Number Condition and Values for daily data of 
            NSE500, NIFTY and BTTIndex dataframes.
            
            Args :
                mqn_df_condition : Data with NSECode, Date, atr_avg, Very Volatile,
                        Volatile, Normal, Quiet for (NSE500, NIFTY and BTTIndex dataframes).
                
                nse_index_backdays_df : It contains back date change values for all NSE500,
                                     NIFTY and BTTIndex dataframes.
                                     
                                     
            Returns :
                    Data with NSECode, Date, atr_avg, Very Volatile, Volatile, Normal,
                    Quiet, mqn_condtion, mqn_val for daily NSE500, NIFTY and BTTIndex
                    dataframes. 
            
        """
        mqn_df_condition = mqn_df_condition.rename(columns = \
                        {"NSECode":"IndexName","Date": "date"})
    
        mqn_100days_back_change_df = nse_index_backdays_df.merge(mqn_df_condition, \
                                    how='left', left_on='date', right_on='date')
        
        mqn_df = pd.DataFrame()
        
        for i in range(0, len(mqn_100days_back_change_df)):
            
            mqn_latest100days_change_df = mqn_100days_back_change_df.iloc[i : i + 100]
            
            i_stop = 100 - (len(mqn_100days_back_change_df)-1)
            
            if abs(i_stop) >= i:
                
                change_sd_100days_back = mqn_latest100days_change_df["change"].std()
                change_mean_100days_back = mqn_latest100days_change_df["change"].mean()

                mqn_val = ( change_mean_100days_back / change_sd_100days_back ) * 10
                
                mqn_latest1_df = mqn_latest100days_change_df.loc[[i]]
               
                if ( mqn_val >= 1.47 ):
                    mqn_condtion = "Strong Bullish"
                    
                if ( mqn_val < 1.47 and mqn_val >= 0.7 ) :
                    mqn_condtion = "Bullish"  
                    
                if ( mqn_val < 0.7 and mqn_val >= 0 ) :
                    mqn_condtion = "Neutral"  
                
                if ( mqn_val < 0 and mqn_val >= -0.7 ) :
                    mqn_condtion = "Bearish" 
                    
                if ( mqn_val < -0.7 ) :
                    mqn_condtion = "Strong Bearish" 
                    
                mqn_latest1_df["mqn_condtion"] = mqn_condtion
                mqn_latest1_df["mqn_val"] = mqn_val
                
                mqn_df = pd.concat([mqn_df, mqn_latest1_df], ignore_index=True)
                # mqn_df = mqn_df.append(mqn_latest1_df, ignore_index=True) 
            
            if abs(i_stop) < i:
            
                mqn_latest1_df = mqn_latest100days_change_df.loc[[i]]
                mqn_df = pd.concat([mqn_df, mqn_latest1_df], ignore_index=True)
                # mqn_df = mqn_df.append(mqn_latest1_df, ignore_index=True)
                    
        mqn_df = mqn_df.fillna(0)   
        
        return mqn_df
            
    
            
        
        
    
    
    
    
    
        
        
        
    
    
    