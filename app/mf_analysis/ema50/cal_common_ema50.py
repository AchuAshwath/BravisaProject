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


class Cal_EMA50():

    """ function contain EMA50 calculation   

    """

    def __init__(self):

        pass

    def cal_EMA50Above(self, df):
        """ calculting the percentage of EMA50 for daily 

            Args: 
                df : indicator_daily

            Returns: 
                ema50_above_df: dataframe of ema50_above calculation percentage data \. 

            Raises:
                No errors/exceptions. 
        """

        data = pd.DataFrame()
        df_groupby_date = df.groupby(["gen_date"])
    
        for date, df in df_groupby_date:
    
            total_count = df["close"].count().item()
         
            df_sorted = df[df["close"] > df["ema50"]]
            count_EMA50Above = df_sorted["ema50"].count()
        
            ema50_above_percentage = (count_EMA50Above / total_count * 100)    
            ema50_above_df = pd.DataFrame({"date": [date], "ema50_above_percentage": [ema50_above_percentage]})

            # data = data.append(ema50_above_df, ignore_index=True)
            data = pd.concat([data, ema50_above_df], ignore_index=True)

        return data
