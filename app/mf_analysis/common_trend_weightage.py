# Script to generate Trend Weightage data 
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


class TrendWeightageCommon():
    
    """ Calculate Trend Weightage Process Data for RT daily, weekly
        and monthly.
    
    """
    
    def __init__(self):
        
        pass
    
    
    def cal_trend_weightage_rt_data(self, rt_df):
        
        """ Assign Defaults Values Trends Indicators such as
            rt_bullish_trending = 2, rt_bearish_trending = 1,
            rt_bullish_non_trending = -1, rt_bearish_non_trending = -2
            and store in one column that is weightage and calculate the sum
            of weightage for particular dates.
            
            Args: 
                rt_df : It contains the data of RT daily, RT weekly,
                        and RT monthly.
            
            Returns: 
                rt_groupby_date_df : date, weightage of the trend Weightage
                                        for daily, weekly and monthly.
        
        """
    
        rt_df.loc[rt_df["rt_bullish_trending"] == 1, "weightage"] = 2
        rt_df.loc[rt_df["rt_bearish_trending"] == 1, "weightage"] = 1
        rt_df.loc[rt_df["rt_bullish_non_trending"] == 1, "weightage"] = -1
        rt_df.loc[rt_df["rt_bearish_non_trending"] == 1, "weightage"] = -2
        
        rt_groupby_date_df = rt_df.groupby(["gen_date"])["weightage"].sum()
        
        rt_groupby_date_df = rt_groupby_date_df.reset_index()
        
        return rt_groupby_date_df

        
    