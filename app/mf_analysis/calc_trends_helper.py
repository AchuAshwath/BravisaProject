# Python script to calculate trends data
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
import numpy as np


class Trends():
    """ Contains common helper methods to calculate trends data for daily and 
        weekly OHLC data.
    """

    def __init__(self):
        pass

    def calc_trends(self, trends_data, trends_back):
        """ Function to calculate trends data for provided indicator data.
            Args: 
                trends_data: current day's trends data. 
                trends_back: previous day's trends data. 
            Returns: 
                trends_data: dataframe containing different trends metric data for a stock.
            Raises: 
                No errors/exceptions.  
        """

        trends_back = trends_back[['company_code','macd_diff']]
        trends_back = trends_back.rename(columns={"macd_diff": "macd_diff_back"})

        trends_data = pd.merge(trends_data, trends_back, on='company_code', how='left')

        # trends_data.fillna(0)

        trends_data.loc[:,'ema13'] = trends_data['ema13'].fillna(0)
        trends_data.loc[:,'ema26'] = trends_data['ema26'].fillna(0) 

        trends_data.loc[:,"ema13_diff"] = (trends_data["ema13"] - trends_data["ema26"]) * 3
        trends_data.loc[:,"ema26_diff"] = (trends_data["ema26"] - trends_data["ema13"]) * 3
        trends_data.loc[:,"14ATR"] = trends_data["atr"]

        trends_data.loc[:,'ema13_diff'] = trends_data['ema13_diff'].fillna(0)
        trends_data.loc[:,'ema26_diff'] = trends_data['ema26_diff'].fillna(0)

        trends_data.loc[:,'rt_bullish'] = trends_data['ema13_diff'] / trends_data["14ATR"]
        trends_data.loc[:,'rt_bearish'] = trends_data['ema26_diff'] / trends_data["14ATR"]

        trends_data.loc[:,'rt_bullish'] = trends_data['rt_bullish'].fillna(0)
        trends_data.loc[:,'rt_bearish'] = trends_data['rt_bearish'].fillna(0)

        trends_data.loc[(trends_data['rt_bullish'] >= 1) & \
                        (trends_data['rt_bullish'] != np.nan), "bullish_trending"] = 1
        trends_data.loc[(trends_data['rt_bearish'] >= 1) & \
                        (trends_data['rt_bearish'] != np.nan), "bearish_trending"] = 1

        trends_data.loc[(trends_data["close"] > trends_data["ema26"]) & \
                        (trends_data['rt_bullish'] < 1) & \
                        (trends_data['rt_bearish'] < 1) & \
                        (trends_data['rt_bullish'] != np.nan) & \
                        (trends_data['rt_bearish'] != np.nan), "bullish_non_trending"] = 1

        trends_data.loc[(trends_data["close"] < trends_data["ema26"]) & \
                        (trends_data['rt_bullish'] < 1) & \
                        (trends_data['rt_bearish'] < 1) & \
                        (trends_data['rt_bullish'] != np.nan) & \
                        (trends_data['rt_bearish'] != np.nan), "bearish_non_trending"] = 1

        trends_data.loc[:,'ema13'] = trends_data['ema13'].fillna(0)
        trends_data.loc[:,'ema26'] = trends_data['ema26'].fillna(0)

        trends_data.loc[:,'bullish_trending'] = trends_data['bullish_trending'].fillna(0)
        trends_data.loc[:,'bearish_trending'] = trends_data['bearish_trending'].fillna(0)
        trends_data.loc[:,'bullish_non_trending'] = trends_data['bullish_non_trending'].fillna(0)
        trends_data.loc[:,'bearish_non_trending'] = trends_data['bearish_non_trending'].fillna(0)

        trends_data.loc[:,'macd_diff'] = trends_data['macd_diff'].fillna(0)
        trends_data.loc[:,'macd_diff_back'] = trends_data['macd_diff_back'].fillna(0)

        # Calculation of Long, Short, long_sideways, short_sideways

        trends_data.loc[(trends_data["macd_diff"] > trends_data["macd_diff_back"]) & \
                        (trends_data["close"] > trends_data["ema26"]) & \
                        (trends_data["ema26"] != 0) & \
                        (trends_data["macd_diff"] != 0) & \
                        (trends_data["macd_diff_back"] != 0) & \
                        (trends_data["macd_diff"] != np.nan) & \
                        (trends_data["macd_diff_back"] != np.nan), 'long'] = 1

        trends_data.loc[(trends_data["macd_diff"] < trends_data["macd_diff_back"]) & \
                        (trends_data["close"] < trends_data["ema26"]) & \
                        (trends_data["ema26"] != 0) & \
                        (trends_data["macd_diff"] != 0) & \
                        (trends_data["macd_diff_back"] != 0) & \
                        (trends_data["macd_diff"] != np.nan) & \
                        (trends_data["macd_diff_back"] != np.nan), 'short'] = 1

        trends_data.loc[(trends_data["macd_diff"] < trends_data["macd_diff_back"]) & \
                        (trends_data["close"] > trends_data["ema26"]) & \
                        (trends_data["ema26"] != 0) & \
                        (trends_data["macd_diff"] != 0) & \
                        (trends_data["macd_diff_back"] != 0) & \
                        (trends_data["macd_diff"] != np.nan) & \
                        (trends_data["macd_diff_back"] != np.nan), 'long_sideways'] = 1

        trends_data.loc[(trends_data["macd_diff"] > trends_data["macd_diff_back"]) & \
                        (trends_data["close"] < trends_data["ema26"]) & \
                        (trends_data["ema26"] != 0) & \
                        (trends_data["macd_diff"] != 0) & \
                        (trends_data["macd_diff_back"] != 0) & \
                        (trends_data["macd_diff"] != np.nan) & \
                        (trends_data["macd_diff_back"] != np.nan), 'short_sideways'] = 1

        trends_data.loc[:,'long'] = trends_data['long'].fillna(0)
        trends_data.loc[:,'short'] = trends_data['short'].fillna(0)
        trends_data.loc[:,'long_sideways'] = trends_data['long_sideways'].fillna(0)
        trends_data.loc[:,'short_sideways'] = trends_data['short_sideways'].fillna(0)

        # Determine buy/sell

        trends_data.loc[(trends_data["rt_bearish"] >= 1) & \
                        (trends_data["macd_diff"] > trends_data["macd_diff_back"]) & \
                        (trends_data["close"] > trends_data["ema26"]) & \
                        (trends_data["ema26"] != 0 ) & \
                        (trends_data["macd_diff"] != 0 ) & \
                        (trends_data["macd_diff_back"] != 0),  'buy'] = 1

        trends_data.loc[(trends_data["rt_bullish"] >= 1) & \
                        (trends_data["macd_diff"] < trends_data["macd_diff_back"]) & \
                        (trends_data["close"] < trends_data["ema26"]) & \
                        (trends_data["ema26"] != 0 ) & \
                        (trends_data["macd_diff"] != 0 ) & \
                        (trends_data["macd_diff_back"] != 0),  'sell'] = 1

        trends_data.loc[:,'buy'] = trends_data['buy'].fillna(0)
        trends_data.loc[:,'sell'] = trends_data['sell'].fillna(0)       
        
        trends_data =trends_data[['company_code', 'company_name', 'nse_code', \
                                  'bse_code', 'open', 'high','low', 'close', 'volume', \
                                  'ema13', 'ema26', 'macd', 'macd_signal', 'macd_diff', \
                                  'atr', 'gen_date', 'ema12', 'ema50', 'bullish_trending', \
                                  'bearish_trending', 'bullish_non_trending', 'bearish_non_trending', \
                                  'long', 'short', 'long_sideways', 'short_sideways', 'buy', 'sell']]
        

        return trends_data