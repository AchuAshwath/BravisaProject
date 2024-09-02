# Script to calculate indicator data for OHLC daily/weekly
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


class Indicators():
    """ Contains functions which calculate five different indicator values for the OHLC data
        (Daily as well as weekly). 
    """

    def __init__(self):
        pass

    def ema_indicator(self, close_curr, ema_prev, period):
        """ Calculates Exponential moving average for a given period. 

            Args: 
                close_curr: current day's 'close' value of a stock. 
                ema_prev: previous day's ema for a given period of a stock. 
                period: time period

            Returns: 
                ema: Exponential moving average for a given time period. 

            Raises: 
                No errors/exceptions.  
        """

        alpha = 2/(1+period)
        ema = ((close_curr - ema_prev)*alpha) + ema_prev

        return ema

    def macd(self, ema_x, ema_y):
        """ Calculates moving average convergence divergence based on EMA value. 

            Args: 
                ema_x: EMA for 12 day time period. 
                ema_y: EMA for 26 day time period. 

            Returns: 
                ema_x - ema_y: difference between 12 and 26 day EMA. 

            Raises:
                No errors/exceptions. 
        """

        return ema_x-ema_y

    def macd_signal(self, macd, signal_prev, n_sign):
        """ Calculates 9 day EMA for MACD line. 

            Args: 
                macd: Moving average convergence divergence (12-26 day ema)
                signal_prev: Previous day's MACD signal value. 
                n_sign: time period provided as input for ema calculation. 

            Returns: 
                ema_macd_line: 9-day EMA of MACD line. 

            Raises:
                No errors/exceptions.  
        """

        ema_macd_line = Indicators.ema_indicator(
            self, macd, signal_prev, n_sign)

        return ema_macd_line

    def macd_diff(self, macd, signal_line):
        """ Calculates the difference between MACD and signal line. 

            Args: 
                macd: Moving average convergence divergence (12-26 day ema)
                signal_line: 9-day EMA of MACD line(returned from macd_signal function).

            Returns: 
                macd-signal_line: difference between macd and macd signal value. 

            Raises:
                No errors/exceptions. 
        """

        return macd - signal_line

    def average_true_range(self, high_curr, low_curr, close_prev, atr_prev):
        """ Calculates Average true range.

            Args:
                high_curr: current day's 'High' value from OHLC. 
                low_curr: current day's 'Low' value from OHLC. 
                close_prev: previous day's 'Close' value from OHLC. 
                atr_prev: previous day's ATR value. 

            Returns: 
                atr_curr: current day's ATR value. 

            Raises: 
                No errors/exceptions. 
        """

        hl = high_curr - low_curr
        hpc = abs(high_curr - close_prev)
        lpc = abs(low_curr - close_prev)

        dataset = pd.DataFrame({'hl': hl, 'hpc': hpc, 'lpc': lpc})

        tr = dataset.max(axis=1)

        atr_curr = ((atr_prev*13) + tr)/14

        return atr_curr
