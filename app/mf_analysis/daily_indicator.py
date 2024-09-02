# Python script to generate indicator data for daily OHLC and insert into DB
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
from ta import trend, volatility
import numpy as np
from mf_analysis.indicators import Indicators


class DailyIndicator():
    """ Methods to calculate indicator data for Daily OHLC and insert them into the Database. """

    def __init__(self):

        self.indicators = Indicators()

    def __del__(self):
        pass

    def technical_indicators_daily(self, conn, ohlc_list):
        """ Calculate indicator data for daily OHLC. 
            Indicators that are calculated are: 
            12EMA, 13EMA, 26EMA, 50EMA, MACD, MACD Signal line, MACD difference, ATR. 

            Args:
                conn: database connection. 
                ohlc_list: dataframe of OHLC list for given day. 

            Returns: 
                indicator_data: dataframe which contains different indicator values that are calculated. 

            Raises: 
                No errors/exceptions. 
        """

        # indicator_data = pd.DataFrame()

        ohlc_list = ohlc_list.drop_duplicates(
            subset=['CompanyCode'], keep='first')

        # Get previous day's indicator data
        trend_data_prev_sql = 'SELECT DISTINCT ON("company_code") * FROM mf_analysis.indicators \
                            ORDER BY "company_code", "gen_date" DESC;'
        trend_data_prev = sqlio.read_sql_query(trend_data_prev_sql, con = conn)

        trends_data = pd.merge(ohlc_list, trend_data_prev,
                               left_on='CompanyCode', right_on='company_code', how='left')

        stock = trends_data['CompanyCode'].values
        company_name = trends_data['Company'].values
        nse_code = trends_data['NSECode'].values
        bse_code = trends_data['BSECode'].values
        gen_date = trends_data['Date'].values

        open_curr = trends_data['Open'].values
        close_curr = trends_data['Close'].values
        high_curr = trends_data['High'].values
        low_curr = trends_data['Low'].values
        volume = trends_data['Volume'].values

        close_prev = trends_data['close'].values
        ema12_prev = trends_data['ema12'].values
        ema13_prev = trends_data['ema13'].values
        ema26_prev = trends_data['ema26'].values
        ema50_prev = trends_data['ema50'].values
        signal_prev = trends_data['macd_signal'].values
        atr_prev = trends_data['atr'].values

        ema12_curr = self.indicators.ema_indicator(close_curr, ema12_prev, 12)
        ema13_curr = self.indicators.ema_indicator(close_curr, ema13_prev, 13)
        ema26_curr = self.indicators.ema_indicator(close_curr, ema26_prev, 26)
        ema50_curr = self.indicators.ema_indicator(close_curr, ema50_prev, 50)

        macd_line = self.indicators.macd(ema12_curr, ema26_curr)
        signal_line = self.indicators.macd_signal(macd_line, signal_prev, 9)
        macd_histogram = self.indicators.macd_diff(macd_line, signal_line)

        atr = self.indicators.average_true_range(
            high_curr, low_curr, close_prev, atr_prev)

        indicator_data = pd.DataFrame(data={'company_code': stock, 'company_name': company_name, 'nse_code': nse_code, 'bse_code': bse_code,
                                            'open': open_curr, 'high': high_curr, 'low': low_curr, 'close': close_curr, 'volume': volume, 'ema13': ema13_curr,
                                            'ema26': ema26_curr, 'macd': macd_line, 'macd_signal': signal_line, 'macd_diff': macd_histogram, 'atr': atr,
                                            'gen_date': gen_date, 'ema12': ema12_curr, 'ema50': ema50_curr})

        # indicator_data = indicator_data.append(df)
        # indicator_data = indicator_data.reset_index(drop=True)

        return indicator_data

    def insert_daily_indicators(self, conn, cur, indicator_data):
        """ Function to insert daily indicator data into the DB.

            Args:
                conn: database connection.
                cur: cursor object using the connection.  
                indicator_data: dataframe containing different indicator values returned from 
                                technical_indicators_daily function.

            Returns: 
                None

            Raises: 
                No errors/exceptions.   
        """

        # Fill null values as -1 to cast volume as integer and replace by it by NaN
        indicator_data["bse_code"].fillna(-1, inplace=True)
        indicator_data = indicator_data.astype({"bse_code": int})
        indicator_data = indicator_data.astype({"bse_code": str})
        indicator_data["bse_code"] = indicator_data["bse_code"].replace(
            '-1', np.nan)

        indicator_data["volume"].fillna(-1, inplace=True)
        indicator_data = indicator_data.astype({"volume": int})
        indicator_data = indicator_data.astype({"volume": str})
        indicator_data["volume"] = indicator_data["volume"].replace(
            '-1', np.nan)

        indicator_data = indicator_data[['company_code', 'company_name', 'nse_code', 'bse_code', 'open', 'high',
                                         'low', 'close', 'volume', 'ema13', 'ema26', 'macd', 'macd_signal', 'macd_diff',
                                         'atr', 'gen_date', 'ema12', 'ema50']]

        exportfilename = "indicator_data.csv"
        exportfile = open(exportfilename, "w")
        indicator_data.to_csv(exportfile, header=True, index=False,
                              float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
        COPY mf_analysis.indicators FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
        os.remove(exportfilename)
