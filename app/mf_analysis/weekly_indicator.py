# Python script to generate indicator data for Weekly OHLC and insert into DB
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


class WeeklyIndicator():
    """ Contains methods which calculate indicator data for weekly OHLC 
        and insert it into the database. 
    """

    def __init__(self):
        self.indicators = Indicators()

    def __del__(self):
        pass

    def technical_indicators_weekly(self, conn, date):
        """ Function to calculate indicator data for weekly OHLC.

            Args: 
                conn: database connection. 
                date: week's end date, i.e. every friday's date.

            Returns: 
                weekly_indicator_data: dataframe containing different indicator values for 
                                       week's OHLC. 

            Raises: 
                No errors/exceptions. 
        """

        weekly_ohlc_full_sql = 'SELECT ohlc.*, btt."CompanyName", btt."NSECode", btt."BSECode" FROM public.ohlc_weekly ohlc \
                                LEFT JOIN public."BTTList" btt \
                                ON ohlc."company_code" = btt."CompanyCode" \
                                WHERE btt."BTTDate" = (SELECT MAX("BTTDate") FROM public."BTTList") \
                                AND ohlc."date" = \''+str(date)+'\' ;'
        weekly_ohlc_full = sqlio.read_sql_query(weekly_ohlc_full_sql, con=conn)
        print(len(weekly_ohlc_full))
        weekly_indicator_data = pd.DataFrame()

        stock_list = weekly_ohlc_full['company_code'].drop_duplicates(
        ).tolist()

        weekly_trend_prev_sql = 'SELECT DISTINCT ON("company_code") * FROM mf_analysis.indicators_weekly \
                            ORDER BY "company_code", "gen_date" DESC;'
        weekly_trend_prev = sqlio.read_sql_query(
            weekly_trend_prev_sql, con=conn)
        print(len(weekly_trend_prev))
        for stock in stock_list:

            stock_indicator_data = weekly_ohlc_full.loc[weekly_ohlc_full['company_code'] == stock]
            stock_indicator_data_prev = weekly_trend_prev.loc[
                weekly_trend_prev['company_code'] == stock]

            if not (stock_indicator_data_prev.empty):

                company_name = stock_indicator_data['CompanyName'].values
                nse_code = stock_indicator_data['NSECode'].values
                bse_code = stock_indicator_data['BSECode'].values
                gen_date = stock_indicator_data['date'].values

                open_curr = stock_indicator_data['open'].values
                close_curr = stock_indicator_data['close'].values
                high_curr = stock_indicator_data['high'].values
                low_curr = stock_indicator_data['low'].values
                volume = stock_indicator_data['volume'].values

                close_prev = stock_indicator_data_prev['close'].values
                ema13_prev = stock_indicator_data_prev['ema13'].values
                ema26_prev = stock_indicator_data_prev['ema26'].values
                ema12_prev = stock_indicator_data_prev['ema12'].values
                ema50_prev = stock_indicator_data_prev['ema50'].values
                signal_prev = stock_indicator_data_prev['macd_signal'].values
                atr_prev = stock_indicator_data_prev['atr'].values

                ema13_curr = self.indicators.ema_indicator(
                    close_curr, ema13_prev, 13)
                ema26_curr = self.indicators.ema_indicator(
                    close_curr, ema26_prev, 26)
                ema12_curr = self.indicators.ema_indicator(
                    close_curr, ema12_prev, 12)
                ema50_curr = self.indicators.ema_indicator(
                    close_curr, ema50_prev, 50)

                macd_line = self.indicators.macd(ema12_curr, ema26_curr)
                signal_line = self.indicators.macd_signal(
                    macd_line, signal_prev, 9)
                macd_histogram = self.indicators.macd_diff(
                    macd_line, signal_line)

                atr = self.indicators.average_true_range(
                    high_curr, low_curr, close_prev, atr_prev)

                df = pd.DataFrame(data={'company_code': stock, 'company_name': company_name, 'bse_code': bse_code, 'nse_code': nse_code, 'open': open_curr,
                                        'high': high_curr, 'low': low_curr, 'close': close_curr, 'volume': volume, 'ema13': ema13_curr, 'ema26': ema26_curr,
                                        'macd': macd_line, 'macd_signal': signal_line, 'macd_diff': macd_histogram, 'atr': atr, 'gen_date': gen_date,
                                        'ema12': ema12_curr, 'ema50': ema50_curr})

                # weekly_indicator_data = weekly_indicator_data.append(df)
                weekly_indicator_data = pd.concat([weekly_indicator_data, df])
                print("weekly_indicator_data: ", len(weekly_indicator_data))

                weekly_indicator_data = weekly_indicator_data.reset_index(
                    drop=True)
                print("weekly_indicator_data after reset index: ", len(weekly_indicator_data))

        # indicator_data.sort_values(by=['gen_date'], inplace=True, ascending=True)
        print(len(weekly_indicator_data))

        return weekly_indicator_data

    def insert_weekly_indicators(self, conn, cur, weekly_indicator_data):
        """ Insert weekly indicator data into the DB.

            Args:
                conn: database connection.
                cur: cursor object using the connection.  
                weekly_indicator_data: dataframe containing different indicator values returned from 
                                       technical_indicators_weekly function.

            Returns: 
                None

            Raises: 
                No errors/exceptions.   
        """
        # check if weekly_indicator_data is not empty
        if not weekly_indicator_data.empty:
            # Fill null values as -1 to cast volume as integer and replace by it by NaN
            weekly_indicator_data["bse_code"].fillna(-1, inplace=True)
            weekly_indicator_data = weekly_indicator_data.astype({"bse_code": int})
            weekly_indicator_data = weekly_indicator_data.astype({"bse_code": str})
            weekly_indicator_data["bse_code"] = weekly_indicator_data["bse_code"].replace(
                '-1', np.nan)

            weekly_indicator_data["volume"].fillna(-1, inplace=True)
            weekly_indicator_data = weekly_indicator_data.astype({"volume": int})
            weekly_indicator_data = weekly_indicator_data.astype({"volume": str})
            weekly_indicator_data["volume"] = weekly_indicator_data["volume"].replace(
                '-1', np.nan)

            # weekly_indicator_data = weekly_indicator_data[['company_code','company_name','nse_code','bse_code','open','high','low','close','volume', \
            #                         'ema13','ema26','macd','macd_signal','macd_diff','atr','gen_date', 'ema12', 'ema50']]

            weekly_indicator_data = weekly_indicator_data[['company_code', 'company_name', 'nse_code', 'bse_code', 'open', 'high', 'low', 'close', 'volume',
                                                        'ema13', 'ema26', 'macd', 'macd_signal', 'macd_diff', 'atr', 'gen_date', 'ema12', 'ema50']]

            exportfilename = "weekly_indicator_data.csv"
            exportfile = open(exportfilename, "w")
            weekly_indicator_data.to_csv(
                exportfile, header=True, index=False,  float_format="%.2f", lineterminator='\r')
            exportfile.close()

            copy_sql = """
            COPY mf_analysis.indicators_weekly FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

            with open(exportfilename, 'r') as f:
                cur.copy_expert(sql=copy_sql, file=f)
                conn.commit()
            f.close()
            os.remove(exportfilename)
        else:
            print("No data to insert")
