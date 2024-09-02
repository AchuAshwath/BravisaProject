# Python script to generate indicator data for Monthly OHLC and insert into DB
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


class MonthlyIndicator():
    """ Contains methods which calculate indicator data for monthly OHLC
        and insert it into the database.
    """

    def __init__(self):
        self.indicators = Indicators()

    def __del__(self):
        pass

    def technical_indicators_monthly_ta(self, conn, date):
        """ Function to calculate indicator data for monthly OHLC.

            Args:
                conn: database connection
                date: month's end date
        """

        monthly_ohlc_list = 'SELECT * FROM public.ohlc_monthly \
                             ORDER BY company_code, "date" asc;'
        monthly_ohlc_full = sqlio.read_sql_query(monthly_ohlc_list, con =conn)

        monthly_indicator_data = pd.DataFrame()

        stock_list = monthly_ohlc_full['company_code'].drop_duplicates().tolist()

        for stock in stock_list:

            stock_monthly_ohlc = monthly_ohlc_full.loc[monthly_ohlc_full['company_code']==stock]
            temp = stock_monthly_ohlc.sort_values(by='date').copy()

            close_val = temp['close']
            open_val = temp['open']
            low_val = temp['low']
            high_val = temp['high']
            volume = temp['volume']
            gen_date = temp['date']
            nse_code = temp['nse_code']
            bse_code = temp['bse_code']
            company_name = temp['company_name']



            #Calculate different indicator values
            ema12 = trend.ema_indicator(close_val, n=12, fillna=False)
            ema13 = trend.ema_indicator(close_val, n=13, fillna=False)
            ema26 = trend.ema_indicator(close_val, n=26, fillna=False)
            ema50 = trend.ema_indicator(close_val, n=50, fillna=False)

            macd_line = trend.macd(close_val, n_fast =12, n_slow=26, fillna=False)
            signal_line = trend.macd_signal(close_val, n_fast=12, n_slow=26, n_sign=9, fillna=False)
            histogram = trend.macd_diff(close_val, n_fast=12, n_slow=26, n_sign=9, fillna=False)

            atr = volatility.average_true_range(high_val, low_val, close_val, n=14, fillna=False)

            df = pd.DataFrame(data = {'company_code': stock, 'company_name': company_name, 'bse_code': bse_code, 'nse_code': nse_code, 'open': open_val, \
                                        'high': high_val, 'low': low_val, 'close': close_val, 'volume': volume, 'ema13': ema13, 'ema26': ema26, \
                                        'macd': macd_line, 'macd_signal': signal_line, 'macd_diff': histogram, 'atr': atr, 'gen_date': gen_date, \
                                        'ema12': ema12, 'ema50': ema50})

            # print("df\n", df)
                
            # monthly_indicator_data = monthly_indicator_data.append(df)
            monthly_indicator_data = pd.concat([monthly_indicator_data, df])

            monthly_indicator_data = monthly_indicator_data.reset_index(drop=True)


        return monthly_indicator_data


    def technical_indicators_monthly(self, conn, date):
        """ Function to calculate indicator data for monthly OHLC.

            Args:
                conn: database connection.
                date: month's end date. 

            Returns:
                monthly_indicator_data: dataframe containing different
                indicator values for month's OHLC.
            
            Raises:
                No errors/exceptions.
        """
        # print("Date\n", date)

        # monthly_ohlc_full_sql = 'SELECT ohlc.*, btt."CompanyName", btt."NSECode", \
        #                          btt."BSECode" FROM public.ohlc_monthly ohlc \
        #                         LEFT JOIN public."BTTList" btt \
        #                         ON ohlc."company_code" = btt."CompanyCode" \
        #                         WHERE btt."BTTDate" = (SELECT MAX("BTTDate") FROM \
        #                         public."BTTList") AND ohlc."date" = \''+str(date)+'\' ;'
        # monthly_ohlc_full = sqlio.read_sql_query(monthly_ohlc_full_sql, con=conn)

        monthly_ohlc_full_sql = 'SELECT * FROM public.ohlc_monthly \
                                 WHERE "date" = \''+str(date)+'\';'
        monthly_ohlc_full = sqlio.read_sql_query(monthly_ohlc_full_sql, con = conn)

        # print("COLUMN monthly_ohlc_full\n",monthly_ohlc_full.columns)
        # 'company_code', 'open', 'high', 'low', 'close', 'volume', 'date'

        monthly_indicator_data = pd.DataFrame()

        stock_list = monthly_ohlc_full['company_code'].drop_duplicates().tolist()

        monthly_trend_prev_sql = 'SELECT DISTINCT ON("company_code") * FROM mf_analysis.indicators_monthly \
                                  WHERE "gen_date" < \''+str(date)+'\'\
                                  ORDER BY "company_code", "gen_date" DESC;'
        monthly_trend_prev = sqlio.read_sql_query(monthly_trend_prev_sql, con=conn)

        for stock in stock_list:
        
            stock_indicator_data = monthly_ohlc_full.loc[monthly_ohlc_full['company_code']==stock]
            stock_indicator_data_prev = monthly_trend_prev.loc[monthly_trend_prev['company_code']==stock]

            # print("stock_indicator_data\n",stock_indicator_data.columns)
            # ['company_code', 'company_name', 'nse_code', 'bse_code', 'open', \
            #  'high', 'low', 'close', 'volume', 'date']

            # print("stock_indicator_data_prev\n",stock_indicator_data_prev.columns)
            # ['company_code', 'company_name', 'nse_code', 'bse_code', 'open', \
            #  'high', 'low', 'close', 'volume', 'ema13', 'ema26', 'macd', \
            #  'macd_signal', 'macd_diff', 'atr', 'gen_date', 'ema12', 'ema50']

            if not (stock_indicator_data_prev.empty):

                company_name = stock_indicator_data['company_name'].values
                nse_code = stock_indicator_data['nse_code'].values
                bse_code = stock_indicator_data['bse_code'].values
                gen_date = stock_indicator_data['date'].values

                open_curr = stock_indicator_data['open'].values
                close_curr = stock_indicator_data['close'].values
                high_curr = stock_indicator_data['high'].values
                low_curr = stock_indicator_data['low'].values
                volume = stock_indicator_data['volume'].values

                close_prev = stock_indicator_data_prev['close'].replace(np.nan, 0).values
                ema13_prev = stock_indicator_data_prev['ema13'].replace(np.nan, 0).values
                ema26_prev = stock_indicator_data_prev['ema26'].replace(np.nan, 0).values
                ema12_prev = stock_indicator_data_prev['ema12'].replace(np.nan, 0).values
                ema50_prev = stock_indicator_data_prev['ema50'].replace(np.nan, 0).values
                signal_prev = stock_indicator_data_prev['macd_signal'].replace(np.nan, 0).values
                atr_prev = stock_indicator_data_prev['atr'].replace(np.nan, 0).values

                ema13_curr = self.indicators.ema_indicator(close_curr, ema13_prev, 13)
                ema26_curr = self.indicators.ema_indicator(close_curr, ema26_prev, 26)
                ema12_curr = self.indicators.ema_indicator(close_curr, ema12_prev, 12)
                ema50_curr = self.indicators.ema_indicator(close_curr, ema50_prev, 50)

                macd_line = self.indicators.macd(ema12_curr, ema26_curr)
                signal_line = self.indicators.macd_signal(macd_line, signal_prev, 9)
                macd_histogram = self.indicators.macd_diff(macd_line, signal_line)

                atr = self.indicators.average_true_range(high_curr, low_curr, close_prev, atr_prev)

                df = pd.DataFrame(data = {'company_code': stock, 'company_name': company_name, 'bse_code': bse_code, 'nse_code': nse_code, 'open': open_curr, \
                                        'high': high_curr, 'low': low_curr, 'close': close_curr, 'volume': volume, 'ema13': ema13_curr, 'ema26': ema26_curr, \
                                        'macd': macd_line, 'macd_signal': signal_line, 'macd_diff': macd_histogram, 'atr': atr, 'gen_date': gen_date, \
                                        'ema12': ema12_curr, 'ema50': ema50_curr})

                # print("df\n", df)
                
                # monthly_indicator_data = monthly_indicator_data.append(df)
                monthly_indicator_data = pd.concat([monthly_indicator_data, df])

                monthly_indicator_data = monthly_indicator_data.reset_index(drop=True)

            else:

                company_name = stock_indicator_data['company_name'].values
                nse_code = stock_indicator_data['nse_code'].values
                bse_code = stock_indicator_data['bse_code'].values
                gen_date = stock_indicator_data['date'].values

                open_curr = stock_indicator_data['open'].values
                close_curr = stock_indicator_data['close'].values
                high_curr = stock_indicator_data['high'].values
                low_curr = stock_indicator_data['low'].values
                volume = stock_indicator_data['volume'].values

                close_prev = stock_indicator_data['close'].values
                
                ema13_curr = np.nan
                ema26_curr = np.nan
                ema12_curr = np.nan
                ema50_curr = np.nan

                macd_line = np.nan
                signal_line = np.nan
                macd_histogram = np.nan
                atr = np.nan

                df = pd.DataFrame(data = {'company_code': stock, 'company_name': company_name, 'bse_code': bse_code, \
                                          'nse_code': nse_code, 'open': open_curr, 'high': high_curr, 'low': low_curr, \
                                          'close': close_curr, 'volume': volume, 'ema13': ema13_curr, 'ema26': ema26_curr, \
                                          'macd': macd_line, 'macd_signal': signal_line, 'macd_diff': macd_histogram, \
                                          'atr': atr, 'gen_date': gen_date, 'ema12': ema12_curr, 'ema50': ema50_curr})

                # monthly_indicator_data = monthly_indicator_data.append(df)
                monthly_indicator_data = pd.concat([monthly_indicator_data, df])

                monthly_indicator_data = monthly_indicator_data.reset_index(drop=True)

        return monthly_indicator_data

    def insert_monthly_indicators(self, conn, cur, monthly_indicator_data):
        """ Insert monthly indicator data into the DB.

            Args:
                conn: database connection.
                cur: cursor object using the connection.  
                monthly_indicator_data: dataframe containing different indicator values returned from 
                                       technical_indicators_monthly function.

            Returns: 
                None

            Raises: 
                No errors/exceptions.   
        """

        # Fill null values as -1 to cast volume as integer and replace by it by NaN

        monthly_indicator_data["bse_code"].fillna(-1, inplace=True)
        monthly_indicator_data = monthly_indicator_data.astype({"bse_code": int})
        monthly_indicator_data = monthly_indicator_data.astype({"bse_code": str})
        monthly_indicator_data["bse_code"] = monthly_indicator_data["bse_code"].replace('-1', np.nan)

        monthly_indicator_data["volume"].fillna(-1, inplace=True)
        monthly_indicator_data = monthly_indicator_data.astype({"volume": int})
        monthly_indicator_data = monthly_indicator_data.astype({"volume": str})
        monthly_indicator_data["volume"] = monthly_indicator_data["volume"].replace('-1', np.nan)

        # monthly_indicator_data = monthly_indicator_data[['company_code','company_name','nse_code','bse_code','open','high','low','close','volume', \
        #                         'ema13','ema26','macd','macd_signal','macd_diff','atr','gen_date', 'ema12', 'ema50']]

        monthly_indicator_data = monthly_indicator_data[['company_code', 'company_name',  'nse_code', \
                                                         'bse_code', 'open', 'high', 'low', 'close', \
                                                         'volume', 'ema13', 'ema26', 'macd', 'macd_signal', \
                                                         'macd_diff', 'atr', 'gen_date', 'ema12', 'ema50']]

        exportfilename = "monthly_indicator_data.csv"
        exportfile = open(exportfilename, "w")
        monthly_indicator_data.to_csv(exportfile, header=True, index=False,  float_format="%.2f", lineterminator='\r')
        exportfile.close()  

        copy_sql = """
        COPY mf_analysis.indicators_monthly FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

        with open(exportfilename, 'r') as f: 
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
        os.remove(exportfilename)