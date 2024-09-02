# Python script to calculate performance values for stocks and store it in DB
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
from utils.db_helper import DB_Helper
import utils.date_set as date_set

class StockPerformance:
    """ Contains functions which fetch OHLC data for the current BTT List 
        and calculate change value for the stocks for different time periods
        (1 day, 5 days, 30 days, 90 days, 6 months, 1 years, 2 years, 5 years).  
    """

    def __init__(self):

        date = datetime.date.today()
        self.back_date = (datetime.date(date.year - 5, date.month, date.day))
        self.date = date

    def get_ohlc_current(self, curr_date, conn):
        """ Gets current day's OHLC data. """

        sql = 'SELECT * FROM public."OHLC" \
                WHERE "Date" = \''+str(curr_date)+'\' \
                AND "CompanyCode" IS NOT NULL;'
        ohlc = sqlio.read_sql_query(sql, con=conn)

        return ohlc

    def get_ohlc_back(self, curr_date,conn):
        """ Gets OHLC data for backdate. """

        back_date=(datetime.date(curr_date.year - 5, curr_date.month, curr_date.day))
        sql = 'SELECT * FROM public."OHLC" \
               WHERE "Date" < \''+str(curr_date)+'\' \
               AND "Date" >= \''+str(back_date)+'\' \
               AND "CompanyCode" IS NOT NULL ;'
        ohlc_back = sqlio.read_sql_query(sql, con=conn)

        return ohlc_back

    def get_btt_list(self, conn):
        """ Gets current month's BTT List. """

        sql = 'SELECT "CompanyCode" FROM public."BTTList" \
               WHERE "BTTDate" = (SELECT MAX("BTTDate") FROM public."BTTList") \
               AND "CompanyCode" IS NOT NULL;'
        btt_list = sqlio.read_sql_query(sql, con=conn)

        return btt_list

    def merge_btt_ohlc(self, ohlc, btt_list):
        """ Merge OHLC with BTT List to get the stocks that are necessary. """

        btt_ohlc_list = pd.merge(btt_list, ohlc, on='CompanyCode', how='left')

        return btt_ohlc_list

    def merge_btt_ohlc_back(self, ohlc_back, btt_list):
        """ Merge OHLC with BTT List to get the stocks that are necessary. """

        btt_ohlc_back = pd.merge(
            btt_list, ohlc_back, on='CompanyCode', how='left')

        return btt_ohlc_back

    def get_stock_change(self, btt_ohlc_list, btt_ohlc_back):
        """ Get change value for stocks for the required time periods. 
            Input parameters: Current day's OHLC list, backdate OHLC.     
        """

        stock_performance = pd.DataFrame()

        stock_list = btt_ohlc_list['CompanyCode'].drop_duplicates().tolist()

        for stock in stock_list:

            per_stock = btt_ohlc_list[btt_ohlc_list['CompanyCode'] == stock]
            per_stock_back = btt_ohlc_back[btt_ohlc_back['CompanyCode'] == stock]

            df = self.calc_change(per_stock, per_stock_back, self.date)

            # stock_performance = stock_performance.append(df)
            stock_performance = pd.concat([stock_performance, df])


        return stock_performance

    def get_closest_date(self, stock_ohlc, date):
        """ Function to get OHLC data for a particular stock for the closest date for which data is 
            available in case data for input date isn't available. 

            Initially checks for input date. If data isn't present for the given backdate, it goes 
            one day back from that date to check for OHLC data. If data isn't present, it checks for 
            two days back from given date. If data isn't present, it moves one day forward from the given date. 
            If data isn't present, it goes three days back from given date to check for OHLC data. 
            Returns None otherwise. 

            Input parameters: Stockwise OHLC history, date.  
        """

        # Get OHLC for stock for dates closest to given date.
        curr_backdate = stock_ohlc['Date'] == date
        one_day_forward = stock_ohlc['Date'] == (date + datetime.timedelta(1))
        one_day_back = stock_ohlc['Date'] == (date + datetime.timedelta(-1))
        two_day_back = stock_ohlc['Date'] == (date + datetime.timedelta(-2))
        three_day_back = stock_ohlc['Date'] == (date + datetime.timedelta(-3))

        # Check for current backdate, else check for nearby dates.
        if not (stock_ohlc.loc[curr_backdate].empty):
            return stock_ohlc.loc[curr_backdate, "Close"].head(1).item()
        else:
            if not (stock_ohlc.loc[one_day_back].empty):
                return stock_ohlc.loc[one_day_back, "Close"].head(1).item()
            else:
                if not (stock_ohlc.loc[two_day_back].empty):
                    return stock_ohlc.loc[two_day_back, "Close"].head(1).item()
                else:
                    if not (stock_ohlc[one_day_forward].empty):
                        return stock_ohlc.loc[one_day_forward, "Close"].head(1).item()
                    else:
                        if not (stock_ohlc.loc[three_day_back].empty):
                            return stock_ohlc.loc[three_day_back, "Close"].head(1).item()
                        else:
                            return None

    def calc_change(self, per_stock, per_stock_back, date):
        """ Function to calculate change values for a given stock for different time 
            periods (1day, 5day, 30day, 90day, 6month, 1year, 2year, 3year, 5year). 

            Change value is calculated based on 'Close' from OHLC. 
            Change = ((Current day's Close - backdate's Close)/backdate's Close) * 100

            Input parameters: OHLC for current date, OHLC for backdate, current date. 
        """

        date = pd.to_datetime(date).date()

        one_day_back = (date + datetime.timedelta(-1))
        five_day_back = (date + datetime.timedelta(-5))
        thirty_day_back = (date + datetime.timedelta(-30))
        ninety_day_back = (date + datetime.timedelta(-90))
        six_month_back = (date + datetime.timedelta(-183))
        one_year_back = (date + datetime.timedelta(-365))
        two_year_back = (date + datetime.timedelta(-730))
        three_year_back = (date + datetime.timedelta(-1095))
        five_year_back = (date + datetime.timedelta(-1825))

        for index, row in per_stock.iterrows():

            one_day_back_close = self.get_closest_date(
                per_stock_back, one_day_back)
            five_day_back_close = self.get_closest_date(
                per_stock_back, five_day_back)
            thirty_day_back_close = self.get_closest_date(
                per_stock_back, thirty_day_back)
            ninety_day_back_close = self.get_closest_date(
                per_stock_back, ninety_day_back)
            six_month_back_close = self.get_closest_date(
                per_stock_back, six_month_back)
            one_year_back_close = self.get_closest_date(
                per_stock_back, one_year_back)
            two_year_back_close = self.get_closest_date(
                per_stock_back, two_year_back)
            three_year_back_close = self.get_closest_date(
                per_stock_back, three_year_back)
            five_year_back_close = self.get_closest_date(
                per_stock_back, five_year_back)

            RR1 = ((row["Close"]-one_day_back_close)/one_day_back_close) * \
                100 if one_day_back_close is not None else np.nan
            RR5 = ((row["Close"]-five_day_back_close)/five_day_back_close) * \
                100 if five_day_back_close is not None else np.nan
            RR30 = ((row["Close"]-thirty_day_back_close)/thirty_day_back_close) * \
                100 if thirty_day_back_close is not None else np.nan
            RR90 = ((row["Close"]-ninety_day_back_close)/ninety_day_back_close) * \
                100 if ninety_day_back_close is not None else np.nan
            RR6month = ((row["Close"]-six_month_back_close)/six_month_back_close) * \
                100 if six_month_back_close is not None else np.nan
            RR1year = ((row["Close"]-one_year_back_close)/one_year_back_close) * \
                100 if one_year_back_close is not None else np.nan
            RR2year = ((row["Close"]-two_year_back_close)/two_year_back_close) * \
                100 if two_year_back_close is not None else np.nan
            RR3year = ((row["Close"]-three_year_back_close)/three_year_back_close) * \
                100 if three_year_back_close is not None else np.nan
            RR5year = ((row["Close"]-five_year_back_close)/five_year_back_close) * \
                100 if five_year_back_close is not None else np.nan

            per_stock.loc[index, 'nse_code'] = per_stock['NSECode'].values
            per_stock.loc[index, 'bse_code'] = per_stock['BSECode'].values
            per_stock.loc[index,
                          'company_code'] = per_stock['CompanyCode'].values
            per_stock.loc[index, 'date'] = date

            per_stock.loc[index, '1day'] = round(
                RR1, 2)if not math.isnan(RR1) else np.nan
            per_stock.loc[index, '5day'] = round(
                RR5, 2)if not math.isnan(RR5) else np.nan
            per_stock.loc[index, '30day'] = round(
                RR30, 2)if not math.isnan(RR30) else np.nan
            per_stock.loc[index, '90day'] = round(
                RR90, 2)if not math.isnan(RR90) else np.nan
            per_stock.loc[index, '6month'] = round(
                RR6month, 2)if not math.isnan(RR6month) else np.nan
            per_stock.loc[index, '1year'] = round(
                RR1year, 2)if not math.isnan(RR1year) else np.nan
            per_stock.loc[index, '2year'] = round(
                RR2year, 2)if not math.isnan(RR2year) else np.nan
            per_stock.loc[index, '3year'] = round(
                RR3year, 2)if not math.isnan(RR3year) else np.nan
            per_stock.loc[index, '5year'] = round(
                RR5year, 2)if not math.isnan(RR5year) else np.nan

        return per_stock

    def insert_stock_performance(self, stock_performance, conn, cur):
        """ Insert Performance data into the DB. """

        stock_performance["bse_code"].fillna(-1, inplace=True)
        stock_performance = stock_performance.astype({"bse_code": int})
        stock_performance = stock_performance.astype({"bse_code": str})
        stock_performance["bse_code"] = stock_performance["bse_code"].replace(
            '-1', np.nan)

        stock_performance = stock_performance[['company_code', '1day', '5day', '30day', '90day', '6month', '1year', '2year', '5year',
                                               'date', 'nse_code', 'bse_code']]

        exportfilename = "stock_performance.csv"
        exportfile = open(exportfilename, "w")
        stock_performance.to_csv(
            exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY dash_process.stock_performance FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
        os.remove(exportfilename)


def main(curr_date):
    """ Function to call the StockPerformance methods in order to run 
        daily stock change calculation process. 
    """

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    stock_performance = StockPerformance()
    date = curr_date

    print("\nGetting OHLC for current date:", date)
    ohlc = stock_performance.get_ohlc_current(date, conn)

    if not(ohlc.empty):

        print("Getting OHLC back")
        ohlc_back = stock_performance.get_ohlc_back(date,conn)

        print("Getting BTTList")
        btt_list = stock_performance.get_btt_list(conn)

        print("Merging current day's OHLC with BTT List")
        btt_ohlc_list = stock_performance.merge_btt_ohlc(ohlc, btt_list)

        print("Merging backdate OHLC with BTT List")
        btt_ohlc_back = stock_performance.merge_btt_ohlc_back(
            ohlc_back, btt_list)

        print("Calculating change value for stocks")
        stock_change = stock_performance.get_stock_change(
            btt_ohlc_list, btt_ohlc_back)

        print("Inserting into the DB")
        stock_performance.insert_stock_performance(stock_change, conn, cur)

    else:

        print("OHLC not found for current date")
        raise ValueError('OHLC data not found for date: ' +
                         date.strftime("%Y-%m-%d"))

    conn.close()
