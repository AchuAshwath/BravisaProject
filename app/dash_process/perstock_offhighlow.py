# Python program to calculate off-high/low percentage for stocks everyday
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


class StockOffHighLow:
    """ Contains methods which gets Off-High/Low for stocks from PRS and calculates 
        percentage of stocks having Off-High/Low: greater than 20%, 15-20%, 10-15%, 
        5-10% and less than 5%. 
    """

    def __init__(self):

        date = datetime.date.today()
        self.back_date = (datetime.date(date.year - 1, date.month, date.day))
        self.date = date

    def get_prs(self, curr_date, conn):
        """ Function to get PRS for current date. """

        sql = 'SELECT "CompanyCode", "Date", "Off-High", "Off-Low", "NSECode", "BSECode" FROM "Reports"."PRS" \
               WHERE "Date" = \''+str(curr_date)+'\';'
        prs = sqlio.read_sql_query(sql, con=conn)

        # use this query when running backdate calculation
        # back_date = (datetime.date(date.year -1, date.month, date.day))
        # sql = 'SELECT "CompanyCode", "Date", "Off-High", "Off-Low", "NSECode", "BSECode" FROM "Reports"."PRS" \
        #    WHERE "Date" >= \''+str(self.back_date)+'\' \
        #    AND "Date" <=  \''+str(self.date)+'\';'
        # prs = sqlio.read_sql_query(sql, con=conn)

        return prs

    def calc_off_high(self, prs):
        """ Calculates Percentage of stocks which have Off High >5%, 5-10%, 10-15%, 
            15-20% and >20%. 
            Input parameters: PRS 
        """

        date_list = prs['Date'].drop_duplicates().tolist()

        stock_off_high = pd.DataFrame()

        for date in date_list:

            prs_ = prs.loc[prs['Date'] == date]
            stock_count = prs_['Off-High'].count()

            less_than_5_count = prs_[prs_["Off-High"] < 5].count()["Off-High"]
            less_than_5_per = (less_than_5_count/stock_count)*100

            _5_to_10_count = prs_[
                (prs_["Off-High"] >= 5) & (prs_["Off-High"] < 10)].count()["Off-High"]
            _5_to_10_per = (_5_to_10_count/stock_count)*100

            _10_to_15_count = prs_[
                (prs_["Off-High"] >= 10) & (prs_["Off-High"] < 15)].count()["Off-High"]
            _10_to_15_per = (_10_to_15_count/stock_count)*100

            _15_to_20_count = prs_[
                (prs_["Off-High"] >= 15) & (prs_["Off-High"] < 20)].count()["Off-High"]
            _15_to_20_per = (_15_to_20_count/stock_count)*100

            greater_than_20_count = prs_[
                prs_["Off-High"] >= 20].count()["Off-High"]
            greater_than_20_per = (greater_than_20_count/stock_count)*100

            percentdata = pd.DataFrame({'less_than_5': [less_than_5_per], '5_to_10': [_5_to_10_per],
                                        '10_to_15': [_10_to_15_per], '15_to_20': [_15_to_20_per],
                                        'greater_than_20': [greater_than_20_per], 'date': [date]})

            # stock_off_high = stock_off_high.append(percentdata, ignore_index=True)
            stock_off_high = pd.concat([stock_off_high, percentdata], ignore_index=True)


        stock_off_high['less_than_5'] = round(stock_off_high['less_than_5'], 2)
        stock_off_high['5_to_10'] = round(stock_off_high['5_to_10'], 2)
        stock_off_high['10_to_15'] = round(stock_off_high['10_to_15'], 2)
        stock_off_high['15_to_20'] = round(stock_off_high['15_to_20'], 2)
        stock_off_high['greater_than_20'] = round(
            stock_off_high['greater_than_20'], 2)

        stock_off_high['date'] = pd.to_datetime(stock_off_high['date'])
        stock_off_high['date'] = stock_off_high['date'].dt.strftime("%Y-%m-%d")

        return stock_off_high

    def calc_off_low(self, prs):
        """ Calculates Percentage of stocks which have Off-Low >5%, 5-10%, 10-15%, 
            15-20% and >20%. 
            Input parameters: PRS 
        """

        date_list = prs['Date'].drop_duplicates().tolist()

        stock_off_low = pd.DataFrame()

        for date in date_list:

            prs_ = prs.loc[prs['Date'] == date]
            stock_count = prs_['Off-Low'].count()

            less_than_5_count = prs_[prs_['Off-Low'] < 5].count()['Off-Low']
            less_than_5_per = (less_than_5_count/stock_count)*100

            _5_to_10_count = prs_[(prs_['Off-Low'] >= 5)
                                  & (prs_['Off-Low'] < 10)].count()['Off-Low']
            _5_to_10_per = (_5_to_10_count/stock_count)*100

            _10_to_15_count = prs_[
                (prs_['Off-Low'] >= 10) & (prs_['Off-Low'] < 15)].count()['Off-Low']
            _10_to_15_per = (_10_to_15_count/stock_count)*100

            _15_to_20_count = prs_[
                (prs_['Off-Low'] >= 15) & (prs_['Off-Low'] < 20)].count()['Off-Low']
            _15_to_20_per = (_15_to_20_count/stock_count)*100

            greater_than_20_count = prs_[
                prs_['Off-Low'] >= 20].count()['Off-Low']
            greater_than_20_per = (greater_than_20_count/stock_count)*100

            df = pd.DataFrame({'less_than_5': [less_than_5_per], '5_to_10': [_5_to_10_per],
                               '10_to_15': [_10_to_15_per], '15_to_20': [_15_to_20_per],
                               'greater_than_20': [greater_than_20_per], 'date': [date]})

            # stock_off_low = stock_off_low.append(df, ignore_index=True)
            stock_off_low = pd.concat([stock_off_low, df], ignore_index=True)

        stock_off_low['less_than_5'] = round(stock_off_low['less_than_5'], 2)
        stock_off_low['5_to_10'] = round(stock_off_low['5_to_10'], 2)
        stock_off_low['10_to_15'] = round(stock_off_low['10_to_15'], 2)
        stock_off_low['15_to_20'] = round(stock_off_low['15_to_20'], 2)
        stock_off_low['greater_than_20'] = round(
            stock_off_low['greater_than_20'], 2)

        stock_off_low['date'] = pd.to_datetime(stock_off_low['date'])
        stock_off_low['date'] = stock_off_low['date'].dt.strftime("%Y-%m-%d")

        return stock_off_low

    def insert_off_high(self, stock_off_high, conn, cur):
        """ Insert Off-High data for stocks. """

        stock_off_high = stock_off_high[[
            'date', 'less_than_5', '5_to_10', '10_to_15', '15_to_20', 'greater_than_20']]

        exportfilename = "stock_off_high.csv"
        exportfile = open(exportfilename, "w")
        stock_off_high.to_csv(exportfile, header=True, index=False,
                              float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY dash_process.stock_off_high FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
        os.remove(exportfilename)

    def insert_off_low(self, stock_off_low, conn, cur):
        """ Insert Off-Low data for stocks. """

        stock_off_low = stock_off_low[[
            'date', 'less_than_5', '5_to_10', '10_to_15', '15_to_20', 'greater_than_20']]

        exportfilename = "stock_off_low.csv"
        exportfile = open(exportfilename, "w")
        stock_off_low.to_csv(exportfile, header=True, index=False,
                             float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY dash_process.stock_off_low FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
        os.remove(exportfilename)


def main(curr_date):
    """ Function to call the methods in StockOffHighLow in order to 
        run daily off-high/low process. 
    """

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    stock_off_high_low = StockOffHighLow()
    date = curr_date

    print("Starting stock off-high/low process for date:", date)

    print("\nGetting PRS for current date")
    prs = stock_off_high_low.get_prs(date,conn)

    if not(prs.empty):

        print("Calculating off-high for stocks")
        stock_off_high = stock_off_high_low.calc_off_high(prs)

        print("Calculating off-low for stocks")
        stock_off_low = stock_off_high_low.calc_off_low(prs)

        print("Inserting off-high data into DB")
        stock_off_high_low.insert_off_high(stock_off_high, conn, cur)

        print("Inserting off-low data into DB")
        stock_off_high_low.insert_off_low(stock_off_low, conn, cur)

    else:

        print("PRS empty for today:", date)
        raise ValueError('PRS data not found for date: ' +
                         date.strftime("%Y-%m-%d"))

    conn.close()
