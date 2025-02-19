# Python script to calculate performance values for indexes and store it in DB
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
from dateutil.relativedelta import relativedelta

class IndexPerformance:
    """ Contains methods to get OHLC for different indexes and calculate
        change values for these indexes for different time periods. 
        (1 day, 5 days, 10 days, 30 days, 60 days, 90 days, 6 months, 1 year, 
         2 years, 3 years, 5 years.)
    """

    def __init__(self):

        date = datetime.date.today()
        self.back_date = (datetime.date(date.year - 5, date.month, date.day))
        self.date = date

    def get_index_ohlc(self, curr_date,conn):
        """ Function to get OHLC for indexes for current date. """

        sql = 'SELECT * FROM public."IndexHistory" \
                WHERE "DATE" = \''+str(curr_date)+'\';'
        index_ohlc = sqlio.read_sql_query(sql, con=conn)

        return index_ohlc

    def get_index_ohlc_back(self, curr_date, conn):
        """ Function to get OHLC for indexes for backdate. """
        back_date = curr_date - relativedelta(years=5)
        sql = 'SELECT * FROM public."IndexHistory" \
               WHERE "DATE" >= \''+str(back_date)+'\' \
               AND "DATE" < \''+str(curr_date)+'\';'
        index_ohlc_back = pd.read_sql_query(sql, con=conn)
        return index_ohlc_back

    def get_index_change(self, index_ohlc, index_ohlc_back):
        """ Get change value for indexes for different time periods. 
            Input parameters: Current day's index OHLC, backdate index OHLC. 
        """

        index_performance = pd.DataFrame()

        index_list = index_ohlc['TICKER'].drop_duplicates().tolist()

        for index in index_list:

            per_index = index_ohlc[index_ohlc['TICKER'] == index]
            per_index_back = index_ohlc_back[index_ohlc_back['TICKER'] == index]

            df = self.calc_index_change(per_index, per_index_back, self.date)

            # index_performance = index_performance.append(df)
            index_performance = pd.concat([index_performance,df])

        return index_performance

    def calc_index_change(self, index_ohlc, index_ohlc_back, date):
        """ Function to calculate change values for a given index for different time 
            periods (1day, 5days, 10days, 30days, 60days, 90 days, 6month, 1year, 2year, 3year, 5year). 

            Change value is calculated based on 'Close' from index OHLC. 
            Change = ((Current day's Close - backdate's Close)/backdate's Close) * 100

            Input parameters: index OHLC for current date, index OHLC for backdate, current date. 
        """

        date = pd.to_datetime(date).date()

        one_day_back = (date + datetime.timedelta(-1))
        five_day_back = (date + datetime.timedelta(-5))
        ten_day_back = (date + datetime.timedelta(-10))
        thirty_day_back = (date + datetime.timedelta(-30))
        sixty_day_back = (date + datetime.timedelta(-60))
        ninety_day_back = (date + datetime.timedelta(-90))
        six_month_back = (date + datetime.timedelta(-183))
        one_year_back = (date + datetime.timedelta(-365))
        two_year_back = (date + datetime.timedelta(-730))
        three_year_back = (date + datetime.timedelta(-1095))
        five_year_back = (date + datetime.timedelta(-1825))

        for index, row in index_ohlc.iterrows():

            one_day_back_close = self.get_closest_date(
                index_ohlc_back, one_day_back)
            five_day_back_close = self.get_closest_date(
                index_ohlc_back, five_day_back)
            ten_day_back_close = self.get_closest_date(
                index_ohlc_back, ten_day_back)
            thirty_day_back_close = self.get_closest_date(
                index_ohlc_back, thirty_day_back)
            sixty_day_back_close = self.get_closest_date(
                index_ohlc_back, sixty_day_back)
            ninety_day_back_close = self.get_closest_date(
                index_ohlc_back, ninety_day_back)
            six_month_back_close = self.get_closest_date(
                index_ohlc_back, six_month_back)
            one_year_back_close = self.get_closest_date(
                index_ohlc_back, one_year_back)
            two_year_back_close = self.get_closest_date(
                index_ohlc_back, two_year_back)
            three_year_back_close = self.get_closest_date(
                index_ohlc_back, three_year_back)
            five_year_back_close = self.get_closest_date(
                index_ohlc_back, five_year_back)

            RR1 = ((row["CLOSE"]-one_day_back_close)/one_day_back_close) * \
                100 if one_day_back_close is not None else np.nan
            RR5 = ((row["CLOSE"]-five_day_back_close)/five_day_back_close) * \
                100 if five_day_back_close is not None else np.nan
            RR10 = ((row["CLOSE"]-ten_day_back_close)/ten_day_back_close) * \
                100 if ten_day_back_close is not None else np.nan
            RR30 = ((row["CLOSE"]-thirty_day_back_close)/thirty_day_back_close) * \
                100 if thirty_day_back_close is not None else np.nan
            RR60 = ((row["CLOSE"]-sixty_day_back_close)/sixty_day_back_close) * \
                100 if sixty_day_back_close is not None else np.nan
            RR90 = ((row["CLOSE"]-ninety_day_back_close)/ninety_day_back_close) * \
                100 if ninety_day_back_close is not None else np.nan
            RR6month = ((row["CLOSE"]-six_month_back_close)/six_month_back_close) * \
                100 if six_month_back_close is not None else np.nan
            RR1year = ((row["CLOSE"]-one_year_back_close)/one_year_back_close) * \
                100 if one_year_back_close is not None else np.nan
            RR2year = ((row["CLOSE"]-two_year_back_close)/two_year_back_close) * \
                100 if two_year_back_close is not None else np.nan
            RR3year = ((row["CLOSE"]-three_year_back_close)/three_year_back_close) * \
                100 if three_year_back_close is not None else np.nan
            RR5year = ((row["CLOSE"]-five_year_back_close)/five_year_back_close) * \
                100 if five_year_back_close is not None else np.nan

            index_name = index_ohlc['TICKER'].head(1).item()

            index_ohlc.loc[index, 'index_name'] = index_name
            index_ohlc.loc[index, 'date'] = date
            index_ohlc.loc[index, '1day'] = round(
                RR1, 2)if not math.isnan(RR1) else np.nan
            index_ohlc.loc[index, '5day'] = round(
                RR5, 2)if not math.isnan(RR5) else np.nan
            index_ohlc.loc[index, '10day'] = round(
                RR10, 2)if not math.isnan(RR10) else np.nan
            index_ohlc.loc[index, '30day'] = round(
                RR30, 2)if not math.isnan(RR30) else np.nan
            index_ohlc.loc[index, '60day'] = round(
                RR60, 2)if not math.isnan(RR60) else np.nan
            index_ohlc.loc[index, '90day'] = round(
                RR90, 2)if not math.isnan(RR90) else np.nan
            index_ohlc.loc[index, '6month'] = round(
                RR6month, 2)if not math.isnan(RR6month) else np.nan
            index_ohlc.loc[index, '1year'] = round(
                RR1year, 2)if not math.isnan(RR1year) else np.nan
            index_ohlc.loc[index, '2year'] = round(
                RR2year, 2)if not math.isnan(RR2year) else np.nan
            index_ohlc.loc[index, '3year'] = round(
                RR3year, 2)if not math.isnan(RR3year) else np.nan
            index_ohlc.loc[index, '5year'] = round(
                RR5year, 2)if not math.isnan(RR5year) else np.nan

        return index_ohlc

    def get_closest_date(self, index_ohlc_back, date):
        """ Function to get OHLC data for a particular index for the closest date for which data is 
            available in case data for input date isn't available. 

            Initially checks for input date. If data isn't present for the given backdate, it goes 
            one day back from that date to check for OHLC data. If data isn't present, it checks for 
            two days back from given date. If data isn't present, it moves one day forward from the given date. 
            If data isn't present, it goes three days back from given date to check for OHLC data. 
            Returns None otherwise. 

            Input parameters: index OHLC history, date.  
        """

        # Get OHLC for stock for dates closest to given date.
        curr_backdate = index_ohlc_back['DATE'] == date
        one_day_forward = index_ohlc_back['DATE'] == (
            date + datetime.timedelta(1))
        one_day_back = index_ohlc_back['DATE'] == (
            date + datetime.timedelta(-1))
        two_day_back = index_ohlc_back['DATE'] == (
            date + datetime.timedelta(-2))
        three_day_back = index_ohlc_back['DATE'] == (
            date + datetime.timedelta(-3))

        # Check for provided date, else check for nearby dates.
        if not (index_ohlc_back.loc[curr_backdate].empty):
            return index_ohlc_back.loc[curr_backdate, "CLOSE"].head(1).item()
        else:
            if not (index_ohlc_back.loc[one_day_back].empty):
                return index_ohlc_back.loc[one_day_back, "CLOSE"].head(1).item()
            else:
                if not (index_ohlc_back.loc[two_day_back].empty):
                    return index_ohlc_back.loc[two_day_back, "CLOSE"].head(1).item()
                else:
                    if not (index_ohlc_back[one_day_forward].empty):
                        return index_ohlc_back.loc[one_day_forward, "CLOSE"].head(1).item()
                    else:
                        if not (index_ohlc_back.loc[three_day_back].empty):
                            return index_ohlc_back.loc[three_day_back, "CLOSE"].head(1).item()
                        else:
                            return None

    def insert_index_change(self, index_performance, conn, cur):
        """ Insert Index Performance data into the DB. """

        stock_performance = index_performance[['index_name', '1day', '5day', '10day', '30day', '60day', '90day',
                                               '6month', '1year', '2year', '3year', '5year', 'date']]

        exportfilename = "index_performance.csv"
        exportfile = open(exportfilename, "w")
        stock_performance.to_csv(
            exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY dash_process.index_performance FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        f.close()
        os.remove(exportfilename)


def main(curr_date):
    """ Function to call the IndexPerformance methods in order to run 
        daily index change calculation process. 
    """

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    index_per = IndexPerformance()
    date = curr_date

    print("Starting index performance process for date:", date)

    print("\nGetting index OHLC data for current date")
    ohlc_current = index_per.get_index_ohlc(date,conn)

    if not(ohlc_current.empty):

        print("Getting backdate index OHLC data")
        ohlc_back = index_per.get_index_ohlc_back(date,conn)

        print("Calculating index change values")
        index_performance = index_per.get_index_change(ohlc_current, ohlc_back)

        print("Inserting index performance data into the DB")
        index_per.insert_index_change(index_performance, conn, cur)

    else:

        print("No index OHLC for today")
        raise ValueError(
            'Index OHLC data not found for date: ' + date.strftime("%Y-%m-%d"))

    conn.close()
