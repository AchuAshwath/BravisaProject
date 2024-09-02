# Script to calculated combined rank from all 5 reports
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
import codecs
from datetime import timedelta
from functools import reduce
import utils.date_set as date_set



class CombinedRank():
    """ Calculating combined rank
        Fetching the data from all RS table and backgroundinfo,
        to calculate combine drank for current date.
    """

    def __init__(self):
        pass

    def get_all_rs(self, date, conn):
        """ Fetching the data of all RS Table

        Operation:
            Fetch the data from PRS,EPS,SMR, FRS-MFRank, and IndustryList,
            for current date. And merge all the data based on CompanyCode.

        Return:
            Merge data of All RS table.
        """

        prs_sql = 'SELECT "CompanyCode", "Combined Strength", "Value Average" from "Reports"."PRS" where "Date" = \''+date+'\';'
        prs = sqlio.read_sql_query(prs_sql, con=conn)

        if not(prs.empty):

            ers_sql = 'SELECT "CompanyCode", "Ranking" from "Reports"."EPS" where "EPSDate" = \''+date+'\';'
            ers = sqlio.read_sql_query(ers_sql, con=conn)

            rrs_sql = 'SELECT "CompanyCode", "SMR Rank" from "Reports"."SMR" where "SMRDate" = \''+date+'\';'
            rrs = sqlio.read_sql_query(rrs_sql, con=conn)

            frs_sql = 'SELECT "CompanyCode", "MFRank" from "Reports"."FRS-MFRank" where "Date" = \''+date+'\';'
            frs = sqlio.read_sql_query(frs_sql, con=conn)

            irs_sql = 'SELECT distinct on(il."CompanyCode") il."CompanyCode", irs."Rank" from public."IndustryList" il \
					left join "Reports"."IRS" irs \
					on il."IndustryIndexName" = irs."IndexName" \
					where il."GenDate" =  \'' + date + '\' \
					and irs."GenDate" = \'' + date + '\' ; '
            irs = sqlio.read_sql_query(irs_sql, con=conn)

            print("PRS:", prs.dtypes)
            print("ERS:",ers.dtypes)
            print("RRS:", rrs.dtypes)
            print("FRS:", frs.dtypes)
            print("IRS:", irs.dtypes)
            StockRSMerge = [prs, ers, rrs, frs, irs]

            StockRS = reduce(lambda left, right: pd.merge(
                left, right, left_on='CompanyCode', right_on='CompanyCode', how='left'), StockRSMerge)

        else:

            StockRS = pd.DataFrame()

        return StockRS

    def calc_combined_rank(self, StockRS, conn):
        """ Calculate Combi Rank

        Args:
            StockRS = merge data of all RS Report.

        Return:
            Combined Rank.
            Combi Rank = PRSRank + EPSRank + RRSRank + FRSRank + IRSRank / 5.
        """

        StockRS = StockRS.rename(columns={'Combined Strength': 'PRSRank', 'Ranking': 'ERSRank', 'SMR Rank': 'RRSRank',
                                          'MFRank': 'FRSRank', 'Rank': 'IRSRank'})

        StockRS.fillna(value=0, inplace=True)

        StockRS['CombinedRS'] = (StockRS['PRSRank'] + StockRS['ERSRank'] +
                                 StockRS['RRSRank'] + StockRS['FRSRank'] + StockRS['IRSRank'])/5

        StockRS['Rank'] = StockRS["CombinedRS"].rank(ascending=False)

        return StockRS

    def merge_background(self, StockRS, conn):
        """ merge combi rank and background info

        Args:
            StockRS = data of calculated combined rank.

        Return:
            Fetch the data from BackgroundInfo table and merge it with input data.
            merge data of backgroundinfo and input data.
        """

        background_sql = 'SELECT * FROM public."BackgroundInfo" ; '
        background_info = sqlio.read_sql_query(background_sql, con=conn)

        StockRS = pd.merge(StockRS, background_info[[
            'CompanyCode', 'CompanyName', 'NSECode', 'BSECode']], left_on='CompanyCode', right_on='CompanyCode', how='left')

        return StockRS

    def insert_combined_rs(self, StockRS, conn, cur, date):
        """ insert the combined rank data into database

        Args:
            StockRS = merge data of backgroundinfo and input data.

        Operation:
            Export the data into combined_rank.csv file and.
            insert into CombinedRS table.
        """

        StockRS['GenDate'] = date

        StockRS["BSECode"].fillna(-1, inplace=True)
        StockRS = StockRS.astype({"BSECode": int})
        StockRS = StockRS.astype({"BSECode": str})
        StockRS["BSECode"] = StockRS["BSECode"].replace('-1', np.nan)

        StockRS = StockRS.astype({"Rank": int})

        StockRS = StockRS[['CompanyCode', 'CompanyName', 'NSECode', 'BSECode', 'PRSRank', 'ERSRank', 'RRSRank', 'FRSRank', 'IRSRank',
                           'CombinedRS', 'Rank', 'Value Average', 'GenDate']]

        exportfilename = "combined_rank.csv"
        exportfile = open(exportfilename, "w", encoding='utf-8')
        StockRS.to_csv(exportfile, header=True, index=False,
                       float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
				COPY "Reports"."CombinedRS" FROM stdin WITH CSV HEADER
				DELIMITER as ','
				"""
        with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)

    def combi_rank(self, conn, cur, date):
        """ Calculating Combi rank for current date

        Operation:
            Fetch the data of 5 ranks for stocks, Calculated combined rank
            and backgroundinfo to calculate combi rank
        """

        print("\nGetting all 5 ranks for stocks")
        stock_rs = self.get_all_rs(date, conn)

        if not(stock_rs.empty):

            print("Calculating combined rank")
            stock_rs = self.calc_combined_rank(stock_rs, conn)

            print("Merging with background info")
            stock_rs = self.merge_background(stock_rs, conn)

            print("Inserting into Table")
            self.insert_combined_rs(stock_rs, conn, cur, date)

        else:

            print("No data for date:", date)

    def history_rank(self, conn, cur):
        """ Generate history for combi rank

        Operation:
            Fetch the data of combined rank for current date.
            and generate history for Combi rank.
       """

        start_date = datetime.date(2020, 8, 1)
        end_date = datetime.date(2020, 8, 31)

        while(end_date >= start_date):

            gen_date = start_date.strftime("%Y-%m-%d")
            print("Calculating combined rank for date:", gen_date)
            self.combi_rank(conn, cur, gen_date)

            start_date = start_date+datetime.timedelta(1)

    def current_rank(self, curr_date, conn, cur):
        """ Calculating combi rank for current date

        Operation:
            Fetching the data of calculating combined
            rank for current date.
        """

        curr_date = curr_date.strftime("%Y-%m-%d")

        print("Calculating combined rank for today:", curr_date)
        self.combi_rank(conn, cur, curr_date)
