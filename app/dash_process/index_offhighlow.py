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

class IndexOffHighLow:
    """ Contains methods which gets Off-High/Low for stocks from PRS and calculates 
        percentage of stocks having Off-High/Low: greater than 20%, 15-20%, 10-15%, 
        5-10% and less than 5% belonging to a particular index. """

    def __init__(self):

        date = datetime.date.today()
        self.back_date = (datetime.date(
            date.year - 1, date.month, date.day)).strftime("%Y-%m-%d")
        self.date = date.strftime("%Y-%m-%d")


    def get_btt(self, conn, cur):

        btt_query = 'select "CompanyCode" from  public."BTTList" where "BTTDate" = (select max("BTTDate") from public."BTTList");'
        btt_data = sqlio.read_sql_query(btt_query, con=conn)

        return btt_data

    def get_prs(self, curr_date,conn, cur):

        prs_sql = 'SELECT "Off-High", "Off-Low", "Date", "CompanyCode" FROM "Reports"."PRS" where  \
                    "Date" = \''+str(curr_date)+'\' and "CompanyCode" is not null;'
        prs_data = sqlio.read_sql_query(prs_sql, con=conn)

        return prs_data

    def merge_btt_prs(self, btt_data, prs_data):

        btt_prs_data = pd.merge(
            btt_data, prs_data, on="CompanyCode", how="left")

        return btt_prs_data

    def get_indexnames(self, conn, cur):

        index_query = 'select "CompanyCode", "SectorIndexName", "SubSectorIndexName", "IndustryIndexName" from  \
                        public."IndustryList" where "GenDate" = (select max("GenDate") from public."IndustryList"); '
        index_data = sqlio.read_sql_query(index_query, con=conn)

        return index_data

    def merge_index_df(self, index_data, btt_prs_data):

        main_data = pd.merge(btt_prs_data, index_data,
                             on="CompanyCode", how="left")

        return main_data

    def industrylist(self, conn, cur):

        sql = 'select "IndexName" from "Reports"."IRS" where "GenDate" = (select max("GenDate") from "Reports"."IRS");'
        indus_data = sqlio.read_sql_query(sql, con=conn)

        return indus_data

    def sectorname(self, main_data, indus_data):

        sector = indus_data["IndexName"].drop_duplicates().tolist()

        sector_data = main_data.loc[main_data["SectorIndexName"].isin(sector)]
        sector_data = sector_data.rename(
            columns={"SectorIndexName": "IndexName"})

        # 1 = offhigh, 3 = date , 4 = indexname
        sector_data_offhigh = sector_data[['Off-High', 'Date', 'IndexName']]
        # 2 = offlow, 3 = date , 4 = indexname
        sector_data_offlow = sector_data[['Off-Low', 'Date', 'IndexName']]

        indexlist = sector_data["IndexName"].drop_duplicates().tolist()

        sector_data_high = self.calc_index_offhigh(
            sector_data_offhigh, indexlist)
        sector_data_low = self.calc_index_offlow(sector_data_offlow, indexlist)

        # print(sector_data_high)

        return sector_data_high, sector_data_low

    def subsectorname(self, main_data, indus_data):

        subsector = indus_data["IndexName"].drop_duplicates().tolist()

        subsector_data = main_data.loc[main_data["SubSectorIndexName"].isin(
            subsector)]
        subsector_data = subsector_data.rename(
            columns={"SubSectorIndexName": "IndexName"})

        # 1 = offhigh, 3 = date , 5=indexname
        subsector_data_offhigh = subsector_data[[
            'Off-High', 'Date', 'IndexName']]
        # 2 = offlow, 3 = date , 5=indexname
        subsector_data_offlow = subsector_data[[
            'Off-Low', 'Date', 'IndexName']]

        indexlist = subsector_data["IndexName"].drop_duplicates().tolist()

        subsector_data_high = self.calc_index_offhigh(
            subsector_data_offhigh, indexlist)
        subsector_data_low = self.calc_index_offlow(
            subsector_data_offlow, indexlist)

        return subsector_data_high, subsector_data_low

    def industryname(self, main_data, indus_data):

        industry = indus_data["IndexName"].drop_duplicates().tolist()

        industry_data = main_data.loc[main_data["IndustryIndexName"].isin(
            industry)]
        industry_data = industry_data.rename(
            columns={"IndustryIndexName": "IndexName"})

        # 1 = offhigh, 3 = date , 6=indexname
        industry_data_offhigh = industry_data[[
            'Off-High', 'Date', 'IndexName']]
        # 2 = offlow, 3 = date , 6=indexname
        industry_data_offlow = industry_data[['Off-Low', 'Date', 'IndexName']]

        indexlist = industry_data["IndexName"].drop_duplicates().tolist()

        industry_data_high = self.calc_index_offhigh(
            industry_data_offhigh, indexlist)
        industry_data_low = self.calc_index_offlow(
            industry_data_offlow, indexlist)

        return industry_data_high, industry_data_low
    
    def subindustryname(self, main_data, indus_data):
        industry = indus_data["IndexName"].drop_duplicates().tolist()

        industry_data = main_data.loc[main_data["SubIndustryIndexName"].isin(
            industry)]
        industry_data = industry_data.rename(
            columns={"SUbIndustryIndexName": "IndexName"})

        # 1 = offhigh, 3 = date , 6=indexname
        industry_data_offhigh = industry_data[[
            'Off-High', 'Date', 'IndexName']]
        # 2 = offlow, 3 = date , 6=indexname
        industry_data_offlow = industry_data[['Off-Low', 'Date', 'IndexName']]

        indexlist = industry_data["IndexName"].drop_duplicates().tolist()

        industry_data_high = self.calc_index_offhigh(
            industry_data_offhigh, indexlist)
        industry_data_low = self.calc_index_offlow(
            industry_data_offlow, indexlist)

        return industry_data_high, industry_data_low
        

    def calc_index_offhigh(self, df, indexlist):

        data = pd.DataFrame()
        OffHighdata = df.groupby(["IndexName", "Date"])

        for name, df in OffHighdata:
            df = df.reset_index(drop=True)
            df["Off-High"] = df["Off-High"].replace(np.nan, 0)
            totalsize = df["Off-High"].count()

            lessthan5size = df[df["Off-High"] < 5].count()["Off-High"]
            percentage_lessthan5 = (lessthan5size/totalsize)*100

            betwn5to10size = df[(df["Off-High"] >= 5) &
                                (df["Off-High"] < 10)].count()["Off-High"]
            percentagebetwn5to10 = (betwn5to10size/totalsize)*100

            betwn10to15size = df[(df["Off-High"] >= 10) &
                                 (df["Off-High"] < 15)].count()["Off-High"]
            percentagebetwn10to15 = (betwn10to15size/totalsize)*100

            betwn15to20size = df[(df["Off-High"] >= 15) &
                                 (df["Off-High"] < 20)].count()["Off-High"]
            percentagebetwn15to20 = (betwn15to20size/totalsize)*100

            greaterthan20size = df[df["Off-High"] >= 20].count()["Off-High"]
            percentage_greaterthan20 = (greaterthan20size/totalsize)*100

            index = df["IndexName"].drop_duplicates().item()
            date = df["Date"].drop_duplicates().item()

            percentdata = pd.DataFrame({"date": [date], "indexname": [index], "percentage_lessthan5":
                                        [percentage_lessthan5], "percentagebetwn5to10": [percentagebetwn5to10],
                                        "percentagebetwn10to15": [percentagebetwn10to15], "percentagebetwn15to20": [percentagebetwn15to20],
                                        "percentage_greaterthan20": [percentage_greaterthan20]})

            # data = data.append(percentdata, ignore_index=True)
            data = pd.concat([data, percentdata], ignore_index=True)

        data['percentage_lessthan5'] = round(data['percentage_lessthan5'], 2)
        data['percentagebetwn5to10'] = round(data['percentagebetwn5to10'], 2)
        data['percentagebetwn10to15'] = round(data['percentagebetwn10to15'], 2)
        data['percentagebetwn15to20'] = round(data['percentagebetwn15to20'], 2)
        data['percentage_greaterthan20'] = round(
            data['percentage_greaterthan20'], 2)

        data["date"] = pd.to_datetime(data["date"])
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")

        return data

    def calc_index_offlow(self, df, indexlist):

        data = pd.DataFrame()
        OffLowdata = df.groupby(["IndexName", "Date"])

        for name, df in OffLowdata:
            df = df.reset_index(drop=True)
            df["Off-Low"] = df["Off-Low"].replace(np.nan, 0)
            totalsize = df["Off-Low"].count()

            lessthan5size = df[df["Off-Low"] < 5].count()["Off-Low"]
            percentage_lessthan5 = (lessthan5size/totalsize)*100

            betwn5to10size = df[(df["Off-Low"] >= 5) &
                                (df["Off-Low"] < 10)].count()["Off-Low"]
            percentagebetwn5to10 = (betwn5to10size/totalsize)*100

            betwn10to15size = df[(df["Off-Low"] >= 10) &
                                 (df["Off-Low"] < 15)].count()["Off-Low"]
            percentagebetwn10to15 = (betwn10to15size/totalsize)*100

            betwn15to20size = df[(df["Off-Low"] >= 15) &
                                 (df["Off-Low"] < 20)].count()["Off-Low"]
            percentagebetwn15to20 = (betwn15to20size/totalsize)*100

            greaterthan20size = df[df["Off-Low"] >= 20].count()["Off-Low"]
            percentage_greaterthan20 = (greaterthan20size/totalsize)*100

            index = df["IndexName"].drop_duplicates().item()
            date = df["Date"].drop_duplicates().item()

            percentdata = pd.DataFrame({"date": [date], "indexname": [index], "percentage_lessthan5":
                                        [percentage_lessthan5], "percentagebetwn5to10": [percentagebetwn5to10],
                                        "percentagebetwn10to15": [percentagebetwn10to15], "percentagebetwn15to20": [percentagebetwn15to20],
                                        "percentage_greaterthan20": [percentage_greaterthan20]})

            data = pd.concat([data, percentdata], ignore_index=True)
            # data = data.append(percentdata, ignore_index=True)

        data['percentage_lessthan5'] = round(data['percentage_lessthan5'], 2)
        data['percentagebetwn5to10'] = round(data['percentagebetwn5to10'], 2)
        data['percentagebetwn10to15'] = round(data['percentagebetwn10to15'], 2)
        data['percentagebetwn15to20'] = round(data['percentagebetwn15to20'], 2)
        data['percentage_greaterthan20'] = round(
            data['percentage_greaterthan20'], 2)

        data["date"] = pd.to_datetime(data["date"])
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")
        return data

    def concate_offhigh(self, sector_data_high, subsector_data_high, industry_data_high):

        df = pd.concat(
            [sector_data_high, subsector_data_high, industry_data_high])
        return df

    def concate_offlow(self, sector_data_low, subsector_data_low, industry_data_low):

        df = pd.concat(
            [sector_data_low, subsector_data_low, industry_data_low])
        return df

    def insert_offhighcal(self, df, conn, cur):

        df = df[['date', 'indexname', 'percentage_lessthan5', 'percentagebetwn5to10',
                 'percentagebetwn10to15', 'percentagebetwn15to20', 'percentage_greaterthan20']]

        exportfilename = "df_high_cal.csv"
        exportfile = open(exportfilename, "w")
        df.to_csv(exportfile, header=True, index=False,
                  float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
                    COPY  dash_process.index_off_high FROM stdin WITH CSV HEADER
                    DELIMITER as ','
                 """
        with open(exportfilename, 'r') as f:

            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)

    def insert_offlowcal(self, df, conn, cur):

        df = df[['date', 'indexname', 'percentage_lessthan5', 'percentagebetwn5to10',
                 'percentagebetwn10to15', 'percentagebetwn15to20', 'percentage_greaterthan20']]

        exportfilename = "df_low_cal.csv"
        exportfile = open(exportfilename, "w")
        df.to_csv(exportfile, header=True, index=False,
                  float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
                    COPY  dash_process.index_off_low FROM stdin WITH CSV HEADER
                    DELIMITER as ','
                 """
        with open(exportfilename, 'r') as f:

            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)


def main(curr_date, conn, cur):

    index = IndexOffHighLow()

    print("prs data for 1 year")
    prs_data = index.get_prs(curr_date, conn, cur)

    print("index data contain sector, sun and ind")
    index_data = index.get_indexnames(conn, cur)

    print("Data which we get it is main")
    maindata = index.merge_index_df(index_data, prs_data)

    print("Industry Data with IndexNames")
    indus_data = index.industrylist(conn, cur)

    print("sector Data with SectorIndexNames")
    sector_data_high, sector_data_low = index.sectorname(maindata, indus_data)

    print("Subsector Data with SubSectorIndexNames")
    subsector_data_high, subsector_data_low = index.subsectorname(
        maindata, indus_data)

    print("industry Data with IndustryIndexNames")
    industry_data_high, industry_data_low = index.industryname(
        maindata, indus_data)
    
    print("industry Data with SubIndustryIndexNames")
    industry_data_high, industry_data_low = index.subindustryname(
        maindata, indus_data)
    
    print("Concating all the data for sec, sub , indus OFFHIGH")
    concat_data_offhigh = index.concate_offhigh(
        sector_data_high, subsector_data_high, industry_data_high)

    print("Concating all the data for sec, sub , indus OFFLOW")
    concat_data_offlow = index.concate_offlow(
        sector_data_low, subsector_data_low, industry_data_low)

    print("Inserting the offhigh cal data to DB")
    index.insert_offhighcal(concat_data_offhigh, conn, cur)

    print("Inserting the offlow cal data to DB")
    index.insert_offlowcal(concat_data_offlow, conn, cur)
