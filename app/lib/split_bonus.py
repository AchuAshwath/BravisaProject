# Script to run split/bonus process
import datetime
import requests
import os.path
import os
from utils.db_helper import DB_Helper
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
import utils.date_set as date_set
from utils.logs import insert_logs


def get_splits(date, conn):

    split_sql = 'SELECT * FROM public."Splits" WHERE "XSDate" = \''+date+'\' ;'
    split = sqlio.read_sql_query(split_sql, con=conn)

    return split


def get_bonus(date, conn):

    bonus_sql = 'SELECT * FROM public."Bonus" WHERE "XBDate" = \''+date+'\' ;'
    bonus = sqlio.read_sql_query(bonus_sql, con=conn)

    return bonus


def update_ohlc(split, bonus, conn, cur, date):
    if not(split.empty):

        update_sql = f'''UPDATE public."OHLC" ohlc
                        SET
                          "Open" = "Open" / split_val,
                          "High" = "High" / split_val,
                          "Low" = "Low" / split_val,
                          "Close" = "Close" / split_val,
                          "Volume" = "Volume" * split_val
                        FROM (SELECT "OldFaceValue"/"NewFaceValue" AS split_val, "CompanyCode", "XSDate"
                              FROM public."Splits"
                              WHERE "XSDate" = '{date}') AS splits
                        WHERE ohlc."CompanyCode" = splits."CompanyCode"
                          AND ohlc."Date" < splits."XSDate";'''

        cur.execute(update_sql)
        conn.commit()
    else:
        print("No split data for today")

    if not(bonus.empty):

        update_sql = f'''UPDATE public."OHLC" ohlc
                        SET
                          "Open" = "Open" / (1 + bonus_val),
                          "High" = "High" / (1 + bonus_val),
                          "Low" = "Low" / (1 + bonus_val),
                          "Close" = "Close" / (1 + bonus_val),
                          "Volume" = "Volume" * (1 + bonus_val)
                        FROM (SELECT "RatioOfferred"/"RatioExisting" AS bonus_val, "CompanyCode", "XBDate"
                              FROM public."Bonus"
                              WHERE "XBDate" = '{date}') AS bonus
                        WHERE ohlc."CompanyCode" = bonus."CompanyCode"
                          AND ohlc."Date" < bonus."XBDate";'''

        cur.execute(update_sql)
        conn.commit()
    else:
        print("No bonus data for today")

def log_split(split,  date, runtime, conn, cur):
    if not(split.empty):
        for index, row in split.iterrows():
            shareholding_sql = f"""SELECT sh.*
                            FROM public."ShareHolding" sh
                            JOIN (
                                SELECT *
                                FROM public."Splits"
                                WHERE "XSDate" = '{date}'
                            ) AS split ON sh."CompanyCode" = split."CompanyCode"
                            WHERE sh."SHPDate" = (
                                SELECT MAX("SHPDate")
                                FROM public."ShareHolding"
                                WHERE "CompanyCode" = split."CompanyCode"
                                AND "SHPDate" <= '{date}'
                            )
                            AND split."XSDate" > sh."SHPDate";"""
            shareholding = sqlio.read_sql_query(shareholding_sql, con=conn)
            
            # get the row of shareholding where CompanyCode = row["CompanyCode"]
            shareholding_row = shareholding.loc[shareholding["CompanyCode"] == row["CompanyCode"]]
            
            print(shareholding_row)
            if (shareholding_row.empty):
                print("No shareholding data for company code", row["CompanyCode"])
                LOGS= {
                    "log_date": date,
                    "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
                    "CompanyCode": row["CompanyCode"],
                    "split_value": row["OldFaceValue"]/row["NewFaceValue"],
                    "bonus_value": 0,
                    "OLD_OS": 0,
                    "NEW_OS": 0,
                    "runtime": runtime
                }
                print("Split log:", LOGS)
                insert_logs("split_bonus", [LOGS], conn, cur)
                
            else:
            
                LOGS= {
                    "log_date": date,
                    "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
                    "CompanyCode": row["CompanyCode"],
                    "split_value": row["OldFaceValue"]/row["NewFaceValue"],
                    "bonus_value": 0,
                    "OLD_OS": shareholding_row["Total"].values[0],
                    "NEW_OS": shareholding_row["Total"].values[0] * (row["OldFaceValue"]/row["NewFaceValue"]),
                    "runtime": runtime
                }
                print("Split log:", LOGS)
                            
                insert_logs("split_bonus", [LOGS], conn, cur)

def log_bonus( bonus, date, runtime, conn, cur):
    if not(bonus.empty):
        for index, row in bonus.iterrows():
            shareholding_sql = f"""SELECT sh.*
                            FROM public."ShareHolding" sh
                            JOIN (
                                SELECT *
                                FROM public."Bonus"
                                WHERE "XBDate" = '{date}'
                            ) AS bonus ON sh."CompanyCode" = bonus."CompanyCode"
                            WHERE sh."SHPDate" = (
                                SELECT MAX("SHPDate")
                                FROM public."ShareHolding"
                                WHERE "CompanyCode" = bonus."CompanyCode"
                                AND "SHPDate" <= '{date}'
                            )
                            AND bonus."XBDate" > sh."SHPDate";"""
            shareholding = sqlio.read_sql_query(shareholding_sql, con=conn)
            
            shareholding_row = shareholding.loc[shareholding["CompanyCode"] == row["CompanyCode"]]
            # print(shareholding_row)
            if (shareholding_row.empty):
                print("No shareholding data for company code", row["CompanyCode"])
                
                LOGS= {
                    "log_date": date,
                    "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
                    "CompanyCode": row["CompanyCode"],
                    "split_value": 0,
                    "bonus_value": 1+ (row["RatioOfferred"]/row["RatioExisting"]),
                    "OLD_OS": 0,
                    "NEW_OS": 0,
                    "runtime": runtime
                }
                print("Bonus log:", LOGS)
                insert_logs("split_bonus", [LOGS], conn, cur)
                
            else:
                
                LOGS= {
                    "log_date": date,
                    "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
                    "CompanyCode": row["CompanyCode"],
                    "split_value": 0,
                    "bonus_value": 1+ (row["RatioOfferred"]/row["RatioExisting"]),
                    "OLD_OS": shareholding_row["Total"].values[0],
                    "NEW_OS": shareholding_row["Total"].values[0] * (1+ (row["RatioOfferred"]/row["RatioExisting"])),
                    "runtime": runtime
                }
                
                insert_logs("split_bonus", [LOGS], conn, cur)
            

def update_shareholding(split, bonus, conn, cur, date, runtime):
    if not(split.empty):
        log_split(split, date, runtime, conn, cur)
        # print("Split data:", split)
        update_sql = f'''
        UPDATE public."ShareHolding" sh
        SET
            "Capital" = "Capital" * split_val,
            "Total" = "Total" * split_val,
            "FaceValue" = "FaceValue" / split_val,
            "NoOfShares" = "NoOfShares" * split_val,
            "Promoters" = "Promoters" * split_val,
            "Directors" = "Directors" * split_val,
            "SubsidiaryCompanies" = "SubsidiaryCompanies" * split_val,
            "OtherCompanies" = "OtherCompanies" * split_val,
            "ICICI" = "ICICI" * split_val,
            "UTI" = "UTI" * split_val,
            "IDBI" = "IDBI" * split_val,
            "GenInsuranceComp" = "GenInsuranceComp" * split_val,
            "LifeInsuranceComp" = "LifeInsuranceComp" * split_val,
            "StateFinCorps" = "StateFinCorps" * split_val,
            "InduFinCorpIndia" = "InduFinCorpIndia" * split_val,
            "ForeignNRI" = "ForeignNRI" * split_val,
            "ForeignCollaborators" = "ForeignCollaborators" * split_val,
            "ForeignOCB" = "ForeignOCB" * split_val,
            "ForeignOthers" = "ForeignOthers" * split_val,
            "ForeignInstitutions" = "ForeignInstitutions" * split_val,
            "ForeignIndustries" = "ForeignIndustries" * split_val,
            "StateGovt" = "StateGovt" * split_val,
            "CentralGovt" = "CentralGovt" * split_val,
            "GovtCompanies" = "GovtCompanies" * split_val,
            "GovtOthers" = "GovtOthers" * split_val,
            "Others" = "Others" * split_val,
            "NBanksMutualFunds" = "NBanksMutualFunds" * split_val,
            "HoldingCompanies" = "HoldingCompanies" * split_val,
            "GeneralPublic" = "GeneralPublic" * split_val,
            "Employees" = "Employees" * split_val,
            "FinancialInstitutions" = "FinancialInstitutions" * split_val,
            "ForeignPromoter" = "ForeignPromoter" * split_val,
            "GDR" = "GDR" * split_val,
            "PersonActingInConcert" = "PersonActingInConcert" * split_val
        FROM (SELECT "OldFaceValue"/"NewFaceValue" AS split_val, "CompanyCode", "XSDate" 
              FROM public."Splits" 
              WHERE "XSDate" = '{date}') AS split
        WHERE sh."CompanyCode" = split."CompanyCode"
          AND sh."SHPDate" = (SELECT MAX("SHPDate") 
                              FROM public."ShareHolding" 
                              WHERE "CompanyCode" = split."CompanyCode" 
                              AND "SHPDate" <= '{date}')
          AND split."XSDate" > sh."SHPDate"; 
        '''
        # print("Executing SQL:\n", update_sql)
        cur.execute(update_sql)
        conn.commit()
    else:
        print("No split data for today")

    if not(bonus.empty):
        log_bonus( bonus, date, runtime, conn, cur)
        # print("Bonus data:", bonus)
        update_sql = f'''
        UPDATE public."ShareHolding" sh
        SET
            "Capital" = "Capital" * (1 + bonus_val),
            "NoOfShares" = "NoOfShares" * (1 + bonus_val),
            "Promoters" = "Promoters" * (1 + bonus_val),
            "Directors" = "Directors" * (1 + bonus_val),
            "SubsidiaryCompanies" = "SubsidiaryCompanies" * (1 + bonus_val),
            "OtherCompanies" = "OtherCompanies" * (1 + bonus_val),
            "ICICI" = "ICICI" * (1 + bonus_val),
            "UTI" = "UTI" * (1 + bonus_val),
            "IDBI" = "IDBI" * (1 + bonus_val),
            "GenInsuranceComp" = "GenInsuranceComp" * (1 + bonus_val),
            "LifeInsuranceComp" = "LifeInsuranceComp" * (1 + bonus_val),
            "StateFinCorps" = "StateFinCorps" * (1 + bonus_val),
            "InduFinCorpIndia" = "InduFinCorpIndia" * (1 + bonus_val),
            "ForeignNRI" = "ForeignNRI" * (1 + bonus_val),
            "ForeignCollaborators" = "ForeignCollaborators" * (1 + bonus_val),
            "ForeignOCB" = "ForeignOCB" * (1 + bonus_val),
            "ForeignOthers" = "ForeignOthers" * (1 + bonus_val),
            "ForeignInstitutions" = "ForeignInstitutions" * (1 + bonus_val),
            "ForeignIndustries" = "ForeignIndustries" * (1 + bonus_val),
            "StateGovt" = "StateGovt" * (1 + bonus_val),
            "CentralGovt" = "CentralGovt" * (1 + bonus_val),
            "GovtCompanies" = "GovtCompanies" * (1 + bonus_val),
            "GovtOthers" = "GovtOthers" * (1 + bonus_val),
            "Others" = "Others" * (1 + bonus_val),
            "NBanksMutualFunds" = "NBanksMutualFunds" * (1 + bonus_val),
            "HoldingCompanies" = "HoldingCompanies" * (1 + bonus_val),
            "GeneralPublic" = "GeneralPublic" * (1 + bonus_val),
            "Employees" = "Employees" * (1 + bonus_val),
            "FinancialInstitutions" = "FinancialInstitutions" * (1 + bonus_val),
            "ForeignPromoter" = "ForeignPromoter" * (1 + bonus_val),
            "GDR" = "GDR" * (1 + bonus_val),
            "PersonActingInConcert" = "PersonActingInConcert" * (1 + bonus_val),
            "Total" = "Total" * (1 + bonus_val)
        FROM (SELECT "RatioOfferred"/"RatioExisting" AS bonus_val, "CompanyCode", "XBDate"
              FROM public."Bonus"
              WHERE "XBDate" = '{date}') AS bonus
        WHERE sh."CompanyCode" = bonus."CompanyCode"
          AND sh."SHPDate" = (SELECT MAX("SHPDate") 
                              FROM public."ShareHolding" 
                              WHERE "CompanyCode" = bonus."CompanyCode" 
                              AND "SHPDate" <= '{date}')
          AND bonus."XBDate" > sh."SHPDate"; 
        '''
        # print("Executing SQL:\n", update_sql)
        cur.execute(update_sql)
        conn.commit()
    else:
        print("No bonus data for today")


    

def main(curr_date):
    start_time = time.time()
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    curr_date = curr_date.strftime("%Y-%m-%d")
	
    print("Getting splits data for date", curr_date)
    split = get_splits(curr_date, conn)

    print("Getting bonus data for date", curr_date)
    bonus = get_bonus(curr_date, conn)

    print("Updating OHLC")
    update_ohlc(split, bonus, conn, cur, curr_date)

    end_time = time.time()
    runtime = end_time - start_time
    
    # log_split_bonus(split, bonus, curr_date, runtime, conn, cur)
    
    print("Updating Shareholding")
    update_shareholding(split, bonus, conn, cur, curr_date, runtime)
    
    conn.close()
