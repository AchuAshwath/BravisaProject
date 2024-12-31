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
from utils.db_helper import DB_Helper


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

# def log_split(split,  date, runtime, conn, cur):
#     if not(split.empty):
#         for index, row in split.iterrows():
#             shareholding_sql = f"""SELECT sh.*
#                             FROM public."ShareHolding" sh
#                             JOIN (
#                                 SELECT *
#                                 FROM public."Splits"
#                                 WHERE "XSDate" = '{date}'
#                             ) AS split ON sh."CompanyCode" = split."CompanyCode"
#                             WHERE sh."SHPDate" = (
#                                 SELECT MAX("SHPDate")
#                                 FROM public."ShareHolding"
#                                 WHERE "CompanyCode" = split."CompanyCode"
#                                 AND "SHPDate" <= '{date}'
#                             )
#                             AND split."XSDate" > sh."SHPDate";"""
#             shareholding = sqlio.read_sql_query(shareholding_sql, con=conn)
            
#             # get the row of shareholding where CompanyCode = row["CompanyCode"]
#             shareholding_row = shareholding.loc[shareholding["CompanyCode"] == row["CompanyCode"]]
            
#             print(shareholding_row)
#             if (shareholding_row.empty):
#                 print("No shareholding data for company code", row["CompanyCode"])
#                 LOGS= {
#                     "log_date": date,
#                     "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
#                     "CompanyCode": row["CompanyCode"],
#                     "split_value": row["OldFaceValue"]/row["NewFaceValue"],
#                     "bonus_value": 0,
#                     "OLD_OS": 0,
#                     "NEW_OS": 0,
#                     "runtime": runtime
#                 }
#                 print("Split log:", LOGS)
#                 insert_logs("split_bonus", [LOGS], conn, cur)
                
#             else:
            
#                 LOGS= {
#                     "log_date": date,
#                     "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
#                     "CompanyCode": row["CompanyCode"],
#                     "split_value": row["OldFaceValue"]/row["NewFaceValue"],
#                     "bonus_value": 0,
#                     "OLD_OS": shareholding_row["Total"].values[0],
#                     "NEW_OS": shareholding_row["Total"].values[0] * (row["OldFaceValue"]/row["NewFaceValue"]),
#                     "runtime": runtime
#                 }
#                 print("Split log:", LOGS)
                            
#                 insert_logs("split_bonus", [LOGS], conn, cur)

# def log_bonus( bonus, date, runtime, conn, cur):
#     if not(bonus.empty):
#         for index, row in bonus.iterrows():
#             shareholding_sql = f"""SELECT sh.*
#                             FROM public."ShareHolding" sh
#                             JOIN (
#                                 SELECT *
#                                 FROM public."Bonus"
#                                 WHERE "XBDate" = '{date}'
#                             ) AS bonus ON sh."CompanyCode" = bonus."CompanyCode"
#                             WHERE sh."SHPDate" = (
#                                 SELECT MAX("SHPDate")
#                                 FROM public."ShareHolding"
#                                 WHERE "CompanyCode" = bonus."CompanyCode"
#                                 AND "SHPDate" <= '{date}'
#                             )
#                             AND bonus."XBDate" > sh."SHPDate";"""
#             shareholding = sqlio.read_sql_query(shareholding_sql, con=conn)
            
#             shareholding_row = shareholding.loc[shareholding["CompanyCode"] == row["CompanyCode"]]
#             # print(shareholding_row)
#             if (shareholding_row.empty):
#                 print("No shareholding data for company code", row["CompanyCode"])
                
#                 LOGS= {
#                     "log_date": date,
#                     "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
#                     "CompanyCode": row["CompanyCode"],
#                     "split_value": 0,
#                     "bonus_value": 1+ (row["RatioOfferred"]/row["RatioExisting"]),
#                     "OLD_OS": 0,
#                     "NEW_OS": 0,
#                     "runtime": runtime
#                 }
#                 print("Bonus log:", LOGS)
#                 insert_logs("split_bonus", [LOGS], conn, cur)
                
#             else:
                
#                 LOGS= {
#                     "log_date": date,
#                     "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
#                     "CompanyCode": row["CompanyCode"],
#                     "split_value": 0,
#                     "bonus_value": 1+ (row["RatioOfferred"]/row["RatioExisting"]),
#                     "OLD_OS": shareholding_row["Total"].values[0],
#                     "NEW_OS": shareholding_row["Total"].values[0] * (1+ (row["RatioOfferred"]/row["RatioExisting"])),
#                     "runtime": runtime
#                 }
                
#                 insert_logs("split_bonus", [LOGS], conn, cur)
            

def update_shareholding(split, bonus, conn, cur, date, runtime):
    if not(split.empty):
        # log_split(split, date, runtime, conn, cur)
        for index, row in split.iterrows():
            share_holding_sql = """
                          SELECT DISTINCT ON ("CompanyCode") * FROM public."ShareHolding" 
                          WHERE "CompanyCode" = %s
                          AND "SHPDate" <= %s
                          order by "CompanyCode", "SHPDate" desc;
                          """
            share_holding = sqlio.read_sql_query(share_holding_sql, con=conn, params=[row["CompanyCode"], date])
            print("CompanyCode and date", row["CompanyCode"], date)
            split_val = row["OldFaceValue"]/row["NewFaceValue"]
            
            if not(share_holding.empty):
                update_df = share_holding.copy()

                update_df['CompanyCode'] = row["CompanyCode"]
                update_df['SHPDate'] = date
                update_df['Capital'] = share_holding['Capital'] * split_val
                update_df['FaceValue'] = share_holding['FaceValue'] / split_val
                update_df['NoOfShares'] = share_holding['NoOfShares'] * split_val
                update_df['Promoters'] = share_holding['Promoters'] * split_val
                update_df['Directors'] = share_holding['Directors'] * split_val
                update_df['SubsidiaryCompanies'] = share_holding['SubsidiaryCompanies'] * split_val
                update_df['OtherCompanies'] = share_holding['OtherCompanies'] * split_val
                update_df['ICICI'] = share_holding['ICICI'] * split_val
                update_df['UTI'] = share_holding['UTI'] * split_val
                update_df['IDBI'] = share_holding['IDBI'] * split_val
                update_df['GenInsuranceComp'] = share_holding['GenInsuranceComp'] * split_val
                update_df['LifeInsuranceComp'] = share_holding['LifeInsuranceComp'] * split_val
                update_df['StateFinCorps'] = share_holding['StateFinCorps'] * split_val
                update_df['InduFinCorpIndia'] = share_holding['InduFinCorpIndia'] * split_val
                update_df['ForeignNRI'] = share_holding['ForeignNRI'] * split_val
                update_df['ForeignCollaborators'] = share_holding['ForeignCollaborators'] * split_val
                update_df['ForeignOCB'] = share_holding['ForeignOCB'] * split_val
                update_df['ForeignOthers'] = share_holding['ForeignOthers'] * split_val
                update_df['ForeignInstitutions'] = share_holding['ForeignInstitutions'] * split_val
                update_df['ForeignIndustries'] = share_holding['ForeignIndustries'] * split_val
                update_df['StateGovt'] = share_holding['StateGovt'] * split_val
                update_df['CentralGovt'] = share_holding['CentralGovt'] * split_val
                update_df['GovtCompanies'] = share_holding['GovtCompanies'] * split_val
                update_df['GovtOthers'] = share_holding['GovtOthers'] * split_val
                update_df['Others'] = share_holding['Others'] * split_val
                update_df['NBanksMutualFunds'] = share_holding['NBanksMutualFunds'] * split_val
                update_df['HoldingCompanies'] = share_holding['HoldingCompanies'] * split_val
                update_df['GeneralPublic'] = share_holding['GeneralPublic'] * split_val
                update_df['Employees'] = share_holding['Employees'] * split_val
                update_df['FinancialInstitutions'] = share_holding['FinancialInstitutions'] * split_val
                update_df['ForeignPromoter'] = share_holding['ForeignPromoter'] * split_val
                update_df['GDR'] = share_holding['GDR'] * split_val
                update_df['PersonActingInConcert'] = share_holding['PersonActingInConcert'] * split_val
                update_df['Total'] = share_holding['Total'] * split_val
                update_df['ModifiedDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                print(update_df)
                
                DB_Helper.insert_df(update_df, "public", "ShareHolding", conn, cur)
                
                LOGS = {
                    "log_date": date,
                    "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
                    "CompanyCode": row["CompanyCode"],
                    "split_value": row["OldFaceValue"]/row["NewFaceValue"],
                    "bonus_value": 0,
                    "OLD_OS": share_holding["Total"].values[0],
                    "NEW_OS": update_df["Total"].values[0],
                    "runtime": runtime
                }
                
                insert_logs("split_bonus", [LOGS], conn, cur)
                          
                
            
        # # print("Split data:", split)
        # update_sql = f'''
        # UPDATE public."ShareHolding" sh
        # SET
        #     "Capital" = "Capital" * split_val,
        #     "Total" = "Total" * split_val,
        #     "FaceValue" = "FaceValue" / split_val,
        #     "NoOfShares" = "NoOfShares" * split_val,
        #     "Promoters" = "Promoters" * split_val,
        #     "Directors" = "Directors" * split_val,
        #     "SubsidiaryCompanies" = "SubsidiaryCompanies" * split_val,
        #     "OtherCompanies" = "OtherCompanies" * split_val,
        #     "ICICI" = "ICICI" * split_val,
        #     "UTI" = "UTI" * split_val,
        #     "IDBI" = "IDBI" * split_val,
        #     "GenInsuranceComp" = "GenInsuranceComp" * split_val,
        #     "LifeInsuranceComp" = "LifeInsuranceComp" * split_val,
        #     "StateFinCorps" = "StateFinCorps" * split_val,
        #     "InduFinCorpIndia" = "InduFinCorpIndia" * split_val,
        #     "ForeignNRI" = "ForeignNRI" * split_val,
        #     "ForeignCollaborators" = "ForeignCollaborators" * split_val,
        #     "ForeignOCB" = "ForeignOCB" * split_val,
        #     "ForeignOthers" = "ForeignOthers" * split_val,
        #     "ForeignInstitutions" = "ForeignInstitutions" * split_val,
        #     "ForeignIndustries" = "ForeignIndustries" * split_val,
        #     "StateGovt" = "StateGovt" * split_val,
        #     "CentralGovt" = "CentralGovt" * split_val,
        #     "GovtCompanies" = "GovtCompanies" * split_val,
        #     "GovtOthers" = "GovtOthers" * split_val,
        #     "Others" = "Others" * split_val,
        #     "NBanksMutualFunds" = "NBanksMutualFunds" * split_val,
        #     "HoldingCompanies" = "HoldingCompanies" * split_val,
        #     "GeneralPublic" = "GeneralPublic" * split_val,
        #     "Employees" = "Employees" * split_val,
        #     "FinancialInstitutions" = "FinancialInstitutions" * split_val,
        #     "ForeignPromoter" = "ForeignPromoter" * split_val,
        #     "GDR" = "GDR" * split_val,
        #     "PersonActingInConcert" = "PersonActingInConcert" * split_val
        # FROM (SELECT "OldFaceValue"/"NewFaceValue" AS split_val, "CompanyCode", "XSDate" 
        #       FROM public."Splits" 
        #       WHERE "XSDate" = '{date}') AS split
        # WHERE sh."CompanyCode" = split."CompanyCode"
        #   AND sh."SHPDate" = (SELECT MAX("SHPDate") 
        #                       FROM public."ShareHolding" 
        #                       WHERE "CompanyCode" = split."CompanyCode" 
        #                       AND "SHPDate" <= '{date}')
        #   AND split."XSDate" > sh."SHPDate"; 
        # '''
        # # print("Executing SQL:\n", update_sql)
        # cur.execute(update_sql)
        # conn.commit()
    else:
        print("No split data for today")

    if not(bonus.empty):
        # log_bonus( bonus, date, runtime, conn, cur)
        # print("Bonus data:", bonus)
        for index, row in bonus.iterrows():
            companycode = row["CompanyCode"]
            share_holding_sql = """
                          SELECT DISTINCT ON ("CompanyCode") * FROM public."ShareHolding" 
                          WHERE "CompanyCode" = %s
                          AND "SHPDate" <= %s
                          order by "CompanyCode", "SHPDate" desc;
                          """
            share_holding = sqlio.read_sql_query(share_holding_sql, con=conn, params=[companycode, date])
            
            print("CompanyCode and date", companycode, date)
            
            bonus_val = 1 + (row["RatioOfferred"]/row["RatioExisting"])
            
            if not(share_holding.empty):
                update_df = share_holding.copy()

                update_df['CompanyCode'] = companycode
                update_df['SHPDate'] = date
                update_df['Capital'] = share_holding['Capital'] * bonus_val
                update_df['FaceValue'] = share_holding['FaceValue'] * bonus_val
                update_df['NoOfShares'] = share_holding['NoOfShares'] * bonus_val
                update_df['Promoters'] = share_holding['Promoters'] * bonus_val
                update_df['Directors'] = share_holding['Directors'] * bonus_val
                update_df['SubsidiaryCompanies'] = share_holding['SubsidiaryCompanies'] * bonus_val
                update_df['OtherCompanies'] = share_holding['OtherCompanies'] * bonus_val
                update_df['ICICI'] = share_holding['ICICI'] * bonus_val
                update_df['UTI'] = share_holding['UTI'] * bonus_val
                update_df['IDBI'] = share_holding['IDBI'] * bonus_val
                update_df['GenInsuranceComp'] = share_holding['GenInsuranceComp'] * bonus_val
                update_df['LifeInsuranceComp'] = share_holding['LifeInsuranceComp'] * bonus_val
                update_df['StateFinCorps'] = share_holding['StateFinCorps'] * bonus_val
                update_df['InduFinCorpIndia'] = share_holding['InduFinCorpIndia'] * bonus_val
                update_df['ForeignNRI'] = share_holding['ForeignNRI'] * bonus_val
                update_df['ForeignCollaborators'] = share_holding['ForeignCollaborators'] * bonus_val
                update_df['ForeignOCB'] = share_holding['ForeignOCB'] * bonus_val
                update_df['ForeignOthers'] = share_holding['ForeignOthers'] * bonus_val
                update_df['ForeignInstitutions'] = share_holding['ForeignInstitutions'] * bonus_val
                update_df['ForeignIndustries'] = share_holding['ForeignIndustries'] * bonus_val
                update_df['StateGovt'] = share_holding['StateGovt'] * bonus_val
                update_df['CentralGovt'] = share_holding['CentralGovt'] * bonus_val
                update_df['GovtCompanies'] = share_holding['GovtCompanies'] * bonus_val
                update_df['GovtOthers'] = share_holding['GovtOthers'] * bonus_val
                update_df['Others'] = share_holding['Others'] * bonus_val
                update_df['NBanksMutualFunds'] = share_holding['NBanksMutualFunds'] * bonus_val
                update_df['HoldingCompanies'] = share_holding['HoldingCompanies'] * bonus_val
                update_df['GeneralPublic'] = share_holding['GeneralPublic'] * bonus_val
                update_df['Employees'] = share_holding['Employees'] * bonus_val
                update_df['FinancialInstitutions'] = share_holding['FinancialInstitutions'] * bonus_val
                update_df['ForeignPromoter'] = share_holding['ForeignPromoter'] * bonus_val
                update_df['GDR'] = share_holding['GDR'] * bonus_val
                update_df['PersonActingInConcert'] = share_holding['PersonActingInConcert'] * bonus_val
                update_df['Total'] = share_holding['Total'] * bonus_val
                update_df['ModifiedDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                DB_Helper.insert_df(update_df, "public", "ShareHolding", conn, cur)
                
                LOGS = {
                    "log_date": date,
                    "log_time": datetime.datetime.now().strftime("%H:%M:%S"),
                    "CompanyCode": companycode,
                    "split_value": 0,
                    "bonus_value": 1+ (row["RatioOfferred"]/row["RatioExisting"]),
                    "OLD_OS": share_holding["Total"].values[0],
                    "NEW_OS": update_df["Total"].values[0],
                    "runtime": runtime
                }
                
                insert_logs("split_bonus", [LOGS], conn, cur)
                
                
            
        # update_sql = f'''
        # UPDATE public."ShareHolding" sh
        # SET
        #     "Capital" = "Capital" * (1 + bonus_val),
        #     "NoOfShares" = "NoOfShares" * (1 + bonus_val),
        #     "Promoters" = "Promoters" * (1 + bonus_val),
        #     "Directors" = "Directors" * (1 + bonus_val),
        #     "SubsidiaryCompanies" = "SubsidiaryCompanies" * (1 + bonus_val),
        #     "OtherCompanies" = "OtherCompanies" * (1 + bonus_val),
        #     "ICICI" = "ICICI" * (1 + bonus_val),
        #     "UTI" = "UTI" * (1 + bonus_val),
        #     "IDBI" = "IDBI" * (1 + bonus_val),
        #     "GenInsuranceComp" = "GenInsuranceComp" * (1 + bonus_val),
        #     "LifeInsuranceComp" = "LifeInsuranceComp" * (1 + bonus_val),
        #     "StateFinCorps" = "StateFinCorps" * (1 + bonus_val),
        #     "InduFinCorpIndia" = "InduFinCorpIndia" * (1 + bonus_val),
        #     "ForeignNRI" = "ForeignNRI" * (1 + bonus_val),
        #     "ForeignCollaborators" = "ForeignCollaborators" * (1 + bonus_val),
        #     "ForeignOCB" = "ForeignOCB" * (1 + bonus_val),
        #     "ForeignOthers" = "ForeignOthers" * (1 + bonus_val),
        #     "ForeignInstitutions" = "ForeignInstitutions" * (1 + bonus_val),
        #     "ForeignIndustries" = "ForeignIndustries" * (1 + bonus_val),
        #     "StateGovt" = "StateGovt" * (1 + bonus_val),
        #     "CentralGovt" = "CentralGovt" * (1 + bonus_val),
        #     "GovtCompanies" = "GovtCompanies" * (1 + bonus_val),
        #     "GovtOthers" = "GovtOthers" * (1 + bonus_val),
        #     "Others" = "Others" * (1 + bonus_val),
        #     "NBanksMutualFunds" = "NBanksMutualFunds" * (1 + bonus_val),
        #     "HoldingCompanies" = "HoldingCompanies" * (1 + bonus_val),
        #     "GeneralPublic" = "GeneralPublic" * (1 + bonus_val),
        #     "Employees" = "Employees" * (1 + bonus_val),
        #     "FinancialInstitutions" = "FinancialInstitutions" * (1 + bonus_val),
        #     "ForeignPromoter" = "ForeignPromoter" * (1 + bonus_val),
        #     "GDR" = "GDR" * (1 + bonus_val),
        #     "PersonActingInConcert" = "PersonActingInConcert" * (1 + bonus_val),
        #     "Total" = "Total" * (1 + bonus_val)
        # FROM (SELECT "RatioOfferred"/"RatioExisting" AS bonus_val, "CompanyCode", "XBDate"
        #       FROM public."Bonus"
        #       WHERE "XBDate" = '{date}') AS bonus
        # WHERE sh."CompanyCode" = bonus."CompanyCode"
        #   AND sh."SHPDate" = (SELECT MAX("SHPDate") 
        #                       FROM public."ShareHolding" 
        #                       WHERE "CompanyCode" = bonus."CompanyCode" 
        #                       AND "SHPDate" <= '{date}')
        #   AND bonus."XBDate" > sh."SHPDate"; 
        # '''
        # # print("Executing SQL:\n", update_sql)
        # cur.execute(update_sql)
        # conn.commit()
    else:
        print("No bonus data for today")


    

def main(curr_date):
    start_time = time.time()
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    curr_date = curr_date.strftime("%Y-%m-%d")
    
    log_query = """
                SELECT * FROM "logs"."split_bonus" 
                WHERE "log_date" = %s   
    """
	
    log_df = sqlio.read_sql_query(log_query, con=conn, params=[curr_date])
    
    if not(log_df.empty):
        print("Already ran for date", curr_date)
        return
    else:
        print("Running for date", curr_date)
        print("Getting splits data for date", curr_date)
        split = get_splits(curr_date, conn)

        print("Getting bonus data for date", curr_date)
        bonus = get_bonus(curr_date, conn)

        print("Updating OHLC")
        update_ohlc(split, bonus, conn, cur, curr_date)

        end_time = time.time()
        runtime = end_time - start_time

        
        print("Updating Shareholding")
        update_shareholding(split, bonus, conn, cur, curr_date, runtime)
        
    conn.close()
