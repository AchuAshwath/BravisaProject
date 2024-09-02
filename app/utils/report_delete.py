import datetime
import psycopg2
import pandas as pd
import numpy as np

from utils.db_helper import DB_Helper

import utils.date_set as date_set

# curr_date = (date_set.get_run_date()).strftime("%Y-%m-%d")


def deleteEPS(curr_date):

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    eps_query = 'SELECT ("CompanyCode") FROM "Reports"."EPS" WHERE "EPSDate"=\''+ (curr_date) + '\' and "CompanyCode" IS NOT NULL ;'
   
    company_code = pd.read_sql_query(eps_query, con=conn)

    # if not(company_code.empty):

    #     company_code = company_code["CompanyCode"].astype(str)
        # print("Deleting Quaterly EPS and TTM - {} in number".format(len(company_code.items())))
    #     for index,comp_code in company_code.items():
            
            # print("Deleting Quartely EPS data for last year Ending")
    #         qtr_eps_sql = 'Delete FROM public."QuarterlyEPS" Where "CompanyCode" = \''+ (comp_code) + '\' and  "YearEnding" = (Select max("YearEnding") FROM public."QuarterlyEPS" )'
    #         cur.execute(qtr_eps_sql)
    #         conn.commit()

            # print("Deleting TTM data for last Year Ending")
    #         ttm_sql = 'Delete FROM public."TTM" Where "CompanyCode" = \''+ (comp_code) + '\' and  "YearEnding" = (Select max("YearEnding") FROM public."TTM")'  
    #         cur.execute(ttm_sql)
    #         conn.commit()
    # else:
        # print("EPS data is not there for:",curr_date)



    print("Deleting EPS data for:",curr_date)
    eps_sql = 'DELETE FROM "Reports"."EPS" WHERE "EPSDate"=\''+(curr_date)+'\';'   
    cur.execute(eps_sql)
    conn.commit()

    conn.close()
  
    
        
def deletePRS(curr_date):

    conn = DB_Helper().db_connect()
    cur = conn.cursor()
    
    # print("Deleting NewHighNewLow for:",curr_date)
    nhnl_sql = 'DELETE FROM "Reports"."NewHighNewLow" WHERE "Date"=\'' + (curr_date) + '\';'
    cur.execute(nhnl_sql)
    conn.commit()

    print("Deleting PRS data for:",curr_date)
    prs_sql = 'DELETE FROM "Reports"."PRS" WHERE "Date"=\'' + (curr_date) +'\';'      
    cur.execute(prs_sql)
    conn.commit()

    conn.close()

def deleteSMR(curr_date):

    conn = DB_Helper().db_connect()
    cur = conn.cursor()
    
    print("Deleting SMR data for:",curr_date)
    smr_sql = 'DELETE FROM "Reports"."SMR" WHERE "SMRDate"=\'' + (curr_date) + '\';'       
    cur.execute(smr_sql)
    conn.commit()

    print("Deleting Ratios MergeList data for:",curr_date)
    rationmergelist_sql = 'DELETE FROM public."RatiosMergeList" WHERE "GenDate"=\'' + (curr_date) + '\'; '
    cur.execute(rationmergelist_sql)
    conn.commit()

    conn.close()


def deleteFRS(curr_date):

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("Deleting MFMergeList data for:",curr_date)
    mfmerge_sql = 'DELETE FROM public."MFMergeList" WHERE "GenDate"=\'' + (curr_date) + '\';'
    cur.execute(mfmerge_sql)
    conn.commit()

    print("Deleting FRS MF Rank data for:",curr_date)
    frs_navrank_sql = 'DELETE FROM "Reports"."FRS-MFRank" WHERE "Date"=\'' + (curr_date) + '\';'
    cur.execute(frs_navrank_sql)
    conn.commit()
    
    print("Deleting FRS-NAVRank data for:",curr_date)
    frs_nav_sql = 'DELETE FROM "Reports"."FRS-NAVRank" WHERE "Date"=\'' + (curr_date) + '\';'
    cur.execute(frs_nav_sql)
    conn.commit()

    print("Deleting FRS-NAV Category Avg data for:",curr_date)
    frs_sql = 'DELETE FROM "Reports"."FRS-NAVCategoryAvg" WHERE "Date"=\'' + (curr_date) + '\';'         
    cur.execute(frs_sql)
    conn.commit()

    print("deleting MF ohlc for:", curr_date)
    mfohlc_sql = 'delete from public.mf_ohlc where date = \'' + (curr_date) + '\';'
    cur.execute(mfohlc_sql)
    conn.commit()

    conn.close()


def deleteIRS(curr_date):

    conn = DB_Helper().db_connect()
    cur = conn.cursor()
   
    print("Deleting Industry List data for:",curr_date)
    indu_sql = 'DELETE FROM public."IndustryList" WHERE "GenDate"=\''+ (curr_date) + '\';'
    cur.execute(indu_sql)
    conn.commit()

    print("Deleting Sector Divisor data for:",curr_date)
    sec_div_sql = 'DELETE FROM public."SectorDivisor" WHERE "Date"=\''+ (curr_date) + '\';'
    cur.execute(sec_div_sql)
    conn.commit()

    print("Deleting SubSector Divisor data for:",curr_date)
    sub_Sec_div_sql = 'DELETE FROM public."SubSectorDivisor" WHERE "Date"=\'' + (curr_date) + '\';'
    cur.execute(sub_Sec_div_sql)
    conn.commit()

    print("Deleting Industry Divisor data for:",curr_date)
    indu_div_sql = 'DELETE FROM public."IndustryDivisor" WHERE "Date"=\'' + (curr_date) + '\';'
    cur.execute(indu_div_sql)
    conn.commit()

    print("Deleting Sector Index List data for:",curr_date)
    sec_ind_sql = 'DELETE FROM public."SectorIndexList" WHERE "GenDate"=\'' + (curr_date) + '\';'
    cur.execute(sec_ind_sql)
    conn.commit()

    print("Deleting SubSector Index List data for:",curr_date)
    subsec_ind_sql = 'DELETE FROM public."SubSectorIndexList" WHERE "GenDate"=\'' + (curr_date) + '\';'
    cur.execute(subsec_ind_sql)
    conn.commit()

    print("Deleting Industry Index List data for:",curr_date)
    indu_ind_sql = 'DELETE FROM public."IndustryIndexList" WHERE "GenDate"=\'' + (curr_date) + '\';'
    cur.execute(indu_ind_sql)
    conn.commit()

    print("Deleting Index History data for:",curr_date)
    ind_hstr_sql = 'DELETE FROM public."IndexHistory" WHERE "DATE"=\'' + (curr_date) + '\';'
    cur.execute(ind_hstr_sql)
    conn.commit()

    print("Deleting IRS data for:",curr_date)
    irs_sql = 'DELETE FROM "Reports"."IRS" WHERE "GenDate"=\'' + (curr_date) + '\';'
    cur.execute(irs_sql)
    conn.commit()

    conn.close()


def deleteCombiRank(curr_date):

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("Deleting Combi Rank data for:",curr_date)
    irs_sql = 'DELETE FROM "Reports"."CombinedRS" WHERE "GenDate"=\'' + (curr_date) + '\';'
    cur.execute(irs_sql)
    conn.commit()

    conn.close()


def deleteOHLC(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting OHLC for date:", curr_date)
    ohlc_sql = 'delete from public."OHLC" where "Date" = \'' + (curr_date) + '\';'
    cur.execute(ohlc_sql)
    conn.commit()
    conn.close()


def deleteIndexOHLC(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting index ohlc for date:", curr_date)
    index_ohlc_sql = 'delete from public."IndexOHLC" where "Date" =  \'' + (curr_date) + '\';'
    cur.execute(index_ohlc_sql)
    conn.commit()
    conn.close()


def deleteNSEIndexChange(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting nse change for date:", curr_date)
    nse_change_sql = 'delete from public.nse_index_change where date = \'' + (curr_date) + '\';'
    cur.execute(nse_change_sql)
    conn.commit()
    conn.close()


def deletePE(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting PE for date:", curr_date)
    pe_sql = 'delete from public."PE" where "GenDate" =  \'' + (curr_date) + '\';'
    cur.execute(pe_sql)
    conn.commit()
    conn.close()

def deleteBTTIndex(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting BTT index for date:", curr_date)
    btt_index_sql = 'delete from public."BTTDivisor" where "Date" =  \'' + (curr_date) + '\';'
    cur.execute(btt_index_sql)
    conn.commit()
    conn.close()


def deleteStockHighLow(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting stock high low for date:", curr_date)
    stock_high_sql = 'delete from dash_process.stock_off_high where date = \'' + (curr_date) + '\';'
    stock_low_sql = 'delete from dash_process.stock_off_low where date = \'' + (curr_date) + '\';'
    cur.execute(stock_high_sql)
    cur.execute(stock_low_sql)
    conn.commit()
    conn.close()


def deleteIndexHighLow(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting index high low for date:", curr_date)
    index_high_sql = 'delete from dash_process.index_off_high where date = \'' + (curr_date) + '\';'
    index_low_sql = 'delete from dash_process.index_off_low where date = \'' + (curr_date) + '\';'
    cur.execute(index_high_sql)
    cur.execute(index_low_sql)
    conn.commit()
    conn.close()


def deleteStockPerformance(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting stock performance for date:", curr_date)
    stock_per_sql = 'delete from dash_process.stock_performance where date = \'' + (curr_date) + '\';'
    cur.execute(stock_per_sql)
    conn.commit()
    conn.close()   


def deleteIndexPerformance(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting index performance for date:", curr_date)
    index_per_sql = 'delete from dash_process.index_performance where date = \'' + (curr_date) + '\';'
    cur.execute(index_per_sql)
    conn.commit()
    conn.close() 


def deleteRTDaily(curr_date): 
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting trends daily for date:", curr_date)
    indicator_sql = 'delete from mf_analysis.indicators where gen_date = \'' + (curr_date) + '\';'
    rt_daily_sql = 'delete from mf_analysis.trends where gen_date = \'' + (curr_date) + '\';'
    ema50_daily_sql = 'delete from mf_analysis.ema50_daily where date = \'' + (curr_date) + '\';'
    rt_weight_sql = 'delete from mf_analysis.trend_weightage_daily where date = \'' + (curr_date) + '\';'
    cur.execute(indicator_sql)
    cur.execute(rt_daily_sql)
    cur.execute(ema50_daily_sql)
    cur.execute(rt_weight_sql)
    conn.commit()
    conn.close() 


def deleteRTWeekly(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting trends weekly for date:", curr_date)
    ohlc_sql = 'delete from public.ohlc_weekly where date =\'' + (curr_date) + '\';'
    indicator_sql = 'delete from mf_analysis.indicators_weekly where gen_date = \'' + (curr_date) + '\';'
    rt_daily_sql = 'delete from mf_analysis.trends_weekly where gen_date = \'' + (curr_date) + '\';'
    ema50_daily_sql = 'delete from mf_analysis.ema50_weekly where date = \'' + (curr_date) + '\';'
    rt_weight_sql = 'delete from mf_analysis.trend_weightage_weekly where date = \'' + (curr_date) + '\';'
    cur.execute(ohlc_sql)
    cur.execute(indicator_sql)
    cur.execute(rt_daily_sql)
    cur.execute(ema50_daily_sql)
    cur.execute(rt_weight_sql)
    conn.commit()
    conn.close()   

def deleteRTMonthly(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting monthly trends for date:", curr_date)
    ohlc_sql = 'delete from public.ohlc_monthly where date =\'' + (curr_date) + '\';'
    indicator_sql = 'delete from mf_analysis.indicators_monthly where gen_date = \'' + (curr_date) + '\';'
    rt_daily_sql = 'delete from mf_analysis.trends_monthly where gen_date = \'' + (curr_date) + '\';'
    ema50_daily_sql = 'delete from mf_analysis.ema50_monthly where date = \'' + (curr_date) + '\';'
    rt_weight_sql = 'delete from mf_analysis.trend_weightage_monthly where date = \'' + (curr_date) + '\';'
    cur.execute(ohlc_sql)
    cur.execute(indicator_sql)
    cur.execute(rt_daily_sql)
    cur.execute(ema50_daily_sql)
    cur.execute(rt_weight_sql)
    conn.commit()
    conn.close()

def deleteMQN(curr_date):
    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    print("deleting mqn for date:", curr_date)
    mqn_sql = 'delete from mf_analysis.market_quality_number where date = \'' + (curr_date) + '\';'
    cur.execute(mqn_sql)
    conn.commit()
    conn.close()

def run_delete(date_from, date_to, time_warning=None):
    sdate=date_from
    edate=date_to
    print("Deleting data for ", sdate, edate, flush=True)
    if(date_from==date_to):
        date_list = [sdate]
        print("Deleting data for ", sdate, flush=True)
        for curr_date in date_list:
            curr_date = curr_date.strftime("%Y-%m-%d")
            deleteEPS(curr_date)
            deleteSMR(curr_date)
            deleteFRS(curr_date)
            deleteOHLC(curr_date)
            deleteIndexOHLC(curr_date)
            deleteNSEIndexChange(curr_date)
            
            deletePE(curr_date)
            deletePRS(curr_date)
            deleteIRS(curr_date)
            deleteBTTIndex(curr_date)
            deleteCombiRank(curr_date)

            # deleteStockHighLow(curr_date)
            # deleteStockPerformance(curr_date)
            # deleteIndexHighLow(curr_date)
            # deleteIndexPerformance(curr_date)

            deleteRTDaily(curr_date)
            deleteRTWeekly(curr_date)
            deleteRTMonthly(curr_date)
            deleteMQN(curr_date)
            
            print("Deleted Data for date: {}\n".format(curr_date), flush=True)
    else:
        date_list = [sdate+datetime.timedelta(days=x) for x in range((edate-sdate).days)]
        print("Deleting data from ", sdate, "to", edate, flush=True)
        for curr_date in date_list:
            curr_date = curr_date.strftime("%Y-%m-%d")
            deleteEPS(curr_date)
            deleteSMR(curr_date)
            deleteFRS(curr_date)
            deleteOHLC(curr_date)
            deleteIndexOHLC(curr_date)
            deleteNSEIndexChange(curr_date)
            
            deletePE(curr_date)
            deletePRS(curr_date)
            deleteIRS(curr_date)
            deleteBTTIndex(curr_date)
            deleteCombiRank(curr_date)

            # deleteStockHighLow(curr_date)
            # deleteStockPerformance(curr_date)
            # deleteIndexHighLow(curr_date)
            # deleteIndexPerformance(curr_date)

            deleteRTDaily(curr_date)
            deleteRTWeekly(curr_date)
            deleteRTMonthly(curr_date)
            deleteMQN(curr_date)
            if time_warning is not None:
                time_warning.config(text="Deleted Data for date: {}\n".format(curr_date), foreground="Green")
            else:
                # Handle the case where time_warning is None
                print("time_warning is None. Cannot access config attribute.")
            
            print("Deleted Data for date: {}\n".format(curr_date), flush=True)


if __name__ == '__main__':

    # start_date = datetime.date(2020,12,07)
    # end_date = datetime.date(2020,12,07)

    # while end_date>=start_date:
    #     curr_date = start_date.strftime("%Y-%m-%d")
    
    # sdate=datetime.date(2021, 1, 1)
    # edate=datetime.date(2021, 2, 1)
    # date_list = [sdate+datetime.timedelta(days=x) for x in range((edate-sdate).days)]
    date_list = [datetime.date(2021,5,7)]
    for curr_date in date_list:
        # curr_date = (date_set.get_run_date()).strftime("%Y-%m-%d")

        curr_date = curr_date.strftime("%Y-%m-%d")
        deleteEPS(curr_date)
        deleteSMR(curr_date)
        deleteFRS(curr_date)

        deleteOHLC(curr_date)
        deleteIndexOHLC(curr_date)
        deleteNSEIndexChange(curr_date)
        deletePE(curr_date)
        deletePRS(curr_date)
        deleteIRS(curr_date)
        deleteBTTIndex(curr_date)
        deleteCombiRank(curr_date)

        # deleteStockHighLow(curr_date)
        # deleteStockPerformance(curr_date)
        # deleteIndexHighLow(curr_date)
        # deleteIndexPerformance(curr_date)

        deleteRTDaily(curr_date)
        deleteRTWeekly(curr_date)
        deleteRTMonthly(curr_date)
        deleteMQN(curr_date)
        
        print("Deleted Data for date: {}\n".format(curr_date), flush=True)
        
    #     start_date += datetime.timedelta(1)
    





