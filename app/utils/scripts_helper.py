from utils.db_helper import DB_Helper
from utils.fb_helper import FB_Helper
from utils.sanitize import san_in

from utils.logs import insert_logs
from lib import btt_list
from lib import ohlc
from lib import index_ohlc
from lib import split_bonus
from lib import PE
from lib.mf_ohlc import MFOHLC
import time

from reports.PRS import PRS
from reports.ERS import ERS
from reports.EERS import EERS
from reports.EPS import EPS
from reports.SMR import SMR
from reports.FRS import FRS
from reports.IRS import IRS
from reports.combined_rank import CombinedRank

from dash_process import index_change
from dash_process import perstock_change
from dash_process import perstock_offhighlow
from dash_process import index_offhighlow

from mf_analysis.rt_daily import DailyRTProcess
from mf_analysis.rt_weekly import WeeklyRTProcess
from mf_analysis.rt_monthly import MonthlyRTProcess
from mf_analysis.ema50.ema50_daily import EMA50_daily
from mf_analysis.ema50.ema50_weekly import EMA50_weekly
from mf_analysis.ema50.ema50_monthly import EMA50_monthly


from datetime import datetime
import datetime
from pandas.tseries.offsets import BMonthEnd,BMonthBegin


from mf_analysis.market_quality_number.mqn_nse500 import MarketQualityNSE500
from mf_analysis.market_quality_number.mqn_nifty import MarketQualityNIFTY
from mf_analysis.market_quality_number.mqn_btt_index import MarketQualityBTT_Index
from utils.BTTIndex import BTTIndex
from utils.index_change import IndexCloseChange



conn = DB_Helper().db_connect()
cur = conn.cursor()
# dbURL = DB_Helper().engine()

def is_not_quarter_end(date):
    quarter_ends = [3, 6, 9, 12]
    
    month = date.month
    
    return month not in quarter_ends


def friday_btt(curr_date, is_holiday):
    prev_date = curr_date+datetime.timedelta(-1)
    conn = DB_Helper().db_connect()
    cur = conn.cursor()
    # dbURL = DB_Helper().engine()
    # fbname_one = FB_Helper().get_fb_name_one(curr_date)
    # fbname_two=FB_Helper().get_fb_name_two(curr_date)
    # fbname_three = FB_Helper().get_fb_name_three(prev_date)

    print("\nCONN: ", conn,"\n", flush=True)
    btt_list.main(curr_date)
    
    # san_in(fbname_three,dbURL)
    # san_in(fbname_one,dbURL)
    # san_in(fbname_two,dbURL)
    if (is_holiday == False):
        EPS().Generate_Daily_Report(curr_date,conn,cur)
        # ERS().Generate_Daily_Report(curr_date,conn,cur)
        EERS().Generate_Daily_Report(curr_date,conn,cur)
        SMR().generate_smr_current(curr_date,conn, cur)
        FRS().generate_current_mfrank(curr_date,conn,cur)
        FRS().generate_current_nav_rank(curr_date,conn,cur)
        MFOHLC().gen_mf_ohlc_current(curr_date,conn,cur)
        ohlc.main(curr_date)
        index_ohlc.main(curr_date)
        split_bonus.main(curr_date)
        PE.current_pe(curr_date)
        PRS().generate_prs_daily(curr_date, conn,cur)
        IRS().gen_irs_current(curr_date,conn, cur)
        
        IndexCloseChange().generating_daily_nse_index_changeValues_df(curr_date, conn, cur)
        BTTIndex().cal_BTT_divisor_Index(curr_date,conn,cur)
        CombinedRank().current_rank(curr_date, conn,cur)
        perstock_offhighlow.main(curr_date)
        try:
            perstock_change.main(curr_date)
        except Exception as e:
            print(e, flush=True)	
        try:
            index_offhighlow.main(curr_date,conn, cur)
        except Exception as e:
            print(e, flush=True)
        index_change.main(curr_date)

        weekly_rt = WeeklyRTProcess()
        weekly_rt.gen_rt_weekly(curr_date,conn, cur)
        weekly_rt.gen_trend_weightage_weekly_data(curr_date,conn, cur)
        
        EMA50_weekly().generating_EMA50_weekly(curr_date, conn, cur)
        MarketQualityNSE500().mqn_nse(curr_date,conn, cur)
        MarketQualityNIFTY().mqn_nifty(curr_date,conn, cur)
    else:
        print("\t", curr_date, "is an off day.", flush=True)
        

    conn.close()


def month_endf(curr_date,is_holiday):
    prev_date = curr_date+datetime.timedelta(-1)
    conn = DB_Helper().db_connect()
    print("DB CONNECTION: \n{}\n".format(conn))
    cur = conn.cursor()
    fbname_one = FB_Helper().get_fb_name_one(curr_date)
    fbname_two=FB_Helper().get_fb_name_two(curr_date)
    fbname_three = FB_Helper().get_fb_name_three(prev_date)

    # san_in(fbname_three,dbURL)
    # san_in(fbname_one,dbURL)
    # san_in(fbname_two,dbURL)

    if (is_holiday == False):
        EPS().Generate_Daily_Report(curr_date,conn,cur)
        # ERS().Generate_Daily_Report(curr_date,conn,cur)
        EERS().Generate_Daily_Report(curr_date,conn,cur)
        SMR().generate_smr_current(curr_date,conn, cur)
        FRS().generate_current_mfrank(curr_date,conn,cur)
        FRS().generate_current_nav_rank(curr_date,conn,cur)
        MFOHLC().gen_mf_ohlc_current(curr_date,conn,cur)
        ohlc.main(curr_date)
        index_ohlc.main(curr_date)
        split_bonus.main(curr_date)
        PE.current_pe(curr_date)
        PRS().generate_prs_daily(curr_date, conn,cur)
        IRS().gen_irs_current(curr_date,conn, cur)

        IndexCloseChange().generating_daily_nse_index_changeValues_df(curr_date, conn, cur)
        BTTIndex().cal_BTT_divisor_Index(curr_date,conn,cur)
        CombinedRank().current_rank(curr_date, conn,cur)
        perstock_offhighlow.main(curr_date)
        try:
            perstock_change.main(curr_date)
        except Exception as e:
            print(e, flush=True)		
        try:
            index_offhighlow.main(curr_date,conn, cur)
        except Exception as e:
            print(e, flush=True)	
        index_change.main(curr_date)

        monthly_rt = MonthlyRTProcess()
        monthly_rt.gen_rt_monthly(curr_date,conn, cur)
        monthly_rt.gen_trend_weightage_monthly_data(curr_date,conn, cur)
        
        EMA50_monthly().generating_EMA50_monthly(curr_date, conn, cur)
        MarketQualityNSE500().mqn_nse(curr_date,conn, cur)
        MarketQualityNIFTY().mqn_nifty(curr_date,conn, cur)
    else:
        print("\t", curr_date, "is an off day.")
    

    conn.close()


def daily_btt(curr_date,is_holiday):
    prev_date = curr_date+datetime.timedelta(-1)
    conn = DB_Helper().db_connect()
    print("DB CONNECTION: \n{}\n".format(conn))
    cur = conn.cursor()
    fbname_one = FB_Helper().get_fb_name_one(curr_date)
    fbname_two=FB_Helper().get_fb_name_two(curr_date)
    fbname_three = FB_Helper().get_fb_name_three(prev_date)

    btt_list.main(curr_date)
    # san_in(fbname_three,dbURL)
    # san_in(fbname_one,dbURL)
    # san_in(fbname_two,dbURL)
    if (is_holiday == False):
        EPS().Generate_Daily_Report(curr_date,conn,cur)
        # ERS().Generate_Daily_Report(curr_date,conn,cur)
        EERS().Generate_Daily_Report(curr_date,conn,cur)

        SMR().generate_smr_current(curr_date,conn, cur)
        FRS().generate_current_mfrank(curr_date,conn,cur)
        FRS().generate_current_nav_rank(curr_date,conn,cur)
        MFOHLC().gen_mf_ohlc_current(curr_date,conn,cur)
        ohlc.main(curr_date)
        index_ohlc.main(curr_date)
        split_bonus.main(curr_date)
        PE.current_pe(curr_date)
        PRS().generate_prs_daily(curr_date, conn,cur)
        IRS().gen_irs_current(curr_date,conn, cur)
        IndexCloseChange().generating_daily_nse_index_changeValues_df(curr_date, conn, cur)
        BTTIndex().cal_BTT_divisor_Index(curr_date,conn,cur)
        CombinedRank().current_rank(curr_date, conn,cur)
        perstock_offhighlow.main(curr_date)
        try:
            perstock_change.main(curr_date)
        except Exception as e:
            print(e, flush=True)
        try:
            index_offhighlow.main(curr_date,conn, cur)
        except Exception as e:
            print(e, flush=True)	
        index_change.main(curr_date)

        daily_rt = DailyRTProcess()
        daily_rt.gen_rt_daily(curr_date,conn, cur)
        daily_rt.gen_trend_weightage_daily_data(curr_date,conn, cur)
        
        EMA50_daily().generating_EMA50_daily(curr_date, conn, cur)
        MarketQualityNSE500().mqn_nse(curr_date,conn, cur)
        MarketQualityNIFTY().mqn_nifty(curr_date,conn, cur)
    else:
        print("\t", curr_date, "is an off day.")

    conn.close()

def daily_scripts(curr_date,is_holiday):
    prev_date = curr_date+datetime.timedelta(-1)
    conn = DB_Helper().db_connect()
    print("\nCONN: ", conn,"\n", flush = True)
    print("\tScript running for ", curr_date, flush = True)
    cur = conn.cursor()
    fbname_one = FB_Helper().get_fb_name_one(curr_date)
    fbname_two=FB_Helper().get_fb_name_two(curr_date)
    fbname_three = FB_Helper().get_fb_name_three(prev_date)

    # san_in(fbname_three,dbURL)
    # san_in(fbname_one,dbURL)
    # san_in(fbname_two,dbURL)

    if (is_holiday == False):
        EPS().Generate_Daily_Report(curr_date,conn,cur)
        # ERS().Generate_Daily_Report(curr_date,conn,cur)
        EERS().Generate_Daily_Report(curr_date,conn,cur)
        SMR().generate_smr_current(curr_date,conn, cur)
        FRS().generate_current_mfrank(curr_date,conn,cur)
        FRS().generate_current_nav_rank(curr_date,conn,cur)
        MFOHLC().gen_mf_ohlc_current(curr_date,conn,cur)
        ohlc.main(curr_date)
        index_ohlc.main(curr_date)
        split_bonus.main(curr_date)
        PE.current_pe(curr_date)
        PRS().generate_prs_daily(curr_date, conn,cur)
        IRS().gen_irs_current(curr_date,conn, cur)
        IndexCloseChange().generating_daily_nse_index_changeValues_df(curr_date, conn, cur)
        BTTIndex().cal_BTT_divisor_Index(curr_date,conn,cur)
        CombinedRank().current_rank(curr_date, conn,cur)
        perstock_offhighlow.main(curr_date)
        try:
            perstock_change.main(curr_date)
        except Exception as e:
            print(e, flush=True)
        try:
            index_offhighlow.main(curr_date,conn, cur)
        except Exception as e:
            print(e, flush=True)	
        index_change.main(curr_date)

        daily_rt = DailyRTProcess()
        daily_rt.gen_rt_daily(curr_date,conn, cur)
        daily_rt.gen_trend_weightage_daily_data(curr_date,conn, cur)
        
        EMA50_daily().generating_EMA50_daily(curr_date, conn, cur)
        MarketQualityNSE500().mqn_nse(curr_date,conn, cur)
        MarketQualityNIFTY().mqn_nifty(curr_date,conn, cur)
    else:
        print("\t", curr_date, "is an off day.")
    
    conn.close()


def friday_scripts(curr_date,is_holiday):
    prev_date = curr_date+datetime.timedelta(-1)
    conn = DB_Helper().db_connect()
    print("DB CONNECTION: \n{}\n".format(conn))
    cur = conn.cursor()
    fbname_one = FB_Helper().get_fb_name_one(curr_date)
    fbname_two=FB_Helper().get_fb_name_two(curr_date)
    fbname_three = FB_Helper().get_fb_name_three(prev_date)

    # san_in(fbname_three,dbURL)
    # san_in(fbname_one,dbURL)
    # san_in(fbname_two,dbURL)
    if (is_holiday == False):
        EPS().Generate_Daily_Report(curr_date,conn,cur)
        # ERS().Generate_Daily_Report(curr_date,conn,cur)
        EERS().Generate_Daily_Report(curr_date,conn,cur)
        SMR().generate_smr_current(curr_date,conn, cur)
        FRS().generate_current_mfrank(curr_date,conn,cur)
        FRS().generate_current_nav_rank(curr_date,conn,cur)
        MFOHLC().gen_mf_ohlc_current(curr_date,conn,cur)
        ohlc.main(curr_date)
        index_ohlc.main(curr_date)
        split_bonus.main(curr_date)
        PE.current_pe(curr_date)
        PRS().generate_prs_daily(curr_date, conn,cur)
        IRS().gen_irs_current(curr_date,conn, cur)
        IndexCloseChange().generating_daily_nse_index_changeValues_df(curr_date, conn, cur)
        BTTIndex().cal_BTT_divisor_Index(curr_date,conn,cur)
        CombinedRank().current_rank(curr_date, conn,cur)
        perstock_offhighlow.main(curr_date)
        try:
            perstock_change.main(curr_date)
        except Exception as e:
            print(e, flush=True)
        try:
            index_offhighlow.main(curr_date,conn, cur)
        except Exception as e:
            print(e, flush=True)	
        index_change.main(curr_date)

        weekly_rt = WeeklyRTProcess()
        weekly_rt.gen_rt_weekly(curr_date,conn, cur)
        weekly_rt.gen_trend_weightage_weekly_data(curr_date,conn, cur)
        
        EMA50_weekly().generating_EMA50_weekly(curr_date, conn, cur)
        MarketQualityNSE500().mqn_nse(curr_date,conn, cur)
        MarketQualityNIFTY().mqn_nifty(curr_date,conn, cur)
    else:
        print("\t", curr_date, "is an off day.")
    
    conn.close()
    




def saturday_fb(curr_date):
    prev_date = curr_date+datetime.timedelta(-1)
    conn = DB_Helper().db_connect()
    print("DB CONNECTION: \n{}\n".format(conn))
    cur = conn.cursor()
    fbname_one = FB_Helper().get_fb_name_one(curr_date)
    fbname_two=FB_Helper().get_fb_name_two(curr_date)
    fbname_three = FB_Helper().get_fb_name_three(prev_date)

    # san_in(fbname_three,dbURL)
    # san_in(fbname_one,dbURL)
    # san_in(fbname_two,dbURL)



def run_scripts_frompy(sdate, edate,is_holiday):
    start_time=time.time()
    # date_list = [sdate+datetime.timedelta(days=x) for x in range((edate-sdate).days)]
    date_list = [sdate + datetime.timedelta(days=x) for x in range((edate - sdate).days + 1)]

    print(date_list)
    print("Running script from {} to {}".format(str(sdate), str(edate)), flush = True)
    # date_list = [datetime.date(2020,6,1)]
    for curr_date in date_list:
        #Last day of current month
        end_offset = BMonthEnd()
        month_end = end_offset.rollforward(curr_date)
        month_end = month_end.to_pydatetime()
        month_end = month_end.date()


        #First working day of month
        start_offset = BMonthBegin()
        month_start = start_offset.rollback(curr_date)
        month_start = month_start.to_pydatetime()
        month_start = month_start.date()
        is_not_qtr_end = is_not_quarter_end(curr_date)
        print(month_start)
        print(is_not_qtr_end)
        
        if curr_date.weekday() == 4 and curr_date == month_start:
            #run friday plus btt script
            friday_btt(curr_date,is_holiday)
            LOGS = {
            "log_date": curr_date,
            "log_time": datetime.datetime.now(),
            "runtime": time.time()-start_time,
            } 
            insert_logs("report_generation", [LOGS], conn, cur)
        elif curr_date == month_end:
            #run month end scripts
            month_endf(curr_date, is_holiday)
            LOGS = {
            "log_date": curr_date,
            "log_time": datetime.datetime.now(),
            "runtime": time.time()-start_time,
            } 
            insert_logs("report_generation", [LOGS], conn, cur)
        elif curr_date==month_start and curr_date.weekday()<4:
            #run daily plus btt script
            daily_btt(curr_date,is_holiday)
            # we can add btt_list column here later
            LOGS = {
            "log_date": curr_date,
            "log_time": datetime.datetime.now(),
            "runtime": time.time()-start_time,
            } 
            insert_logs("report_generation", [LOGS], conn, cur)
        elif curr_date.weekday() == 4:
            #run friday script
            friday_scripts(curr_date,is_holiday)
            LOGS = {
            "log_date": curr_date,
            "log_time": datetime.datetime.now(),
            "runtime": time.time()-start_time,
            } 
            insert_logs("report_generation", [LOGS], conn, cur)
        elif curr_date.weekday()<4:
            #run daily scripts
            daily_scripts(curr_date,is_holiday)
            LOGS = {
            "log_date": curr_date,
            "log_time": datetime.datetime.now(),
            "runtime": time.time()-start_time,
            } 
            insert_logs("report_generation", [LOGS], conn, cur)
        elif curr_date.weekday() == 5:
            saturday_fb(curr_date)
        else:
            print('nothing')
            

        print("\n\n\tPROCESS COMPLETE FOR DATE {}\n\n".format(str(curr_date)))
    
    print("Returning from run_scripts_frompy")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time taken: {elapsed_time} seconds")
    
    return "Finished"