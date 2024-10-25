#Script to compile index list and generate daily IRS report
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
# from utils.db_helper import DB_Helper
import utils.date_set as date_set
import re
import copy


if os.name == 'nt':
    my_path = os.path.abspath(os.path.dirname(os.getcwd()))
    file_path = os.path.join(my_path, "IRSFiles\\IRSHistory\\")

else:
  my_path = os.path.abspath(os.path.dirname(__file__))
  file_path = os.path.join(my_path, "irs-history/")


class IRS:
  """ Genarting Index and Rank for sector, subsector
      Industry, and  generating Industry List for
      current date.
  """
  def __init__(self):
    pass


  def get_month_first_day(self):
    """Fetch the first of day current month.

    Return:
        First day of month.
    """

    month_first_day = datetime.date.today().replace(day=1)
    month_first_day = month_first_day.strftime("%Y-%m-%d")

    return month_first_day


  def get_six_month_back(self,date):
    """Fetch the date for six month back.

    Return:
        date of six month back (-180)
     """

    six_month_back = (pd.to_datetime(date) - pd.DateOffset(days=180)).strftime("%Y-%m-%d")

    return six_month_back


  def get_closest_quarter(self,target):
    """ Fetch the closest quarter from current date.

    Return:
      Closest quarter.
    """

    # candidate list, nicely enough none of these
    # are in February, so the month lengths are fixed

    candidates = [
      datetime.date(target.year - 1, 12, 31),
      datetime.date(target.year, 3, 31),
      datetime.date(target.year, 6, 30),
      datetime.date(target.year, 9, 30),
      datetime.date(target.year, 12, 31),
    ]
    # take the minimum according to the absolute distance to
    # the target date.

    for date in candidates:
      if target < date:
        candidates.remove(date)

    return min(candidates, key=lambda d: abs(target - d))


  def get_previous_quarter(self,target):
    """ Fetch previous quarter from the from closest quarter.

      Return:
          Previous Quarter.
    """

    curr_qrt = self.get_closest_quarter(target)
    curr_qrt_dec = datetime.date(curr_qrt.year, curr_qrt.month, (curr_qrt.day - 2))
    prev_qrt = self.get_closest_quarter(curr_qrt_dec)

    return prev_qrt


  def get_year_before_quarter(self,target):
    """Fetch quarter for one year before.

    Return:
        One year back quarter.
    """

    curr_qrt = self.get_closest_quarter(target)
    one_qtr_back = self.get_previous_quarter(curr_qrt)
    two_qtr_back = self.get_previous_quarter(one_qtr_back)
    three_qtr_back = self.get_previous_quarter(two_qtr_back)
    four_qtr_back = self.get_previous_quarter(three_qtr_back)

    return four_qtr_back


  def get_btt_list(self,conn, date):
    """Fetch tha data from BTTList for MAX Date.

    Return:
      BTTList data for max date.
    """

    date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

    if date <= datetime.date(2018,12,27):

      date = date.strftime("%Y-%m-%d")
      BTT_back = datetime.date(2018, 12, 27).strftime("%Y-%m-%d")
      btt_list_query = 'SELECT * FROM public."BTTList"  WHERE "BTTDate" = \''+ BTT_back + '\';'

    else:

        date = date.strftime("%Y-%m-%d")

        today = date
        today = datetime.datetime.strptime(today, "%Y-%m-%d")

        BTT_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")

        next_month = today.month + 1 if today.month + 1 <= 12 else 1
        next_year = today.year if today.month + 1 <= 12 else today.year + 1
        BTT_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")
        # print("BTT_Back: ", BTT_back)
        # print("BTT_Next: ", BTT_next)
        btt_list_query = 'select *  from public."BTTList" where "BTTDate" >= \''+ BTT_back + '\' \
                          and "BTTDate" < \''+BTT_next+'\';'
    btt_list = sqlio.read_sql_query(btt_list_query, con=conn)

    return btt_list


  def get_ohlc_data(self,conn, date):
    """Fetch data from OHLC for current date.

    Return:
      OHLC data of current date
    """

    ohlc_sql = 'SELECT * FROM public."OHLC" WHERE "Date" = \''+date+ '\' AND "CompanyCode" is not null;'
    ohlc = sqlio.read_sql_query(ohlc_sql, con=conn)

    return ohlc


  def merge_btt_ohlc(self,btt_list , ohlc):
    """ Merge BTT OHLC

    Args:
      btt_list = BTTList data of current date,
      ohlc = OHCL data of current date.

    Return:
      Merge data of BTTList and OHLC.
    """

    btt_merge = pd.merge(btt_list, ohlc, on='CompanyCode', how='left')
    # print("BTT_merge: \n", btt_merge)

    btt_merge.rename(columns={'NSECode_x': 'NSECode', 'BSECode_x': 'BSECode', 'ISIN_x': 'ISIN'}, inplace=True)

    columns_to_remove = ['ISIN_y', 'Company', 'Date_x',
                        'MFList', 'BTTDate', 'ISIN_y', 'Date_y', 'Value']

    btt_merge = btt_merge.drop(columns_to_remove, axis=1)

    return btt_merge

  
  def calc_change_rate(self, btt_merge, conn, date):
      """ Calculate change rate
  
      Args:
        btt_merge = merge data of BTTList and OHLC.
  
      Operation:
        Fetch the data from OHLC for 1 week back. And calculate the change rate
        Change Rate = ((current close – previous close)/ previous close) *  100.
  
      Return:
        Value of change rate.
      """
  
      week_back = (pd.to_datetime(date) + timedelta(-7)).strftime("%Y-%m-%d")
  
      ohlc_prev_sql = f"""
                      SELECT DISTINCT ON("CompanyCode") * 
                      FROM public."OHLC" 
                      WHERE "Date" < '{date}' 
                      ORDER BY "CompanyCode", "Date" DESC;
                      """
      ohlc_prev = sqlio.read_sql_query(ohlc_prev_sql, con=conn)
      
      btt_merge = btt_merge.drop_duplicates(subset='CompanyCode')
  
      for index, row in btt_merge.iterrows():
          company_code = row['CompanyCode']
          
          # Check for null values in Open, High, Low, Close and replace them with values from ohlc_prev
          if pd.isnull(row['Open']) or pd.isnull(row['High']) or pd.isnull(row['Low']) or pd.isnull(row['Close']):
              ohlc_prev_row = ohlc_prev.loc[ohlc_prev['CompanyCode'] == company_code]
              if not ohlc_prev_row.empty:
                  btt_merge.at[index, 'Open'] = ohlc_prev_row['Open'].values[0] if pd.isnull(row['Open']) else row['Open']
                  btt_merge.at[index, 'High'] = ohlc_prev_row['High'].values[0] if pd.isnull(row['High']) else row['High']
                  btt_merge.at[index, 'Low'] = ohlc_prev_row['Low'].values[0] if pd.isnull(row['Low']) else row['Low']
                  btt_merge.at[index, 'Close'] = ohlc_prev_row['Close'].values[0] if pd.isnull(row['Close']) else row['Close']
  
          current_close_list = btt_merge.loc[(btt_merge["CompanyCode"] == company_code)]["Close"]
          current_close = current_close_list.item() if len(current_close_list.index) == 1 else np.nan
  
          prev_close_list = ohlc_prev.loc[(ohlc_prev["CompanyCode"] == company_code)]["Close"]
          prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan
  
          change = (current_close - prev_close) / prev_close * 100 if prev_close != 0 else np.nan
  
          btt_merge.loc[index, 'PrevClose'] = prev_close
          btt_merge.loc[index, 'Change'] = change
  
      return btt_merge

  def get_background_info(self,btt_merge, conn):
    """ Get Background info

    Args:
        btt_list = Data of change rate.

    Operation:
        Fetch the data from BackgroundInfo and merge it with input data.

    Return:
        Merge data of btt list and BackgroundInfo.
    """

    backgroundinfo_sql = 'SELECT "IndustryCode", "CompanyCode" from public."BackgroundInfo" ;'
    backgroundinfo = sqlio.read_sql_query(backgroundinfo_sql, con=conn)

    btt_merge.rename(columns={'CompanyCode_x':'CompanyCode'}, inplace=True)

    ohlc_backgroundinfo = pd.merge(btt_merge, backgroundinfo, left_on="CompanyCode", right_on="CompanyCode", how='left')


    return ohlc_backgroundinfo


  def get_industry_data(self,ohlc_backgroundinfo, conn):
    """ Get Industry data

    Args:
      ohlc_backgroundinfo = Merge data of btt list and BackgroundInfo.

    Operation:
      Fetch tha data from IndustryMapping table and merge it with the input data.

    Return:
      Merge data of IndustryMapping and ohlc baclgroundinfo.
    """

    # industry_mapping_sql = 'SELECT "IndustryCode", "Sector","Industry", "SubSector", "SectorIndexName", "SubSectorIndexName", "IndustryIndexName"  \
    #                         FROM public."IndustryMapping";'
    industry_mapping_sql = 'SELECT "IndustryCode","IndustryName","Industry","Code","SubSector","SubSectorCode","Sector","SectorCode","SubIndustry","SubIndustryCode","IndustryIndexName","SubSectorIndexName","SectorIndexName","SubIndustryIndexName" \
                              FROM public."IndustryMapping";'
    industry_mapping = sqlio.read_sql_query(industry_mapping_sql, con=conn)

    industry_merge = pd.merge(ohlc_backgroundinfo, industry_mapping, on="IndustryCode", how='left')

    return industry_merge


  def calc_free_float(self,industry_merge, date, conn):
    """ Calculate Free Float,

    Args:
      industry_merge = Merge data of IndustryMapping and ohlc baclgroundinfo.

    Operation:
      Fetch the data from ShareHolding and get the value of FF and OS
      and calculate the value of FreeFloat
      FF = foreignInstitution + others + MBankMutualFund + GeneralPublic +FinancialInstitution,
      FreeFloat = FF / OS

    Return:
      Value of Free Float.
    """

    shareholding_query = 'SELECT DISTINCT ON ("CompanyCode") * FROM public."ShareHolding" \
                          WHERE "SHPDate" <= \''+date+'\' \
                          order by "CompanyCode", "SHPDate" desc;'
    shareholding = sqlio.read_sql_query(shareholding_query, con=conn)

    shareholding[shareholding.columns[2:36]] =shareholding[shareholding.columns[2:36]].replace(r'[$,]', value = '', regex=True).astype(float)

    merge_list = pd.merge(industry_merge, shareholding, left_on="CompanyCode",right_on="CompanyCode", how='left')
    merge_list.rename(columns={'Total':'OS'}, inplace=True)

    merge_list['FF'] = merge_list["ForeignInstitutions"] + merge_list["Others"] + \
                                  merge_list["NBanksMutualFunds"] + merge_list["GeneralPublic"] + \
                                  merge_list["FinancialInstitutions"]


    merge_list['OS'] =merge_list['OS'].replace(r'[$,]', value = '', regex=True).astype(float)

    merge_list["FreeFloat"] = merge_list['FF'] / merge_list['OS']

    return merge_list


  def cal_ff_ohlc(self,industry_list, conn, date):
    """ Calculate the value of FF

    Args:
        industry_list = Value of Free Float.
        date = current date

    Operation:
        Fetch the value of Open, High',Low,Close, OS, FreeFloat.
        And Calculate the values of, ff_open,ff _high, ff_low, ff_close and os_close
        ff_open = ff_val * open_val * os_val.
        ff _high = ff_val * high_val * os_val.
        ff_low = ff_val * low_val * os_val.
        ff_close = ff_val * close_val * os_val.
        os_close = os_val *  close_val

    Return:
        Value FF.
    """

    for index, row in industry_list.iterrows():
      print("CompanyCode : ", row['CompanyCode'])
      

      open_list = industry_list.loc[(industry_list['CompanyCode']==row['CompanyCode'])]['Open']
      open_val = open_list.item() if len(open_list.index) == 1 else np.nan

      high_list = industry_list.loc[(industry_list['CompanyCode']==row['CompanyCode'])]['High']
      high_val = high_list.item() if len(high_list.index) == 1 else np.nan

      low_list = industry_list.loc[(industry_list['CompanyCode']==row['CompanyCode'])]['Low']
      low_val = low_list.item() if len(low_list.index) == 1 else np.nan

      close_list = industry_list.loc[(industry_list['CompanyCode']==row['CompanyCode'])]['Close']
      close_val = close_list.item() if len(close_list.index) == 1 else np.nan

      os_list = industry_list.loc[(industry_list['CompanyCode']==row['CompanyCode'])]['OS']
      os_val = os_list.item() if len(os_list.index) == 1 else np.nan

      ff_list = industry_list.loc[(industry_list['CompanyCode']==row['CompanyCode'])]['FreeFloat']
      ff_val = ff_list.item() if len(ff_list.index) == 1 else np.nan
      
      print("Open: ", open_val)
      print("High: ", high_val)
      print("Low: ", low_val)
      print("Close: ", close_val)
      print("OS: ", os_val)
      print("FreeFloat: ", ff_val)
      print()
      print("FF_Open: ", ff_val * open_val * os_val)
      print("FF_High: ", ff_val * high_val * os_val)
      print("FF_Low: ", ff_val * low_val * os_val)
      print("FF_Close: ", ff_val * close_val * os_val)
      
      ff_open = ff_val * open_val * os_val
      ff_high = ff_val * high_val * os_val
      ff_low = ff_val * low_val * os_val
      ff_close = ff_val * close_val * os_val
      os_close = os_val * close_val

      mcap_open = os_val * open_val
      mcap_high = os_val * high_val
      mcap_low = os_val * low_val
      mcap_close = os_val * close_val


      industry_list.loc[index, 'FF_Open'] = ff_open
      industry_list.loc[index, 'FF_High'] = ff_high
      industry_list.loc[index, 'FF_Low'] = ff_low
      industry_list.loc[index, 'FF_Close'] = ff_close
      industry_list.loc[index, 'OS_Close'] = os_close
      industry_list.loc[index, 'MCap_Open'] = mcap_open
      industry_list.loc[index, 'MCap_High'] = mcap_high
      industry_list.loc[index, 'MCap_Low'] = mcap_low
      industry_list.loc[index, 'MCap_Close'] = mcap_close


    return industry_list


  def industry_ttm(self,industry_list, conn, date):
    """ Get the data of Industry and TTM.

      Args:
          industry_list = Data of FF.

      Operation:
          Fetch the data from TTM for current date and year back,
          and merge it with industry list data.

      Return:
          Merge data of industry list and TTM.
    """

    curr_date = pd.to_datetime(date).date()
    year_back = self.get_year_before_quarter(curr_date)
    year_back = year_back.strftime("%Y-%m-%d")


    ttm_sql = 'SELECT DISTINCT ON("CompanyCode") * FROM public."TTM" \
          WHERE "YearEnding" <= \'' +date+ '\'  \
          ORDER by "CompanyCode", "YearEnding" DESC ;'
    ttm_list = sqlio.read_sql_query(ttm_sql, con = conn)

    ttm_back_sql = 'SELECT DISTINCT ON("CompanyCode") * FROM public."TTM" \
          WHERE "YearEnding" <= \'' +year_back+ '\'  \
          ORDER by "CompanyCode", "YearEnding" DESC ;'
    ttm_back_list = sqlio.read_sql_query(ttm_back_sql, con = conn)

    ttm_back_list.rename(columns={'PAT':'prev_pat', 'Equity': 'prev_equity'}, inplace=True)


    industry_list = pd.merge(industry_list, ttm_list[['CompanyCode', 'PAT', 'Equity']], left_on='CompanyCode', \
                            right_on='CompanyCode', how='left')

    industry_list = pd.merge(industry_list, ttm_back_list[['CompanyCode', 'prev_pat', 'prev_equity']], left_on='CompanyCode', \
                            right_on='CompanyCode', how='left')

    return industry_list


  def insert_industry_list(self,industry_list, conn, cur, date):
    """ Insert industry data into database.

      Args:
          industry_list = data of free float, ff and merge data of industry list and TTM.

      Operation:
          Export the data into industryList_export.csv
          and insert into IndustrytList table.
    """

    industry_list['GenDate'] = pd.to_datetime(date).strftime("%Y-%m-%d")

    industry_list["BSECode"].fillna(-1, inplace=True)
    industry_list = industry_list.astype({"BSECode": int})
    industry_list = industry_list.astype({"BSECode": str})
    industry_list["BSECode"] = industry_list["BSECode"].replace('-1', np.nan)



    industry_list = industry_list[['CompanyCode', 'CompanyName', 'NSECode', 'BSECode', 'ISIN', 'Industry', 'Sector', 'SubSector', 'SubIndustry', \
                                  'Open', 'High', 'Low', 'Close', 'Volume', 'PrevClose', 'Change', 'OS', 'FaceValue', 'FreeFloat', \
                                    'GenDate', 'FF_Open', 'FF_High', 'FF_Low', 'FF_Close', 
                                    'MCap_Open', 'MCap_High', 'MCap_Low', 'MCap_Close', 
                                    'IndustryIndexName', 'SectorIndexName', 'SubSectorIndexName', 'SubIndustryIndexName', \
                                    'PAT', 'Equity', 'OS_Close', 'prev_pat', 'prev_equity']]

    industry_list['FaceValue'] =industry_list['FaceValue'].replace(r'[$,]', value = '', regex=True).astype(float)


    exportfilename = "industryList_export.csv"
    exportfile = open(exportfilename,"w")
    industry_list.to_csv(exportfile, header=True, index=False, lineterminator='\r')
    exportfile.close()


    copy_sql = """
        COPY public."IndustryList" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
    with open(exportfilename, 'r') as f:

      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


  def   get_industry_list_backdate(self,conn, date):
    """ Fetch the data from industryList for MAX date.

    Return:
        Data of industryList for max date.
    """

    # sql = 'SELECT * FROM public."IndustryList" \
    #       WHERE "GenDate" = (SELECT MAX("GenDate") FROM public."IndustryList") ;'
    sql = 'SELECT * FROM public."IndustryList" \
          WHERE "GenDate" = DATE(\' {} \') ;'.format(str(date))
    # print("SQL QUERY: ", sql)
    master_list = sqlio.read_sql_query(sql, con=conn)

    return master_list


  def group_sector_divisor(self,master_list, conn):
    """ Fetch the data for sector and divisor.

    Args:
        master_list = Data of industryList for max date.

    Return:
        Value of sector divisor.
    """

    sector_divisor = master_list.groupby('SectorIndexName', as_index=True).agg({'SectorIndexName':'count', 'FF_Open':'sum', 'MCap_Open':'sum'})
    # print("\t\tsector_divisor 1 : ", sector_divisor) # 10
    sector_divisor = sector_divisor.rename(columns = {'SectorIndexName':'Count', 'FF_Open':'SumFF_Open', 'MCap_Open': 'SumMCap_Open'})
    # print("\t\tsector_divisor 2 : ", sector_divisor)
    sector_divisor = sector_divisor.reset_index()
    # print("\t\tsector_divisor 3 : ", sector_divisor)
    sector_divisor = sector_divisor.rename(columns = {'SectorIndexName':'IndexName'})
    # print("\t\tsector_divisor 4 : ", sector_divisor)
    # print("\n{}\n", sector_divisor)

    return sector_divisor


  def group_subsector_divisor(self,master_list, conn):
    """ Fetch the data for subsector divisor.

    Args:
        master_list = Data of industryList for max date.

    Return:
        Value of subsector divisor.
    """

    subsector_divisor = master_list.groupby('SubSectorIndexName', as_index=True).agg({'SubSectorIndexName':'count', 'FF_Open':'sum', 'MCap_Open':'sum'})
    # print("\t\tsubsector_divisor 1 : ", subsector_divisor) # 37
    subsector_divisor = subsector_divisor.rename(columns = {'SubSectorIndexName':'Count', 'FF_Open':'SumFF_Open', 'MCap_Open': 'SumMCap_Open'})
    # print("\t\tsubsector_divisor 1 : ", subsector_divisor)
    subsector_divisor = subsector_divisor.reset_index()
    # print("\t\tsubsector_divisor 1 : ", subsector_divisor)
    subsector_divisor = subsector_divisor.rename(columns = {'SubSectorIndexName':'IndexName'})
    # print("\t\tsubsector_divisor 1 : ", subsector_divisor)

    return subsector_divisor


  def group_industry_divisor(self,master_list, conn):
    """ Fetch the data for industry divisor.

    Args:
        master_list = Data of industryList for max date.

    Return:
        Value of Industry divisor.
    """

    industry_divisor = master_list.groupby('IndustryIndexName', as_index=True).agg({'IndustryIndexName':'count', 'FF_Open':'sum', 'MCap_Open':'sum'})

    industry_divisor = industry_divisor.rename(columns = {'IndustryIndexName':'Count', 'FF_Open':'SumFF_Open', 'MCap_Open': 'SumMCap_Open'})
    industry_divisor = industry_divisor.reset_index()
    industry_divisor = industry_divisor.rename(columns = {'IndustryIndexName':'IndexName'})
    # print("Industry Devisor: ", industry_divisor)
    return industry_divisor

  def group_sub_industry_divisor(self,master_list, conn):
    """ Fetch the data for sub industry divisor.

    Args:
        master_list = Data of industryList for max date.

    Return:
        Value of SubIndustry divisor.
    """

    sub_industry_divisor = master_list.groupby('SubIndustryIndexName', as_index=True).agg({'SubIndustryIndexName':'count', 'FF_Open':'sum', 'MCap_Open':'sum'})

    sub_industry_divisor = sub_industry_divisor.rename(columns = {'SubIndustryIndexName':'Count', 'FF_Open':'SumFF_Open' , 'MCap_Open': 'SumMCap_Open'})
    sub_industry_divisor = sub_industry_divisor.reset_index()
    sub_industry_divisor = sub_industry_divisor.rename(columns = {'SubIndustryIndexName':'IndexName'})
    # print("Industry Devisor: ", industry_divisor)
    return sub_industry_divisor


  def calc_sector_divisor(self,master_list, sector_divisor, conn, date):
    """ Calculating the sector divisor.

    Args:
        master_list = Data of industryList for max date,
        sector_divisor = Data of group sector divisor.

    Operation:
        Fetch the data from IndexHistory, SectorDivisor, and IndustryList table
        merge the executed data and calculate the value for ff_close_current,
        divisor_current, os_current.

    Return:
         Data of sector divisor.
    """

    indexhistory_sql  = ('SELECT DISTINCT ON("TICKER") * FROM public."IndexHistory" \
                            WHERE "DATE" < \''+date+'\'  \
                            ORDER by "TICKER","DATE" desc ;')
    indexhistory = sqlio.read_sql_query(indexhistory_sql, con = conn)



    divisor_backdate_sql = 'SELECT DISTINCT ON("IndexName") * FROM public."SectorDivisor" \
                        WHERE "Date" < \''+date+'\' \
                        ORDER BY "IndexName", "Date" DESC; '
    divisor_backdate = sqlio.read_sql_query(divisor_backdate_sql, con = conn)

    sql = 'SELECT "SectorIndexName", "Sector", \
                SUM("FF_Open") AS ff_open_sum, \
                SUM("MCap_Open") AS mcap_open_sum, \
                SUM("OS") AS os_sum  \
                from public."IndustryList" \
                WHERE "GenDate" = \''+date+'\' AND "SectorIndexName" is not null\
                GROUP BY "SectorIndexName", "Sector" ;'
    sector_divisor_list = sqlio.read_sql_query(sql, con = conn)


    Sector = master_list["SectorIndexName"]

    sector_prevclose = indexhistory.loc[indexhistory["TICKER"].isin(Sector)]
    sector_prevclose = sector_prevclose.rename(columns = {"TICKER":"IndexName"})

    prev_industry_list_sql = f"""
                              SELECT DISTINCT ON("CompanyCode") * 
                              FROM public."IndustryList"
                              WHERE "GenDate" = (
                                  SELECT MAX("GenDate") 
                                  FROM public."IndustryList" 
                                  WHERE "GenDate" < '{date}'
                              )
                              ORDER BY "CompanyCode", "GenDate" DESC;
                              """
    prev_industry_list = sqlio.read_sql_query(prev_industry_list_sql, con = conn)

    current_industry_list_sql = ('SELECT DISTINCT ON("CompanyCode") * FROM public."IndustryList" \
                        WHERE "GenDate" = \''+date+'\'  \
                        ORDER by "CompanyCode", "GenDate" desc ;')
    current_industry_list = sqlio.read_sql_query(current_industry_list_sql, con = conn)

    merge_sector_divisor = pd.merge(sector_divisor, sector_prevclose, on="IndexName", how="left")

    
    # # self.export_table("test_12_merge_sector_devisor", merge_sector_divisor)

    # print("\t\tmerge_sector_divisor", merge_sector_divisor)
    # print("\n")
    # for index, row in merge_sector_divisor.iterrows():
    #   print(row, "\n")

    # merge_sector_divisor['Divisor'] = merge_sector_divisor['SumFF_Close'] / merge_sector_divisor['CLOSE']
    # merge_sector_divisor = merge_sector_divisor.rename(columns = {"SumFF_Close":"IndexValue"})

    for index, row in merge_sector_divisor.iterrows():

      os_prev_list = divisor_backdate.loc[divisor_backdate['IndexName']==row['IndexName']]['OS']
      os_prev = os_prev_list.item() if len(os_prev_list.index) == 1 else np.nan

      os_current_list = sector_divisor_list.loc[sector_divisor_list['SectorIndexName'] == row['IndexName']]['os_sum']
      os_current = os_current_list.item() if len(os_current_list.index) == 1 else np.nan

      divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['Divisor']
      divisor_back = divisor_back_list.item() if len(divisor_back_list.index) == 1 else np.nan
      
      mcap_divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['MCap_Divisor'] #
      mcap_divisor_back = mcap_divisor_back_list.item() if len(mcap_divisor_back_list.index) == 1 else np.nan   

      prev_close_list = indexhistory.loc[indexhistory['TICKER'] == row['IndexName']]['OPEN']
      prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

      print(row['IndexName'])

      if(os_prev == os_current):
        print("OS previous is equal to OS current")

        ff_close_prev_list = merge_sector_divisor.loc[merge_sector_divisor['IndexName'] == row['IndexName']]['SumFF_Open']
        ff_close_prev = ff_close_prev_list.item() if len(ff_close_prev_list.index) == 1 else np.nan

        mcap_open_prev_list = merge_sector_divisor.loc[merge_sector_divisor['IndexName'] == row['IndexName']]['SumMCap_Open']
        mcap_open_prev = mcap_open_prev_list.item() if len(mcap_open_prev_list.index) == 1 else np.nan

        divisor_current = divisor_back
        mcap_divisor_current = mcap_divisor_back 

        print("FF_Open_Sum: ", ff_close_prev)
        print("Mcap_Divisor: ",mcap_divisor_current)
        print("Divisor: ", divisor_current)

        merge_sector_divisor.loc[index, 'IndexValue'] = ff_close_prev
        merge_sector_divisor.loc[index, 'SumMCap_Open'] = mcap_open_prev
        merge_sector_divisor.loc[index, 'MCapDivisor'] = mcap_divisor_current
        merge_sector_divisor.loc[index, 'Divisor'] = divisor_current
        merge_sector_divisor.loc[index, 'OS'] = os_prev

      else:
        print("OS previous is not equal to OS current")
        print("OS previous: ", os_prev)
        print("OS current: ", os_current)

        ff_close_current_list = sector_divisor_list.loc[sector_divisor_list['SectorIndexName'] == row['IndexName']]['ff_open_sum']
        ff_close_current = ff_close_current_list.item() if len(ff_close_current_list.index) == 1 else np.nan

        mcap_open_current_list = sector_divisor_list.loc[sector_divisor_list['SectorIndexName'] == row['IndexName']]['mcap_open_sum']
        mcap_open_current = mcap_open_current_list.item() if len(mcap_open_current_list.index) == 1 else np.nan

        current_company_count = merge_sector_divisor.loc[merge_sector_divisor['IndexName'] == row['IndexName']]['Count'].values[0]
        # print("current_company_count", current_company_count)
        prev_company_count = len(prev_industry_list.loc[prev_industry_list['SectorIndexName'] == row['IndexName']])

        if prev_close is not np.nan:

          prev_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['CLOSE']

          prev_mcap_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['MCap_CLOSE']   #

          current_companies = current_industry_list.loc[current_industry_list['SectorIndexName'] == row['IndexName']]
          print("current_company_count: ", current_company_count)
          
          # prev_companies = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]
          # print("prev_company_count: ", prev_company_count)
          current_companies_prev_list = prev_industry_list.loc[prev_industry_list['CompanyCode'].isin(current_companies['CompanyCode'])]
          print("Companies from current list on previous list :", len(current_companies_prev_list))
            
          new_companies = current_companies[~current_companies['CompanyCode'].isin(current_companies_prev_list['CompanyCode'])]
          print("new_companies: ", len(new_companies))
          
          
          if(len(current_companies_prev_list)==len(current_companies)):
            print("only OS has changed, no new companies added")
            # merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))
            
          elif(len(current_companies_prev_list)<len(current_companies)):
            print("new companies added")
            # prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
            # prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
            
            # new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
            # addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
            
          elif(len(current_companies_prev_list)>len(current_companies)):
            print("companies removed")
          
          merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))

          prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
          print("prev_companies_with_same_OS: ", len(prev_companies_with_same_OS))
          # print(row['IndexName'])
          # print(prev_companies_with_same_OS.columns)
          prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['SectorIndexName_prev'] == row['IndexName']]['FF_Close_prev'].sum() 
          prev_mcap_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['SectorIndexName_prev'] == row['IndexName']]['MCap_Close_prev'].sum()
          # prev_close_sum = prev_industry_list.loc[prev_industry_list['SubIndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
          print("prev_close_sum: ", prev_close_sum) 
          print("prev_mcap_close_sum: ", prev_mcap_close_sum)   
          # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close']
          # keep only row with the same companycode from current companies
          # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
          
          prev_companies_with_different_OS = merged_companies[merged_companies['OS_prev'] != merged_companies['OS_current']]
          print("prev_companies_with_different_OS: ", len(prev_companies_with_different_OS))
          # companies_with_diff_OS = prev_companies_with_different_OS[prev_companies_with_different_OS['IndustryIndexName'] == row['IndexName']]
          
          prev_companies_with_different_OS['prev_close_sum_for_diff_OS'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['FreeFloat_current'] * prev_companies_with_different_OS['PrevClose_current']
          changed_prev_close_sum = prev_companies_with_different_OS['prev_close_sum_for_diff_OS'].sum()
          prev_companies_with_different_OS['prev_MCap_Close'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['PrevClose_current']
          changed_prev_mcap_close_sum = prev_companies_with_different_OS['prev_MCap_Close'].sum()
           
          print("changed_prev_close_sum: ", changed_prev_close_sum)
          print("changed_prev_mcap_close_sum: ", changed_prev_mcap_close_sum)
          
          # prev_mcap_close_sum = prev_industry_list.loc[prev_industry_list['SectorIndexName'] == row['IndexName']]['MCap_Close'].sum()
          print("", new_companies[['OS', 'FreeFloat', 'PrevClose']])

          new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
          addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
          print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)

          new_companies['prev_MCap_Close'] = new_companies['OS'] * new_companies['PrevClose']
          addition_to_prev_mcap_close_sum = new_companies['prev_MCap_Close'].sum()
          print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)


          if not prev_close_index.empty:
              prev_close_index = prev_close_index.iloc[0]
              prev_mcap_close_index = prev_mcap_close_index.iloc[0]
          else:
              prev_close_index = np.nan
              prev_mcap_close_index = np.nan

          divisor = (prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum) / prev_close_index
          mcap_divisor = (prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum) / prev_mcap_close_index
          print()
          print("prev_close_sum: ", prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum)
          print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)
          print("prev_close_index: ", prev_close_index)
          print("divisor: ", divisor)
          print("prev_mcap_close_sum: ", prev_mcap_close_sum)
          print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)
          print("prev_mcap_close_index: ", prev_mcap_close_index)
          print("mcap_divisor : ", mcap_divisor)

          merge_sector_divisor.loc[index, 'IndexValue'] = prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum
          merge_sector_divisor.loc[index, 'MCap_Open_sum'] = prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum
          merge_sector_divisor.loc[index, 'MCapDivisor'] = mcap_divisor
          merge_sector_divisor.loc[index, 'Divisor'] = divisor
          merge_sector_divisor.loc[index, 'OS'] = os_current

        else:
            divisor_current = ff_close_current / 1000
            MCapdivisor_current = mcap_open_current / 1000
            print("Divisor: ", divisor_current)
            print("FF_Open_sum: ", ff_close_current)
            print("Mcap_Open_sum: ", mcap_open_current)
            print("MCapDivisor: ", MCapdivisor_current)
            merge_sector_divisor.loc[index, 'MCap_Open_sum'] = mcap_open_current
            merge_sector_divisor.loc[index, 'Divisor'] = divisor_current
            merge_sector_divisor.loc[index, 'IndexValue'] = ff_close_current
            merge_sector_divisor.loc[index, 'MCapDivisor'] = MCapdivisor_current
            merge_sector_divisor.loc[index, 'OS'] = os_current




    return merge_sector_divisor


  def calc_subsector_divisor(self,master_list, subsector_divisor, conn, date):
    """ Calculating the subsector divisor.

    Args:
        master_list = Data of industryList for max date,
        subsector_divisor = Data of Group subsector divisor.

    Operation:
        Fetch the data from IndexHistory, SubSectorDivisor, and IndustryList table
        merge the executed data and calculate the value for ff_close_current,
        divisor_current, os_current.

    Return:
         Data of subsector divisor.
    """
    indexhistory_sql  = ('SELECT DISTINCT ON("TICKER") * FROM public."IndexHistory" \
                            WHERE "DATE" < \''+date+'\'  \
                            ORDER by "TICKER","DATE" desc ;')
    indexhistory = sqlio.read_sql_query(indexhistory_sql, con = conn)

    divisor_backdate_sql = 'SELECT DISTINCT ON("IndexName") * FROM public."SubSectorDivisor" \
                        WHERE "Date" < \''+date+'\' \
                        ORDER BY "IndexName", "Date" DESC; '
    divisor_backdate = sqlio.read_sql_query(divisor_backdate_sql, con = conn)

    sql = 'SELECT "SubSectorIndexName", "SubSector", \
                SUM("FF_Open") AS ff_open_sum, \
                SUM("MCap_Open") AS mcap_open_sum, \
                SUM("OS") AS os_sum  \
                from public."IndustryList" \
                WHERE "GenDate" = \''+date+'\' AND "SubSectorIndexName" is not null\
                GROUP BY "SubSectorIndexName", "SubSector" ;'
    subsector_divisor_list = sqlio.read_sql_query(sql, con = conn)

    SubSector = master_list["SubSectorIndexName"]

    subsector_prevclose = indexhistory.loc[indexhistory["TICKER"].isin(SubSector)]
    subsector_prevclose = subsector_prevclose.rename(columns = {"TICKER":"IndexName"})

    prev_industry_list_sql = f"""
                              SELECT DISTINCT ON("CompanyCode") * 
                              FROM public."IndustryList"
                              WHERE "GenDate" = (
                                  SELECT MAX("GenDate") 
                                  FROM public."IndustryList" 
                                  WHERE "GenDate" < '{date}'
                              )
                              ORDER BY "CompanyCode", "GenDate" DESC;
                              """
    prev_industry_list = sqlio.read_sql_query(prev_industry_list_sql, con = conn)

    current_industry_list_sql = ('SELECT DISTINCT ON("CompanyCode") * FROM public."IndustryList" \
                        WHERE "GenDate" = \''+date+'\'  \
                        ORDER by "CompanyCode", "GenDate" desc ;')
    current_industry_list = sqlio.read_sql_query(current_industry_list_sql, con = conn)

    merge_subsector_divisor = pd.merge(subsector_divisor, subsector_prevclose, on="IndexName", how="left")

    # print("\t merge_subsector_divisor \n")
    # for index, row in merge_subsector_divisor.iterrows():
    #   print(row, "\n")

    # merge_subsector_divisor['Divisor'] = merge_subsector_divisor['SumFF_Close'] / merge_subsector_divisor['CLOSE']
    # merge_subsector_divisor = merge_subsector_divisor.rename(columns = {"SumFF_Close":"IndexValue"})

    for index, row in merge_subsector_divisor.iterrows():

        os_prev_list = divisor_backdate.loc[divisor_backdate['IndexName']==row['IndexName']]['OS']
        os_prev = os_prev_list.item() if len(os_prev_list.index) == 1 else np.nan

        os_current_list = subsector_divisor_list.loc[subsector_divisor_list['SubSectorIndexName'] == row['IndexName']]['os_sum']
        os_current = os_current_list.item() if len(os_current_list.index) == 1 else np.nan

        divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['Divisor']
        divisor_back = divisor_back_list.item() if len(divisor_back_list.index) == 1 else np.nan
        
        mcap_divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['MCap_Divisor'] #
        mcap_divisor_back = mcap_divisor_back_list.item() if len(mcap_divisor_back_list.index) == 1 else np.nan   

        prev_close_list = indexhistory.loc[indexhistory['TICKER'] == row['IndexName']]['OPEN']
        prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

        print(row['IndexName'])
        if(os_prev == os_current):

            print('OS previos is equal to OS current')
            ff_close_prev_list = merge_subsector_divisor.loc[merge_subsector_divisor['IndexName'] == row['IndexName']]['SumFF_Open']
            ff_close_prev = ff_close_prev_list.item() if len(ff_close_prev_list.index) == 1 else np.nan

            mcap_open_prev_list = merge_subsector_divisor.loc[merge_subsector_divisor['IndexName'] == row['IndexName']]['SumMCap_Open']
            mcap_open_prev = mcap_open_prev_list.item() if len(mcap_open_prev_list.index) == 1 else np.nan

            divisor_current = divisor_back
            mcap_divisor_current = mcap_divisor_back 

            print("FF_Open_Sum: ", ff_close_prev)
            print("Mcap_Divisor: ",mcap_divisor_current)
            print("Divisor: ", divisor_current)

            merge_subsector_divisor.loc[index, 'IndexValue'] = ff_close_prev
            merge_subsector_divisor.loc[index, 'SumMCap_Open'] = mcap_open_prev
            merge_subsector_divisor.loc[index, 'MCapDivisor'] = mcap_divisor_current
            merge_subsector_divisor.loc[index, 'Divisor'] = divisor_current
            merge_subsector_divisor.loc[index, 'OS'] = os_prev

        else:
            print("OS previous is not equal to OS current")
            print("OS previous: ", os_prev)
            print("OS current: ", os_current)

            ff_close_current_list = subsector_divisor_list.loc[subsector_divisor_list['SubSectorIndexName'] == row['IndexName']]['ff_open_sum']
            ff_close_current = ff_close_current_list.item() if len(ff_close_current_list.index) == 1 else np.nan

            mcap_open_current_list = subsector_divisor_list.loc[subsector_divisor_list['SubSectorIndexName'] == row['IndexName']]['mcap_open_sum']
            mcap_open_current = mcap_open_current_list.item() if len(mcap_open_current_list.index) == 1 else np.nan

            current_company_count = merge_subsector_divisor.loc[merge_subsector_divisor['IndexName'] == row['IndexName']]['Count'].values[0]
            # print("current_company_count", current_company_count)
            prev_company_count = len(prev_industry_list.loc[prev_industry_list['SectorIndexName'] == row['IndexName']])

            if prev_close is not np.nan:

              prev_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['CLOSE']

              prev_mcap_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['MCap_CLOSE']   #

              current_companies = current_industry_list.loc[current_industry_list['SubSectorIndexName'] == row['IndexName']]
              print("current_company_count: ", current_company_count)
              
              # prev_companies = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]
              # print("prev_company_count: ", prev_company_count)
              current_companies_prev_list = prev_industry_list.loc[prev_industry_list['CompanyCode'].isin(current_companies['CompanyCode'])]
              print("Companies from current list on previous list :", len(current_companies_prev_list))
                
              new_companies = current_companies[~current_companies['CompanyCode'].isin(current_companies_prev_list['CompanyCode'])]
              print("new_companies: ", len(new_companies))
              
              
              if(len(current_companies_prev_list)==len(current_companies)):
                print("only OS has changed, no new companies added")
                # merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))
                
              elif(len(current_companies_prev_list)<len(current_companies)):
                print("new companies added")
                # prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
                # prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
                
                # new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
                # addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
                
              elif(len(current_companies_prev_list)>len(current_companies)):
                print("companies removed")
              
              merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))

              prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
              print("prev_companies_with_same_OS: ", len(prev_companies_with_same_OS))
              # print(row['IndexName'])
              # print(prev_companies_with_same_OS.columns)
              prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['SubSectorIndexName_prev'] == row['IndexName']]['FF_Close_prev'].sum() 
              prev_mcap_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['SubSectorIndexName_prev'] == row['IndexName']]['MCap_Close_prev'].sum()
              # prev_close_sum = prev_industry_list.loc[prev_industry_list['SubIndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
              print("prev_close_sum: ", prev_close_sum) 
              print("prev_mcap_close_sum: ", prev_mcap_close_sum)   
              # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close']
              # keep only row with the same companycode from current companies
              # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
              
              prev_companies_with_different_OS = merged_companies[merged_companies['OS_prev'] != merged_companies['OS_current']]
              print("prev_companies_with_different_OS: ", len(prev_companies_with_different_OS))
              # companies_with_diff_OS = prev_companies_with_different_OS[prev_companies_with_different_OS['IndustryIndexName'] == row['IndexName']]
              
              prev_companies_with_different_OS['prev_close_sum_for_diff_OS'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['FreeFloat_current'] * prev_companies_with_different_OS['PrevClose_current']
              changed_prev_close_sum = prev_companies_with_different_OS['prev_close_sum_for_diff_OS'].sum()
              prev_companies_with_different_OS['prev_MCap_Close'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['PrevClose_current']
              changed_prev_mcap_close_sum = prev_companies_with_different_OS['prev_MCap_Close'].sum()
              
              print("changed_prev_close_sum: ", changed_prev_close_sum)
              print("changed_prev_mcap_close_sum: ", changed_prev_mcap_close_sum)
              
              # prev_mcap_close_sum = prev_industry_list.loc[prev_industry_list['SectorIndexName'] == row['IndexName']]['MCap_Close'].sum()
              print("", new_companies[['OS', 'FreeFloat', 'PrevClose']])

              new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
              addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
              print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)

              new_companies['prev_MCap_Close'] = new_companies['OS'] * new_companies['PrevClose']
              addition_to_prev_mcap_close_sum = new_companies['prev_MCap_Close'].sum()
              print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)


              if not prev_close_index.empty:
                  prev_close_index = prev_close_index.iloc[0]
                  prev_mcap_close_index = prev_mcap_close_index.iloc[0]
              else:
                  prev_close_index = np.nan
                  prev_mcap_close_index = np.nan

              divisor = (prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum) / prev_close_index
              mcap_divisor = (prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum) / prev_mcap_close_index
              print()
              print("prev_close_sum: ", prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum)
              print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)
              print("prev_close_index: ", prev_close_index)
              print("divisor: ", divisor)
              print("prev_mcap_close_sum: ", prev_mcap_close_sum)
              print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)
              print("prev_mcap_close_index: ", prev_mcap_close_index)
              print("mcap_divisor : ", mcap_divisor)

              merge_subsector_divisor.loc[index, 'IndexValue'] = prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum
              merge_subsector_divisor.loc[index, 'MCap_Open_sum'] = prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum
              merge_subsector_divisor.loc[index, 'MCapDivisor'] = mcap_divisor
              merge_subsector_divisor.loc[index, 'Divisor'] = divisor
              merge_subsector_divisor.loc[index, 'OS'] = os_current


            else:
              divisor_current = ff_close_current / 1000
              MCapdivisor_current = mcap_open_current / 1000
              print("Divisor: ", divisor_current)
              print("FF_Open_sum: ", ff_close_current)
              print("Mcap_Open_sum: ", mcap_open_current)
              print("MCapDivisor: ", MCapdivisor_current)
              merge_subsector_divisor.loc[index, 'MCap_Open_sum'] = mcap_open_current
              merge_subsector_divisor.loc[index, 'Divisor'] = divisor_current
              merge_subsector_divisor.loc[index, 'IndexValue'] = ff_close_current
              merge_subsector_divisor.loc[index, 'MCapDivisor'] = MCapdivisor_current
              merge_subsector_divisor.loc[index, 'OS'] = os_current



    return merge_subsector_divisor


  def calc_industry_divisor(self,master_list, industry_divisor, conn, date):
    """ Calculating the indusrty divisor.

    Args:
        master_list = Data of industryList for max date,
        industrty_divisor = data of group industry divisor.

    Operation:
        Fetch the data from IndexHistory, IndustryDivisor, and IndustryList table
        merge the executed data and calculate the value for ff_close_current,
        divisor_current, os_current.

    Return:
         Data of industry divisor.
    """

    indexhistory_sql  = ('SELECT DISTINCT ON("TICKER") * FROM public."IndexHistory" \
                            WHERE "DATE" < \''+date+'\'  \
                            ORDER by "TICKER","DATE" desc ;')
    indexhistory = sqlio.read_sql_query(indexhistory_sql, con = conn)

    divisor_backdate_sql = 'SELECT DISTINCT ON("IndexName") * FROM public."IndustryDivisor" \
                        WHERE "Date" < \''+date+'\' \
                        ORDER BY "IndexName", "Date" DESC; '
    divisor_backdate = sqlio.read_sql_query(divisor_backdate_sql, con = conn)

    sql = 'SELECT "IndustryIndexName", "Industry", \
                SUM("FF_Open") AS ff_open_sum, \
                SUM("MCap_Open") AS mcap_open_sum, \
                SUM("OS") AS os_sum \
                from public."IndustryList" \
                WHERE "GenDate" = \''+date+'\' AND "IndustryIndexName" is not null\
                GROUP BY "IndustryIndexName", "Industry" ;'
    industry_divisor_list = sqlio.read_sql_query(sql, con = conn)

    industry = master_list["IndustryIndexName"]

    industry_prevclose = indexhistory.loc[indexhistory["TICKER"].isin(industry)]
    industry_prevclose = industry_prevclose.rename(columns = {"TICKER":"IndexName"})

    prev_industry_list_sql = f"""
                              SELECT DISTINCT ON("CompanyCode") * 
                              FROM public."IndustryList"
                              WHERE "GenDate" = (
                                  SELECT MAX("GenDate") 
                                  FROM public."IndustryList" 
                                  WHERE "GenDate" < '{date}'
                              )
                              ORDER BY "CompanyCode", "GenDate" DESC;
                              """
    prev_industry_list = sqlio.read_sql_query(prev_industry_list_sql, con = conn)

    current_industry_list_sql = ('SELECT DISTINCT ON("CompanyCode") * FROM public."IndustryList" \
                        WHERE "GenDate" = \''+date+'\'  \
                        ORDER by "CompanyCode", "GenDate" desc ;')
    current_industry_list = sqlio.read_sql_query(current_industry_list_sql, con = conn)

    merge_industry_divisor = pd.merge(industry_divisor, industry_prevclose, on="IndexName", how="left")
    # self.export_table("merge_industry_divisor", merge_industry_divisor)
    # merge_industry_divisor['Divisor'] = merge_industry_divisor['SumFF_Close'] / merge_industry_divisor['CLOSE']
    # merge_industry_divisor = merge_industry_divisor.rename(columns = {"SumFF_Close":"IndexValue"})

    # print("\t merge_industry_divisor \n")
    # for index, row in merge_industry_divisor.iterrows():
    #   print(row, "\n")

    for index, row in merge_industry_divisor.iterrows():

      os_prev_list = divisor_backdate.loc[divisor_backdate['IndexName']==row['IndexName']]['OS']
      os_prev = os_prev_list.item() if len(os_prev_list.index) == 1 else np.nan

      os_current_list = industry_divisor_list.loc[industry_divisor_list['IndustryIndexName'] == row['IndexName']]['os_sum']
      os_current = os_current_list.item() if len(os_current_list.index) == 1 else np.nan

      divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['Divisor']
      divisor_back = divisor_back_list.item() if len(divisor_back_list.index) == 1 else np.nan

      mcap_divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['MCap_Divisor'] 
      mcap_divisor_back = mcap_divisor_back_list.item() if len(mcap_divisor_back_list.index) == 1 else np.nan


      prev_close_list = indexhistory.loc[indexhistory['TICKER'] == row['IndexName']]['OPEN']
      prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

      print(row['IndexName'])

      if os_prev == os_current:
          print("OS previous is equal to OS current")

          ff_close_prev_list = merge_industry_divisor.loc[merge_industry_divisor['IndexName'] == row['IndexName']]['SumFF_Open']
          ff_close_prev = ff_close_prev_list.item() if len(ff_close_prev_list.index) == 1 else np.nan

          mcap_open_prev_list = merge_industry_divisor.loc[merge_industry_divisor['IndexName'] == row['IndexName']]['SumMCap_Open']   
          mcap_open_prev = mcap_open_prev_list.item() if len(mcap_open_prev_list.index) == 1 else np.nan  

          divisor_current = divisor_back
          mcap_divisor_current = mcap_divisor_back    

          print("FF_Open_Sum: ", ff_close_prev)
          print("Mcap_Divisor: ",mcap_divisor_current)
          print("Divisor: ", divisor_current)

          merge_industry_divisor.loc[index, 'IndexValue'] = ff_close_prev
          merge_industry_divisor.loc[index, 'SumMCap_Open'] = mcap_open_prev
          merge_industry_divisor.loc[index, 'MCapDivisor'] = mcap_divisor_current
          merge_industry_divisor.loc[index, 'Divisor'] = divisor_current
          merge_industry_divisor.loc[index, 'OS'] = os_prev

      else:
          print("OS previous is not equal to OS current")
          print("OS previous: ", os_prev)
          print("OS current: ", os_current)

          ff_close_current_list = industry_divisor_list.loc[industry_divisor_list['IndustryIndexName'] == row['IndexName']]['ff_open_sum']
          ff_close_current = ff_close_current_list.item() if len(ff_close_current_list.index) == 1 else np.nan

          mcap_open_current_list = industry_divisor_list.loc[industry_divisor_list['IndustryIndexName'] == row['IndexName']]['mcap_open_sum']
          mcap_open_current = mcap_open_current_list.item() if len(mcap_open_current_list.index) == 1 else np.nan

          current_company_count = merge_industry_divisor.loc[merge_industry_divisor['IndexName'] == row['IndexName']]['Count'].values[0]
          # prev_company_count = len(prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']])

          if prev_close is not np.nan:
        
            # print("List has different number of companies")
            
            prev_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['CLOSE']

            prev_mcap_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['MCap_CLOSE']   #

            current_companies = current_industry_list.loc[current_industry_list['IndustryIndexName'] == row['IndexName']]
            print("current_company_count: ", current_company_count)
            
            # prev_companies = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]
            # print("prev_company_count: ", prev_company_count)
            current_companies_prev_list = prev_industry_list.loc[prev_industry_list['CompanyCode'].isin(current_companies['CompanyCode'])]
            print("Companies from current list on previous list :", len(current_companies_prev_list))
              
            new_companies = current_companies[~current_companies['CompanyCode'].isin(current_companies_prev_list['CompanyCode'])]
            print("new_companies: ", len(new_companies))
            
            
            if(len(current_companies_prev_list)==len(current_companies)):
              print("only OS has changed, no new companies added")
              # merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))
              
            elif(len(current_companies_prev_list)<len(current_companies)):
              print("new companies added")
              # prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
              # prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
              
              # new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
              # addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
              
            elif(len(current_companies_prev_list)>len(current_companies)):
              print("companies removed")
            
            merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))

            prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
            prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['IndustryIndexName_prev'] == row['IndexName']]['FF_Close_prev'].sum()
            prev_mcap_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['IndustryIndexName_prev'] == row['IndexName']]['MCap_Close_prev'].sum()
            # prev_close_sum = prev_industry_list.loc[prev_industry_list['SubIndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
            print("prev_close_sum: ", prev_close_sum) 
            print("prev_mcap_close_sum: ", prev_mcap_close_sum)   
            # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close']
            # keep only row with the same companycode from current companies
            # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
            
            prev_companies_with_different_OS = merged_companies[merged_companies['OS_prev'] != merged_companies['OS_current']]
            print("prev_companies_with_different_OS: ", len(prev_companies_with_different_OS))
            # companies_with_diff_OS = prev_companies_with_different_OS[prev_companies_with_different_OS['IndustryIndexName'] == row['IndexName']]
            
            prev_companies_with_different_OS['prev_close_sum_for_diff_OS'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['FreeFloat_current'] * prev_companies_with_different_OS['PrevClose_current']
            changed_prev_close_sum = prev_companies_with_different_OS['prev_close_sum_for_diff_OS'].sum()
            prev_companies_with_different_OS['prev_MCap_Close'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['PrevClose_current']
            changed_prev_mcap_close_sum = prev_companies_with_different_OS['prev_MCap_Close'].sum()
            
            print("changed_prev_close_sum: ", changed_prev_close_sum)
            print("changed_prev_mcap_close_sum: ", changed_prev_mcap_close_sum)
            
            # prev_mcap_close_sum = prev_industry_list.loc[prev_industry_list['SectorIndexName'] == row['IndexName']]['MCap_Close'].sum()
            print("", new_companies[['OS', 'FreeFloat', 'PrevClose']])

            new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
            addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
            print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)

            new_companies['prev_MCap_Close'] = new_companies['OS'] * new_companies['PrevClose']
            addition_to_prev_mcap_close_sum = new_companies['prev_MCap_Close'].sum()
            print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)


            if not prev_close_index.empty:
                prev_close_index = prev_close_index.iloc[0]
                prev_mcap_close_index = prev_mcap_close_index.iloc[0]
            else:
                prev_close_index = np.nan
                prev_mcap_close_index = np.nan

            divisor = (prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum) / prev_close_index
            mcap_divisor = (prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum) / prev_mcap_close_index
            print()
            print("prev_close_sum: ", prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum)
            print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)
            print("prev_close_index: ", prev_close_index)
            print("divisor: ", divisor)
            print("prev_mcap_close_sum: ", prev_mcap_close_sum)
            print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)
            print("prev_mcap_close_index: ", prev_mcap_close_index)
            print("mcap_divisor : ", mcap_divisor)

            merge_industry_divisor.loc[index, 'IndexValue'] = prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum
            merge_industry_divisor.loc[index, 'MCap_Open_sum'] = prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum
            merge_industry_divisor.loc[index, 'MCapDivisor'] = mcap_divisor
            merge_industry_divisor.loc[index, 'Divisor'] = divisor
            merge_industry_divisor.loc[index, 'OS'] = os_current
          else:
              divisor_current = ff_close_current / 1000
              MCapdivisor_current = mcap_open_current / 1000
              print("Divisor: ", divisor_current)
              print("FF_Open_sum: ", ff_close_current)
              print("Mcap_Open_sum: ", mcap_open_current)
              print("MCapDivisor: ", MCapdivisor_current)
              merge_industry_divisor.loc[index, 'MCap_Open_sum'] = mcap_open_current
              merge_industry_divisor.loc[index, 'Divisor'] = divisor_current
              merge_industry_divisor.loc[index, 'IndexValue'] = ff_close_current
              merge_industry_divisor.loc[index, 'MCapDivisor'] = MCapdivisor_current
              merge_industry_divisor.loc[index, 'OS'] = os_current

    return merge_industry_divisor




  
  
  def calc_sub_industry_divisor(self,master_list, sub_industry_divisor, conn, date):
    """ Calculating the indusrty divisor.

    Args:
        master_list = Data of industryList for max date,
        industrty_divisor = data of group industry divisor.

    Operation:
        Fetch the data from IndexHistory, IndustryDivisor, and IndustryList table
        merge the executed data and calculate the value for ff_close_current,
        divisor_current, os_current.

    Return:
         Data of industry divisor.
    """

    indexhistory_sql  = ('SELECT DISTINCT ON("TICKER") * FROM public."IndexHistory" \
                        WHERE "DATE" < \''+date+'\'  \
                        ORDER by "TICKER","DATE" desc ;')
    indexhistory = sqlio.read_sql_query(indexhistory_sql, con = conn)

    divisor_backdate_sql = 'SELECT DISTINCT ON("IndexName") * FROM public."SubIndustryDivisor" \
                    WHERE "Date" < \''+date+'\' \
                    ORDER BY "IndexName", "Date" DESC; '
    divisor_backdate = sqlio.read_sql_query(divisor_backdate_sql, con = conn)

    sql = 'SELECT "SubIndustryIndexName", "SubIndustry", \
                SUM("FF_Open") AS ff_open_sum, \
                SUM("MCap_Open") AS mcap_open_sum, \
                SUM("OS") AS os_sum \
                from public."IndustryList" \
                WHERE "GenDate" = \''+date+'\' AND "SubIndustryIndexName" is not null\
                GROUP BY "SubIndustryIndexName", "SubIndustry" ;'
    sub_industry_divisor_list = sqlio.read_sql_query(sql, con = conn)

    print("inside calc_sub_industry_divisor", len(sub_industry_divisor_list))

    sub_industry = master_list["SubIndustryIndexName"]

    sub_industry_prevclose = indexhistory.loc[indexhistory["TICKER"].isin(sub_industry)]
    sub_industry_prevclose = sub_industry_prevclose.rename(columns = {"TICKER":"IndexName"})

    prev_industry_list_sql = f"""
                              SELECT DISTINCT ON("CompanyCode") * 
                              FROM public."IndustryList"
                              WHERE "GenDate" = (
                                  SELECT MAX("GenDate") 
                                  FROM public."IndustryList" 
                                  WHERE "GenDate" < '{date}'
                              )
                              ORDER BY "CompanyCode", "GenDate" DESC;
                              """
    prev_industry_list = sqlio.read_sql_query(prev_industry_list_sql, con = conn)

    current_industry_list_sql = ('SELECT DISTINCT ON("CompanyCode") * FROM public."IndustryList" \
                        WHERE "GenDate" = \''+date+'\'  \
                        ORDER by "CompanyCode", "GenDate" desc ;')
    current_industry_list = sqlio.read_sql_query(current_industry_list_sql, con = conn)


    merge_sub_industry_divisor = pd.merge(sub_industry_divisor, sub_industry_prevclose, on="IndexName", how="left")
    # self.export_table("merge_industry_divisor", merge_industry_divisor)
    # merge_industry_divisor['Divisor'] = merge_industry_divisor['SumFF_Close'] / merge_industry_divisor['CLOSE']
    # merge_industry_divisor = merge_industry_divisor.rename(columns = {"SumFF_Close":"IndexValue"})

    # print("\t merge_industry_divisor \n")
    # for index, row in merge_industry_divisor.iterrows():
    #   print(row, "\n")

    for index, row in merge_sub_industry_divisor.iterrows():

        os_prev_list = divisor_backdate.loc[divisor_backdate['IndexName']==row['IndexName']]['OS']
        os_prev = os_prev_list.item() if len(os_prev_list.index) == 1 else np.nan

        os_current_list = sub_industry_divisor_list.loc[sub_industry_divisor_list['SubIndustryIndexName'] == row['IndexName']]['os_sum']
        os_current = os_current_list.item() if len(os_current_list.index) == 1 else np.nan

        divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['Divisor']
        divisor_back = divisor_back_list.item() if len(divisor_back_list.index) == 1 else np.nan\
        
        # mcap_divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['MCapDivisor'] #
        # mcap_divisor_back = mcap_divisor_back_list.item() if len(mcap_divisor_back_list.index) == 1 else np.nan

        mcap_divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['MCap_Divisor'] 
        mcap_divisor_back = mcap_divisor_back_list.item() if len(mcap_divisor_back_list.index) == 1 else np.nan


        prev_close_list = indexhistory.loc[indexhistory['TICKER'] == row['IndexName']]['OPEN']
        prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

        print(row['IndexName'])

        if os_prev == os_current:
            print("OS previous is equal to OS current")

            ff_close_prev_list = merge_sub_industry_divisor.loc[merge_sub_industry_divisor['IndexName'] == row['IndexName']]['SumFF_Open']
            ff_close_prev = ff_close_prev_list.item() if len(ff_close_prev_list.index) == 1 else np.nan

            mcap_open_prev_list = merge_sub_industry_divisor.loc[merge_sub_industry_divisor['IndexName'] == row['IndexName']]['SumMCap_Open']   
            mcap_open_prev = mcap_open_prev_list.item() if len(mcap_open_prev_list.index) == 1 else np.nan  

            divisor_current = divisor_back
            mcap_divisor_current = mcap_divisor_back    

            print("FF_Open_Sum: ", ff_close_prev)
            print("Mcap_Divisor: ",mcap_divisor_current)
            print("Divisor: ", divisor_current)

            merge_sub_industry_divisor.loc[index, 'IndexValue'] = ff_close_prev
            merge_sub_industry_divisor.loc[index, 'SumMCap_Open'] = mcap_open_prev
            merge_sub_industry_divisor.loc[index, 'MCapDivisor'] = mcap_divisor_current
            merge_sub_industry_divisor.loc[index, 'Divisor'] = divisor_current
            merge_sub_industry_divisor.loc[index, 'OS'] = os_prev

        else:
            print("OS previous is not equal to OS current")
            print("OS previous: ", os_prev)
            print("OS current: ", os_current)

            ff_close_current_list = sub_industry_divisor_list.loc[sub_industry_divisor_list['SubIndustryIndexName'] == row['IndexName']]['ff_open_sum']
            ff_close_current = ff_close_current_list.item() if len(ff_close_current_list.index) == 1 else np.nan

            mcap_open_current_list = sub_industry_divisor_list.loc[sub_industry_divisor_list['SubIndustryIndexName'] == row['IndexName']]['mcap_open_sum']
            mcap_open_current = mcap_open_current_list.item() if len(mcap_open_current_list.index) == 1 else np.nan

            current_company_count = merge_sub_industry_divisor.loc[merge_sub_industry_divisor['IndexName'] == row['IndexName']]['Count'].values[0]
            prev_company_count = len(prev_industry_list.loc[prev_industry_list['SubIndustryIndexName'] == row['IndexName']])

            if prev_close is not np.nan:

              prev_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['CLOSE']

              prev_mcap_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['MCap_CLOSE']   #

              current_companies = current_industry_list.loc[current_industry_list['SubIndustryIndexName'] == row['IndexName']]
              print("current_company_count: ", current_company_count)
              
              # prev_companies = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]
              # print("prev_company_count: ", prev_company_count)
              current_companies_prev_list = prev_industry_list.loc[prev_industry_list['CompanyCode'].isin(current_companies['CompanyCode'])]
              print("Companies from current list on previous list :", len(current_companies_prev_list))
                
              new_companies = current_companies[~current_companies['CompanyCode'].isin(current_companies_prev_list['CompanyCode'])]
              print("new_companies: ", len(new_companies))
              
              
              if(len(current_companies_prev_list)==len(current_companies)):
                print("only OS has changed, no new companies added")
                # merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))
                
              elif(len(current_companies_prev_list)<len(current_companies)):
                print("new companies added")
                # prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
                # prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
                
                # new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
                # addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
                
              elif(len(current_companies_prev_list)>len(current_companies)):
                print("companies removed")
              
              merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))

              prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
              print("prev_companies_with_same_OS: ", len(prev_companies_with_same_OS))
              # print(row['IndexName'])
              # print(prev_companies_with_same_OS.columns)
              prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['SubIndustryIndexName_prev'] == row['IndexName']]['FF_Close_prev'].sum() 
              prev_mcap_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS['SubIndustryIndexName_prev'] == row['IndexName']]['MCap_Close_prev'].sum()
              # prev_close_sum = prev_industry_list.loc[prev_industry_list['SubIndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
              print("prev_close_sum: ", prev_close_sum) 
              print("prev_mcap_close_sum: ", prev_mcap_close_sum)   
              # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close']
              # keep only row with the same companycode from current companies
              # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
              
              prev_companies_with_different_OS = merged_companies[merged_companies['OS_prev'] != merged_companies['OS_current']]
              print("prev_companies_with_different_OS: ", len(prev_companies_with_different_OS))
              # companies_with_diff_OS = prev_companies_with_different_OS[prev_companies_with_different_OS['IndustryIndexName'] == row['IndexName']]
              
              prev_companies_with_different_OS['prev_close_sum_for_diff_OS'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['FreeFloat_current'] * prev_companies_with_different_OS['PrevClose_current']
              changed_prev_close_sum = prev_companies_with_different_OS['prev_close_sum_for_diff_OS'].sum()
              prev_companies_with_different_OS['prev_MCap_Close'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['PrevClose_current']
              changed_prev_mcap_close_sum = prev_companies_with_different_OS['prev_MCap_Close'].sum()
              
              print("changed_prev_close_sum: ", changed_prev_close_sum)
              print("changed_prev_mcap_close_sum: ", changed_prev_mcap_close_sum)
              
              # prev_mcap_close_sum = prev_industry_list.loc[prev_industry_list['SectorIndexName'] == row['IndexName']]['MCap_Close'].sum()
              print("", new_companies[['OS', 'FreeFloat', 'PrevClose']])

              new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
              addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
              print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)

              new_companies['prev_MCap_Close'] = new_companies['OS'] * new_companies['PrevClose']
              addition_to_prev_mcap_close_sum = new_companies['prev_MCap_Close'].sum()
              print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)


              if not prev_close_index.empty:
                  prev_close_index = prev_close_index.iloc[0]
                  prev_mcap_close_index = prev_mcap_close_index.iloc[0]
              else:
                  prev_close_index = np.nan
                  prev_mcap_close_index = np.nan

              divisor = (prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum) / prev_close_index
              mcap_divisor = (prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum) / prev_mcap_close_index
              print()
              print("prev_close_sum: ", prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum)
              print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)
              print("prev_close_index: ", prev_close_index)
              print("divisor: ", divisor)
              print("prev_mcap_close_sum: ", prev_mcap_close_sum)
              print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)
              print("prev_mcap_close_index: ", prev_mcap_close_index)
              print("mcap_divisor : ", mcap_divisor)

              merge_sub_industry_divisor.loc[index, 'IndexValue'] = prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum
              merge_sub_industry_divisor.loc[index, 'MCap_Open_sum'] = prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum
              merge_sub_industry_divisor.loc[index, 'MCapDivisor'] = mcap_divisor
              merge_sub_industry_divisor.loc[index, 'Divisor'] = divisor
              merge_sub_industry_divisor.loc[index, 'OS'] = os_current

            else:
              divisor_current = ff_close_current / 1000
              MCapdivisor_current = mcap_open_current / 1000
              print("Divisor: ", divisor_current)
              print("FF_Open_sum: ", ff_close_current)
              print("Mcap_Open_sum: ", mcap_open_current)
              print("MCapDivisor: ", MCapdivisor_current)
              merge_sub_industry_divisor.loc[index, 'MCap_Open_sum'] = mcap_open_current
              merge_sub_industry_divisor.loc[index, 'Divisor'] = divisor_current
              merge_sub_industry_divisor.loc[index, 'IndexValue'] = ff_close_current
              merge_sub_industry_divisor.loc[index, 'MCapDivisor'] = MCapdivisor_current
              merge_sub_industry_divisor.loc[index, 'OS'] = os_current

    return merge_sub_industry_divisor
  




  def insert_sector_divisor(self,merge_sector_divisor, conn, cur, date):
    """ Insert Sector Divisor data into database.

    Args:
        merge_sector_divisor = Data of sector divisor.

    Operation:
        Export the data into SectorDivisor.csv file
        and insert into SectorDivisor table.
    """

    merge_sector_divisor['Date'] = date
    # rename columns of merge_sector_divisor
    merge_sector_divisor = merge_sector_divisor.rename(columns = {'MCapDivisor':'MCap_Divisor', 'MCap_Open_sum' : 'SumMCap_Open'})

    merge_sector_divisor = merge_sector_divisor.loc[:,~merge_sector_divisor.columns.duplicated()]

    merge_sector_divisor = merge_sector_divisor[['IndexName', 'IndexValue', 'Divisor','MCap_Divisor','SumMCap_Open','Date', 'OS']]

    exportfilename = "SectorDivisor.csv"
    exportfile = open(exportfilename,"w")
    merge_sector_divisor.to_csv(exportfile, header=True, index=False, lineterminator='\r')
    exportfile.close()


    copy_sql = """
        COPY public."SectorDivisor" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
    with open(exportfilename, 'r') as f:

      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


  def insert_subsector_divisor(self,merge_subsector_divisor, conn, cur, date):
    """ Insert SubSector Divisor data into database.

    Args:
        merge_subsector_divisor = Data of subsector divisor.

    Operation:
        Export the data into SubSectorDivisor.csv file
        and insert into SubSectorDivisor table.
    """

    merge_subsector_divisor['Date'] = date
    merge_subsector_divisor = merge_subsector_divisor.rename(columns = {'MCapDivisor':'MCap_Divisor', 'MCap_Open_sum' : 'SumMCap_Open'})

    merge_subsector_divisor = merge_subsector_divisor.loc[:,~merge_subsector_divisor.columns.duplicated()]

    merge_subsector_divisor = merge_subsector_divisor[['IndexName', 'IndexValue', 'Divisor','MCap_Divisor','SumMCap_Open','Date', 'OS']]

    exportfilename = "SubSectorDivisor.csv"
    exportfile = open(exportfilename,"w")
    merge_subsector_divisor.to_csv(exportfile, header=True, index=False,  lineterminator='\r')
    exportfile.close()


    copy_sql = """
        COPY public."SubSectorDivisor" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
    with open(exportfilename, 'r') as f:

      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


  def insert_industry_divisor(self,merge_industry_divisor, conn, cur, date):
    """ Insert Industry Divisor data into database.

    Args:
        merge_industry_divisor = Data of Industry Divisor.

    Operation:
        Export the data into IndustryDivisor.csv file
        and insert into IndustryDivisor table.
     """

    merge_industry_divisor['Date'] = date

    merge_industry_divisor = merge_industry_divisor.rename(columns = {'MCapDivisor':'MCap_Divisor', 'MCap_Open_sum' : 'SumMCap_Open'})

    merge_industry_divisor = merge_industry_divisor.loc[:,~merge_industry_divisor.columns.duplicated()]


    merge_industry_divisor = merge_industry_divisor[['IndexName', 'IndexValue', 'Divisor','MCap_Divisor','SumMCap_Open','Date', 'OS']]

    exportfilename = "IndustryDivisor.csv"
    exportfile = open(exportfilename,"w")
    merge_industry_divisor.to_csv(exportfile, header=True, index=False,  lineterminator='\r')
    exportfile.close()


    copy_sql = """
        COPY public."IndustryDivisor" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
    with open(exportfilename, 'r') as f:

      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)

  def insert_sub_industry_divisor(self,merge_sub_industry_divisor, conn, cur, date):
    """ Insert Industry Divisor data into database.

    Args:
        merge_industry_divisor = Data of Industry Divisor.

    Operation:
        Export the data into IndustryDivisor.csv file
        and insert into IndustryDivisor table.
     """

    merge_sub_industry_divisor['Date'] = date
    merge_sub_industry_divisor= merge_sub_industry_divisor.rename(columns = {'MCapDivisor':'MCap_Divisor', 'MCap_Open_sum' : 'SumMCap_Open'})

    merge_sub_industry_divisor = merge_sub_industry_divisor.loc[:,~merge_sub_industry_divisor.columns.duplicated()]

    merge_sub_industry_divisor = merge_sub_industry_divisor[['IndexName', 'IndexValue', 'Divisor','MCap_Divisor','SumMCap_Open','Date', 'OS']]

    exportfilename = "SubIndustryDivisor.csv"
    exportfile = open(exportfilename,"w")
    merge_sub_industry_divisor.to_csv(exportfile, header=True, index=False,  lineterminator='\r')
    exportfile.close()


    copy_sql = """
        COPY public."SubIndustryDivisor" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """
    with open(exportfilename, 'r') as f:

      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


  def gen_sector_index(self,conn, date):
    """ Generate the sector index

    Operation:
        Fetch the data from SectorDivisor and IndustryList for current date,
        and calculate the Open, close, high, low, PE , EPS and Earning Growth.

        'open' = ff_open/divisor, 'high' = ff_high/divisor, 'low' = ff_low/divisor,

        'close' = ff_close/divisor, 'earnings growth' = (eps – eps back / eps back) * 100 ,

        'pe' = sum os close / ff earning, eps = ff earning / ff equity

    Return:
        Data of sector index.
     """
      # Generating sector index list

    sector_divisor_sql = 'SELECT * FROM public."SectorDivisor" WHERE "Date" = \''+date+'\' ;'
    sector_divisor = sqlio.read_sql_query(sector_divisor_sql, con = conn)

    sector_ff_ohlc_sql = 'SELECT "SectorIndexName", "Sector", \
                        SUM("OS_Close") AS sum_os_close,\
                        SUM("FF_Open") AS ff_open_sum, \
                        SUM("FF_High") AS ff_high_sum, \
                        SUM("FF_Low") AS ff_low_sum, \
                        SUM("FF_Close") AS ff_close_sum, \
                        SUM("Volume") AS sum_vol, \
                        SUM("OS") AS os_sum, \
                        SUM("PAT") AS earnings, \
                        SUM("Equity") AS sum_equity, \
                        SUM("prev_pat") AS prev_earnings, \
                        SUM("prev_equity") AS prev_equity, \
                        count(*) AS company_count\
                        FROM public."IndustryList" \
                        WHERE "GenDate" = \''+date+'\' AND "SectorIndexName" is not null \
                        GROUP BY "SectorIndexName", "Sector" ;'
    sector_ff_ohlc = sqlio.read_sql_query(sector_ff_ohlc_sql, con = conn)
    # print("\n\n{}\n\n".format(sector_ff_ohlc))
    for index, row in sector_ff_ohlc.iterrows():

     
      ff_open_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['ff_open_sum']
      if not ff_open_list.empty: 
          ff_open = ff_open_list.iloc[0]
      else:
          ff_open = np.nan
          print(sector_ff_ohlc[index])
          print("ff_open: ", ff_open_list)
      # ff_open = ff_open_list.item() if len(ff_open_list.index) == 1 else np.nan

      ff_high_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['ff_high_sum']
      if not ff_high_list.empty:
        ff_high = ff_high_list.iloc[0]
      else:
        ff_high = np.nan

      # ff_high = ff_high_list.item() if len(ff_high_list.index) == 1 else np.nan
      # print("ff_high: ", ff_high)
      ff_low_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['ff_low_sum']
      if not ff_low_list.empty:
        ff_low = ff_low_list.iloc[0]
      else:
        ff_low = np.nan

      # ff_low = ff_low_list.item() if len(ff_low_list.index) == 1 else np.nan
      # print("ff_low: ", ff_low)
      ff_close_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['ff_close_sum']
      if not ff_close_list.empty:
         ff_close = ff_close_list.iloc[0]
      else:
         ff_close = np.nan
      # ff_close = ff_close_list.item() if len(ff_close_list.index) == 1 else np.nan
      # print("ff_close: ", ff_close)
      # ff_os_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['os_sum']
      # if not ff_os_list.empty:
      #   ff_os = ff_os_list.iloc[0]
      # else:
      #   ff_os = np.nan

      # ff_os = ff_os_list.item() if len(ff_os_list.index) == 1 else np.nan

      ff_earnings_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['earnings']
      if not ff_earnings_list.empty:  
        ff_earnings = ff_earnings_list.iloc[0]
      else:
        ff_earnings = np.nan
      # ff_earnings = ff_earnings_list.item() if len(ff_earnings_list.index) == 1 else np.nan

      ff_equity_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['sum_equity']
      if not ff_equity_list.empty:
        ff_equity = ff_equity_list.iloc[0]
      else:
        ff_equity = np.nan

      # ff_equity = ff_equity_list.item() if len(ff_equity_list.index) == 1 else np.nan

      prev_earnings_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['prev_earnings']
      if not prev_earnings_list.empty:
        prev_earnings = prev_earnings_list.iloc[0]
      else:
        prev_earnings = np.nan
      # prev_earnings = prev_earnings_list.item() if len(prev_earnings_list.index) == 1 else np.nan

      prev_equity_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['prev_equity']
      if not prev_equity_list.empty: 
        prev_equity = prev_equity_list.iloc[0]
      else:
        prev_equity = np.nan  
      # prev_equity = prev_equity_list.item() if len(prev_equity_list.index) == 1 else np.nan

      divisor_list = sector_divisor.loc[(sector_divisor['IndexName']==row['SectorIndexName'])]['Divisor']
      if not divisor_list.empty:
        divisor = divisor_list.iloc[0]
      else:
        divisor = np.nan

      # divisor = divisor_list.item() if (len(divisor_list.index) == 1 and divisor_list.item() is not None)  else np.nan
      # print("\ndivisor: ", divisor , "\n", len(divisor_list.index), divisor_list,  "\n\n")
      sum_os_close_list = sector_ff_ohlc.loc[(sector_ff_ohlc['SectorIndexName']==row['SectorIndexName'])]['sum_os_close']
      if not sum_os_close_list.empty:
        sum_os_close = sum_os_close_list.iloc[0]
      else:
        sum_os_close = np.nan

      # sum_os_close = sum_os_close_list.item() if len(sum_os_close_list.index) == 1 else np.nan

      # print(sector_ff_ohlc[sector_ff_ohlc['SectorIndexName']==row['SectorIndexName']])
      # print("Divisor: ", divisor, "ff_open: ", ff_open, "ff_high: ", ff_high, "ff_low: ", ff_low, "ff_close: ", ff_close, "ff_earnings: ", ff_earnings, "ff_equity: ", ff_equity, "prev_earnings: ", prev_earnings, "prev_equity: ", prev_equity, "sum_os_close: ", sum_os_close, flush = True)
      
      # div_open = ff_open/divisor
      # div_high = ff_high/divisor
      # div_low = ff_low/divisor
      # div_close = ff_close/divisor
      div_open = ff_open/divisor if divisor and ff_open is not None and divisor != 0 else np.nan
      div_high = ff_high/divisor if divisor and ff_high is not None and divisor != 0 else np.nan
      div_low = ff_low/divisor if divisor and ff_low is not None and divisor != 0 else np.nan
      div_close = ff_close/divisor if divisor and ff_close is not None and divisor != 0 else np.nan
      # if(str(div_open) != 'nan'):
      #   print("\nFFOPEN: {} \nDIVISOR: {}\nOPENVALUE: {}".format(ff_open, divisor, ff_open/divisor), flush = True)

      pe = (sum_os_close)/ff_earnings if ff_earnings !=0 else np.nan
      eps = ff_earnings/ff_equity if ff_equity !=0 else np.nan
      eps_back = prev_earnings/prev_equity if prev_equity !=0 else np.nan
      earnings_growth = ((eps-eps_back)/abs(eps_back))*100 if eps_back != 0 else np.nan


      sector_ff_ohlc.loc[index, 'Open'] = div_open
      sector_ff_ohlc.loc[index, 'High'] = div_high
      sector_ff_ohlc.loc[index, 'Low'] = div_low
      sector_ff_ohlc.loc[index, 'Close'] = div_close
      sector_ff_ohlc.loc[index, 'PE'] = pe
      sector_ff_ohlc.loc[index, 'EPS'] = eps
      sector_ff_ohlc.loc[index, 'Earnings Growth'] = earnings_growth


    return sector_ff_ohlc


  def insert_sector_index(self,sector_index, conn, cur, date):
    """ Insert sector index data into database.

    Args:
        sector_index = data of Open, close, high, low, PE , EPS
                      and Earning Growth of sector index.

    Operation:
        Export the data into SectorIndexList.csv file
        and insert into SectorIndexList table.
    """

    sector_index['GenDate'] = date

    sector_index = sector_index[['GenDate', 'SectorIndexName', 'Sector', 'Open', 'High', 'Low', 'Close', 'sum_vol', 'PE', 'EPS', \
                                    'company_count', 'os_sum', 'Earnings Growth']]

    exportfilename = "SectorIndexList.csv"
    exportfile = open(exportfilename,"w")
    sector_index.to_csv(exportfile, header=True, index=False,  lineterminator = '\r')
    exportfile.close()


    copy_sql = """
        COPY public."SectorIndexList" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

    with open(exportfilename, 'r') as f:
      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


  def gen_subsector_index(self,conn, date):
    """ Generate the subsector index

    Operation:
        Fetch the data from SubSectorDivisor and IndustryList for current date,
        and calculate the Open, close, high, low, PE , EPS and Earning Growth

        'open' = ff_open/divisor, 'high' = ff_high/divisor, 'low' = ff_low/divisor,

        'close' = ff_close/divisor, 'earnings growth' = (eps – eps back / eps back) * 100 ,

        'pe' = sum os close / ff earning, eps = ff earning / ff equity.

    Return:
        Data of SubSector index.
    """
      # Generating SubSector index list

    subsector_divisor_sql = 'SELECT * FROM public."SubSectorDivisor" WHERE "Date" = \''+date+'\' ;'
    subsector_divisor = sqlio.read_sql_query(subsector_divisor_sql, con = conn)

    subsector_ff_ohlc_sql = 'SELECT "SubSectorIndexName", "SubSector", \
                        SUM("OS_Close") AS sum_os_close, \
                        SUM("FF_Open") AS ff_open_sum, \
                        SUM("FF_High") AS ff_high_sum, \
                        SUM("FF_Low") AS ff_low_sum, \
                        SUM("FF_Close") AS ff_close_sum, \
                        SUM("Volume") AS sum_vol, \
                        SUM("OS") AS os_sum, \
                        SUM("PAT") AS earnings, \
                        SUM("Equity") AS sum_equity, \
                        SUM("prev_pat") AS prev_earnings, \
                        SUM("prev_equity") AS prev_equity, \
                        count(*) AS company_count\
                        FROM public."IndustryList" \
                        WHERE "GenDate" = \''+date+'\' AND "SubSectorIndexName" is not null \
                        GROUP BY "SubSectorIndexName", "SubSector" ;'
    subsector_ff_ohlc = sqlio.read_sql_query(subsector_ff_ohlc_sql, con = conn)

    for index, row in subsector_ff_ohlc.iterrows():
      
      
      ff_open_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['ff_open_sum']
      if not ff_open_list.empty: 
        ff_open = ff_open_list.iloc[0]
      else:
          ff_open = np.nan
          print(subsector_ff_ohlc[index])
          print("ff_open: ", ff_open_list)
      # ff
      # ff_open = ff_open_list.item() if len(ff_open_list.index) == 1 else np.nan

      ff_high_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['ff_high_sum']
      if not ff_high_list.empty:
        ff_high = ff_high_list.iloc[0]
      else:
        ff_high = np.nan
      # ff_high = ff_high_list.item() if len(ff_high_list.index) == 1 else np.nan

      ff_low_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['ff_low_sum']
      if not ff_low_list.empty:
        ff_low = ff_low_list.iloc[0]
      else:
        ff_low = np.nan
      # ff_low = ff_low_list.item() if len(ff_low_list.index) == 1 else np.nan

      ff_close_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['ff_close_sum']
      if not ff_close_list.empty:
         ff_close = ff_close_list.iloc[0]
      else:
         ff_close = np.nan
      # ff_close = ff_close_list.item() if len(ff_close_list.index) == 1 else np.nan

      ff_os_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['os_sum']
      ff_os = ff_os_list.item() if len(ff_os_list.index) == 1 else np.nan

      ff_earnings_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['earnings']
      if not ff_earnings_list.empty:  
        ff_earnings = ff_earnings_list.iloc[0]
      else:
        ff_earnings = np.nan
      # ff_earnings = ff_earnings_list.item() if len(ff_earnings_list.index) == 1 else np.nan

      ff_equity_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['sum_equity']
      if not ff_equity_list.empty:
        ff_equity = ff_equity_list.iloc[0]
      else:
        ff_equity = np.nan
      # ff_equity = ff_equity_list.item() if len(ff_equity_list.index) == 1 else np.nan

      prev_earnings_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['prev_earnings']
      if not prev_earnings_list.empty:
        prev_earnings = prev_earnings_list.iloc[0]
      else:
        prev_earnings = np.nan
      # prev_earnings = prev_earnings_list.item() if len(prev_earnings_list.index) == 1 else np.nan

      prev_equity_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['prev_equity']
      if not prev_equity_list.empty:
        prev_equity = prev_equity_list.iloc[0]
      else:
        prev_equity = np.nan
      # prev_equity = prev_equity_list.item() if len(prev_equity_list.index) == 1 else np.nan

      divisor_list = subsector_divisor.loc[(subsector_divisor['IndexName']==row['SubSectorIndexName'])]['Divisor']
      if not divisor_list.empty:
        divisor = divisor_list.iloc[0]
      else:
        divisor = np.nan
      # divisor = divisor_list.item() if len(divisor_list.index) == 1 else np.nan

      # if(divisor == np.nan):
      #   print("Devisor gen_subsector_index: ", divisor)

      sum_os_close_list = subsector_ff_ohlc.loc[(subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName'])]['sum_os_close']
      if not sum_os_close_list.empty:
        sum_os_close = sum_os_close_list.iloc[0]
      else:
        sum_os_close = np.nan
      # sum_os_close = sum_os_close_list.item() if len(sum_os_close_list.index) == 1 else np.nan


      # div_open = ff_open/divisor
      # div_high = ff_high/divisor
      # div_low = ff_low/divisor
      # div_close = ff_close/divisor
      # print(subsector_ff_ohlc[subsector_ff_ohlc['SubSectorIndexName']==row['SubSectorIndexName']])

      # print("Divisor: ", divisor, "ff_open: ", ff_open, "ff_high: ", ff_high, "ff_low: ", ff_low, "ff_close: ", ff_close, "ff_earnings: ", ff_earnings, "ff_equity: ", ff_equity, "prev_earnings: ", prev_earnings, "prev_equity: ", prev_equity, "sum_os_close: ", sum_os_close, flush = True)
      div_open = ff_open/divisor if divisor and ff_open is not None and divisor != 0 else np.nan
      div_high = ff_high/divisor if divisor and ff_high is not None and divisor != 0 else np.nan
      div_low = ff_low/divisor if divisor and ff_low is not None and divisor != 0 else np.nan
      div_close = ff_close/divisor if divisor and ff_close is not None and divisor != 0 else np.nan

      pe = (sum_os_close)/ff_earnings if ff_earnings !=0 else np.nan
      eps = ff_earnings/ff_equity if ff_equity !=0 else np.nan
      eps_back = prev_earnings/prev_equity if prev_equity !=0 else np.nan
      earnings_growth = ((eps-eps_back)/abs(eps_back))*100 if eps_back != 0 else np.nan


      subsector_ff_ohlc.loc[index, 'Open'] = div_open
      subsector_ff_ohlc.loc[index, 'High'] = div_high
      subsector_ff_ohlc.loc[index, 'Low'] = div_low
      subsector_ff_ohlc.loc[index, 'Close'] = div_close
      subsector_ff_ohlc.loc[index, 'PE'] = pe
      subsector_ff_ohlc.loc[index, 'EPS'] = eps
      subsector_ff_ohlc.loc[index, 'Earnings Growth'] = earnings_growth


    return subsector_ff_ohlc


  def insert_subsector_index(self,subsector_index, conn, cur, date):
    """ Insert subsector index data into database.

    Args:
        subsector_index = data of Open, close, high, low, PE , EPS
                      and Earning Growth of subsector index.

    Operation:
        Export the data into SubSectorIndexList.csv file
        and insert into SubSectorIndexList table.
    """

    subsector_index['GenDate'] = date

    subsector_index = subsector_index[['GenDate', 'SubSectorIndexName', 'SubSector', 'Open', 'High', 'Low', 'Close', 'sum_vol', 'PE', 'EPS', \
                                'company_count', 'os_sum', 'Earnings Growth']]

    exportfilename = "SubSectorIndexList.csv"
    exportfile = open(exportfilename,"w")
    subsector_index.to_csv(exportfile, header=True, index=False,  lineterminator = '\r')
    exportfile.close()


    copy_sql = """
        COPY public."SubSectorIndexList" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

    with open(exportfilename, 'r') as f:
      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


  def gen_industry_index(self,conn, date):
    """ Generate the Industry index

    Operation:
        Fetch the data from IndustryDivisor and IndustryList for current date,
        and calculate the Open, close, high, low, PE , EPS and Earning Growth,

        'open' = ff_open/divisor, 'high' = ff_high/divisor, 'low' = ff_low/divisor,

        'close' = ff_close/divisor, 'earnings growth' = (eps – eps back / eps back) * 100 ,

        'pe' = sum os close / ff earning, eps = ff earning / ff equity.

    Return:
        Data of Industry index.
    """
      # Generating Industry index list

    industry_divisor_sql = 'SELECT * FROM public."IndustryDivisor" WHERE "Date" = \''+date+'\' ;'
    industry_divisor = sqlio.read_sql_query(industry_divisor_sql, con = conn)

    industry_ff_ohlc_sql = 'SELECT "IndustryIndexName", "Industry", \
                        SUM("OS_Close") AS sum_os_close,\
                        SUM("FF_Open") AS ff_open_sum, \
                        SUM("FF_High") AS ff_high_sum, \
                        SUM("FF_Low") AS ff_low_sum, \
                        SUM("FF_Close") AS ff_close_sum, \
                        SUM("Volume") AS sum_vol, \
                        SUM("OS") AS os_sum, \
                        SUM("PAT") AS earnings, \
                        SUM("Equity") AS sum_equity,   \
                        SUM("prev_pat") AS prev_earnings, \
                        SUM("prev_equity") AS prev_equity, \
                        count(*) AS company_count\
                        FROM public."IndustryList" \
                        WHERE "GenDate" = \''+date+'\' AND "IndustryIndexName" is not null \
                        GROUP BY "IndustryIndexName", "Industry" ;'
    industry_ff_ohlc = sqlio.read_sql_query(industry_ff_ohlc_sql, con = conn)

    for index, row in industry_ff_ohlc.iterrows():


      ff_open_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['ff_open_sum']
      if not ff_open_list.empty: 
        ff_open = ff_open_list.iloc[0]
      else:
          ff_open = np.nan
      # ff_open = ff_open_list.item() if len(ff_open_list.index) == 1 else np.nan
      # ff_open = ff_open_list.item() if len(ff_open_list.index) == 1 else np.nan

      ff_high_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['ff_high_sum']
      if not ff_high_list.empty:
        ff_high = ff_high_list.iloc[0]
      else:
        ff_high = np.nan
        print(industry_ff_ohlc[index])
        print("ff_high: ", ff_high_list)
      # ff_high = ff_high_list.item() if len(ff_high_list.index) == 1 else np.nan

      ff_low_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['ff_low_sum']
      if not ff_low_list.empty:
        ff_low = ff_low_list.iloc[0]
      else:
        ff_low = np.nan
      # ff_low = ff_low_list.item() if len(ff_low_list.index) == 1 else np.nan

      ff_close_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['ff_close_sum']
      if not ff_close_list.empty:
         ff_close = ff_close_list.iloc[0]
      else:
         ff_close = np.nan
      # ff_close = ff_close_list.item() if len(ff_close_list.index) == 1 else np.nan

      ff_os_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['os_sum']
      ff_os = ff_os_list.item() if len(ff_os_list.index) == 1 else np.nan

      ff_earnings_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['earnings']
      if not ff_earnings_list.empty:  
        ff_earnings = ff_earnings_list.iloc[0]
      else:
        ff_earnings = np.nan
      # ff_earnings = ff_earnings_list.item() if len(ff_earnings_list.index) == 1 else np.nan

      ff_equity_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['sum_equity']
      if not ff_equity_list.empty:
        ff_equity = ff_equity_list.iloc[0]
      else:
        ff_equity = np.nan
      # ff_equity = ff_equity_list.item() if len(ff_equity_list.index) == 1 else np.nan

      prev_earnings_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['prev_earnings']
      if not prev_earnings_list.empty:
        prev_earnings = prev_earnings_list.iloc[0]
      else:
        prev_earnings = np.nan
      # prev_earnings = prev_earnings_list.item() if len(prev_earnings_list.index) == 1 else np.nan

      prev_equity_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['prev_equity']
      if not prev_equity_list.empty: 
        prev_equity = prev_equity_list.iloc[0]  
      else:
        prev_equity = np.nan
      # prev_equity = prev_equity_list.item() if len(prev_equity_list.index) == 1 else np.nan

      divisor_list = industry_divisor.loc[(industry_divisor['IndexName']==row['IndustryIndexName'])]['Divisor']
      if not divisor_list.empty:
        divisor = divisor_list.iloc[0]
      else:
        divisor = np.nan
      # divisor = divisor_list.item() if len(divisor_list.index) == 1 else np.nan

      sum_os_close_list = industry_ff_ohlc.loc[(industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName'])]['sum_os_close']
      if not sum_os_close_list.empty:
        sum_os_close = sum_os_close_list.iloc[0]
      else:
        sum_os_close = np.nan
      # sum_os_close = sum_os_close_list.item() if len(sum_os_close_list.index) == 1 else np.nan

      # if np.isnan(divisor) or divisor == 0:
      #   print("Devisor gen_industry_index: ", divisor)

      # print(industry_ff_ohlc[industry_ff_ohlc['IndustryIndexName']==row['IndustryIndexName']])
      # print("Divisor: ", divisor, "ff_open: ", ff_open, "ff_high: ", ff_high, "ff_low: ", ff_low, "ff_close: ", ff_close, "ff_earnings: ", ff_earnings, "ff_equity: ", ff_equity, "prev_earnings: ", prev_earnings, "prev_equity: ", prev_equity, "sum_os_close: ", sum_os_close, flush = True)


      # div_open = ff_open/divisor if divisor != 0 and not np.isnan(ff_open) else np.nan
      # div_high = ff_high/divisor if divisor != 0 and not np.isnan(ff_high) else np.nan
      # div_low = ff_low/divisor if divisor != 0 and not np.isnan(ff_low) else np.nan
      # div_close = ff_close/divisor if divisor != 0 and not np.isnan(ff_close) else np.nan

      div_open = ff_open/divisor if divisor and ff_open is not None and divisor != 0 else np.nan
      div_high = ff_high/divisor if divisor and ff_high is not None and divisor != 0 else np.nan
      div_low = ff_low/divisor if divisor and ff_low is not None and divisor != 0 else np.nan
      div_close = ff_close/divisor if divisor and ff_close is not None and divisor != 0 else np.nan


      pe = (sum_os_close)/ff_earnings if ff_earnings !=0 else np.nan
      eps = ff_earnings/ff_equity if ff_equity !=0 else np.nan
      eps_back = prev_earnings/prev_equity if prev_equity !=0 else np.nan
      earnings_growth = ((eps-eps_back)/abs(eps_back))*100 if eps_back != 0 else np.nan


      industry_ff_ohlc.loc[index, 'Open'] = div_open
      industry_ff_ohlc.loc[index, 'High'] = div_high
      industry_ff_ohlc.loc[index, 'Low'] = div_low
      industry_ff_ohlc.loc[index, 'Close'] = div_close
      industry_ff_ohlc.loc[index, 'PE'] = pe
      industry_ff_ohlc.loc[index, 'EPS'] = eps
      industry_ff_ohlc.loc[index, 'Earnings Growth'] = earnings_growth


    return industry_ff_ohlc


  def insert_industry_index(self,industry_index, conn, cur, date):
    """ Insert Industry index data into database.

    Args:
        industry_index = data of Open, close, high, low, PE , EPS
                      and Earning Growth of industry index.

    Operation:
        Export the data into IndustryIndexList.csv file
        and insert into IndustryIndexList table.
    """

    industry_index['GenDate'] = date
    industry_index = industry_index[['GenDate', 'IndustryIndexName', 'Industry', 'Open', 'High', 'Low', 'Close', 'sum_vol', 'PE', 'EPS', \
                                'company_count', 'os_sum', 'Earnings Growth']]

    exportfilename = "IndustryIndexList.csv"
    exportfile = open(exportfilename,"w")
    industry_index.to_csv(exportfile, header=True, index=False,  lineterminator = '\r')
    exportfile.close()


    copy_sql = """
        COPY public."IndustryIndexList" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

    with open(exportfilename, 'r') as f:
      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


  def gen_sub_industry_index(self,conn, date):
    """ Generate the Industry index

    Operation:
        Fetch the data from IndustryDivisor and IndustryList for current date,
        and calculate the Open, close, high, low, PE , EPS and Earning Growth,

        'open' = ff_open/divisor, 'high' = ff_high/divisor, 'low' = ff_low/divisor,

        'close' = ff_close/divisor, 'earnings growth' = (eps – eps back / eps back) * 100 ,

        'pe' = sum os close / ff earning, eps = ff earning / ff equity.

    Return:
        Data of Industry index.
    """
      # Generating Industry index list

    industry_divisor_sql = 'SELECT * FROM public."SubIndustryDivisor" WHERE "Date" = \''+date+'\' ;'
    industry_divisor = sqlio.read_sql_query(industry_divisor_sql, con = conn)

    industry_ff_ohlc_sql = 'SELECT "SubIndustryIndexName", "SubIndustry", \
                        SUM("OS_Close") AS sum_os_close,\
                        SUM("FF_Open") AS ff_open_sum, \
                        SUM("FF_High") AS ff_high_sum, \
                        SUM("FF_Low") AS ff_low_sum, \
                        SUM("FF_Close") AS ff_close_sum, \
                        SUM("Volume") AS sum_vol, \
                        SUM("OS") AS os_sum, \
                        SUM("PAT") AS earnings, \
                        SUM("Equity") AS sum_equity,   \
                        SUM("prev_pat") AS prev_earnings, \
                        SUM("prev_equity") AS prev_equity, \
                        count(*) AS company_count\
                        FROM public."IndustryList" \
                        WHERE "GenDate" = \''+date+'\' AND "SubIndustryIndexName" is not null \
                        GROUP BY "SubIndustryIndexName", "SubIndustry" ;'
    industry_ff_ohlc = sqlio.read_sql_query(industry_ff_ohlc_sql, con = conn)

    for index, row in industry_ff_ohlc.iterrows():
      # Assuming 'row' is from a DataFrame, likely iterated using DataFrame.iterrows()

    
      ff_open_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['ff_open_sum']
      if not ff_open_list.empty: 
        ff_open = ff_open_list.iloc[0]
      else:
          ff_open = np.nan
          print(industry_ff_ohlc[index])
          print("ff_open: ", ff_open_list)
      # ff_open = ff_open_list.item() if len(ff_open_list.index) == 1 else np.nan


      ff_high_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['ff_high_sum']
      if not ff_high_list.empty:
        ff_high = ff_high_list.iloc[0]
      else:
        ff_high = np.nan
      # ff_high = ff_high_list.item() if len(ff_high_list.index) == 1 else np.nan

      ff_low_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['ff_low_sum']
      if not ff_low_list.empty:
        ff_low = ff_low_list.iloc[0]
      else:
        ff_low = np.nan
      # ff_low = ff_low_list.item() if len(ff_low_list.index) == 1 else np.nan

      ff_close_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['ff_close_sum']
      if not ff_close_list.empty:
         ff_close = ff_close_list.iloc[0]
      else:
         ff_close = np.nan
      # ff_close = ff_close_list.item() if len(ff_close_list.index) == 1 else np.nan

      ff_os_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['os_sum']
      ff_os = ff_os_list.item() if len(ff_os_list.index) == 1 else np.nan

      ff_earnings_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['earnings']
      if not ff_earnings_list.empty:  
        ff_earnings = ff_earnings_list.iloc[0]
      else:
        ff_earnings = np.nan
      # ff_earnings = ff_earnings_list.item() if len(ff_earnings_list.index) == 1 else np.nan

      ff_equity_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['sum_equity']
      if not ff_equity_list.empty:
        ff_equity = ff_equity_list.iloc[0]
      else:
        ff_equity = np.nan
      # ff_equity = ff_equity_list.item() if len(ff_equity_list.index) == 1 else np.nan

      prev_earnings_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['prev_earnings']
      if not prev_earnings_list.empty:
        prev_earnings = prev_earnings_list.iloc[0]
      else:
        prev_earnings = np.nan
      # prev_earnings = prev_earnings_list.item() if len(prev_earnings_list.index) == 1 else np.nan

      prev_equity_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['prev_equity']
      if not prev_equity_list.empty: 
        prev_equity = prev_equity_list.iloc[0]
      else: 
          prev_equity = np.nan
      # prev_equity = prev_equity_list.item() if len(prev_equity_list.index) == 1 else np.nan

      divisor_list = industry_divisor.loc[(industry_divisor['IndexName']==row['SubIndustryIndexName'])]['Divisor']
      if not divisor_list.empty:
        divisor = divisor_list.iloc[0]
      else:
        divisor = np.nan  
      # divisor = divisor_list.item() if len(divisor_list.index) == 1 else np.nan

      sum_os_close_list = industry_ff_ohlc.loc[(industry_ff_ohlc['SubIndustryIndexName']==row['SubIndustryIndexName'])]['sum_os_close']
      if not sum_os_close_list.empty: 
        sum_os_close = sum_os_close_list.iloc[0]
      else:
        sum_os_close = np.nan
      # sum_os_close = sum_os_close_list.item() if len(sum_os_close_list.index) == 1 else np.nan

      # if np.isnan(divisor) or divisor == 0:
      #   print("Devisor gen_Subindustry_index: ", divisor)

      # print(industry_ff_ohlc[industry_ff_ohlc['SubIndustryIndexName']==['SubIndustryIndexName']])
      # print("Divisor: ", divisor, "ff_open: ", ff_open, "ff_high: ", ff_high, "ff_low: ", ff_low, "ff_close: ", ff_close, "ff_earnings: ", ff_earnings, "ff_equity: ", ff_equity, "prev_earnings: ", prev_earnings, "prev_equity: ", prev_equity, "sum_os_close: ", sum_os_close, flush = True)

      # div_open = ff_open/divisor if divisor != 0 and not np.isnan(ff_open) else np.nan
      div_open = ff_open/divisor if divisor and ff_open is not None and divisor != 0 else np.nan
      div_high = ff_high/divisor if divisor and ff_high is not None and divisor != 0 else np.nan
      div_low = ff_low/divisor if divisor and ff_low is not None and divisor != 0 else np.nan
      div_close = ff_close/divisor if divisor and ff_close is not None and divisor != 0 else np.nan


      pe = (sum_os_close)/ff_earnings if ff_earnings !=0 else np.nan
      eps = ff_earnings/ff_equity if ff_equity !=0 else np.nan
      eps_back = prev_earnings/prev_equity if prev_equity !=0 else np.nan
      earnings_growth = ((eps-eps_back)/abs(eps_back))*100 if eps_back != 0 else np.nan


      industry_ff_ohlc.loc[index, 'Open'] = div_open
      industry_ff_ohlc.loc[index, 'High'] = div_high
      industry_ff_ohlc.loc[index, 'Low'] = div_low
      industry_ff_ohlc.loc[index, 'Close'] = div_close
      industry_ff_ohlc.loc[index, 'PE'] = pe
      industry_ff_ohlc.loc[index, 'EPS'] = eps
      industry_ff_ohlc.loc[index, 'Earnings Growth'] = earnings_growth


    return industry_ff_ohlc
  
  def insert_sub_industry_index(self,industry_index, conn, cur, date):
      
    industry_index['GenDate'] = date
    industry_index = industry_index[['GenDate', 'SubIndustryIndexName', 'SubIndustry', 'Open', 'High', 'Low', 'Close', 'sum_vol', 'PE', 'EPS', \
                                'company_count', 'os_sum', 'Earnings Growth']]

    exportfilename = "SubIndustryIndexList.csv"
    exportfile = open(exportfilename,"w")
    industry_index.to_csv(exportfile, header=True, index=False,  lineterminator = '\r')
    exportfile.close()


    copy_sql = """
        COPY public."SubIndustryIndexList" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

    with open(exportfilename, 'r') as f:
      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)
  

  def calc_rank_index(self,sector_index, subsector_index, industry_index, sub_industry_index,conn, date):
    """ calculating the rank for sector, subsector and industry.

    Args:
        sector_index = data of Open, close, high, low, PE , EPS
                      and Earning Growth of sector index.

        subsector_index = data of Open, close, high, low, PE , EPS
                        and Earning Growth of subsector index.

        industry_index = data of Open, close, high, low, PE , EPS
                        and Earning Growth of industry index.

    Operation:
        Fetch the data from IndexHistory and calculate the value for six month change
        for sector index, subsector index and industry index,
        change six month = (curr close - prev close / prev close)* 100
        index = change * 100.

    Return:
        Rank value of Sector Index, SubSector Index and Industry index.
    """

    six_month_back = self.get_six_month_back(date)

    indexlist_history_sql = 'SELECT DISTINCT ON("TICKER") * FROM public."IndexHistory" \
                            where "DATE" <= \''+six_month_back+'\' \
                            ORDER BY "TICKER", "DATE" DESC; '
    index_history = sqlio.read_sql_query(indexlist_history_sql, con = conn)


    #Sector rank
    if not(sector_index.empty):

      for index, row in sector_index.iterrows():

        prev_close_list = index_history.loc[(index_history['TICKER']==row['SectorIndexName'])]['CLOSE']
        prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

        current_close_list = sector_index.loc[(sector_index['SectorIndexName']==row['SectorIndexName'])]['Close']
        current_close = current_close_list.item() if len(current_close_list.index) == 1 else np.nan

        change_six_months = (current_close - prev_close) / prev_close * 100 if not pd.isnull(prev_close) and prev_close!=0 else pd.NA

        sector_index.loc[index, 'Change'] = change_six_months

      sector_index['Rank'] = sector_index['Change'].rank(ascending=True, pct=True) * 100

      sector_index = sector_index.rename(columns = {"SectorIndexName":"IndexName", "Sector": "Index"})

    else:

      print("No Sector data")


    #SubSector Rank
    if not(subsector_index.empty):

      for index, row in subsector_index.iterrows():

        prev_close_list = index_history.loc[(index_history['TICKER']==row['SubSectorIndexName'])]['CLOSE']
        prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

        current_close_list = subsector_index.loc[(subsector_index['SubSectorIndexName']==row['SubSectorIndexName'])]['Close']
        current_close = current_close_list.item() if len(current_close_list.index) == 1 else np.nan

        change_six_months = (current_close - prev_close) / prev_close * 100 if not pd.isnull(prev_close) and prev_close!=0 else pd.NA


        subsector_index.loc[index, 'Change'] = change_six_months

      subsector_index['Rank'] = subsector_index['Change'].rank(ascending=True, pct=True) * 100

      subsector_index = subsector_index.rename(columns = {"SubSectorIndexName":"IndexName", "SubSector": "Index"})

    else:

      print("No Subsector data")


    #Industry Rank
    if not(industry_index.empty):

      for index, row in industry_index.iterrows():

        print("change six months for sub industry :", row['IndustryIndexName'])
        prev_close_list = index_history.loc[(index_history['TICKER']==row['IndustryIndexName'])]['CLOSE']
        prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

        current_close_list = industry_index.loc[(industry_index['IndustryIndexName']==row['IndustryIndexName'])]['Close']
        current_close = current_close_list.item() if len(current_close_list.index) == 1 else np.nan
        print("current_close: ", current_close)
        print("prev_close: ", prev_close)

        change_six_months = (current_close - prev_close) / prev_close * 100 if not pd.isnull(prev_close) and prev_close!=0 else pd.NA

        industry_index.loc[index, 'Change'] = change_six_months

      industry_index['Rank'] = industry_index['Change'].rank(ascending=True, pct=True) * 100

      industry_index = industry_index.rename(columns = {"IndustryIndexName":"IndexName", "Industry": "Index"})

    else:

      print("No Industry data")
      
    #SubIndustry Rank
    if not(sub_industry_index.empty):
      for index, row in sub_industry_index.iterrows():
        print("change six months for sub industry :", row['SubIndustryIndexName'])

        prev_close_list = index_history.loc[(index_history['TICKER']==row['SubIndustryIndexName'])]['CLOSE']
        prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

        current_close_list = sub_industry_index.loc[(sub_industry_index['SubIndustryIndexName']==row['SubIndustryIndexName'])]['Close']
        current_close = current_close_list.item() if len(current_close_list.index) == 1 else np.nan
        
        print("current_close: ", current_close)
        print("prev_close: ", prev_close)

        change_six_months = (current_close - prev_close) / prev_close * 100 if not pd.isnull(prev_close) and prev_close!=0 else pd.NA

        sub_industry_index.loc[index, 'Change'] = change_six_months

      sub_industry_index['Rank'] = sub_industry_index['Change'].rank(ascending=True, pct=True) * 100

      sub_industry_index = sub_industry_index.rename(columns = {"SubIndustryIndexName":"IndexName", "SubIndustry": "Index"})

    else:

      print("No SubIndustry data")


    irs_list = pd.concat([sector_index, subsector_index, industry_index, sub_industry_index], axis=0, sort=True)
    # print('irslist: ',irs_list)


    return irs_list
  
  def calc_pe_high_low(self, irs_list, conn, date):
      """Calculating the High and Low for PE.

      Args:
          irs_list: Rank value of Sector Index, SubSector Index and Industry index.

      Operation:
          Fetch the data from IRS table, and calculate the current High,
          current Low and Previous High, Previous Low for PE.

      Return:
          Value of PE High an PE Low.
      """
      # Fetch necessary data from the database
      irs_back_sql = f'SELECT DISTINCT ON("IndexName") * from "Reports"."IRS" WHERE "GenDate" < \'{date}\' ORDER BY "IndexName", "GenDate" desc ;'
      irs_back = sqlio.read_sql_query(irs_back_sql, con=conn)

      industry_list_sql = f'SELECT * from "public"."IndustryList" WHERE "GenDate" = \'{date}\' ORDER BY "GenDate" desc ;'
      industry_list = sqlio.read_sql_query(industry_list_sql, con=conn)

      sector_divisor_sql = f'SELECT * FROM public."SectorDivisor" WHERE "Date" = \'{date}\' ;'
      sector_divisor = sqlio.read_sql_query(sector_divisor_sql, con=conn)

      subsector_divisor_sql = f'SELECT * FROM public."SubSectorDivisor" WHERE "Date" = \'{date}\' ;'
      subsector_divisor = sqlio.read_sql_query(subsector_divisor_sql, con=conn)

      industry_divisor_sql = f'SELECT * FROM public."IndustryDivisor" WHERE "Date" = \'{date}\' ;'
      industry_divisor = sqlio.read_sql_query(industry_divisor_sql, con=conn)

      sub_industry_divisor_sql = f'SELECT * FROM public."SubIndustryDivisor" WHERE "Date" = \'{date}\' ;'
      sub_industry_divisor = sqlio.read_sql_query(sub_industry_divisor_sql, con=conn)

      irs_list['MCap_Open_sum'] = np.nan
      irs_list['MCap_High_sum'] = np.nan
      irs_list['MCap_Low_sum'] = np.nan
      irs_list['MCap_Close_sum'] = np.nan


      irs_list['MCap_Open_sum'] = irs_list['MCap_Open_sum'].astype(np.float64)
      irs_list['MCap_High_sum'] = irs_list['MCap_High_sum'].astype(np.float64)
      irs_list['MCap_Low_sum'] = irs_list['MCap_Low_sum'].astype(np.float64)
      irs_list['MCap_Close_sum'] = irs_list['MCap_Close_sum'].astype(np.float64)

      # Process each row in irs_list
      for index, row in irs_list.iterrows():
          index_name = row['IndexName']
          print(f"Processing index: {index_name}")

          # Filter the industry_list based on index_name
          filtered_rows = industry_list[
              (industry_list['IndustryIndexName'] == index_name) |
              (industry_list['SubIndustryIndexName'] == index_name) |
              (industry_list['SectorIndexName'] == index_name) |
              (industry_list['SubSectorIndexName'] == index_name)
          ]
          
          # Print the number of filtered rows
          print(f"Number of filtered rows: {len(filtered_rows)}")

          if len(filtered_rows) == 0:
              print(f"No matching rows found for index: {index_name}")
              continue

          # Calculate sums
          # open_sum = filtered_rows['MCap_Open'].sum()
          # high_sum = filtered_rows['MCap_High'].sum()
          # low_sum = filtered_rows['MCap_Low'].sum()
          # close_sum = filtered_rows['MCap_Close'].sum()

          # Print the calculated sums
          # print(f"Sums for index {index_name} - Open: {open_sum}, High: {high_sum}, Low: {low_sum}, Close: {close_sum}")

          irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_sum'] = filtered_rows['MCap_Open'].sum()
          irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_sum']  = filtered_rows['MCap_High'].sum()
          irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_sum']  = filtered_rows['MCap_Low'].sum()
          irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_sum']  = filtered_rows['MCap_Close'].sum()



          # Print the values in irs_list to confirm assignment
          print(f"Assigned values in irs_list - Open: {irs_list.loc[index, 'MCap_Open_sum']}, High: {irs_list.loc[index, 'MCap_High_sum']}, Low: {irs_list.loc[index, 'MCap_Low_sum']}, Close: {irs_list.loc[index, 'MCap_Close_sum']}")

          print(irs_list.loc[index,:])
      import copy
      # Processing PE High and Low
      irs_pe = copy.deepcopy(irs_list)

      for index, row in irs_list.iterrows():
          index_name = row['IndexName']

          im = re.split('-', index_name)[0]
          if im == 'SUBINDUSTRY':
              sub_industry_divisor_value = sub_industry_divisor.loc[sub_industry_divisor['IndexName'] == row['IndexName'], 'MCap_Divisor']
              sub_industry_divisor_value = sub_industry_divisor_value.iloc[0] if not sub_industry_divisor_value.empty else np.nan

              irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_sum'] / sub_industry_divisor_value
              irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_sum'] / sub_industry_divisor_value
              irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_sum'] / sub_industry_divisor_value
              irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_sum'] / sub_industry_divisor_value

              # irs_list.loc[index, 'MCap_Open_Index'] = irs_list.loc[index, 'MCap_Open_sum'] / sub_industry_divisor_value
              # # print(len(irs_list.loc[index, 'MCap_Open_Index'] ), 'lenght of subindustry look for duplicate indexnames')
              # irs_list.loc[index, 'MCap_High_Index'] = irs_list.loc[index, 'MCap_High_sum'] / sub_industry_divisor_value
              # irs_list.loc[index, 'MCap_Low_Index'] = irs_list.loc[index, 'MCap_Low_sum'] / sub_industry_divisor_value
              # irs_list.loc[index, 'MCap_Close_Index'] = irs_list.loc[index, 'MCap_Close_sum'] / sub_industry_divisor_value

          elif im == 'INDUSTRY':
              industry_divisor_value = industry_divisor.loc[industry_divisor['IndexName'] == row['IndexName'], 'MCap_Divisor']
              industry_divisor_value = industry_divisor_value.iloc[0] if not industry_divisor_value.empty else np.nan

              irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_sum'] / industry_divisor_value
              irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_sum'] / industry_divisor_value
              irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_sum'] / industry_divisor_value
              irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_sum'] / industry_divisor_value

              # irs_list.loc[index, 'MCap_Open_Index'] = irs_list.loc[index, 'MCap_Open_sum'] / industry_divisor_value
              # irs_list.loc[index, 'MCap_High_Index'] = irs_list.loc[index, 'MCap_High_sum'] / industry_divisor_value
              # irs_list.loc[index, 'MCap_Low_Index'] = irs_list.loc[index, 'MCap_Low_sum'] / industry_divisor_value
        # irs_list.loc[index, 'MCap_Close_Index'] = irs_list.loc[index, 'MCap_Close_sum'] / industry_divisor_value

          elif im == 'SUBSECTOR':
            subsector_divisor_value = subsector_divisor.loc[subsector_divisor['IndexName'] == row['IndexName'], 'MCap_Divisor']
            subsector_divisor_value = subsector_divisor_value.iloc[0] if not subsector_divisor_value.empty else np.nan

            irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_sum'] / subsector_divisor_value
            irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_sum'] / subsector_divisor_value
            irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_sum'] / subsector_divisor_value
            irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_sum'] / subsector_divisor_value


            # irs_list.loc[index, 'MCap_Open_Index'] = irs_list.loc[index, 'MCap_Open_sum'] / subsector_divisor_value
            # irs_list.loc[index, 'MCap_High_Index'] = irs_list.loc[index, 'MCap_High_sum'] / subsector_divisor_value
            # irs_list.loc[index, 'MCap_Low_Index'] = irs_list.loc[index, 'MCap_Low_sum'] / subsector_divisor_value
            # irs_list.loc[index, 'MCap_Close_Index'] = irs_list.loc[index, 'MCap_Close_sum'] / subsector_divisor_value

          elif im == 'SECTOR':
            sector_divisor_value = sector_divisor.loc[sector_divisor['IndexName'] == row['IndexName'], 'MCap_Divisor']
            sector_divisor_value = sector_divisor_value.iloc[0] if not sector_divisor_value.empty else np.nan

            irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Open_sum'] / sector_divisor_value
            irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_High_sum'] / sector_divisor_value
            irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Low_sum'] / sector_divisor_value
            irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_Index'] = irs_list.loc[irs_list['IndexName'] == index_name, 'MCap_Close_sum'] / sector_divisor_value

            # irs_list.loc[index, 'MCap_Open_Index'] = irs_list.loc[index, 'MCap_Open_sum'] / sector_divisor_value
            # irs_list.loc[index, 'MCap_High_Index'] = irs_list.loc[index, 'MCap_High_sum'] / sector_divisor_value
            # irs_list.loc[index, 'MCap_Low_Index'] = irs_list.loc[index, 'MCap_Low_sum'] / sector_divisor_value
            # irs_list.loc[index, 'MCap_Close_Index'] = irs_list.loc[index, 'MCap_Close_sum'] / sector_divisor_value

          print("Open:", irs_list.loc[index, 'MCap_Open_Index'])
          print("High:", irs_list.loc[index, 'MCap_High_Index'])
          print("Low:", irs_list.loc[index, 'MCap_Low_Index'])
          print("Close:", irs_list.loc[index, 'MCap_Close_Index'])

      for index, row in irs_list.iterrows():
          pe_current_high_list = irs_list.loc[irs_list['IndexName'] == row['IndexName'], 'PE']
          pe_current_high_date_list = irs_list.loc[irs_list['IndexName'] == row['IndexName'], 'GenDate']

          pe_current_high = pe_current_high_list.iloc[0] if len(pe_current_high_list.index) == 1 else np.nan
          pe_current_high_date = pe_current_high_date_list.iloc[0] if len(pe_current_high_date_list.index) == 1 else np.nan

          pe_back_high_list = irs_back.loc[irs_back['IndexName'] == row['IndexName'], 'PE High']
          pe_back_high_date_list = irs_back.loc[irs_back['IndexName'] == row['IndexName'], 'PE High Date']

          pe_back_high = pe_back_high_list.iloc[0] if len(pe_back_high_list.index) == 1 else np.nan
          pe_back_high_date = pe_back_high_date_list.iloc[0] if len(pe_back_high_date_list.index) == 1 else np.nan

          if pd.isnull(pe_back_high):
              irs_list.at[index, 'PE High'] = pe_current_high if not pd.isnull(pe_current_high) else np.nan
              irs_list.at[index, 'PE High Date'] = pe_current_high_date
          elif pe_current_high > pe_back_high:
              irs_list.at[index, 'PE High'] = pe_current_high if not pd.isnull(pe_current_high) else np.nan
              irs_list.at[index, 'PE High Date'] = pe_current_high_date
          else:
              irs_list.at[index, 'PE High'] = pe_back_high if not pd.isnull(pe_back_high) else np.nan
              irs_list.at[index, 'PE High Date'] = pe_back_high_date

      for index, row in irs_list.iterrows():
          pe_current_low_list = irs_list.loc[irs_list['IndexName'] == row['IndexName'], 'PE']
          pe_current_low_date_list = irs_list.loc[irs_list['IndexName'] == row['IndexName'], 'GenDate']

          pe_current_low = pe_current_low_list.iloc[0] if len(pe_current_low_list.index) == 1 else np.nan
          pe_current_low_date = pe_current_low_date_list.iloc[0] if len(pe_current_low_date_list.index) == 1 else np.nan

          pe_back_low_list = irs_back.loc[irs_back['IndexName'] == row['IndexName'], 'PE Low']
          pe_back_low_date_list = irs_back.loc[irs_back['IndexName'] == row['IndexName'], 'PE Low Date']

          pe_back_low = pe_back_low_list.iloc[0] if len(pe_back_low_list.index) == 1 else np.nan
          pe_back_low_date = pe_back_low_date_list.iloc[0] if len(pe_back_low_date_list.index) == 1 else np.nan

          if pd.isnull(pe_back_low):
              irs_list.at[index, 'PE Low'] = pe_current_low if not pd.isnull(pe_current_low) else np.nan
              irs_list.at[index, 'PE Low Date'] = pe_current_low_date
          elif pe_current_low < pe_back_low:
              irs_list.at[index, 'PE Low'] = pe_current_low if not pd.isnull(pe_current_low) else np.nan
              irs_list.at[index, 'PE Low Date'] = pe_current_low_date
          else:
              irs_list.at[index, 'PE Low'] = pe_back_low if not pd.isnull(pe_back_low) else np.nan
              irs_list.at[index, 'PE Low Date'] = pe_back_low_date
      # irs_list['PE High']= np.nan
      # irs_list['PE High Date'] = np.nan
      # irs_list['PE Low'] = np.nan
      # irs_list['PE Low Date'] = np.nan

      return irs_list, irs_pe


  # def calc_pe_high_low(self,irs_list, conn, date):
  #   """ Calculating the High and Low for PE.

  #   Args:
  #       irs_list = Rank value of Sector Index, SubSector Index and Industry index.

  #   Operation:
  #       Fetch the data from IRS table, and calculate the current High ,
  #       current Low and Previous High, Previous Low for PE.

  #   Return:
  #       Value of PE High an PE Low.
  #   """
  #   # print(irs_list)
  #   irs_back_sql = 'SELECT DISTINCT ON("IndexName") * from "Reports"."IRS"	\
  #                   WHERE "GenDate" < \'' +date+ '\'	\
  #                   ORDER BY "IndexName", "GenDate" desc ;'
  #   irs_back = sqlio.read_sql_query(irs_back_sql, con = conn)

  #   industry_list_sql = 'SELECT * from "public"."IndustryList"	\
  #                       WHERE "GenDate" = \'' +date+ '\'	\
  #                       ORDER BY "GenDate" desc ;'

  #   industry_list = sqlio.read_sql_query(industry_list_sql, con = conn)

  #   sector_divisor_sql = 'SELECT * FROM public."SectorDivisor" WHERE "Date" = \''+date+'\' ;'
  #   sector_divisor = sqlio.read_sql_query(sector_divisor_sql, con = conn)

  #   subsector_divisor_sql = 'SELECT * FROM public."SubSectorDivisor" WHERE "Date" = \''+date+'\' ;'
  #   subsector_divisor = sqlio.read_sql_query(subsector_divisor_sql, con = conn)

  #   industry_divisor_sql = 'SELECT * FROM public."IndustryDivisor" WHERE "Date" = \''+date+'\' ;'
  #   industry_divisor = sqlio.read_sql_query(industry_divisor_sql, con = conn)

  #   sub_industry_divisor_sql = 'SELECT * FROM public."SubIndustryDivisor" WHERE "Date" = \''+date+'\' ;'
  #   sub_industry_divisor = sqlio.read_sql_query(sub_industry_divisor_sql, con = conn)

  #   # for index, row in irs_list.iterrows():
 


  #   #PE High
  #   for index, row in irs_list.iterrows():
  #     index_name = row['IndexName']
  #     print(index_name)
  #     filtered_rows = industry_list[
  #         (industry_list['IndustryIndexName'] == index_name) |
  #         (industry_list['SubIndustryIndexName'] == index_name) |
  #         (industry_list['SectorIndexName'] == index_name) |
  #         (industry_list['SubSectorIndexName'] == index_name)
  #     ]

  #     open_sum = filtered_rows['MCap_Open'].sum()
  #     high_sum = filtered_rows['MCap_High'].sum()
  #     low_sum = filtered_rows['MCap_Low'].sum()
  #     close_sum = filtered_rows['MCap_Close'].sum()

  #     irs_list.loc[index, 'MCap_Open_sum'] = open_sum
  #     irs_list.loc[index, 'MCap_High_sum'] = high_sum
  #     irs_list.loc[index, 'MCap_Low_sum'] = low_sum
  #     irs_list.loc[index, 'MCap_Close_sum'] = close_sum

  #     im = re.split('-', index_name)[0]
  #     if im == 'SUBINDUSTRY':
          
  #         sub_industry_divisor_list = sub_industry_divisor.loc[(sub_industry_divisor['IndexName']==row['IndexName'])]['MCap_Divisor']
  #                     # Check if the resulting Series is not empty
  #         print("sub_industry_divisor_list: ", sub_industry_divisor_list)
  #         if not sub_industry_divisor_list.empty:
  #             # Take the first value using .iloc[0]
  #             sub_industry_divisor_value = sub_industry_divisor_list.iloc[0]
  #             print("sub_industry divisor value :",sub_industry_divisor_value)
  #         else:
  #             # Set the value to np.nan if the Series is empty

  #             sub_industry_divisor_value = np.nan
  #             print("sub_industry divisor value :",sub_industry_divisor_list)

  #         # sub_industry_divisor_value = sub_industry_divisor_list.item() if len(sub_industry_divisor_list.index) == 1 else np.nan
          
  #         irs_list.loc[index, 'MCap_Open_Index'] = open_sum / sub_industry_divisor_value
  #         irs_list.loc[index, 'MCap_High_Index'] = high_sum / sub_industry_divisor_value
  #         irs_list.loc[index, 'MCap_Low_Index'] = low_sum / sub_industry_divisor_value
  #         irs_list.loc[index, 'MCap_Close_Index'] = close_sum / sub_industry_divisor_value

  #         print("Open :",irs_list.loc[index, 'MCap_Open_Index'])
  #         print("High :",irs_list.loc[index, 'MCap_High_Index'])
  #         print("Low :",irs_list.loc[index, 'MCap_Low_Index'])
  #         print("Close :",irs_list.loc[index, 'MCap_Close_Index'])


  #     elif im == 'INDUSTRY':  
          
  #         industry_divisor_list = industry_divisor.loc[(industry_divisor['IndexName']==row['IndexName'])]['MCap_Divisor']
  #         # if index_name == 'INDUSTRY-Personal Care Products':
  #         print("idustry divisor list",industry_divisor_list)
  #         if not industry_divisor_list.empty: 
  #             industry_divisor_value = industry_divisor_list.iloc[0]
  #             print("idustry divisor value",industry_divisor_value)

  #         else:
  #             industry_divisor_value = np.nan
  #             print(industry_divisor_list)

  #         # industry_divisor_value = industry_divisor_list.item() if len(industry_divisor_list.index) == 1 else np.nan
          
  #         irs_list.loc[index, 'MCap_Open_Index'] = open_sum / industry_divisor_value
  #         irs_list.loc[index, 'MCap_High_Index'] = high_sum / industry_divisor_value
  #         irs_list.loc[index, 'MCap_Low_Index'] =low_sum / industry_divisor_value
  #         irs_list.loc[index, 'MCap_Close_Index'] = close_sum / industry_divisor_value
          
  #         print("Open :",irs_list.loc[index, 'MCap_Open_Index'])
  #         print("High :",irs_list.loc[index, 'MCap_High_Index'])
  #         print("Low :",irs_list.loc[index, 'MCap_Low_Index'])
  #         print("Close :",irs_list.loc[index, 'MCap_Close_Index'])

  #     elif im == 'SUBSECTOR':
  #         # print(index_name)
  #         # print('1')
  #         # print(irs_list.loc[index, 'IndexName'])
  #         subsector_divisor_list = subsector_divisor.loc[(subsector_divisor['IndexName']==row['IndexName'])]['MCap_Divisor']
  #         # if index_name=='SUBSECTOR-Consumer Durables & Apparel':
  #         print("subsector divisor list:",subsector_divisor_list)
  #         if not subsector_divisor_list.empty:
  #           subsector_divisor_value = subsector_divisor_list.iloc[0]
  #           print("subsector divisor list:",subsector_divisor_list)

  #         else:
              
  #             subsector_divisor_value = np.nan
  #             print(subsector_divisor_list)

  #         # subsector_divisor_value = subsector_divisor_list.item() if len(subsector_divisor_list.index) == 1 else np.nan

  #         # print(subsector_divisor_value,irs_list.loc[index, 'MCap_Open_sum']) 
  #         # print(irs_list.loc[index, 'MCap_Open_sum'] / subsector_divisor_value)
  #         irs_list.loc[index, 'MCap_Open_Index'] = open_sum / subsector_divisor_value
  #         irs_list.loc[index, 'MCap_High_Index'] = high_sum / subsector_divisor_value
  #         irs_list.loc[index, 'MCap_Low_Index'] = low_sum / subsector_divisor_value
  #         irs_list.loc[index, 'MCap_Close_Index'] = close_sum / subsector_divisor_value

          
  #         print("Open :",irs_list.loc[index, 'MCap_Open_Index'])
  #         print("High :",irs_list.loc[index, 'MCap_High_Index'])
  #         print("Low :",irs_list.loc[index, 'MCap_Low_Index'])
  #         print("Close :",irs_list.loc[index, 'MCap_Close_Index'])


  #         # print(irs_list.loc[index, 'MCap_Open_Index'])
  #     elif im == 'SECTOR':
  #         # print('inside sec')
  #         # print(index_name)
  #         sector_divisor_list = sector_divisor.loc[(sector_divisor['IndexName']==row['IndexName'])]['MCap_Divisor']
  #         print("sector divisor list:",sector_divisor_list)
  #         if not sector_divisor_list.empty:
  #           sector_divisor_value = sector_divisor_list.iloc[0]
  #           print("sector divisor value:",sector_divisor_value)
  #         else:
  #             sector_divisor_value = np.nan
  #             print(sector_divisor_list)

  #         # sector_divisor_value = sector_divisor_list.item() if len(sector_divisor_list.index) == 1 else np.nan
  #         # print(sector_divisor_value, irs_list.loc[index, 'MCap_Open_sum'])
  #         irs_list.loc[index, 'MCap_Open_Index'] = open_sum / sector_divisor_value
  #         irs_list.loc[index, 'MCap_High_Index'] = high_sum / sector_divisor_value
  #         irs_list.loc[index, 'MCap_Low_Index'] = low_sum / sector_divisor_value
  #         irs_list.loc[index, 'MCap_Close_Index'] = close_sum / sector_divisor_value

          
  #         print("Open :",irs_list.loc[index, 'MCap_Open_Index'])
  #         print("High :",irs_list.loc[index, 'MCap_High_Index'])
  #         print("Low :",irs_list.loc[index, 'MCap_Low_Index'])
  #         print("Close :",irs_list.loc[index, 'MCap_Close_Index'])

  #     irs_pe = irs_list

  #     #PE for current OHLC
  #     pe_current_high_list = irs_list.loc[(irs_list['IndexName']==row['IndexName'])]['PE']
  #     pe_current_high_date_list = irs_list.loc[(irs_list['IndexName']==row['IndexName'])]['GenDate']

  #     pe_current_high = pe_current_high_list.item() if len(pe_current_high_list.index) == 1 else np.nan
  #     pe_current_high_date = pe_current_high_date_list.item() if len(pe_current_high_date_list.index) == 1 else np.nan


  #     #PE High for backdate
  #     pe_back_high_list = irs_back.loc[(irs_back['IndexName']==row['IndexName'])]['PE High']
  #     pe_back_high_date_list = irs_back.loc[(irs_back['IndexName']==row['IndexName'])]['PE High Date']

  #     pe_back_high = pe_back_high_list.item() if len(pe_back_high_list.index) == 1 else np.nan
  #     pe_back_high_date = pe_back_high_date_list.item() if len(pe_back_high_date_list.index) == 1 else np.nan


  #     #Compare current PE with backdate PE High value
  #     if(math.isnan(pe_back_high)):

  #       irs_list.loc[index, 'PE High'] = pe_current_high if not pd.isnull(pe_current_high) else np.nan
  #       irs_list.loc[index, 'PE High Date'] = pe_current_high_date


  #     elif (pe_current_high > pe_back_high):

  #       irs_list.loc[index, 'PE High'] = pe_current_high if not pd.isnull(pe_current_high) else np.nan
  #       irs_list.loc[index, 'PE High Date'] = pe_current_high_date


  #     else:

  #       irs_list.loc[index, 'PE High'] = pe_back_high if not pd.isnull(pe_back_high) else np.nan
  #       irs_list.loc[index, 'PE High Date'] = pe_back_high_date


  #   #PE Low
  #   for index, row in irs_list.iterrows():

  #     #PE for current OHLC
  #     pe_current_low_list = irs_list.loc[(irs_list['IndexName']==row['IndexName'])]['PE']
  #     pe_current_low_date_list = irs_list.loc[(irs_list['IndexName']==row['IndexName'])]['GenDate']

  #     pe_current_low = pe_current_low_list.item() if len(pe_current_low_list.index) == 1 else np.nan
  #     pe_current_low_date = pe_current_low_date_list.item() if len(pe_current_low_date_list.index) == 1 else np.nan


  #     #PE Low for backdate
  #     pe_back_low_list = irs_back.loc[(irs_back['IndexName']==row['IndexName'])]['PE Low']
  #     pe_back_low_date_list = irs_back.loc[(irs_back['IndexName']==row['IndexName'])]['PE Low Date']

  #     pe_back_low = pe_back_low_list.item() if len(pe_back_low_list.index) == 1 else np.nan
  #     pe_back_low_date = pe_back_low_date_list.item() if len(pe_back_low_date_list.index) == 1 else np.nan


  #     #Compare current PE with backdate PE Low value
  #     if(math.isnan(pe_back_low)):

  #       irs_list.loc[index, 'PE Low'] = pe_current_low if not pd.isnull(pe_current_low) else np.nan
  #       irs_list.loc[index, 'PE Low Date'] = pe_current_low_date


  #     elif (pe_current_low < pe_back_low):

  #       irs_list.loc[index, 'PE Low'] = pe_current_low if not pd.isnull(pe_current_low) else np.nan
  #       irs_list.loc[index, 'PE Low Date'] = pe_current_low_date


  #     else:

  #       irs_list.loc[index, 'PE Low'] = pe_back_low if not pd.isnull(pe_back_low) else np.nan
  #       irs_list.loc[index, 'PE Low Date'] = pe_back_low_date
  #   # print(irs_list)
  #   irs_list = irs_list.replace({np.nan: None})

  #   return irs_list, irs_pe


  def insert_irs(self,irs_list, conn, cur, date):
    """ Insert the IRS data into database.

    Args:
        irs_list = Data of sector Rank, SubSector Rank, Industry Rank
                    and High, Low value of PE.

    Operation:
        Export the data into 'IRS.csv' and 'indexlist_history.csv' file
        and insert into IRS and IndexHistory table.
    """

    irs_list = irs_list[['GenDate', 'IndexName', 'Index', 'Open', 'High', 'Low', 'Close', \
                         'MCap_Open_Index', 'MCap_High_Index', 'MCap_Low_Index', 'MCap_Close_Index', 'sum_vol', 'PE', 'EPS', \
                        'company_count', 'os_sum', 'Rank', 'Change', 'Earnings Growth','ff_open_sum', 'ff_high_sum', \
                          'ff_low_sum', 'ff_close_sum', 'MCap_Open_sum', 'MCap_High_sum', 'MCap_Low_sum', 'MCap_Close_sum',\
                              'PE High', 'PE High Date', 'PE Low', 'PE Low Date']]
                        # 'PE High', 'PE High Date', 'PE Low', 'PE Low Date']]
    
    irs_list = irs_list.replace({np.nan: None})
    # print(irs_list['PE High'])


    exportfilename = "IRS.csv"
    exportfile = open(exportfilename,"w")
    irs_list.to_csv(exportfile, header=True, index=False,  lineterminator = '\r')
    exportfile.close()

    copy_sql = """
        COPY "Reports"."IRS" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

    with open(exportfilename, 'r') as f:
      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


    index_history = irs_list[['IndexName', 'GenDate', 'Open', 'High', 'Low', 'Close', 'MCap_Open_Index', 'MCap_High_Index', 'MCap_Low_Index', 'MCap_Close_Index', 'sum_vol']]

    index_history = index_history.rename(columns = {"MCap_Open_Index": "MCap_OPEN", "MCap_High_Index": "MCap_HIGH", "MCap_Low_Index": "MCap_LOW", "MCap_Close_Index": "MCap_CLOSE"})                                                     

    exportfilename = "indexlist_history.csv"
    exportfile = open(exportfilename,"w")
    index_history.to_csv(exportfile, header=True, index=False,  lineterminator = '\r')
    exportfile.close()

    copy_sql = """
        COPY public."IndexHistory" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

    with open(exportfilename, 'r') as f:
      cur.copy_expert(sql=copy_sql, file=f)
      conn.commit()
      f.close()
    os.remove(exportfilename)


  def insert_irs_history(self,conn, cur):
    """ Fetch the data of index rank and pe high low , export csv file
        “indexlist_history.csv”,
        and insert into IRS table.
        Insert IRS History.
    """

    sector_csv =  file_path+ 'sectorindex.csv'
    sector_history = pd.read_csv(sector_csv, encoding = 'latin1')

    subsector_csv = file_path + 'subsectorindex.csv'
    subsector_history = pd.read_csv(subsector_csv, encoding = 'latin1')

    industry_csv = file_path + 'industryindex.csv'
    industry_history = pd.read_csv(industry_csv, encoding = 'latin1')


    sector_history = sector_history[['GenDate', 'SectorIndexName', 'Sector', 'Open', 'High', 'Low', 'Close', 'Volume', 'PE', 'Count' ,'OS']]
    subsector_history = subsector_history[['GenDate', 'SubSectorIndexName', 'SubSector', 'Open', 'High', 'Low', 'Close', 'Volume', 'PE', 'Count','OS']]
    industry_history = industry_history[['GenDate', 'IndustryIndexName', 'Industry', 'Open', 'High', 'Low', 'Close', 'Volume', 'PE', 'Count','OS']]

    sector_history['GenDate'] =  pd.to_datetime(sector_history['GenDate'])
    sector_history['GenDate'] = sector_history['GenDate'].dt.strftime("%Y-%m-%d")

    subsector_history['GenDate'] =  pd.to_datetime(subsector_history['GenDate'])
    subsector_history['GenDate'] = subsector_history['GenDate'].dt.strftime("%Y-%m-%d")

    industry_history['GenDate'] =  pd.to_datetime(industry_history['GenDate'])
    industry_history['GenDate'] = industry_history['GenDate'].dt.strftime("%Y-%m-%d")

    cols_to_add = pd.DataFrame(columns=['EPS', 'Earnings Growth'])
    sector_history = pd.concat([sector_history, cols_to_add], axis='columns')
    subsector_history = pd.concat([subsector_history, cols_to_add], axis='columns')
    industry_history = pd.concat([industry_history, cols_to_add], axis='columns')

    sector_history.sort_values(by='GenDate',  inplace=True)
    subsector_history.sort_values(by='GenDate',  inplace=True)
    industry_history.sort_values(by='GenDate',  inplace=True)

    datelist = industry_history['GenDate'].drop_duplicates().tolist()

    for date in datelist:

      # print("\nInserting IRS for date:", date)

      sector_index = sector_history.loc[sector_history['GenDate']==date]
      subsector_index = subsector_history.loc[subsector_history['GenDate']==date]
      industry_index = industry_history.loc[industry_history['GenDate']==date]


      #Calculate Rank for backdate and PE/High Low
      # print("Calculating Change and Rank")
      irs_list = self.calc_rank_index(sector_index, subsector_index, industry_index, conn, date)

      # print("Calculating PE High/Low")
      self.calc_pe_high_low(irs_list, conn, date)


      irs_list = irs_list.astype({"Count": int})
      irs_list = irs_list[['GenDate', 'IndexName', 'Index', 'Open', 'High', 'Low', 'Close', 'Volume', 'PE', 'EPS', \
              'Count', 'OS', 'Rank', 'Change', 'Earnings Growth', 'PE High', 'PE High Date', 'PE Low', 'PE Low Date']]


      exportfilename = "indexlist_history.csv"
      exportfile = open(exportfilename,"w")
      irs_list.to_csv(exportfile, header=True, index=False,  lineterminator = '\r')
      exportfile.close()

      copy_sql = """
        COPY "Reports"."IRS" FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

      with open(exportfilename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
      f.close()
      os.remove(exportfilename)


  def pe_update(self,conn, cur, date):
    """ Update PE data

    Operation:
        Fetch the data from IRS table.
        and update the value of PE High and PE Low.
    """

    irs_sql = 'SELECT * FROM "Reports"."IRS" WHERE "GenDate" = \''+date+'\' ; '
    irs = sqlio.read_sql_query(irs_sql, con = conn)

    if not(irs.empty):

      irs_curr = self.calc_pe_high_low(irs, conn, date)

      for index, row in irs_curr.iterrows():

        pe_high = row['PE High']
        pe_high_date = row['PE High Date']

        pe_low = row['PE Low']
        pe_low_date = row['PE Low Date']
        # pe_high_date = pd.to_datetime(pe_high_date).date()
        # pe_low_date = pd.to_datetime(pe_low_date).date()
        if pd.isnull(pe_high_date):
            pe_high_date = None

        if pd.isnull(pe_low_date):
            pe_low_date = None

        index_name = row['IndexName']

        update_irs_sql = 'UPDATE "Reports"."IRS" \
                          SET  \
                            "PE High" = %s, \
                            "PE High Date" = %s, \
                            "PE Low" = %s, \
                            "PE Low Date" = %s \
                          WHERE "GenDate" = %s \
                          AND "IndexName" = %s; '

        cur.execute(update_irs_sql, (pe_high, pe_high_date, pe_low, pe_low_date, date, index_name))
        conn.commit()

    else:

      print("No IRS for today..")


  def gen_industry_list(self,conn,cur,date):
    """ Generate data for industry list

    Operation:
      Fetch the data from BTTList, OHLC data, percentage change,background info,
      Industry mapping info,Free Float,Free Float with OHLC and earnings from TTM.

    Report:
      Generate Industry List for current date.
    """


    global today
    today = date


    # print ("\n\nGetting BTT List")
    btt_list = self.get_btt_list(conn, today)
    # print("LENGTH OF BTTLIST: ", len(btt_list))

    # date = date.strftime("%Y-%m-%d")

    # print("Getting OHLC for date: ", today)
    ohlc = self.get_ohlc_data(conn, date)
    # print("LENGTH OF OHLC: ", len(ohlc))

    if not(ohlc.empty):

      # print("Merging BTTList with OHLC")
      btt_ohlc_merge = self.merge_btt_ohlc(btt_list , ohlc)
      # print("\nbtt_ohlc_merge: ", len(btt_ohlc_merge))
      # btt_ohlc_merge = btt_ohlc_merge.drop_duplicates(subset='CompanyCode')
      # print("Calculating percentage change")
      ohlc_change = self.calc_change_rate(btt_ohlc_merge, conn, today)
      # print("\nohlc_change: ", len(ohlc_change))

      # print("Getting background info")
      ohlc_backgroundinfo_merge = self.get_background_info(ohlc_change, conn)
      # print("\nohlc_backgroundinfo_merge: ", len(ohlc_backgroundinfo_merge))

      # print("Getting Industry mapping info")
      industry_merge = self.get_industry_data(ohlc_backgroundinfo_merge, conn)
      # print("\nindustry_merge: ", len(industry_merge))

      # print("Calculating Free Float")
      industry_list_calc_free_float = self.calc_free_float(industry_merge, date, conn)
      # print("\nindustry_list: ", len(industry_list))

      # print("Calculating Free Float with OHLC")
      industry_list_ff_ohlc =  self.cal_ff_ohlc(industry_list_calc_free_float, conn, today)
      # print("\nindustry_list: ", len(industry_list))

      # print("Getting earnings from TTM")
      industry_list = self.industry_ttm(industry_list_ff_ohlc, conn, today)
      # print("\nindustry_list: ", len(industry_list))

      # print("Inserting into Industry List")
      self.insert_industry_list(industry_list, conn, cur, today)


    else:

      # print("OHLC empty for Date: ", today)
      industry_list = pd.DataFrame()
      btt_ohlc_merge = pd.DataFrame()
      ohlc_change = pd.DataFrame()
      ohlc_backgroundinfo_merge = pd.DataFrame()
      industry_merge = pd.DataFrame()
      industry_list_calc_free_float = pd.DataFrame()
      industry_list_ff_ohlc = pd.DataFrame()


    # conn.close()

    return btt_list, ohlc, btt_ohlc_merge, ohlc_change, ohlc_backgroundinfo_merge, industry_merge, industry_list_calc_free_float, industry_list_ff_ohlc, industry_list

  def export_table(self, name,table):
    exportfilename = ""+name+"_export.csv"
    exportfile = open(exportfilename,"w")

    table.to_csv(exportfile, header=True, index=False,  lineterminator='\r')
    exportfile.close()

  def gen_divisor(self,conn,cur,date):
    """ Generating divisor for sector, subsector, and industry.

    Operation:
        Fetch the data from Industry list, Sector divisor,
        SubSector divisor and Industry divisor.
    """

    global today
    today = date

    # print("\nGetting Industry list for date: ", today)
    master_list = self.get_industry_list_backdate(conn, today)
    # self.export_table("1_master_list", master_list)
    # print("\tmaster_list: ", len(master_list))
    # print("Calculating divisor for Sector")
    sector_divisor = self.group_sector_divisor(master_list, conn)
    # self.export_table("2_sector_divisor", sector_divisor)
    # print("\sector_divisor: ", len(sector_divisor))
    # print("Calculating divisor for Sub Sector")
    subsector_divisor = self.group_subsector_divisor(master_list, conn)
    # self.export_table("3_group_subsector_divisor", subsector_divisor)
    # print("\tsubsector_divisor ", len(subsector_divisor))
    # print("Calculating divisor for Industry")
    industry_divisor = self.group_industry_divisor(master_list, conn)

    sub_industry_divisor = self.group_sub_industry_divisor(master_list, conn)

    # print("Calculating PrevCLose for Sector")
    sector_divisor_index = self.calc_sector_divisor(master_list, sector_divisor, conn, today)
    # self.export_table("5_sector_divisor_index", sector_divisor_index)
    # print("\tsector_divisor_index ", len(sector_divisor_index))
    # print("Calculating PrevCLose for SubSector")
    subsector_divisor_index = self.calc_subsector_divisor(master_list, subsector_divisor, conn, today)
    # self.export_table("6_subsector_divisor_index", subsector_divisor_index)
    # print("\tsubsector_divisor_index ", len(subsector_divisor_index))
    # print("Calculating PrevCLose for Industry")
    industry_divisor_index = self.calc_industry_divisor(master_list, industry_divisor, conn, today)

    # print("Calculating PrevCLose for Industry")
    sub_industry_divisor_index = self.calc_sub_industry_divisor(master_list, sub_industry_divisor, conn, today)

    # print("Inserting sector divisor data")
    self.insert_sector_divisor(sector_divisor_index, conn, cur, today)

    # print("Inserting subsector divisor data")
    self.insert_subsector_divisor(subsector_divisor_index, conn, cur, today)

    # print("Inserting industry divisor data")
    self.insert_industry_divisor(industry_divisor_index, conn, cur, today)

    # print("Inserting sub industry divisor data")
    self.insert_sub_industry_divisor(sub_industry_divisor_index, conn, cur, today)

#check if the variables are None type
    if sector_divisor is None:
      print("Sector Divisor is None")
      sector_divisor = pd.DataFrame()
    if subsector_divisor is None:
      print("Subsector Divisor is None")
      subsector_divisor = pd.DataFrame()
    if industry_divisor is None:
      print("Industry Divisor is None")
      industry_divisor = pd.DataFrame()
    if sub_industry_divisor is None:
      print("Sub Industry Divisor is None")
      sub_industry_divisor = pd.DataFrame()
    if sector_divisor_index is None:
      print("Sector Divisor Index is None")
      sector_divisor_index = pd.DataFrame()
    if subsector_divisor_index is None:
      print("Subsector Divisor Index is None")
      subsector_divisor_index = pd.DataFrame()
    if industry_divisor_index is None:
      print("Industry Divisor Index is None")
      industry_divisor_index = pd.DataFrame()
    if sub_industry_divisor_index is None:
      print("Sub Industry Divisor Index is None")
      sub_industry_divisor_index = pd.DataFrame()
    if master_list is None:
      print("Master List is None")
      master_list = pd.DataFrame()
    

    return master_list, sector_divisor, subsector_divisor, industry_divisor, sub_industry_divisor, sector_divisor_index, subsector_divisor_index, industry_divisor_index, sub_industry_divisor_index


  def gen_index_list(self,conn,cur,date):
    """Generate the index list for sector, subsector and industry.

    Operation:
        Fetch the index data of sector, subsector and
        industry and High Low value of PE.
    """

    global today
    today = date

    # print("\nGenerating Index List for Sector")
    sector_index = self.gen_sector_index(conn, today)
    # self.export_table("1_sector_index", sector_index)

    # print("Generating Index List for Sub Sector")
    subsector_index = self.gen_subsector_index(conn, today)
    # self.export_table("2_subsector_index", subsector_index)

    # print("Generating Index List for Industry")
    industry_index = self.gen_industry_index(conn, today)
    # self.export_table("3_industry_index", industry_index)

    # print("Generating Index List for SubIndustry")
    sub_industry_index = self.gen_sub_industry_index(conn, today)
    # self.export_table("3_industry_index", industry_index)

    # print("Inserting into sector index")
    self.insert_sector_index(sector_index, conn, cur, today)

    # print("Inserting into subsector index")
    self.insert_subsector_index(subsector_index, conn, cur, today)

    # print("Inserting into industry index")
    self.insert_industry_index(industry_index, conn, cur, today)

    # print("Inserting into SubIndustry index")
    self.insert_sub_industry_index(sub_industry_index, conn, cur, today)


    # print("Calculating Rank for indexes")
    irs_list_rank = self.calc_rank_index(sector_index, subsector_index, industry_index, sub_industry_index, conn, today)

    irs_list_rank = copy.deepcopy(irs_list_rank)

    # print("Calculating PE High/Low for indexes")
    irs_list, irs_pe = self.calc_pe_high_low(irs_list_rank, conn, today)

    # print("Inserting IRS")
    self.insert_irs(irs_list, conn, cur, today)

    # self.pe_update(conn, cur, date)
    if subsector_index is None:
      print("Subsector Index is None")
      subsector_index = pd.DataFrame()
    if industry_index is None:
      print("Industry Index is None")
      industry_index = pd.DataFrame()
    if sub_industry_index is None:
      print("Sub Industry Index is None")
      sub_industry_index = pd.DataFrame()
    if irs_list_rank is None:
      print("IRS List Rank is None")
      irs_list_rank = pd.DataFrame()
    if irs_list is None:
      print("IRS List is None")
      irs_list = pd.DataFrame()

    return sector_index, subsector_index, industry_index, sub_industry_index, irs_list_rank, irs_list, irs_pe


  def gen_irs_current(self,curr_date,conn,cur):
    """ Generating IRS for current date

    Operation:
        Fetch data from industry list, divisor and Index list
        and generate the IRS for current date.
    """

    print("Generating IRS for today: ", curr_date)
    gen_date = curr_date.strftime("%Y-%m-%d")
    btt_list, ohlc, btt_ohlc_merge, ohlc_change, ohlc_backgroundinfo_merge, industry_merge, industry_list_calc_free_float, industry_list_ff_ohlc, industry_list = self.gen_industry_list(conn,cur,gen_date)
    self.export_table("returnval", ohlc)

    if not(ohlc.empty):
      master_list, sector_divisor, subsector_divisor, industry_divisor, sub_industry_divisor, sector_divisor_index, subsector_divisor_index, industry_divisor_index, sub_industry_divisor_index = self.gen_divisor(conn,cur,gen_date)
      sector_index, subsector_index, industry_index, sub_industry_index,irs_list_rank, irs_list, irs_pe = self.gen_index_list(conn,cur,gen_date)
      print("\nCompleted IRS Ranking")

      if sector_divisor is None:
        print("Sector Divisor is None")
        sector_divisor = pd.DataFrame()
      if subsector_divisor is None:
        print("Subsector Divisor is None")
        subsector_divisor = pd.DataFrame()
      if industry_divisor is None:
        print("Industry Divisor is None")
        industry_divisor = pd.DataFrame()
      if sub_industry_divisor is None:
        print("Sub Industry Divisor is None")
        sub_industry_divisor = pd.DataFrame()
      if sector_divisor_index is None:
        print("Sector Divisor Index is None")
        sector_divisor_index = pd.DataFrame()
      if subsector_divisor_index is None:
        print("Subsector Divisor Index is None")
        subsector_divisor_index = pd.DataFrame()
      if industry_divisor_index is None:
        print("Industry Divisor Index is None")
        industry_divisor_index = pd.DataFrame()
      if sub_industry_divisor_index is None:
        print("Sub Industry Divisor Index is None")
        sub_industry_divisor_index = pd.DataFrame()
      if sector_index is None:
        print("Sector Index is None")
        sector_index = pd.DataFrame()
      if subsector_index is None:
        print("Subsector Index is None")
        subsector_index = pd.DataFrame()
      if industry_index is None:
        print("Industry Index is None")
        industry_index = pd.DataFrame()
      if sub_industry_index is None:
        print("Sub Industry Index is None")
        sub_industry_index = pd.DataFrame()
      if irs_pe is None:
        print("IRS PE is None")
        irs_pe = pd.DataFrame()
      if irs_list is None:
        print("IRS List is None")
        irs_list = pd.DataFrame()
      # check if the variables are None type
    else:
      print("\t\t\nIRS could not be generated - No OHLC data for: ", gen_date, "\n")
      exportfilename = "IRS_NOOHLC.csv"
      exportfile = open(exportfilename ,"a+")
      exportfile.write("IRS could not be generated - No OHLC data for: "+ gen_date+ "\n")
      exportfile.close()

      sector_divisor = pd.DataFrame()
      subsector_divisor = pd.DataFrame()
      industry_divisor = pd.DataFrame()
      sub_industry_divisor = pd.DataFrame()
      sector_divisor_index = pd.DataFrame()
      subsector_divisor_index = pd.DataFrame()
      industry_divisor_index = pd.DataFrame()
      sub_industry_divisor_index = pd.DataFrame()
      sector_index = pd.DataFrame()
      subsector_index = pd.DataFrame()
      industry_index = pd.DataFrame()
      sub_industry_index = pd.DataFrame()
      irs_list = pd.DataFrame()
      ohlc = pd.DataFrame()
      industry_list = pd.DataFrame()
      btt_list = pd.DataFrame()
      btt_ohlc_merge = pd.DataFrame()
      ohlc_change = pd.DataFrame()
      ohlc_backgroundinfo_merge = pd.DataFrame()
      industry_merge = pd.DataFrame()
      industry_list_calc_free_float = pd.DataFrame()
      industry_list_ff_ohlc = pd.DataFrame()
      irs_list_rank = pd.DataFrame()
      master_list = pd.DataFrame()
      irs_pe = pd.DataFrame()



      
    return btt_list, ohlc, btt_ohlc_merge, ohlc_change, ohlc_backgroundinfo_merge, industry_merge, industry_list_calc_free_float, industry_list_ff_ohlc, master_list,  sector_divisor, subsector_divisor, industry_divisor, sub_industry_divisor, sector_divisor_index, subsector_divisor_index, industry_divisor_index, sub_industry_divisor_index, sector_index, subsector_index, industry_index, sub_industry_index, irs_list_rank, irs_list, irs_pe, industry_list
    # return  industry_list