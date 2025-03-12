# Python script to generate indicator data for daily OHLC and insert into DB
import datetime
import ta
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
from ta import trend, volatility
import numpy as np
import math
from dateutil.relativedelta import relativedelta
import utils.date_set as date_set


class BTTIndex():
    """ Methods to calculate indicator data for Daily OHLC and insert them into the Database. """

    def __init__(self):
        self.start_date = datetime.date(2016,1,1)
        self.end_date = datetime.date.today() + datetime.timedelta(-1)

    def __del__(self):
        pass
    
    def calc_btt_divisor(self, conn, date):

    
        """ Calculating the btt divisor.

        Operation:
            Fetch the data from btt_prevData, BTTDivisor, and IndustryList table.
            Calculate the value for ff_close_current, divisor_current, os_current.

        Return:
            Data of btt divisor.
        """

        btt_prevData_sql  = ('SELECT  distinct on ("TICKER") * FROM public."IndexHistory" \
                                WHERE "DATE" < \''+str(date)+'\'  \
                                and "TICKER" = \'BTTIndex\' \
                                ORDER by "TICKER","DATE" desc ;')
        btt_prevData = sqlio.read_sql_query(btt_prevData_sql, con = conn)

        btt_prevData_OG = btt_prevData.copy()   

        divisor_backdate_sql = 'SELECT * FROM public."BTTDivisor" \
                            WHERE "Date" < \''+str(date)+'\' \
                            ORDER BY "Date" DESC limit 1; '
        divisor_backdate = sqlio.read_sql_query(divisor_backdate_sql, con = conn)
        

        sql = 'SELECT SUM("FF_Close") AS ff_close_sum, \
                    SUM("OS") AS os_sum  \
                    from public."IndustryList" \
                    WHERE "GenDate" = \''+str(date)+'\';'
                    
        btt_divisor_list = sqlio.read_sql_query(sql, con = conn)
        
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
        
        btt_prevData = btt_prevData.rename(columns = {"TICKER":"IndexName"})

        os_prev_list = divisor_backdate['OS']
        os_prev = os_prev_list.item() if len(os_prev_list.index) == 1 else np.nan
        # print("OS Prev: ", os_prev)
        
        os_current_list = btt_divisor_list['os_sum']
        os_current = os_current_list.item() if len(os_current_list.index) == 1 else np.nan
        # print("OS Current: ", os_current)
        
        divisor_back_list = divisor_backdate['Divisor']
        divisor_back = divisor_back_list 
        
        prev_close_list = btt_prevData['CLOSE']
        prev_close = prev_close_list
        # print("Prev Close: ", prev_close)

        if(os_prev == os_current):

            ff_close_prev_list = btt_divisor_list['ff_close_sum']
            ff_close_prev = ff_close_prev_list 

            divisor_current = divisor_back
            
            btt_prevData['BTTIndexValue'] = ff_close_prev
            btt_prevData['Divisor'] = divisor_current
            btt_prevData['OS'] = os_prev
        
        else:
            if prev_close is not np.nan:

                prev_close_index = btt_prevData['CLOSE']
                # print("Prev Close Index: ", prev_close_index)

                current_companies = current_industry_list
                # print("current_company_count: ", len(current_companies))
              
              # prev_companies = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]
                # print("prev_company_count: ", prev_companies)
                current_companies_prev_list = prev_industry_list.loc[prev_industry_list['CompanyCode'].isin(current_companies['CompanyCode'])]
                # print("Companies from current list on previous list :", len(current_companies_prev_list))
                
                new_companies = current_companies[~current_companies['CompanyCode'].isin(current_companies_prev_list['CompanyCode'])]
                # print("new_companies: ", len(new_companies))
              
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
                print(current_companies_prev_list)
                print(current_companies)
                merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))

                prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
                print("prev_companies_with_same_OS: ", len(prev_companies_with_same_OS))
                # print(row['IndexName'])
                print(prev_companies_with_same_OS.columns)
                prev_close_sum = prev_companies_with_same_OS['FF_Close_prev'].sum() 

                # prev_close_sum = prev_industry_list.loc[prev_industry_list['SubIndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
                # print("prev_close_sum: ", prev_close_sum) 
                #print("prev_mcap_close_sum: ", prev_mcap_close_sum)   
                # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close']
                # keep only row with the same companycode from current companies
                # prev_close_sum = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
                
                prev_companies_with_different_OS = merged_companies[merged_companies['OS_prev'] != merged_companies['OS_current']]
                print("prev_companies_with_different_OS: ", len(prev_companies_with_different_OS))
                # companies_with_diff_OS = prev_companies_with_different_OS[prev_companies_with_different_OS['IndustryIndexName'] == row['IndexName']]
                
                prev_companies_with_different_OS['prev_close_sum_for_diff_OS'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['FreeFloat_current'] * prev_companies_with_different_OS['PrevClose_current']
                # print(prev_companies_with_different_OS)
                changed_prev_close_sum = prev_companies_with_different_OS['prev_close_sum_for_diff_OS'].sum()
                # prev_companies_with_different_OS['prev_MCap_Close'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['PrevClose_current']
                # changed_prev_mcap_close_sum = prev_companies_with_different_OS['prev_MCap_Close'].sum()
                
                print("changed_prev_close_sum: ", changed_prev_close_sum)
                # print("changed_prev_mcap_close_sum: ", changed_prev_mcap_close_sum)
                
                # prev_mcap_close_sum = prev_industry_list.loc[prev_industry_list['SectorIndexName'] == row['IndexName']]['MCap_Close'].sum()
                print("", new_companies[['OS', 'FreeFloat', 'PrevClose']])

                new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
                print("new_companies: ", new_companies)
                addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
                print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)


                if not prev_close_index.empty:
                    prev_close_index = prev_close_index.iloc[0]
                    divisor = (prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum) / prev_close_index
                    print()
                    print("prev_close_sum: ", prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum)
                    print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)
                    print("prev_close_index: ", prev_close_index)
                    print("divisor: ", divisor)


                    btt_prevData['BTTIndexValue'] = prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum
                    btt_prevData['Divisor'] = divisor
                    btt_prevData['OS'] = os_current
                    
                    print(type(btt_prevData))
                    print(btt_prevData)                    
                    return btt_prevData
                else:
                    print("there is no previous close")
                    # divisor_current = ff_close_current / 1000
                    # print("Divisor: ", divisor_current)
                    # #print("FF_Open_sum: ", ff_close_current)
                    # #print("Mcap_Open_sum: ", mcap_open_current)
                    # #print("MCapDivisor: ", MCapdivisor_current)

                    # btt_prevData.loc['Divisor'] = divisor_current
                    # btt_prevData.loc['IndexValue'] = ff_close_current
                    # btt_prevData.loc['OS'] = os_current
                    
                    ff_close_current_list = btt_divisor_list['ff_close_sum']
                    ff_close_current = ff_close_current_list 

                    divisor_current = ff_close_current/1000

                    btt_prevData['BTTIndexValue'] = ff_close_current
                    btt_prevData['Divisor'] = divisor_current
                    btt_prevData['OS'] = os_current
                    
                    print("Divisor: ", divisor_current)
                    print("IndexValue: ", ff_close_current)
                    print("OS: ", os_current)
                    print(type(btt_prevData))





                

        return btt_prevData

    def insert_btt_divisor(self,btt_prevData, conn, cur, date):
        """ Insert btt Divisor data into database.

        Args:
            btt_prevData = Data of btt divisor.

        Operation:
            Export the data into BTTDivisor.csv file 
            and insert into BTTDivisor table.
        """

        btt_prevData["OS"].fillna(-1, inplace=True)
        btt_prevData = btt_prevData.astype({"OS": int})
        btt_prevData = btt_prevData.astype({"OS": str})
        btt_prevData["OS"] = btt_prevData["OS"].replace('-1', np.nan)
            
        btt_prevData['Date'] = date
        btt_prevData = btt_prevData[['IndexName','BTTIndexValue', 'Divisor', 'Date', 'OS']]

        exportfilename = "BTTDivisor.csv"
        exportfile = open(exportfilename,"w")
        btt_prevData.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        
        
        copy_sql = """
            COPY public."BTTDivisor" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        with open(exportfilename, 'r') as f:
        
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
            os.remove(exportfilename)

    def gen_btt_index(self,conn,cur,date):

        """ Generate the btt index

        Operation:
            Fetch the data from bttDivisor and IndustryList for current date,
            and calculate the Open, close, high, low, PE , EPS and Earning Growth.

            'open' = ff_open/divisor, 'high' = ff_high/divisor, 'low' = ff_low/divisor, 

            'close' = ff_close/divisor, 'earnings growth' = (eps – eps back / eps back) * 100 ,

            'pe' = sum os close / ff earning, eps = ff earning / ff equity 
            
        Return:
            Data of btt index.
        """
        # Generating btt index list

        btt_divisor_sql = 'SELECT * FROM public."BTTDivisor" WHERE "Date" = \''+str(date)+'\' ;'
        btt_divisor = sqlio.read_sql_query(btt_divisor_sql, con = conn)

        btt_ff_ohlc_sql = 'SELECT \
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
                            WHERE "GenDate" = \''+str(date)+'\';'
        btt_ff_ohlc = sqlio.read_sql_query(btt_ff_ohlc_sql, con = conn)

        six_month_back = (pd.to_datetime(date) - pd.DateOffset(days=180)).strftime("%Y-%m-%d")
        

        prev_close_BTTIndex_sql = 'SELECT "Close" FROM "Reports"."IRS" \
                                where "IndexName" = \'BTTIndex\' and \
                                  "GenDate" <= \''+ str(six_month_back) +'\' \
                                  order by "GenDate" desc limit 1'
        prev_close_BTTIndex = sqlio.read_sql_query(prev_close_BTTIndex_sql, con = conn)
 
        prev_close_list = prev_close_BTTIndex['Close'] 

        prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

        if btt_divisor.isnull().values.any():

            div_open = 1000
            div_high = 1000
            div_low = 1000
            div_close = 1000
            ff_vol = 1000
            pe = np.nan
            eps = 0
            ff_os = 0
            earnings_growth = 0
            change = 0
            
        else:

            ff_open_list = btt_ff_ohlc['ff_open_sum']
            ff_open = ff_open_list.item() if len(ff_open_list.index) == 1 and ff_open_list.item() is not None else 1000
                    
            ff_high_list = btt_ff_ohlc['ff_high_sum']
            ff_high = ff_high_list.item() if len(ff_high_list.index) == 1 and ff_high_list.item() is not None else 1000

            ff_low_list = btt_ff_ohlc['ff_low_sum']
            ff_low = ff_low_list.item() if len(ff_low_list.index) == 1 and ff_low_list.item() is not None else 1000

            ff_close_list = btt_ff_ohlc['ff_close_sum']
            ff_close = ff_close_list.item() if len(ff_close_list.index) == 1 and ff_close_list.item() is not None else 1000

            ff_vol_list = btt_ff_ohlc['sum_vol']
            ff_vol = ff_vol_list.item() if len(ff_vol_list.index) == 1 and ff_vol_list.item() is not None else 1000

            ff_os_list = btt_ff_ohlc['os_sum']
            ff_os = ff_os_list.item() if len(ff_os_list.index) == 1 and ff_os_list.item() is not None else 1000

            ff_earnings_list = btt_ff_ohlc['earnings']
            ff_earnings = ff_earnings_list.item() if len(ff_earnings_list.index) == 1 and ff_earnings_list.item() is not None else 1000
            
            ff_equity_list = btt_ff_ohlc['sum_equity']
            ff_equity = ff_equity_list.item() if len(ff_equity_list.index) == 1 and ff_equity_list.item() is not None else 1000

            prev_earnings_list = btt_ff_ohlc['prev_earnings']
            prev_earnings = prev_earnings_list.item() if len(prev_earnings_list.index) == 1 and prev_earnings_list.item() is not None else 1000

            prev_equity_list = btt_ff_ohlc['prev_equity']
            prev_equity = prev_equity_list.item() if len(prev_equity_list.index) == 1 and prev_equity_list.item() is not None else 1000
            
            print("BTT Divisor: ", btt_divisor['Divisor'])
            divisor_list = btt_divisor['Divisor'] 
            if divisor_list.isnull().any() or len(divisor_list) != 1:
                divisor = 1000
            else:
                divisor = divisor_list.item()
            divisor = divisor_list.item() if len(divisor_list.index) == 1 else np.nan

            sum_os_close_list = btt_ff_ohlc['sum_os_close']
            sum_os_close = sum_os_close_list.item() if len(sum_os_close_list.index) == 1 and sum_os_close_list.item() is not None else 1000

            div_open = ff_open/divisor
            div_high = ff_high/divisor
            div_low = ff_low/divisor
            div_close = ff_close/divisor

            pe = (sum_os_close)/ff_earnings if ff_earnings !=0 else np.nan
            eps = ff_earnings/ff_equity if ff_equity !=0 else np.nan
            eps_back = prev_earnings/prev_equity if prev_equity !=0 else np.nan
            earnings_growth = ((eps-eps_back)/abs(eps_back))*100 if eps_back != 0 else np.nan

        btt_ff_ohlc['Open'] = div_open
        btt_ff_ohlc['High'] = div_high
        btt_ff_ohlc['Low'] = div_low
        btt_ff_ohlc['Close'] = div_close
        btt_ff_ohlc['Volume'] = ff_vol
        btt_ff_ohlc['PE'] = pe
        btt_ff_ohlc['EPS'] = eps

        # change = current close - prev close/prev close * 100
        change = (div_close - prev_close) / prev_close * 100 if prev_close != np.nan else 0
        
        btt_ff_ohlc['Change'] = change
        btt_ff_ohlc['OS'] = ff_os
        btt_ff_ohlc['Earnings Growth'] = earnings_growth

        btt_ff_ohlc['GenDate'] = date

        pe_back_sql = 'SELECT * from "Reports"."IRS" \
				   WHERE "IndexName"=\'BTTIndex\' and "GenDate" < \''+str(date)+'\' \
				   ORDER BY  "GenDate" desc limit 1;'

        pe_back_list = sqlio.read_sql_query(pe_back_sql, con=conn)

        # PE High
        for index, row in btt_ff_ohlc.iterrows():

            # PE for current OHLC
            pe_current_high_list = btt_ff_ohlc['PE']
            pe_current_high_date_list = btt_ff_ohlc['GenDate']

            pe_current_high = pe_current_high_list.item() if len(
                pe_current_high_list.index) == 1 else np.nan
            pe_current_high_date = pe_current_high_date_list.item() if len(
                pe_current_high_date_list.index) == 1 else np.nan

            # PE High for backdate
            pe_back_high_list = pe_back_list['PE High'] 
            pe_back_high_date_list = pe_back_list['PE High Date']

            pe_back_high = pe_back_high_list.item() if len(
                pe_back_high_list.index) == 1 else np.nan
            pe_back_high_date = pe_back_high_date_list.item() if len(
                pe_back_high_date_list.index) == 1 else np.nan
            
            # Compare current PE with backdate PE High value
            if pe_back_high is None:

                btt_ff_ohlc.loc[index, 'PE High'] = pe_current_high
                btt_ff_ohlc.loc[index, 'PE High Date'] = pe_current_high_date

            elif (pe_current_high > pe_back_high):

                btt_ff_ohlc.loc[index, 'PE High'] = pe_current_high
                btt_ff_ohlc.loc[index, 'PE High Date'] = pe_current_high_date

            else:

                btt_ff_ohlc.loc[index, 'PE High'] = pe_back_high
                btt_ff_ohlc.loc[index, 'PE High Date'] = pe_back_high_date

        # # PE Low
        for index, row in btt_ff_ohlc.iterrows():

            # PE for current OHLC
            pe_current_low_list = btt_ff_ohlc['PE']
            pe_current_low_date_list = btt_ff_ohlc['GenDate']

            pe_current_low = pe_current_low_list.item() if len(
                pe_current_low_list.index) == 1 else np.nan
            pe_current_low_date = pe_current_low_date_list.item() if len(
                pe_current_low_date_list.index) == 1 else np.nan

            # PE Low for backdate
            pe_back_low_list = pe_back_list['PE Low']
            pe_back_low_date_list = pe_back_list['PE Low Date']

            pe_back_low = pe_back_low_list.item() if len(
                pe_back_low_list.index) == 1 else np.nan
            pe_back_low_date = pe_back_low_date_list.item() if len(
                pe_back_low_date_list.index) == 1 else np.nan

            # Compare current PE with backdate PE Low value
            if pe_back_low is None:

                btt_ff_ohlc.loc[index, 'PE Low'] = pe_current_low
                btt_ff_ohlc.loc[index, 'PE Low Date'] = pe_current_low_date

            elif (pe_current_low < pe_back_low):

                btt_ff_ohlc.loc[index, 'PE Low'] = pe_current_low
                btt_ff_ohlc.loc[index, 'PE Low Date'] = pe_current_low_date

            else:

                btt_ff_ohlc.loc[index, 'PE Low'] = pe_back_low
                btt_ff_ohlc.loc[index, 'PE Low Date'] = pe_back_low_date
        
        return btt_ff_ohlc

    def insert_btt_index(self,btt_index, conn, cur, date):
        """ Insert btt index data into database.

        Args:
            btt_index = data of Open, close, high, low, PE , EPS 
                            and Earning Growth of btt index.

        Operation:
            Export the data into BTTIndexList.csv file 
            and insert into BTTIndexList table.
        """
        
        btt_index['TICKER'] = 'BTTIndex'
        btt_index['IndexName'] = 'BTTIndex'
        btt_index['Index'] = 'BTT'
        btt_index['Rank'] = np.nan
        btt_index['MCap_OPEN'] = np.nan
        btt_index['MCap_HIGH'] = np.nan
        btt_index['MCap_LOW'] = np.nan
        btt_index['MCap_CLOSE'] = np.nan
        btt_index['MCap_Open_Index'] = np.nan
        btt_index['MCap_High_Index'] = np.nan
        btt_index['MCap_Low_Index'] = np.nan
        btt_index['MCap_Close_Index'] = np.nan
        btt_index[['ff_open_sum', 'ff_high_sum', \
                          'ff_low_sum', 'ff_close_sum', 'MCap_Open_sum', 'MCap_High_sum', 'MCap_Low_sum', 'MCap_Close_sum',]] = np.nan
        
        
  
        btt_index_history = btt_index[['TICKER', 'GenDate', 'Open', 'High', 'Low', 'Close', 'MCap_OPEN', 'MCap_HIGH', 'MCap_LOW', 'MCap_CLOSE', 'sum_vol']]

        exportfilename = "BTT_btt_prevData.csv"
        exportfile = open(exportfilename,"w")
        btt_index_history.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator = '\r')
        exportfile.close()

        copy_sql = """
            COPY public."IndexHistory" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        
        with open(exportfilename, 'r') as f: 
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        # os.remove(exportfilename)


        # ************************************

        btt_index_IRS = btt_index[['GenDate', 'IndexName', 'Index', 'Open', 'High', 'Low', 'Close', \
                         'MCap_Open_Index', 'MCap_High_Index', 'MCap_Low_Index', 'MCap_Close_Index', 'sum_vol', 'PE', 'EPS', \
                        'company_count', 'os_sum', 'Rank', 'Change', 'Earnings Growth','ff_open_sum', 'ff_high_sum', \
                          'ff_low_sum', 'ff_close_sum', 'MCap_Open_sum', 'MCap_High_sum', 'MCap_Low_sum', 'MCap_Close_sum',\
                              'PE High', 'PE High Date', 'PE Low', 'PE Low Date']]

        exportfilename1 = "BTT_IndexIRS.csv"
        exportfile = open(exportfilename1,"w")
        btt_index_IRS.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator = '\r')
        exportfile.close()


        copy_sql = """
            COPY "Reports"."IRS" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        
        with open(exportfilename1, 'r') as f: 
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        # os.remove(exportfilename1)

    def gen_btt_index_day_change(self, conn, cur, btt_index, date):

        """ Generate the btt index

        Operation:
            change = (current_close - prev_close)  / prev_close * 100
            
        Return:
            Data of index change for one day.
        """

        current_close_BTTIndex_query = 'select "CLOSE" from public."IndexHistory" \
                                        where "DATE" = \''+ str(date) +'\' and "TICKER" = \'BTTIndex\' \
                                         order by "DATE" desc limit 1;'
        current_close_BTTIndex = sqlio.read_sql_query(current_close_BTTIndex_query, con = conn)

        cur_close_list = current_close_BTTIndex['CLOSE'] 
        cur_close = cur_close_list.item() if len(cur_close_list.index) == 1 else np.nan
        print("Current Close: ", cur_close)
 

        prev_close_BTTIndex_query = 'select "CLOSE" from public."IndexHistory" \
                                     where "DATE" < \''+ str(date) +'\' and "TICKER" = \'BTTIndex\' \
                                     order by "DATE" desc limit 1;'
        prev_close_BTTIndex = sqlio.read_sql_query(prev_close_BTTIndex_query, con = conn)

        prev_close_list = prev_close_BTTIndex['CLOSE'] 
        prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan
        print("Prev Close: ", prev_close)
        change = ((cur_close - prev_close) / prev_close) * 100 if prev_close != np.nan else 0

        btt_index = pd.DataFrame({"Change": [change],"GenDate": [date]})
        
        return btt_index

    def insert_btt_index_day_change(self,btt_index, conn, cur, date):

        """ Insert btt index data into database.

        Args:
            btt_index = data of Open, close, high, low, PE , EPS 
                            and Earning Growth of btt index.

        Operation:
            Export the data into BTTIndexList.csv file 
            and insert into BTTIndexList table.
        """
        
        btt_index['IndexName'] = 'BTTIndex'

        btt_index_IRS = btt_index[['IndexName', 'Change', 'GenDate']]

        exportfilename1 = "BTT_IndexDayChange.csv"
        exportfile = open(exportfilename1,"w")
        btt_index_IRS.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator = '\r')
        exportfile.close()


        copy_sql = """
            COPY public."nse_index_change" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        
        with open(exportfilename1, 'r') as f: 
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename1)
   
    def cal_BTT_divisor_Index(self, curr_date, conn, cur):

        gen_date = curr_date.strftime("%Y-%m-%d")

        today = gen_date

        print("\n\nGetting BTT Divisor for date: ", today)
        
        """ Generating divisor for btt, subbtt, and industry.

        Operation:
            Fetch the data from Industry list, btt divisor.
        """

        print("Calculating PrevCLose for btt")
        btt_divisor_index = self.calc_btt_divisor( conn, today)
        
        print("Inserting btt divisor data")
        self.insert_btt_divisor(btt_divisor_index, conn, cur, today)

        print("\n\nGetting BTT Index list for date: ", today)
        btt_index = self.gen_btt_index(conn, cur, today)

        print("\n\nInsert BTT list for date: ", today)
        self.insert_btt_index(btt_index, conn, cur, today)

        print("\n\nGetting BTT list for date: ", today)
        btt_index_day_change = self.gen_btt_index_day_change(conn, cur, btt_index, today)            

        print("\n\nInsert BTT list for date: ")
        self.insert_btt_index_day_change(btt_index_day_change, conn, cur, today)


    def cal_BTT_divisor_Index_history(self, conn, cur):
        
        """ Generating divisor for btt.

        Operation:
            Fetch the data from btt divisor.

        """

        sql = 'SELECT distinct on ("GenDate") "GenDate"  FROM public."IndustryList"; '
        master_list = sqlio.read_sql_query(sql, con=conn)

        master_prev_date = (pd.to_datetime(master_list['GenDate'].iloc[0]) + datetime.timedelta(-1)).strftime("%Y-%m-%d")
        master_prev_date = datetime.datetime.strptime(master_prev_date,"%Y-%m-%d").date()
        
        new_row = pd.DataFrame({'GenDate':master_prev_date}, index =[0]) 
        # simply concatenate both dataframes 
        master_list = pd.concat([new_row, master_list]).reset_index(drop = True) 
        
        master_date_list = master_list["GenDate"].tolist()
       
        for today in master_date_list:
            print("\n\nGetting BTT Divisor for date: ", today)

            print("\n\nCalculating PrevCLose for btt")
            btt_divisor_index = self.calc_btt_divisor(conn, today)
            
            print("Inserting btt divisor data")
            self.insert_btt_divisor(btt_divisor_index, conn, cur, today)

            print("\n\nGetting BTT list for date with 6 month change : ", today)
            btt_index = self.gen_btt_index(conn, cur, today)            

            print("\n\nInsert BTT list for date with 1 day change : ", today)
            self.insert_btt_index(btt_index, conn, cur, today)

            print("\n\nGetting BTT list for date: ", today)
            btt_index_day_change = self.gen_btt_index_day_change(conn, cur, btt_index, today)

            print("\n\nInsert BTT list for date: ")
            self.insert_btt_index_day_change(btt_index_day_change, conn, cur, today)
