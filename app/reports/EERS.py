#Script to compile and insert daily EPS reports per each company on BTT List
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
from concurrent.futures import ThreadPoolExecutor, as_completed


today = np.nan


class EERS:

    """ Generating EPS Report for Quarters
         And calculating the data of EPS growth and Sales growth
        EPS Rating for quarters and TTM data for previous one, two and three years
    """

    def __init__(self):
        pass
    
    def get_closest_quarter(self, target):
        """Fetch Closest quarter from the current date """
        
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
        # print("date",min(candidates, key=lambda d: abs(target - d)))

        return min(candidates, key=lambda d: abs(target - d))
    


    def get_previous_quarter(self, target):
        """ Fetch previous quarter from current quarter """

        curr_qrt = self.get_closest_quarter(target)
        curr_qrt_dec = datetime.date(curr_qrt.year, curr_qrt.month, (curr_qrt.day - 2))
        prev_qrt = self.get_closest_quarter(curr_qrt_dec)
        return prev_qrt

    def get_year_before_quarter(self, target):
        """Fetching last four quarter from the current quarter"""
        
        curr_qrt = self.get_closest_quarter(target)
        one_qtr_back = self.get_previous_quarter(curr_qrt)
        two_qtr_back = self.get_previous_quarter(one_qtr_back)
        three_qtr_back = self.get_previous_quarter(two_qtr_back)
        four_qtr_back = self.get_previous_quarter(three_qtr_back)
        return four_qtr_back

    def get_two_years_before_quarter(self, target):
        """ fetching the two year back quarter"""
        
        year_back = self.get_year_before_quarter(target)
        two_years_back = self.get_year_before_quarter(year_back)
        return two_years_back

    def get_three_years_before_quarter(self, target):
        """ fetch quarter from last three year back """
        
        year_back = self.get_year_before_quarter(target)
        two_years_back = self.get_year_before_quarter(year_back)
        three_years_back = self.get_year_before_quarter(two_years_back)
        return three_years_back

    def get_four_years_before_quarter(self, target):
        """ fetch quarter from last four year back """
        
        year_back = self.get_year_before_quarter(target)
        two_years_back = self.get_year_before_quarter(year_back)
        three_years_back = self.get_year_before_quarter(two_years_back)
        four_years_back = self.get_year_before_quarter(three_years_back)
        return four_years_back


    def set_daily_qtr_eps(self, conn, today):
        """Fetching the data for daily Quarterly EPS,

        Operation:
            Takes the data from quarterly results, and compares against
            quarterly EPS to get latest available data
            and calculating the value of 'sales','Expenses',
            'EBIDTA','Extraordinary','Ext_Flag','EPS' and 'NPM',

        Return:
            Quarterly EPS list .
        """

        start_qtr = self.get_previous_quarter(today).strftime("%Y-%m-%d")
        end_qtr = self.get_closest_quarter(today).strftime("%Y-%m-%d") 
        print("End Qtr: ", end_qtr)
        # print("START QTR: {} END QTR: {}".format(start_qtr, end_qtr))
        sql = 'SELECT QR.* from public."QuarterlyResults" QR LEFT JOIN public."QuarterlyEERS" QE on QR."CompanyCode"= QE."CompanyCode" and QE."YearEnding" = QR."YearEnding"\
        WHERE QR."YearEnding" <= \'' + end_qtr + '\' AND QE."CompanyCode" is null;'
        # AND QR."ModifiedDate"<=\''+str(today + datetime.timedelta(1))+'\';'

        quarterly_list = pd.read_sql_query(sql, con = conn)
        
        print("CompanyCode NULL List: ", len(quarterly_list))
        # Renaming the column name
        quarterly_list = quarterly_list.rename(columns={'InterestCharges': 'Interest', 'PL_Before_Tax': 'OPM', 'TaxCharges': 'Tax',\
        'PL_After_TaxFromOrdineryActivities': 'PATRAW' , 'EquityCapital': 'Equity', 'ReservesAndSurplus': 'Reserves' })
        
        # column sclicing
        quarterly_list[quarterly_list.columns[5:60]] = quarterly_list[quarterly_list.columns[5:60]].replace(r'[?$,]', '', regex=True).astype(float)

        quarterly_list['Sales'] =  quarterly_list['TotalIncomeFromOperations'] + quarterly_list['IntOrDiscOnAdvOrBills']  + quarterly_list['IncomeOnInvestments']\
        + quarterly_list['IntOnBalancesWithRBI'] + quarterly_list['Others']  + quarterly_list['OtherRecurringIncome']

        quarterly_list['Expenses'] = quarterly_list['StockAdjustment'] + quarterly_list['RawMaterialConsumed']+quarterly_list['PurchaseOfTradedGoods']\
        +quarterly_list['PowerAndFuel']+quarterly_list['EmployeeExpenses']+quarterly_list['Excise']+quarterly_list['AdminAndSellingExpenses']+quarterly_list['ResearchAndDevelopmentExpenses']\
        + quarterly_list['ExpensesCapitalised'] + quarterly_list['OtherExpeses'] 

        quarterly_list['EBIDTA'] = quarterly_list['Sales'] - quarterly_list['Expenses']

        quarterly_list['Extraordinary'] = quarterly_list['ExtraOrdinaryItem'] + quarterly_list['ExceptionalItems']
        # RULE_CHANGE
        quarterly_list['Ext_Flag'] = [0 if x == 0 else 0 for x in quarterly_list['Extraordinary']] # 0 if x == 0 else 0

        quarterly_list['PAT'] = np.nan

        # print("quarterly_list: ", len(quarterly_list), flush = True)

        for index, row in quarterly_list.iterrows():
            quarterly_list.loc[index, 'PAT'] =	row['PATRAW'] # if row['Ext_Flag'] == 0 else np.nan ##????

        quarterly_list['EERS'] = quarterly_list['EBIDTA']/quarterly_list['Equity']

        
        quarterly_list['NPM'] = quarterly_list['PAT']/quarterly_list['Sales'] * 100
        
        quarterly_eps_list = quarterly_list[['CompanyCode', 'YearEnding', 'Months', 'Quarter', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary',\
        'OPM', 'Tax', 'PATRAW', 'PAT', 'Equity', 'Reserves', 'EERS', 'NPM', 'Ext_Flag']]


        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(columns = ['Q1 EERS Growth','Q1 Sales Growth', 'Q2 EERS', 'Q2 EERS Growth', 'Q2 Sales','Q2 Sales Growth'])], sort=False)

        return quarterly_eps_list

    def consolidated_set_daily_qtr_eps(self, conn, today):
        """Fetching the data for daily Quarterly EPS,

        Operation:
            Takes the data from quarterly results, and compares against
            quarterly EPS to get latest available data
            and calculating the value of 'sales','Expenses',
            'EBIDTA','Extraordinary','Ext_Flag','EPS' and 'NPM',

        Return:
            Quarterly EPS list .
        """
        start_qtr = self.get_previous_quarter(today).strftime("%Y-%m-%d")
        end_qtr = self.get_closest_quarter(today).strftime("%Y-%m-%d") 
        print("End Qtr: ", end_qtr)
        # print("START QTR: {} END QTR: {}".format(start_qtr, end_qtr))
        sql = 'SELECT QR.* from public."ConsolidatedQuarterlyResults" QR LEFT JOIN public."ConsolidatedQuarterlyEERS" QE on QR."CompanyCode"= QE."CompanyCode" and QE."YearEnding" = QR."YearEnding"\
        WHERE QR."YearEnding" <= \'' + end_qtr + '\' AND QE."CompanyCode" is null;'
       

        quarterly_list = pd.read_sql_query(sql, con = conn)
        
        print("CompanyCode NULL List: ", len(quarterly_list))
        # Renaming the column name
        quarterly_list = quarterly_list.rename(columns={'InterestCharges': 'Interest', 'PL_Before_Tax': 'OPM', 'TaxCharges': 'Tax',\
        'NetPLAfterMIAssociates': 'PATRAW' , 'EquityCapital': 'Equity', 'ReservesAndSurplus': 'Reserves' })
        
        # column sclicing
        quarterly_list[quarterly_list.columns[5:60]] = quarterly_list[quarterly_list.columns[5:60]].replace(r'[?$,]', '', regex=True).astype(float)

        quarterly_list['Sales'] =  quarterly_list['TotalIncomeFromOperations'] + quarterly_list['IntOrDiscOnAdvOrBills']  + quarterly_list['IncomeOnInvestements'] \
        + quarterly_list['IntOnBalancesWithRBI'] + quarterly_list['Others']  + quarterly_list['OtherRecurringIncome']

        quarterly_list['Expenses'] = quarterly_list['StockAdjustment'] + quarterly_list['RawMaterialConsumed']+quarterly_list['PurchaseOfTradedGoods']\
        +quarterly_list['PowerAndFuel']+quarterly_list['EmployeeExpenses']+quarterly_list['Excise']+quarterly_list['AdminAndSellingExpenses']+quarterly_list['ResearchAndDevelopmentExpenses']\
        + quarterly_list['ExpensesCapitalised'] + quarterly_list['OtherExpeses'] 

        quarterly_list['EBIDTA'] = quarterly_list['Sales'] - quarterly_list['Expenses']

        quarterly_list['Extraordinary'] = quarterly_list['ExtraOrdinaryItem'] + quarterly_list['ExceptionalItems']
        # RULE_CHANGE
        quarterly_list['Ext_Flag'] = [0 if x == 0 else 0 for x in quarterly_list['Extraordinary']] # 0 if x == 0 else 0

        quarterly_list['PAT'] = np.nan

        # print("quarterly_list: ", len(quarterly_list), flush = True)

        for index, row in quarterly_list.iterrows():
            quarterly_list.loc[index, 'PAT'] =	row['PATRAW'] if row['Ext_Flag'] == 0 else np.nan

        quarterly_list['EERS'] = quarterly_list['EBIDTA']/quarterly_list['Equity']

        
        quarterly_list['NPM'] = quarterly_list['PAT']/quarterly_list['Sales'] * 100
        
        quarterly_eps_list = quarterly_list[['CompanyCode', 'YearEnding', 'Months', 'Quarter', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary',\
        'OPM', 'Tax', 'PATRAW', 'PAT', 'Equity', 'Reserves', 'EERS', 'NPM', 'Ext_Flag']]

        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(columns = ['Q1 EERS Growth','Q1 Sales Growth', 'Q2 EERS', 'Q2 EERS Growth', 'Q2 Sales','Q2 Sales Growth'])], sort=False)
        
        return quarterly_eps_list
        
        

    def insert_quarterly_eps_resulsts(self, quarterly_eps_list, conn):
        """Insert the quartely eps data into database,

        Args:
            quarterly_eps_lis = data of sales,expenses,EBITA, Q1, Q2 EPS and Sales growth,

        Operation:
            Exporting the data into QuarterlyEPSListExport.csv file and
            Inserting the data into QuarterlyEPS Table.
        """

        cur = conn.cursor()
        # filling NaN values with -1 in "Months" column 
        quarterly_eps_list["Months"].fillna(-1, inplace=True)
        quarterly_eps_list = quarterly_eps_list.astype({"Months": int})
        quarterly_eps_list = quarterly_eps_list.astype({"Months": str})
        quarterly_eps_list["Months"] = quarterly_eps_list["Months"].replace('-1', np.nan)

        # filling NaN values with -1 in "Quarter" column 
        quarterly_eps_list["Quarter"].fillna(-1, inplace=True)
        quarterly_eps_list = quarterly_eps_list.astype({"Quarter": int})
        quarterly_eps_list = quarterly_eps_list.astype({"Quarter": str})
        quarterly_eps_list["Quarter"] = quarterly_eps_list["Quarter"].replace('-1', np.nan)
        
        # filling NaN values with -1 in "Ext_Flag" column 
        quarterly_eps_list["Ext_Flag"].fillna(-1, inplace=True)
        quarterly_eps_list = quarterly_eps_list.astype({"Ext_Flag": int})
        quarterly_eps_list = quarterly_eps_list.astype({"Ext_Flag": str})
        quarterly_eps_list["Ext_Flag"] = quarterly_eps_list["Ext_Flag"].replace('-1', np.nan)
        
        quarterly_eps_list = quarterly_eps_list[['CompanyCode', 'YearEnding', 'Months', 'Quarter', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary',\
        'OPM', 'Tax', 'PATRAW', 'PAT', 'Equity', 'Reserves', 'EERS', 'NPM', 'Ext_Flag', 'Q1 EERS Growth', 'Q1 Sales Growth', 'Q2 EERS', 'Q2 EERS Growth', 'Q2 Sales', 'Q2 Sales Growth']]

        exportfilename = "QuarterlyEERSListExport.csv"
        exportfile = open(exportfilename,"w+")
        quarterly_eps_list.to_csv(exportfile, header=True, index=False, lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY "public"."QuarterlyEERS" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)
        updatequery = 'UPDATE public."QuarterlyEERS" SET "Ext_Flag" = true WHERE "Ext_Flag" IS NULL'
                            
        cur.execute(updatequery)
        conn.commit()


    
    def insert_consolidated_quarterly_eps_results(self, quarterly_eps_list, conn):
        """Insert the quartely eps data into database,

        Args:
            quarterly_eps_lis = data of sales,expenses,EBITA, Q1, Q2 EPS and Sales growth,

        Operation:
            Exporting the data into QuarterlyEPSListExport.csv file and
            Inserting the data into QuarterlyEPS Table.
        """

        cur = conn.cursor()
        # filling NaN values with -1 in "Months" column 
        quarterly_eps_list["Months"].fillna(-1, inplace=True)
        quarterly_eps_list = quarterly_eps_list.astype({"Months": int})
        quarterly_eps_list = quarterly_eps_list.astype({"Months": str})
        quarterly_eps_list["Months"] = quarterly_eps_list["Months"].replace('-1', np.nan)

        # filling NaN values with -1 in "Quarter" column 
        quarterly_eps_list["Quarter"].fillna(-1, inplace=True)
        quarterly_eps_list = quarterly_eps_list.astype({"Quarter": int})
        quarterly_eps_list = quarterly_eps_list.astype({"Quarter": str})
        quarterly_eps_list["Quarter"] = quarterly_eps_list["Quarter"].replace('-1', np.nan)
        
        # filling NaN values with -1 in "Ext_Flag" column 
        quarterly_eps_list["Ext_Flag"].fillna(-1, inplace=True)
        quarterly_eps_list = quarterly_eps_list.astype({"Ext_Flag": int})
        quarterly_eps_list = quarterly_eps_list.astype({"Ext_Flag": str})
        quarterly_eps_list["Ext_Flag"] = quarterly_eps_list["Ext_Flag"].replace('-1', np.nan)
        
        # print("Quarterly eps",quarterly_eps_list["YearEnding"].head())

        ###
        # print("Duplicates:\n", [~quarterly_eps_list.duplicated(subset=['CompanyCode', 'YearEnding'])])
        # quarterly_eps_list_duplicated = quarterly_eps_list.drop_duplicates(subset=['CompanyCode', 'YearEnding'])
        # quarterly_eps_list_duplicated = quarterly_eps_list[quarterly_eps_list.duplicated()]
        # print("Quaterly EPS:\n", quarterly_eps_list)
        ###
        quarterly_eps_list = quarterly_eps_list[['CompanyCode', 'YearEnding', 'Months', 'Quarter', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary',\
        'OPM', 'Tax', 'PATRAW', 'PAT', 'Equity', 'Reserves', 'EERS', 'NPM', 'Ext_Flag', 'Q1 EERS Growth', 'Q1 Sales Growth', 'Q2 EERS', 'Q2 EERS Growth', 'Q2 Sales', 'Q2 Sales Growth']]

        exportfilename = "ConsolidatedQuarterlyEERSListExport.csv"
        exportfile = open(exportfilename,"w+")
        quarterly_eps_list.to_csv(exportfile, header=True, index=False, lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY "public"."ConsolidatedQuarterlyEERS" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        # os.remove(exportfilename)

        updatequery = 'UPDATE public."ConsolidatedQuarterlyEERS" SET "Ext_Flag" = true WHERE "Ext_Flag" IS NULL'
                            
        cur.execute(updatequery)
        conn.commit()

    def quarterly_one_eps_sales_growth(self, quarterly_eps_list, conn,today):
        """Calculating the EPS and Sales Growth for current quarter,

        Args:
            quarterly_eps_list = quarterly eps list data of daily qtr eps,
        
        Operation:
            Take the data from 'QuarterlyEPS' table for previous 4 quarter, 
            and calculate the Q1 EPS Growth and Q1 Sales Growth
            EPS Growth  = (eps_curr - eps_prev / abs(eps_prev) * 100)
            Sales Growth = (((sales_current-sales_prev)/abs(sales_prev))*100),

        Return:
            EPS and Sales Growth of previous one year quarter.
          """

        qtr_one_start = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        prev_year =  self.get_year_before_quarter(self.get_closest_quarter(today))
        prev_2year =  self.get_year_before_quarter(prev_year).strftime("%Y-%m-%d")
        prev_4year = self.get_four_years_before_quarter(today).strftime("%Y-%m-%d")

        
        sql = 'SELECT * FROM public."QuarterlyEERS" where "Ext_Flag" is not null and "YearEnding" >= \'' + prev_4year + '\' and "YearEnding" <= \'' + today.strftime("%Y-%m-%d") + '\';'
        quarterly_eps_year_back = sqlio.read_sql_query(sql, con = conn)	
        quarterly_eps_list['Sales'] =quarterly_eps_list['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        quarterly_eps_year_back['Sales'] = quarterly_eps_year_back['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        

        #Order quarterly_eps_list by yearending asc
        quarterly_eps_list = quarterly_eps_list.sort_values(by = 'YearEnding', ascending = True)

        for index, row in quarterly_eps_list.iterrows():

            prev_year_qtr = self.get_year_before_quarter(row['YearEnding'])

            eps_current = row['EERS']

            eps_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]["EERS"]
            
            if len(eps_prev_list.index) > 1:
                    eps_prev = eps_prev_list[eps_prev_list.index[0]]
            else:
                eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan

            if (math.isnan(eps_prev)):
                eps_prev_list = quarterly_eps_list.loc[(quarterly_eps_list["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_list["YearEnding"] == prev_year_qtr)]["EERS"]
                if len(eps_prev_list.index) > 1:
                    eps_prev = eps_prev_list[eps_prev_list.index[0]]
                else:
                    eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan
        
                # calculating Q1 EPS Growth
                quarterly_eps_list.loc[index, 'Q1 EERS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            
            # q1_f_epsprev.write("CompanyCode: {}\neps_current: {}\neps_prev: {}\nabs(eps_prev): {}\nlen(eps_prev_list.index): {}\n".format(row['CompanyCode'], eps_current, eps_prev, abs(eps_prev), len(eps_prev_list.index)))
            quarterly_eps_list.loc[index, 'Q1 EERS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            # q1_f_epsprev.write("Q1 EPS Growth: "+str(((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan)+"\n\n")


            sales_current = row['Sales']

            sales_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['Sales']
            
            if len(sales_prev_list.index) > 1:
                    sales_prev = sales_prev_list[sales_prev_list.index[0]]
            else:
                sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan
            
            if (math.isnan(sales_prev)):

                sales_prev_list = quarterly_eps_list.loc[(quarterly_eps_list["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_list["YearEnding"] == prev_year_qtr)]['Sales']
                # sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan
                if len(sales_prev_list.index) > 1:
                    sales_prev = sales_prev_list[sales_prev_list.index[0]]
                else:
                    sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan
                # calculating Q1Sales Growth
                quarterly_eps_list.loc[index, 'Q1 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev  !=0 else np.nan

            # q1_f_salesprev.write("CompanyCode: {}\nsales_current: {}\nsales_prev: {}\nlen(sales_prev_list.index): {}\n\n".format(row['CompanyCode'], sales_current, sales_prev, len(sales_prev_list.index)))
            quarterly_eps_list.loc[index, 'Q1 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev  !=0 else np.nan

        # q1_f_epsprev.close()
        # q1_f_salesprev.close()
        return quarterly_eps_list

    def consolidated_quarterly_one_eps_sales_growth(self, quarterly_eps_list, conn, today):
        """Calculating the EPS and Sales Growth for current quarter,

        Args:
            quarterly_eps_list = quarterly eps list data of daily qtr eps,
        
        Operation:
            Take the data from 'QuarterlyEPS' table for previous 4 quarter, 
            and calculate the Q1 EPS Growth and Q1 Sales Growth
            EPS Growth  = (eps_curr - eps_prev / abs(eps_prev) * 100)
            Sales Growth = (((sales_current-sales_prev)/abs(sales_prev))*100),

        Return:
            EPS and Sales Growth of previous one year quarter.
          """

        qtr_one_start = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        prev_year =  self.get_year_before_quarter(self.get_closest_quarter(today))
        prev_2year =  self.get_year_before_quarter(prev_year).strftime("%Y-%m-%d")
        prev_4year = self.get_four_years_before_quarter(today).strftime("%Y-%m-%d")

        
        sql = 'SELECT * FROM public."ConsolidatedQuarterlyEERS" where "Ext_Flag" is not null and "YearEnding" >= \'' + prev_4year + '\' and "YearEnding" <= \'' + today.strftime("%Y-%m-%d") + '\';'
        quarterly_eps_year_back = sqlio.read_sql_query(sql, con = conn)	
        quarterly_eps_list['Sales'] =quarterly_eps_list['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        quarterly_eps_year_back['Sales'] = quarterly_eps_year_back['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        

        #Order quarterly_eps_list by yearending asc
        quarterly_eps_list = quarterly_eps_list.sort_values(by = 'YearEnding', ascending = True)

        # self.export_table("Q1_quarterly_eps_list", quarterly_eps_list)
        # self.export_table("Q1_quarterly_eps_year_back", quarterly_eps_year_back)

        # q1_f_epsprev = open("zz_Consolidated_Q1_EPSPREV.txt".format(today), "w+")
        # q1_f_epsprev.write("\n\n------------------------------------------------------------------------------------------------\n\n")
        # q1_f_salesprev = open("zz_Consolidated_Q1_SALESPREV.txt".format(today), "w+")
        # q1_f_salesprev.write("\n\n------------------------------------------------------------------------------------------------\n\n")

        for index, row in quarterly_eps_list.iterrows():

            prev_year_qtr = self.get_year_before_quarter(row['YearEnding'])
            # print("prev_year_qtr: ",prev_year_qtr)

            eps_current = row['EERS']

            eps_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]["EERS"]
            
            if len(eps_prev_list.index) > 1:
                    eps_prev = eps_prev_list[eps_prev_list.index[0]]
            else:
                eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan

            if (math.isnan(eps_prev)):
                eps_prev_list = quarterly_eps_list.loc[(quarterly_eps_list["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_list["YearEnding"] == prev_year_qtr)]["EERS"]
                # eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan
                if len(eps_prev_list.index) > 1:
                    eps_prev = eps_prev_list[eps_prev_list.index[0]]
                else:
                    eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan
                # calculating Q1 EPS Growth
                quarterly_eps_list.loc[index, 'Q1 EERS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            
            # q1_f_epsprev.write("CompanyCode: {}\neps_current: {}\neps_prev: {}\nabs(eps_prev): {}\nlen(eps_prev_list.index): {}\n".format(row['CompanyCode'], eps_current, eps_prev, abs(eps_prev), len(eps_prev_list.index)))
            quarterly_eps_list.loc[index, 'Q1 EERS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            # q1_f_epsprev.write("Q1 EPS Growth: "+str(((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan)+"\n\n")


            sales_current = row['Sales']

            sales_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['Sales']
            if len(sales_prev_list.index) > 1:
                    sales_prev = sales_prev_list[sales_prev_list.index[0]]
            else:
                sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan

            if (math.isnan(sales_prev)):

                sales_prev_list = quarterly_eps_list.loc[(quarterly_eps_list["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_list["YearEnding"] == prev_year_qtr)]['Sales']
                # sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan
                if len(sales_prev_list.index) > 1:
                    sales_prev = sales_prev_list[sales_prev_list.index[0]]
                else:
                    sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan
                # calculating Q1Sales Growth
                quarterly_eps_list.loc[index, 'Q1 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev  !=0 else np.nan

            # q1_f_salesprev.write("CompanyCode: {}\nsales_current: {}\nsales_prev: {}\nlen(sales_prev_list.index): {}\n\n".format(row['CompanyCode'], sales_current, sales_prev, len(sales_prev_list.index)))
            quarterly_eps_list.loc[index, 'Q1 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev  !=0 else np.nan

        # q1_f_epsprev.close()
        # q1_f_salesprev.close()
        return quarterly_eps_list

    def quarterly_two_eps_sales_growth(self, quarterly_eps_list, conn,today):
        """Calculating the EPS and Sales Growth for current quarter,

        Args:
            quarterly_eps_list = quarterly eps list data of daily qtr eps,
        
        Operation:
            Take the data from 'QuarterlyEPS' table for two year back quarter, 
            and calculate the Q2 EPS Growth and Q2 Sales Growth
            EPS Growth  = (eps_curr - eps_prev / abs(eps_prev) * 100)
            Sales Growth = (((sales_current-sales_prev)/abs(sales_prev))*100),

        Return:
            EPS and Sales Growth of previous two year quarter.
         """
         
        qtr_two_start = self.get_previous_quarter(today).strftime("%Y-%m-%d")
        prev_year = self.get_year_before_quarter(self.get_previous_quarter(today))
        prev_2year =  self.get_year_before_quarter(prev_year).strftime("%Y-%m-%d")
        
        sql = 'SELECT * FROM public."QuarterlyEERS" where "Ext_Flag" is not null and "YearEnding" >= \'' + prev_2year + '\' AND "YearEnding"<= \'' + today.strftime("%Y-%m-%d") + '\';'
        quarterly_eps_year_back = pd.read_sql_query(sql, con = conn)

        quarterly_eps_list['Sales'] =quarterly_eps_list['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        quarterly_eps_year_back['Sales'] = quarterly_eps_year_back['Sales'].replace(r'[?$,]', '', regex=True).astype(float)

        # self.export_table("Q2_quarterly_eps_list", quarterly_eps_list)
        # self.export_table("Q2_quarterly_eps_year_back", quarterly_eps_year_back)


        for index, row in quarterly_eps_list.iterrows():

            prev_qtr = self.get_previous_quarter(row['YearEnding'])
            prev_year_qtr = self.get_year_before_quarter(prev_qtr)

            #################
            # calculating Q2 EPS Growth
            eps_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['EERS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            eps_current_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_qtr)]['EERS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan
                        
            quarterly_eps_list.loc[index, 'Q2 EERS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            quarterly_eps_list.loc[index , 'Q2 EERS'] = eps_current


            #################
            #  calculating Q2 sales Growth
            sales_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan
            sales_current_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_qtr)]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan

            quarterly_eps_list.loc[index, 'Q2 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev !=0 else np.nan
            quarterly_eps_list.loc[index , 'Q2 Sales'] = sales_current

            #################  

        self.export_table("EERS LIST", quarterly_eps_list)
        return quarterly_eps_list
    

    def consolidated_quarterly_two_eps_sales_growth(self, quarterly_eps_list, conn,today):
        """Calculating the EPS and Sales Growth for current quarter,

        Args:
            quarterly_eps_list = quarterly eps list data of daily qtr eps,
        
        Operation:
            Take the data from 'QuarterlyEPS' table for two year back quarter, 
            and calculate the Q2 EPS Growth and Q2 Sales Growth
            EPS Growth  = (eps_curr - eps_prev / abs(eps_prev) * 100)
            Sales Growth = (((sales_current-sales_prev)/abs(sales_prev))*100),

        Return:
            EPS and Sales Growth of previous two year quarter.
         """
        
        qtr_two_start = self.get_previous_quarter(today).strftime("%Y-%m-%d")
        prev_year = self.get_year_before_quarter(self.get_previous_quarter(today))
        prev_2year =  self.get_year_before_quarter(prev_year).strftime("%Y-%m-%d")
        
        sql = 'SELECT * FROM public."ConsolidatedQuarterlyEERS" where "Ext_Flag" is not null and "YearEnding" >= \'' + prev_2year + '\' and "YearEnding" <= \'' + today.strftime("%Y-%m-%d") + '\';'
        quarterly_eps_year_back = pd.read_sql_query(sql, con = conn)

        quarterly_eps_list['Sales'] =quarterly_eps_list['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        quarterly_eps_year_back['Sales'] = quarterly_eps_year_back['Sales'].replace(r'[?$,]', '', regex=True).astype(float)

        # self.export_table("Q2_quarterly_eps_list", quarterly_eps_list)
        # self.export_table("Q2_quarterly_eps_year_back", quarterly_eps_year_back)


        for index, row in quarterly_eps_list.iterrows():

            prev_qtr = self.get_previous_quarter(row['YearEnding'])
            prev_year_qtr = self.get_year_before_quarter(prev_qtr)

            #################
            # calculating Q2 EPS Growth
            eps_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['EERS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            eps_current_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_qtr)]['EERS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan
            
            
            quarterly_eps_list.loc[index, 'Q2 EERS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            quarterly_eps_list.loc[index , 'Q2 EERS'] = eps_current

            

            #################
            #  calculating Q2 sales Growth
            sales_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan
            sales_current_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_qtr)]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan


            quarterly_eps_list.loc[index, 'Q2 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev !=0 else np.nan
            quarterly_eps_list.loc[index , 'Q2 Sales'] = sales_current

            #################  
        return quarterly_eps_list


    def ttm_quarterly_list(self, date, conn):
        """ Generating the TTM data for newly available data in quarterly list,

        Operation:
            Take the data from QuarterlyEPS and TTM table for previous four year quarter
            and fetch the data of 'Sales','Expenses','EBIDTA','Interest','Depreciation'
            'Extraordinary','OPM','Tax','PAT', Equity','Reserves',' Months','Quarter',
            and calculate the value for EPS and NPM
            EPS' = (PAT/Equity)
            NPM = ((PAT/Sales)*100),
        
        Return:
            TTM data of EPS and NPM.
        """
            #TTM(Training twelve month )
        
        qtr_now = self.get_closest_quarter(date)
        print("TTM Date: ", qtr_now)
        quarter_back = self.get_year_before_quarter(qtr_now)

        prev_4year =  self.get_four_years_before_quarter(date)
        
        sql = 'SELECT QE.* from public."QuarterlyEERS" QE LEFT JOIN public."EERS_TTM" TTM on QE."CompanyCode"= TTM."CompanyCode" and TTM."YearEnding" = QE."YearEnding"\
        WHERE QE."YearEnding" <= \'' + qtr_now.strftime("%Y-%m-%d") + '\' AND TTM."CompanyCode" is null AND QE."Ext_Flag" is not null;'
        quarterly_eps_list = sqlio.read_sql_query(sql, con = conn)

        qeps_back = 'SELECT * FROM public."QuarterlyEERS" WHERE "YearEnding" >= \''+prev_4year.strftime("%Y-%m-%d")+'\' AND "YearEnding"<= \'' + date.strftime("%Y-%m-%d") + '\';'
        qrtlylist = sqlio.read_sql_query(qeps_back, con = conn)

        #print(sql)

        qrtlylist[qrtlylist.columns[4:16]] = qrtlylist[qrtlylist.columns[4:16]].replace(r'[?$,]', '', regex=True).astype(np.float32)
        
        ttm_calc = pd.DataFrame(columns=['CompanyCode', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',\
        'PAT', 'Equity',  'Reserves', 'Months', 'Quarter', 'EERS', 'NPM'])
        
        ttm_calc = pd.merge(ttm_calc, quarterly_eps_list[['CompanyCode', 'YearEnding']], left_on='CompanyCode', right_on='CompanyCode', how='right')

        #print(ttm_calc.columns)
        # f_eps = open("_EPS_TTM_NULLDIG-{}.txt".format(date), "w+")
        # f_eps.write("\n\n------------------------------------------------------------------------------------------------\n\n")
        # f_eps.write("\t\t FOR DATE: " + str(today) + "\n")

        for index, row in quarterly_eps_list.iterrows():

            qtr_year_back = self.get_year_before_quarter(row["YearEnding"])

            qrtlylist_year = qrtlylist[(qrtlylist["YearEnding"] <= row["YearEnding"]) & (qrtlylist["YearEnding"] > qtr_year_back)]

            ttm_months = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Months']
            ttm_sum_months = ttm_months.agg('sum')

            ttm_quarters = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Quarter']
            ttm_sum_quarters = len(ttm_quarters.index)

            ttm_sales = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Sales']
            ttm_sum_sales = ttm_sales.agg('sum')

            ttm_expenses = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Expenses']
            ttm_sum_expenses = ttm_expenses.agg('sum')

            ttm_equity = qrtlylist_year.loc[(qrtlylist_year["CompanyCode"] == row['CompanyCode'])]
            ttm_equity = ttm_equity.sort_values("YearEnding", ascending=False).copy()
            ttm_equity = ttm_equity['Equity'].head(1).item()  if len(ttm_equity.index) >= 1 else np.nan
            ttm_last_equity = ttm_equity

            ttm_EBIDTA = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['EBIDTA']
            ttm_sum_EBIDTA = ttm_EBIDTA.agg('sum')

            ttm_interest = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Interest']
            ttm_sum_interest = ttm_interest.agg('sum')

            ttm_depreciation = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Depreciation']
            ttm_sum_depreciation = ttm_depreciation.agg('sum')

            ttm_extraordinary = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Extraordinary']
            ttm_sum_extraordinary = ttm_extraordinary.agg('sum')

            ttm_opm = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['OPM']
            ttm_sum_opm = ttm_opm.agg('sum')

            ttm_tax = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Tax']
            ttm_sum_tax = ttm_tax.agg('sum')

            ttm_pat = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['PAT']
            ttm_sum_pat = ttm_pat.agg('sum')

            ttm_reserves = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Reserves']
            ttm_sum_reserves = ttm_reserves.agg('sum')

            ttm_calc.loc[index, 'Sales'] = ttm_sum_sales
            ttm_calc.loc[index, 'Expenses'] = ttm_sum_expenses
            ttm_calc.loc[index, 'EBIDTA'] = ttm_sum_EBIDTA
            ttm_calc.loc[index, 'Interest'] = ttm_sum_interest
            ttm_calc.loc[index, 'Depreciation'] = ttm_sum_depreciation
            ttm_calc.loc[index, 'Extraordinary'] = ttm_sum_extraordinary
            ttm_calc.loc[index, 'OPM'] = ttm_sum_opm
            ttm_calc.loc[index, 'Tax'] = ttm_sum_tax
            ttm_calc.loc[index, 'PAT'] = ttm_sum_pat
            ttm_calc.loc[index, 'Equity'] = ttm_last_equity
            ttm_calc.loc[index, 'Reserves'] = ttm_sum_reserves
            ttm_calc.loc[index, 'Months'] = ttm_sum_months		
            ttm_calc.loc[index, 'Quarter'] = ttm_sum_quarters

            # line = "{} : {} / {} if {} !=0 RESULT={}\n".format(str(row['CompanyCode']), str(ttm_calc.loc[index,'PAT']), str(ttm_calc.loc[index,'Equity']), str(ttm_calc.loc[index,'Equity']), str(ttm_calc.loc[index, 'PAT']/ttm_calc.loc[index, 'Equity']))
            # f_eps.write(line)
            ttm_calc.loc[index, 'EERS'] = ttm_calc.loc[index, 'EBIDTA']/ttm_calc.loc[index, 'Equity'] if ttm_calc.loc[index, 'Equity'] != 0 else np.nan
            ttm_calc.loc[index, 'NPM'] = ttm_calc.loc[index, 'PAT']/ttm_calc.loc[index, 'Sales'] * 100 if ttm_calc.loc[index, 'Sales'] != 0 else np.nan

        # f_eps.close()

        ttm_calc = ttm_calc[['CompanyCode', 'YearEnding', 'Months', 'Quarter','Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',\
        'PAT', 'Equity', 'Reserves', 'EERS', 'NPM']]

        return ttm_calc

    def consolidated_ttm_quarterly_list(self, date, conn):
        """ Generating the TTM data for newly available data in quarterly list,

        Operation:
            Take the data from ConsolidatedQuarterlyEPS and TTM table for previous four year quarter
            and fetch the data of 'Sales','Expenses','EBIDTA','Interest','Depreciation'
            'Extraordinary','OPM','Tax','PAT', Equity','Reserves',' Months','Quarter',
            and calculate the value for EPS and NPM
            EPS' = (PAT/Equity)
            NPM = ((PAT/Sales)*100),
        
        Return:
            TTM data of EPS and NPM.
        """
            #TTM(Training twelve month )
        
        qtr_now = self.get_closest_quarter(date)
        quarter_back = self.get_year_before_quarter(qtr_now)

        prev_4year =  self.get_four_years_before_quarter(date)
        
        sql = 'SELECT QE.* from public."ConsolidatedQuarterlyEERS" QE LEFT JOIN public."ConsolidatedEERSTTM" TTM on QE."CompanyCode"= TTM."CompanyCode" and TTM."YearEnding" = QE."YearEnding"\
        WHERE QE."YearEnding" <= \'' + qtr_now.strftime("%Y-%m-%d") + '\' AND TTM."CompanyCode" is null AND QE."Ext_Flag" is not null;'
        quarterly_eps_list = sqlio.read_sql_query(sql, con = conn)

        qeps_back = 'SELECT * FROM public."ConsolidatedQuarterlyEERS" WHERE "YearEnding" >= \''+prev_4year.strftime("%Y-%m-%d")+'\' and "YearEnding" <= \'' + date.strftime("%Y-%m-%d") + '\';'
        qrtlylist = sqlio.read_sql_query(qeps_back, con = conn)

        #print(sql)

        qrtlylist[qrtlylist.columns[4:16]] = qrtlylist[qrtlylist.columns[4:16]].replace(r'[?$,]', '', regex=True).astype(np.float32)
        
        ttm_calc = pd.DataFrame(columns=['CompanyCode', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',\
        'PAT', 'Equity',  'Reserves', 'Months', 'Quarter', 'EERS', 'NPM'])
        
        ttm_calc = pd.merge(ttm_calc, quarterly_eps_list[['CompanyCode', 'YearEnding']], left_on='CompanyCode', right_on='CompanyCode', how='right')

        #print(ttm_calc.columns)

        for index, row in quarterly_eps_list.iterrows():

            qtr_year_back = self.get_year_before_quarter(row["YearEnding"])

            qrtlylist_year = qrtlylist[(qrtlylist["YearEnding"] <= row["YearEnding"]) & (qrtlylist["YearEnding"] > qtr_year_back)]

            ttm_months = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Months']
            ttm_sum_months = ttm_months.agg('sum')

            ttm_quarters = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Quarter']
            ttm_sum_quarters = len(ttm_quarters.index)

            ttm_sales = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Sales']
            ttm_sum_sales = ttm_sales.agg('sum')

            ttm_expenses = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Expenses']
            ttm_sum_expenses = ttm_expenses.agg('sum')

            ttm_equity = qrtlylist_year.loc[(qrtlylist_year["CompanyCode"] == row['CompanyCode'])]
            ttm_equity = ttm_equity.sort_values("YearEnding", ascending=False).copy()
            ttm_equity = ttm_equity['Equity'].head(1).item()  if len(ttm_equity.index) >= 1 else np.nan
            ttm_last_equity = ttm_equity

            ttm_EBIDTA = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['EBIDTA']
            ttm_sum_EBIDTA = ttm_EBIDTA.agg('sum')

            ttm_interest = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Interest']
            ttm_sum_interest = ttm_interest.agg('sum')

            ttm_depreciation = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Depreciation']
            ttm_sum_depreciation = ttm_depreciation.agg('sum')

            ttm_extraordinary = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Extraordinary']
            ttm_sum_extraordinary = ttm_extraordinary.agg('sum')

            ttm_opm = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['OPM']
            ttm_sum_opm = ttm_opm.agg('sum')

            ttm_tax = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Tax']
            ttm_sum_tax = ttm_tax.agg('sum')

            ttm_pat = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['PAT']
            ttm_sum_pat = ttm_pat.agg('sum')

            ttm_reserves = qrtlylist_year.loc[qrtlylist_year["CompanyCode"] == row['CompanyCode']]['Reserves']
            ttm_sum_reserves = ttm_reserves.agg('sum')

            ttm_calc.loc[index, 'Sales'] = ttm_sum_sales
            ttm_calc.loc[index, 'Expenses'] = ttm_sum_expenses
            ttm_calc.loc[index, 'EBIDTA'] = ttm_sum_EBIDTA
            ttm_calc.loc[index, 'Interest'] = ttm_sum_interest
            ttm_calc.loc[index, 'Depreciation'] = ttm_sum_depreciation
            ttm_calc.loc[index, 'Extraordinary'] = ttm_sum_extraordinary
            ttm_calc.loc[index, 'OPM'] = ttm_sum_opm
            ttm_calc.loc[index, 'Tax'] = ttm_sum_tax
            ttm_calc.loc[index, 'PAT'] = ttm_sum_pat
            ttm_calc.loc[index, 'Equity'] = ttm_last_equity
            ttm_calc.loc[index, 'Reserves'] = ttm_sum_reserves
            ttm_calc.loc[index, 'Months'] = ttm_sum_months		
            ttm_calc.loc[index, 'Quarter'] = ttm_sum_quarters

            ttm_calc.loc[index, 'EERS'] = ttm_calc.loc[index, 'EBIDTA']/ttm_calc.loc[index, 'Equity'] if ttm_calc.loc[index, 'Equity'] != 0 else np.nan
            ttm_calc.loc[index, 'NPM'] = ttm_calc.loc[index, 'PAT']/ttm_calc.loc[index, 'Sales'] * 100 if ttm_calc.loc[index, 'Sales'] != 0 else np.nan

        ttm_calc = ttm_calc[['CompanyCode', 'YearEnding', 'Months', 'Quarter','Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',\
        'PAT', 'Equity', 'Reserves', 'EERS', 'NPM']]

        return ttm_calc

    def ttm_eps_sales_growth(self, quarterly_eps_list, today, conn):
        """ Generating the data of TTM, sales and EPS Growth,

            Args:
                quarterly_eps_list = EPS rating data.

            Operation:
                Take the data from TTM table for five year back quarter
                and calculate the 'EPS Growth' and 'Sales Growth' for 
                previous one year(TTM1), previous two year(TTM2), and previous three year(TTM3) quarter
                EPS Growth = (curr_eps - prev_eps / abs(prev_eps)*100)
                Sales Growth = (sales_curr - sales_prev /abs(sales_prev)*100)
            
            Return:
                value of EPS Growth, Sales Growth and EPS Rating.
            """
        # today = datetime.datetime.now() + datetime.timedelta(0)
        # today = today.date()


        four_year_back =  self.get_four_years_before_quarter(self.get_closest_quarter(today))
        five_year_back = self.get_year_before_quarter(four_year_back).strftime("%Y-%m-%d")

        sql = 'SELECT * from public."EERS_TTM" WHERE "YearEnding" >= \'' + five_year_back + '\' AND "YearEnding"<= \'' + today.strftime("%Y-%m-%d") + '\';'
        ttm_backyear = pd.read_sql_query(sql, con = conn)
        # self.export_table(ttm_backyear, 'ttm_backyear')
        # raise Exception("Break")
        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(columns = ['TTM1 EERS', 'TTM1 Sales', 'TTM2 EERS', 'TTM2 Sales',\
        'TTM3 EERS', 'TTM3 Sales'])], sort=False)

        
        ttm_backyear['Sales'] = ttm_backyear['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        ttm1_eps_prev_count = 0
        ttm2_eps_prev_count = 0
        ttm3_eps_prev_count = 0
        ttm1_sales_prev_count = 0
        ttm2_sales_prev_count = 0
        ttm3_sales_prev_count = 0

        for index, row in quarterly_eps_list.iterrows():

            ##TTM1

            prev_year_qtr = self.get_year_before_quarter(row['YearEnding'])
            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['EERS']
            eps_prev = eps_prev_list.iloc[0] if len(eps_prev_list.index) >= 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == row["YearEnding"])]['EERS']
            eps_current = eps_current_list.iloc[0] if len(eps_current_list.index) >= 1 else np.nan

            # ^^^^^

            quarterly_eps_list.loc[index, 'TTM1 EERS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan
            
            if(eps_prev == 0 or eps_prev == np.nan):
                ttm1_eps_prev_count += 1

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['Sales']
            sales_prev = sales_prev_list.iloc[0] if len(sales_prev_list.index) >= 1 else np.nan	
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == row["YearEnding"])]['Sales']
            sales_current = sales_current_list.iloc[0] if len(sales_current_list.index) >= 1 else np.nan	

            quarterly_eps_list.loc[index, 'TTM1 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan
            if(sales_prev == 0 or sales_prev == np.nan):
                ttm1_sales_prev_count += 1

            ttm_eps_val = quarterly_eps_list.loc[index, 'TTM1 EERS'] 
            ttm_eps_set_val = ( ((0.3)* ttm_eps_val) if not math.isnan(ttm_eps_val) else 0 ) 
            quarterly_eps_list.loc[index, 'EERS Rating'] = quarterly_eps_list.loc[index, 'EERS Rating'] + ttm_eps_set_val

            

            ##TTM2

            prev_two_year_qtr = self.get_two_years_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['EERS']
            eps_prev = eps_prev_list.iloc[0] if len(eps_prev_list.index) >= 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['EERS']
            eps_current = eps_current_list.iloc[0] if len(eps_current_list.index) >= 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM2 EERS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan


            if(eps_prev == 0 or eps_prev == np.nan):
                ttm2_eps_prev_count += 1

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['Sales']
            sales_prev = sales_prev_list.iloc[0] if len(sales_prev_list.index) >= 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['Sales']
            sales_current = sales_current_list.iloc[0] if len(sales_current_list.index) >= 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM2 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan

            if(sales_prev == 0 or sales_prev == np.nan):
                ttm2_sales_prev_count += 1

            ttm_eps_val = quarterly_eps_list.loc[index, 'TTM2 EERS'] 
            ttm_eps_set_val = ( ((0.2)* ttm_eps_val) if not math.isnan(ttm_eps_val) else 0 ) 
            quarterly_eps_list.loc[index, 'EERS Rating'] = quarterly_eps_list.loc[index, 'EERS Rating'] + ttm_eps_set_val

            ##TTM3

            prev_three_year_qtr = self.get_three_years_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_three_year_qtr)]['EERS']
            eps_prev = eps_prev_list.iloc[0] if len(eps_prev_list.index) >= 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['EERS']
            eps_current = eps_current_list.iloc[0] if len(eps_current_list.index) >= 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM3 EERS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan
            

            if(eps_prev == 0 or eps_prev == np.nan):
                ttm3_eps_prev_count += 1

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_three_year_qtr)]['Sales']
            sales_prev = sales_prev_list.iloc[0] if len(sales_prev_list.index) >= 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['Sales']
            sales_current = sales_current_list.iloc[0] if len(sales_current_list.index) >= 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM3 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan

            if(sales_prev == 0 or sales_prev == np.nan):
                ttm3_sales_prev_count += 1

            ttm_eps_val = (0.1) * quarterly_eps_list.loc[index, 'TTM3 EERS'] 
            ttm_eps_set_val = (ttm_eps_val if not math.isnan(ttm_eps_val)  else 0 ) 
            quarterly_eps_list.loc[index, 'EERS Rating'] = quarterly_eps_list.loc[index, 'EERS Rating'] + ttm_eps_set_val

        return quarterly_eps_list

    def consolidated_ttm_eps_sales_growth(self, quarterly_eps_list, today, conn):
        """ Generating the data of TTM, sales and EPS Growth,

            Args:
                quarterly_eps_list = EPS rating data.

            Operation:
                Take the data from TTM table for five year back quarter
                and calculate the 'EPS Growth' and 'Sales Growth' for 
                previous one year(TTM1), previous two year(TTM2), and previous three year(TTM3) quarter
                EPS Growth = (curr_eps - prev_eps / abs(prev_eps)*100)
                Sales Growth = (sales_curr - sales_prev /abs(sales_prev)*100)
            
            Return:
                value of EPS Growth, Sales Growth and EPS Rating.
            """
        # today = datetime.datetime.now() + datetime.timedelta(0)
        # today = today.date()


        four_year_back =  self.get_four_years_before_quarter(self.get_closest_quarter(today))
        five_year_back = self.get_year_before_quarter(four_year_back).strftime("%Y-%m-%d")

        sql = 'SELECT * from public."ConsolidatedEERSTTM" WHERE "YearEnding" >= \'' + five_year_back + '\' and "YearEnding" <= \'' + today.strftime("%Y-%m-%d") + '\';'
        ttm_backyear = pd.read_sql_query(sql, con = conn)

        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(columns = ['TTM1 EERS', 'TTM1 Sales', 'TTM2 EERS', 'TTM2 Sales',\
        'TTM3 EERS', 'TTM3 Sales'])], sort=False)


        ttm_backyear['Sales'] = ttm_backyear['Sales'].replace(r'[?$,]', '', regex=True).astype(float)

        for index, row in quarterly_eps_list.iterrows():

            ##TTM1

            prev_year_qtr = self.get_year_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['EERS']
            eps_prev = eps_prev_list.iloc[0] if len(eps_prev_list.index) >= 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == row["YearEnding"])]['EERS']
            eps_current = eps_current_list.iloc[0] if len(eps_current_list.index) >= 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM1 EERS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['Sales']
            sales_prev = sales_prev_list.iloc[0] if len(sales_prev_list.index) >= 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == row["YearEnding"])]['Sales']
            sales_current = sales_current_list.iloc[0] if len(sales_current_list.index) >= 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM1 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan

            ttm_eps_val = quarterly_eps_list.loc[index, 'TTM1 EERS'] 
            ttm_eps_set_val = ( ((0.3)* ttm_eps_val) if not math.isnan(ttm_eps_val) else 0 ) 
            quarterly_eps_list.loc[index, 'EERS Rating'] = quarterly_eps_list.loc[index, 'EERS Rating'] + ttm_eps_set_val

            ##TTM2

            prev_two_year_qtr = self.get_two_years_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['EERS']
            eps_prev = eps_prev_list.iloc[0] if len(eps_prev_list.index) >= 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['EERS']
            eps_current = eps_current_list.iloc[0] if len(eps_current_list.index) >= 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM2 EERS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['Sales']
            sales_prev = sales_prev_list.iloc[0] if len(sales_prev_list.index) >= 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['Sales']
            sales_current = sales_current_list.iloc[0] if len(sales_current_list.index) >= 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM2 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan

            ttm_eps_val = quarterly_eps_list.loc[index, 'TTM2 EERS'] 
            ttm_eps_set_val = ( ((0.2)* ttm_eps_val) if not math.isnan(ttm_eps_val) else 0 ) 
            quarterly_eps_list.loc[index, 'EERS Rating'] = quarterly_eps_list.loc[index, 'EERS Rating'] + ttm_eps_set_val

            ##TTM3

            prev_three_year_qtr = self.get_three_years_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_three_year_qtr)]['EERS']
            eps_prev = eps_prev_list.iloc[0] if len(eps_prev_list.index) >= 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['EERS']
            eps_current = eps_current_list.iloc[0] if len(eps_current_list.index) >= 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM3 EERS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_three_year_qtr)]['Sales']
            sales_prev = sales_prev_list.iloc[0]if len(sales_prev_list.index) >= 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['Sales']
            sales_current = sales_current_list.iloc[0] if len(sales_current_list.index) >= 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM3 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan


            ttm_eps_val = (0.1) * quarterly_eps_list.loc[index, 'TTM3 EERS'] 
            ttm_eps_set_val = (ttm_eps_val if not math.isnan(ttm_eps_val)  else 0 ) 
            quarterly_eps_list.loc[index, 'EERS Rating'] = quarterly_eps_list.loc[index, 'EERS Rating'] + ttm_eps_set_val
            

        return quarterly_eps_list

    def get_all_ttm(self, quarterly_eps_list, conn, today):
        """ Fetch the all data of TTM,

        Args:
            quartely_eps_list = quartely eps data of EPS and Sales growth,

        Operation:
            Fetching TTM quartely data. 
        """

        ttm = self.ttm_quarterly_list(today, conn)
        self.export_table("EERS_TTM", ttm)

        self.insert_ttm(ttm, conn)

    def get_all_consolidated_ttm(self, quarterly_eps_list, conn,today):
        """ Fetch the all data of TTM,

        Args:
            quartely_eps_list = quartely eps data of EPS and Sales growth,

        Operation:
            Fetching TTM quartely data. 
        """

        c_ttm = self.consolidated_ttm_quarterly_list(today, conn)

        self.insert_c_ttm(c_ttm, conn)


    def insert_ttm(self, ttm, conn):
        """ Insert the TTM data into database

        Args:
            ttm = quartely EPS data,

        Operation:
            Export the data into ttm_export.csv,
            and insert into TTM table.
        """

        cur = conn.cursor()

        # Column Sclicing
        ttm[ttm.columns[4:17]] = ttm[ttm.columns[4:17]].apply(pd.to_numeric, errors='coerce')

        # ttm = ttm.round(2) # Change made on June 3rd (Recently)

        exportfilename = "ttm_export.csv"
        exportfile = open(exportfilename,"w+")
        ttm.to_csv(exportfile, header=True, index=False, lineterminator='\r')
        exportfile.close()
            
        copy_sql = """
                COPY "public"."EERS_TTM" FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        # os.remove(exportfilename)

    def insert_c_ttm(self, ttm, conn):
        """ Insert the TTM data into database

        Args:
            ttm = quartely EPS data,

        Operation:
            Export the data into ttm_export.csv,
            and insert into TTM table.
        """

        cur = conn.cursor()

        # Column Sclicing
        ttm[ttm.columns[4:17]] = ttm[ttm.columns[4:17]].apply(pd.to_numeric, errors='coerce')

        # ttm = ttm.round(2) # Change made on June 3rd (Recently)

        exportfilename = "consolidated_ttm_export.csv"
        exportfile = open(exportfilename,"w+")
        ttm.to_csv(exportfile, header=True, index=False, lineterminator='\r')
        exportfile.close()
            
        copy_sql = """
                COPY "public"."ConsolidatedEERSTTM" FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)

    def get_ttm(self, today, conn):
        """ Get the TTM data

        Operation:
            Fetch the data from TTM table for closest quarter,

        Return:
            TTM data for closest quarter.
        """

        # today = datetime.datetime.now() + datetime.timedelta(0)
        # today = today.date()

        sql = 'SELECT * FROM public."EERS_TTM" where "YearEnding" <=\''+self.get_closest_quarter(today).strftime("%Y-%m-%d")+'\';'
        ttm = pd.read_sql_query(sql, con = conn)
        return ttm 

    def eps_rating(self, quarterly_eps_list,conn,today):
        """ Calculating EPS Rating 

        Args:
            quartely_eps_list = merge data of BTTList and QuarterlyEPS table,

        Operation:
            Calculate the EPS Rating 
            EPS Rating (Q1_EPS_Growth / Q2_EPS_Growth )
            Q1_EPS_Growth ((0.2)* Q1 EPS Growth of quarterly eps data),
            Q2_EPS_Growth ((0.2)* Q2 EPS Growth of quarterly eps data),
            
        Return:
            Value of EPS Rating.
         """

        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")

        for index, row in quarterly_eps_list.iterrows():

            Q1_EPS_Growth = (0.2)*quarterly_eps_list.loc[index, 'Q1 EERS Growth']
            Q2_EPS_Growth = (0.2)*quarterly_eps_list.loc[index, 'Q2 EERS Growth']

            quarterly_eps_list.loc[index, 'EERS Rating'] = (Q1_EPS_Growth if not (math.isnan(Q1_EPS_Growth) or Q1_EPS_Growth == np.nan)  else 0) +\
                                                        (Q2_EPS_Growth if not(math.isnan(Q2_EPS_Growth) or Q2_EPS_Growth == np.nan) else 0)

        quarterly_eps_list = self.ttm_eps_sales_growth(quarterly_eps_list, today, conn)
        self.export_table("TTM FINAL", quarterly_eps_list)

        return quarterly_eps_list

        
    def consolidated_eps_rating(self, c_quarterly_eps_list,conn,today):
        """ Calculating EPS Rating 

        Args:
            quartely_eps_list = merge data of BTTList and QuarterlyEPS table,

        Operation:
            Calculate the EPS Rating 
            EPS Rating (Q1_EPS_Growth / Q2_EPS_Growth )
            Q1_EPS_Growth ((0.2)* Q1 EPS Growth of quarterly eps data),
            Q2_EPS_Growth ((0.2)* Q2 EPS Growth of quarterly eps data),
            
        Return:
            Value of EPS Rating.
         """

        # today = datetime.datetime.now() + datetime.timedelta(0)
        # today = today.date()

        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")

        for index, row in c_quarterly_eps_list.iterrows():
            if(c_quarterly_eps_list.loc[index, 'Q1 EERS Growth'] is not None):
                Q1_EPS_Growth = (0.2)*c_quarterly_eps_list.loc[index, 'Q1 EERS Growth']
            else:
                Q1_EPS_Growth = np.nan
            if(c_quarterly_eps_list.loc[index, 'Q2 EERS Growth'] is not None):
                Q2_EPS_Growth = (0.2)*c_quarterly_eps_list.loc[index, 'Q2 EERS Growth']
            else:
                Q2_EPS_Growth = np.nan

            c_quarterly_eps_list.loc[index, 'EERS Rating'] = (Q1_EPS_Growth if not (math.isnan(Q1_EPS_Growth) or Q1_EPS_Growth == np.nan)  else 0) +\
                                                        (Q2_EPS_Growth if not(math.isnan(Q2_EPS_Growth) or Q2_EPS_Growth == np.nan) else 0)

        c_quarterly_eps_list = self.consolidated_ttm_eps_sales_growth(c_quarterly_eps_list, today, conn)

        return c_quarterly_eps_list

    def stock_percentile_ranking(self, quarterly_eps_list):
        """ Calculating Stock Percentile ranking
        
        Args:
            quartely_eps_list = data of EPS Rating

        Return:
            EPS Stock Percentile Rating.
        """

        quarterly_eps_list['EERS Ranking'] = quarterly_eps_list['EERS Rating'].rank(ascending=False)
        quarterly_eps_list['EERS Ranking'] = ((len(quarterly_eps_list.index)-quarterly_eps_list['EERS Ranking']+1)/len(quarterly_eps_list.index))*100

        return quarterly_eps_list

    def insert_EPS(self, name,table, conn, cur, today):
        """ Insert EPS data into database

        Args:
            table = table with appropriate Column,

        Operation:
            insert the data into EPS Table.	
        """

        table["EERS Date"] = today.strftime("%Y-%m-%d")

        table["BSECode"].fillna(-1, inplace=True)
        table = table.astype({"BSECode": int})
        table = table.astype({"BSECode": str})
        table["BSECode"] = table["BSECode"].replace('-1', np.nan)

        table["Months"].fillna(-1, inplace=True)
        table = table.astype({"Months": int})
        table = table.astype({"Months": str})
        table["Months"] = table["Months"].replace('-1', np.nan)


        table["Quarter"].fillna(-1, inplace=True)
        table = table.astype({"Quarter": int})
        table = table.astype({"Quarter": str})
        table["Quarter"] = table["Quarter"].replace('-1', np.nan)

        table = table.rename(columns = {'Sales' : 'Q1 Sales', 'EERS': 'Q1 EERS'})
        # Creating table
        table = table[['CompanyCode', 'NSECode', 'BSECode', 'CompanyName', 'ISIN', 'Months', 'Quarter', 'YearEnding', \
        'Q1 EERS', 'Q1 EERS Growth' , 'Q1 Sales','Q1 Sales Growth' , 'Q2 EERS', 'Q2 EERS Growth', 'Q2 Sales','Q2 Sales Growth', \
        'TTM1 EERS', 'TTM1 Sales', 'TTM2 EERS', 'TTM2 Sales', 'TTM3 EERS', 'TTM3 Sales', 'NPM', 'EERS Rating', 'EERS Ranking', 'EERS Date']]

        # print(table)

        exportfilename = name+"_export.csv"
        exportfile = open(exportfilename,"w+")

        table.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        # self.export_table("End_quarter_result", table)
        # raise Exception("Break for testing")
        copy_sql = """
                COPY "Reports"."EERS" FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)
        

    def insert_sa_EPS(self, name,table, conn, cur, today):
        """ Insert EPS data into database

        Args:
            table = table with appropriate Column,

        Operation:
            insert the data into EPS Table.	
        """

        table["EERS Date"] = today.strftime("%Y-%m-%d")

        table["BSECode"].fillna(-1, inplace=True)
        table = table.astype({"BSECode": int})
        table = table.astype({"BSECode": str})
        table["BSECode"] = table["BSECode"].replace('-1', np.nan)

        table["Months"].fillna(-1, inplace=True)
        table = table.astype({"Months": int})
        table = table.astype({"Months": str})
        table["Months"] = table["Months"].replace('-1', np.nan)


        table["Quarter"].fillna(-1, inplace=True)
        table = table.astype({"Quarter": int})
        table = table.astype({"Quarter": str})
        table["Quarter"] = table["Quarter"].replace('-1', np.nan)

        table = table.rename(columns = {'Sales' : 'Q1 Sales', 'EERS': 'Q1 EERS'})
        # Creating table
        table = table[['CompanyCode', 'NSECode', 'BSECode', 'CompanyName', 'ISIN', 'Months', 'Quarter', 'YearEnding', \
        'Q1 EERS', 'Q1 EERS Growth' , 'Q1 Sales','Q1 Sales Growth' , 'Q2 EERS', 'Q2 EERS Growth', 'Q2 Sales','Q2 Sales Growth', \
        'TTM1 EERS', 'TTM1 Sales', 'TTM2 EERS', 'TTM2 Sales', 'TTM3 EERS', 'TTM3 Sales', 'NPM', 'EERS Rating', 'EERS Ranking', 'EERS Date']]

        # print(table)

        exportfilename = name+"_export.csv"
        exportfile = open(exportfilename,"w+")

        table.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
                COPY "Reports"."STANDALONE_EERS" FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)
        

    def insert_Consolidated_EPS(self, name,table, conn, cur, today):
        """ Insert EPS data into database

        Args:
            table = table with appropriate Column,

        Operation:
            insert the data into EPS Table.	
        """

        table["EERS Date"] = today.strftime("%Y-%m-%d")

        table["BSECode"].fillna(-1, inplace=True)
        table = table.astype({"BSECode": int})
        table = table.astype({"BSECode": str})
        table["BSECode"] = table["BSECode"].replace('-1', np.nan)

        table["Months"].fillna(-1, inplace=True)
        table = table.astype({"Months": int})
        table = table.astype({"Months": str})
        table["Months"] = table["Months"].replace('-1', np.nan)


        table["Quarter"].fillna(-1, inplace=True)
        table = table.astype({"Quarter": int})
        table = table.astype({"Quarter": str})
        table["Quarter"] = table["Quarter"].replace('-1', np.nan)

        table = table.rename(columns = {'Sales' : 'Q1 Sales', 'EERS': 'Q1 EERS'})
        # Creating table
        table = table[['CompanyCode', 'NSECode', 'BSECode', 'CompanyName', 'ISIN', 'Months', 'Quarter', 'YearEnding', \
        'Q1 EERS', 'Q1 EERS Growth' , 'Q1 Sales','Q1 Sales Growth' , 'Q2 EERS', 'Q2 EERS Growth', 'Q2 Sales','Q2 Sales Growth', \
        'TTM1 EERS', 'TTM1 Sales', 'TTM2 EERS', 'TTM2 Sales', 'TTM3 EERS', 'TTM3 Sales', 'NPM', 'EERS Rating', 'EERS Ranking', 'EERS Date']]

        # print(table)

        exportfilename = name+"_export.csv"
        exportfile = open(exportfilename,"w+")

        table.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
                COPY "Reports"."Consolidated_EERS" FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()


    def get_eps_list(self, conn, today):
        btt_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
        next_month = today.month + 1 if today.month + 1 <= 12 else 1
        next_year = today.year if today.month + 1 <= 12 else today.year + 1
        btt_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")
        today = datetime.date(today.year, today.month, today.day).strftime("%Y-%m-%d")
        
        print("BTT Back: {} BTT Next: {}".format(btt_back, btt_next))
        print("Today: ", today)
        
        # Fetch data from both tables
        btt_df = sqlio.read_sql_query('SELECT * FROM public."BTTList" WHERE "BTTDate" >= %(btt_back)s AND "BTTDate" < %(btt_next)s;', con=conn, params={'btt_back': btt_back, 'btt_next': btt_next})
        eps_df = sqlio.read_sql_query('SELECT * FROM public."QuarterlyEERS" WHERE "YearEnding" <= \'' + today + '\';', con=conn)  # Fetch all data from QuarterlyEPS table
        self.export_table("BTTDF", btt_df)
        self.export_table("EERSDF", eps_df)
        # Filter rows in eps_df based on conditions
        eps_df = eps_df[eps_df['YearEnding'] == eps_df.groupby('CompanyCode')['YearEnding'].transform('max')]  # Filter based on max YearEnding
        
        # Merge DataFrames
        merged_df = pd.merge(btt_df, eps_df, on="CompanyCode", how="left")

        self.export_table("GET EERS LIST", merged_df)
        return merged_df


    def get_consolidated_eps_list(self, conn, today):
        btt_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
        next_month = today.month + 1 if today.month + 1 <= 12 else 1
        next_year = today.year if today.month + 1 <= 12 else today.year + 1
        btt_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")
        today = datetime.date(today.year, today.month, today.day).strftime("%Y-%m-%d")
        
        print("BTT Back: {} BTT Next: {}".format(btt_back, btt_next))
        print("Today: ", today)
        
        # Fetch data from both tables
        btt_df = sqlio.read_sql_query('SELECT * FROM public."BTTList" WHERE "BTTDate" >= %(btt_back)s AND "BTTDate" < %(btt_next)s;', con=conn, params={'btt_back': btt_back, 'btt_next': btt_next})
        eps_df = sqlio.read_sql_query('SELECT * FROM public."ConsolidatedQuarterlyEERS" WHERE "YearEnding" <= \'' + today + '\';', con=conn)  # Fetch all data from QuarterlyEPS table
        self.export_table("BTTDF", btt_df)
        self.export_table("EERSDF", eps_df)
        # Filter rows in eps_df based on conditions
        eps_df = eps_df[eps_df['YearEnding'] == eps_df.groupby('CompanyCode')['YearEnding'].transform('max')]  # Filter based on max YearEnding
        
        # Merge DataFrames
        merged_df = pd.merge(btt_df, eps_df, on="CompanyCode", how="left")

        # self.export_table("GET EPS LIST", merged_df)
        return merged_df




    def today_quarterly_eps(self, conn,cur, today):
        """ generate the data for today quartely EPS,

        Operation:
            fetch the data of EPS list, Q1, Q2 Eps and Sales growth and TTM data,
            to get today quarterly EPS data.
        """ 
    
        print("\nCompiling quarterly EPS list", flush = True)
        quarterly_eps_list = self.set_daily_qtr_eps(conn,today)
        # print("quarterly_eps_list.empty", quarterly_eps_list.empty, len(quarterly_eps_list), flush = True)

        qtr_one_start = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        prev_year =  self.get_year_before_quarter(self.get_closest_quarter(today))
        prev_2year =  self.get_year_before_quarter(prev_year).strftime("%Y-%m-%d")
        prev_4year = self.get_four_years_before_quarter(today).strftime("%Y-%m-%d")

        print("prev_4year: ", prev_4year)

        if not(quarterly_eps_list.empty):
            print("Calculating quarterly one EPS Sales growth", flush = True)
            quarterly_eps_list = self.quarterly_one_eps_sales_growth(quarterly_eps_list, conn, today)
            # self.export_table("Q1_final_quarterly_eps_list", quarterly_eps_list)
            print("Calculating quarterly two EPS Sales growth", flush = True)
            quarterly_eps_list = self.quarterly_two_eps_sales_growth(quarterly_eps_list, conn, today)
            # self.export_table("Q2_final_quarterly_eps_list", quarterly_eps_list)

            print("Inserting Quarterly EPS Results", flush = True)
            self.insert_quarterly_eps_resulsts(quarterly_eps_list, conn)

            print("Calculating TTM values", flush = True)
            self.get_all_ttm(quarterly_eps_list, conn,today)
        
        else:
            print("Data not present for quarterly EPS list for ....Date: "+str(today))

        
    def consolidated_today_quarterly_eps(self, conn, cur,today):
        """ generate the data for today quartely EPS,

        Operation:
            fetch the data of EPS list, Q1, Q2 Eps and Sales growth and TTM data,
            to get today quarterly EPS data.
        """ 
    
        print("\nCompiling quarterly EPS list", flush = True)
        # quarterly_eps_list = self.set_daily_qtr_eps(conn,today)
        c_quarterly_eps_list = self.consolidated_set_daily_qtr_eps(conn,today) # step 1
        # print("c_quarterly_eps_list.empty", c_quarterly_eps_list.empty, len(c_quarterly_eps_list), flush = True)
        if not(c_quarterly_eps_list.empty):
            print("Calculating quarterly one EPS Sales growth", flush = True)
            # quarterly_eps_list = self.quarterly_one_eps_sales_growth(quarterly_eps_list, conn, today)
            c_quarterly_eps_list = self.consolidated_quarterly_one_eps_sales_growth(c_quarterly_eps_list, conn,today)
            print("Calculating quarterly two EPS Sales growth", flush = True)
            # quarterly_eps_list = self.quarterly_two_eps_sales_growth(quarterly_eps_list, conn, today)
            c_quarterly_eps_list = self.consolidated_quarterly_two_eps_sales_growth(c_quarterly_eps_list, conn, today)
            
            print("Inserting Quarterly EPS Results", flush = True)
            self.insert_consolidated_quarterly_eps_results(c_quarterly_eps_list, conn)
            # Have to insert for consolidated too? Yup, just did..

            print("Calculating TTM values", flush = True)
            self.get_all_consolidated_ttm(c_quarterly_eps_list, conn,today) # Consolidated flow added inside
        
        else:
            print("Data not present for Consolidated quarterly EPS list for ....Date: "+str(today))




    def current_sa_eps_report(self, conn,cur,today):
        """Fetch the eps report for current date
        
        Operation:
            Fetch the data of EPS Rating, Stock Percentile Ranking and EPS data,
            and Generating EPS Report for current date.
        """

        print("Initiating QuarterlyEPS & TTM Process.... Date: "+str(today), flush = True)
        self.today_quarterly_eps(conn, cur, today)

        quarterly_eps_list = self.get_eps_list(conn,today)	
        
        if not(quarterly_eps_list.empty):

            quarterly_eps_list_null =  quarterly_eps_list[quarterly_eps_list['YearEnding'].isnull()]
            quarterly_eps_list =  quarterly_eps_list[quarterly_eps_list['YearEnding'].notnull()]

            print("Calculating EPS Rating", flush = True)
            quarterly_eps_list = self.eps_rating(quarterly_eps_list, conn,today)	
            return_list = quarterly_eps_list
            print("Merging Null Set Back into list", flush = True)
            quarterly_eps_list = pd.concat([quarterly_eps_list, quarterly_eps_list_null], sort=True)
            print("Calculating Stock Percentile Ranking", flush = True)
            quarterly_eps_list = self.stock_percentile_ranking(quarterly_eps_list)
            print("Inserting into EPS..", flush = True)
            self.insert_sa_EPS("EERS_SA_" + today.strftime("%Y-%m-%d"), quarterly_eps_list, conn, cur,today)
            return return_list, quarterly_eps_list_null
        
        else:
            print("Data not present for QuarterlyEPS & TTM Process.... Date: "+str(today))
            return_list, quarterly_eps_list_null = None, None
            return return_list, quarterly_eps_list_null




    def current_cons_eps_report(self, conn,cur,today):
        """Fetch the eps report for current date
        
        Operation:
            Fetch the data of EPS Rating, Stock Percentile Ranking and EPS data,
            and Generating EPS Report for current date.
        """
		
        print("Initiating ConsolidatedQuarterlyEPS & ConsolidatedTTM Process.... Date: "+str(today), flush = True)
        self.consolidated_today_quarterly_eps(conn, cur, today) 
        
        c_quarterly_eps_list = self.get_consolidated_eps_list(conn, today)
        return_list = pd.DataFrame()
        if not(c_quarterly_eps_list.empty):

            c_quarterly_eps_list_null =  c_quarterly_eps_list[c_quarterly_eps_list['YearEnding'].isnull()]
            c_quarterly_eps_list =  c_quarterly_eps_list[c_quarterly_eps_list['YearEnding'].notnull()]

            print("Calculating Consolidated EPS Rating", flush = True)
            c_quarterly_eps_list = self.consolidated_eps_rating(c_quarterly_eps_list, conn,today)   

            return_list = c_quarterly_eps_list

            print("Merging Null Set Back into list", flush = True)
            c_quarterly_eps_list = pd.concat([c_quarterly_eps_list, c_quarterly_eps_list_null], sort=True)

            print("Calculating Consolidated Stock Percentile Ranking", flush = True)
            c_quarterly_eps_list = self.stock_percentile_ranking(c_quarterly_eps_list)

            print("Inserting into Consolidated_EPS..", flush = True)
            self.insert_Consolidated_EPS("ERS_cons_" + today.strftime("%Y-%m-%d"), c_quarterly_eps_list, conn, cur,today)
            return return_list
        
        else:
            print("Data not present for ConsolidatedQuarterlyEPS & ConsolidatedTTM Process.... Date: "+str(today))
            return return_list



    def current_eps_report(self, conn, cur, today):
        """Fetch the eps report for current date
        
        Operation:
            Fetch the data of EPS Rating, Stock Percentile Ranking and EPS data,
            and Generating EPS Report for current date.
        """
        quarterly_eps_list, quarterly_eps_list_null = self.current_sa_eps_report(conn,cur, today)
        c_quarterly_eps_list = self.current_cons_eps_report(conn, cur, today)
		
        if quarterly_eps_list  is not None:
            for index, row in c_quarterly_eps_list.iterrows():
                if(str(row["TTM3 EERS"]) != 'nan'):
                    quarterly_eps_list.drop(quarterly_eps_list.loc[quarterly_eps_list['CompanyCode']==row['CompanyCode']].index, inplace=True)
                    row['CompanyName'] += " (C)"

                    try:
                        quarterly_eps_list = quarterly_eps_list.append(row, ignore_index = True)
                    except AttributeError:
                        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(row).transpose()], ignore_index=True)


            print("Merging Null Set Back into list", flush = True)
            quarterly_eps_list = pd.concat([quarterly_eps_list, quarterly_eps_list_null], sort=True)
            print("Calculating Stock Percentile Ranking", flush = True)
            quarterly_eps_list = self.stock_percentile_ranking(quarterly_eps_list)
            print("Inserting into EPS..", flush = True)
            self.insert_EPS("ERS_" + today.strftime("%Y-%m-%d"), quarterly_eps_list, conn, cur,today)



    def export_table(self, name,table):
        exportfilename = "_"+name+"_export.csv"
        exportfile = open(exportfilename,"w")

        table.to_csv(exportfile, header=True, index=False, float_format="%2f", lineterminator='\r')
        exportfile.close()



    def Generate_Daily_Report(self, curr_date, conn, cur):
        """Generating EPS reports"""
        
        today = curr_date
        
        self.current_eps_report(conn,cur, today)





