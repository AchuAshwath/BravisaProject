import datetime
import os.path
import os
import pandas as pd
import numpy as np
import pandas.io.sql as sqlio
import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed


class EPS:

    def __init__(self):
        pass

    def get_closest_quarter(self, target):
        candidates = [
            datetime.date(target.year - 1, 12, 31),
            datetime.date(target.year, 3, 31),
            datetime.date(target.year, 6, 30),
            datetime.date(target.year, 9, 30),
            datetime.date(target.year, 12, 31),
        ]

        for date in candidates:
            if target < date:
                candidates.remove(date)

        return min(candidates, key=lambda d: abs(target - d))


    def get_previous_quarter(self, target):
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
    



    def set_daily_qtr_eps(self, today, conn):
        
        end_qtr = self.get_closest_quarter(today).strftime("%Y-%m-%d") 
        print("End Qtr: ", end_qtr)

        sql = f'''SELECT QR.* from public."QuarterlyResults" QR LEFT JOIN public."QuarterlyEPS" QE ON QR."CompanyCode"= QE."CompanyCode" 
            AND QE."YearEnding" = QR."YearEnding"
            WHERE QR."YearEnding" <= '{end_qtr}'
            AND QE."CompanyCode" is null'''
            # AND QR."ModifiedDate"<='{today}';'''

        #For running backdated daily reports in case of bugs add the following to the above query
        #AND QR."ModifiedDate"<=\''+str(today + datetime.timedelta(1)))+'\'

        quarterly_list = pd.read_sql_query(sql, con = conn)
        
        print("CompanyCode NULL List: ", len(quarterly_list))

        quarterly_list = quarterly_list.rename(columns={'InterestCharges': 'Interest', 'PL_Before_Tax': 'OPM', 'TaxCharges': 'Tax',\
        'PL_After_TaxFromOrdineryActivities': 'PATRAW' , 'EquityCapital': 'Equity', 'ReservesAndSurplus': 'Reserves' })
        

        quarterly_list[quarterly_list.columns[5:60]] = quarterly_list[quarterly_list.columns[5:60]].replace(r'[?$,]', '', regex=True).astype(float)

        quarterly_list['Sales'] =  quarterly_list['TotalIncomeFromOperations'] + quarterly_list['IntOrDiscOnAdvOrBills']  + quarterly_list['IncomeOnInvestments']\
        + quarterly_list['IntOnBalancesWithRBI'] + quarterly_list['Others']  + quarterly_list['OtherRecurringIncome']

        quarterly_list['Expenses'] = quarterly_list['StockAdjustment'] + quarterly_list['RawMaterialConsumed']+quarterly_list['PurchaseOfTradedGoods']\
        +quarterly_list['PowerAndFuel']+quarterly_list['EmployeeExpenses']+quarterly_list['Excise']+quarterly_list['AdminAndSellingExpenses']+quarterly_list['ResearchAndDevelopmentExpenses']\
        + quarterly_list['ExpensesCapitalised'] + quarterly_list['OtherExpeses'] 

        quarterly_list['EBIDTA'] = quarterly_list['Sales'] - quarterly_list['Expenses']

        quarterly_list['Extraordinary'] = quarterly_list['ExtraOrdinaryItem'] + quarterly_list['ExceptionalItems']

        quarterly_list['Ext_Flag'] = [0 if x == 0 else 0 for x in quarterly_list['Extraordinary']] # 0 if x == 0 else 0

        quarterly_list['PAT'] = np.nan

        quarterly_list['PAT'] =	quarterly_list['PATRAW'] # if row['Ext_Flag'] == 0 else np.nan ##????

        quarterly_list['EPS'] = quarterly_list['PAT']/quarterly_list['Equity']
        
        quarterly_list['NPM'] = quarterly_list['PAT']/quarterly_list['Sales'] * 100
        
        quarterly_eps_list = quarterly_list[['CompanyCode', 'YearEnding', 'Months', 'Quarter', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary',\
        'OPM', 'Tax', 'PATRAW', 'PAT', 'Equity', 'Reserves', 'EPS', 'NPM', 'Ext_Flag']]

        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(columns = ['Q1 EPS Growth','Q1 Sales Growth', 'Q2 EPS', 'Q2 EPS Growth', 'Q2 Sales','Q2 Sales Growth'])], sort=False)

        self.export_table("1. Daily qtrs", quarterly_eps_list)

        return quarterly_eps_list



    def consolidated_set_daily_qtr_eps(self, today, conn):

        end_qtr = self.get_closest_quarter(today).strftime("%Y-%m-%d") 
        print("End Qtr: ", end_qtr)
        
        #Getting data from ConsolidatedQuarterlyResults that doesn't contain any data from ConsolidatedQuarterlyEPS to avoid redundancy
        sql = f'''SELECT QR.* from public."ConsolidatedQuarterlyResults" QR LEFT JOIN public."ConsolidatedQuarterlyEPS" QE ON QR."CompanyCode"= QE."CompanyCode" 
        AND QE."YearEnding" = QR."YearEnding"
        WHERE QR."YearEnding" <= '{end_qtr}'
        AND QE."CompanyCode" is null'''
        # AND QR."ModifiedDate"<='{today}';'''

        #For running backdated daily reports in case of bugs add the following to the above query
        #AND QR."ModifiedDate"<=\''+str(today + datetime.timedelta(1)))+'\'

        quarterly_list = pd.read_sql_query(sql, con = conn)
        
        print("CompanyCode NULL List: ", len(quarterly_list)) # No of tables to be taken
        
        quarterly_list = quarterly_list.rename(columns={'InterestCharges': 'Interest', 'PL_Before_Tax': 'OPM', 'TaxCharges': 'Tax',
                                                        'PL_After_TaxFromOrdineryActivities': 'PATRAW' , 'EquityCapital': 'Equity', 'ReservesAndSurplus': 'Reserves' })
        
        
        quarterly_list[quarterly_list.columns[5:60]] = quarterly_list[quarterly_list.columns[5:60]].replace(r'[?$,]', '', regex=True).astype(float)


        quarterly_list['Sales'] =  quarterly_list['TotalIncomeFromOperations'] + quarterly_list['IntOrDiscOnAdvOrBills']  + quarterly_list['IncomeOnInvestements'] + quarterly_list['IntOnBalancesWithRBI'] + quarterly_list['Others']  + quarterly_list['OtherRecurringIncome']

        quarterly_list['Expenses'] = quarterly_list['StockAdjustment'] + quarterly_list['RawMaterialConsumed']+quarterly_list['PurchaseOfTradedGoods']
        + quarterly_list['PowerAndFuel']+quarterly_list['EmployeeExpenses']+quarterly_list['Excise']+quarterly_list['AdminAndSellingExpenses']+quarterly_list['ResearchAndDevelopmentExpenses']
        + quarterly_list['ExpensesCapitalised'] + quarterly_list['OtherExpeses'] 

        quarterly_list['EBIDTA'] = quarterly_list['Sales'] - quarterly_list['Expenses']

        quarterly_list['Extraordinary'] = quarterly_list['ExtraOrdinaryItem'] + quarterly_list['ExceptionalItems']
        
        quarterly_list['Ext_Flag'] = [0 if x == 0 else 0 for x in quarterly_list['Extraordinary']] # 0 if x == 0 else 0

        quarterly_list['PAT'] = np.nan

        quarterly_list['PAT'] =	quarterly_list['PATRAW'] if quarterly_list['Ext_Flag'] == 0 else np.nan

        quarterly_list['EPS'] = quarterly_list['PAT']/quarterly_list['Equity']

        quarterly_list['NPM'] = quarterly_list['PAT']/quarterly_list['Sales'] * 100
        
        quarterly_eps_list = quarterly_list[['CompanyCode', 'YearEnding', 'Months', 'Quarter', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary',
        'OPM', 'Tax', 'PATRAW', 'PAT', 'Equity', 'Reserves', 'EPS', 'NPM', 'Ext_Flag']]

        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(columns = ['Q1 EPS Growth','Q1 Sales Growth', 'Q2 EPS', 'Q2 EPS Growth', 'Q2 Sales','Q2 Sales Growth'])], sort=False)
        
        return quarterly_eps_list
    




    def quarterly_eps_sales_growth(self, quarterly_eps_list, today, conn):
        prev_4year = self.get_four_years_before_quarter(today).strftime("%Y-%m-%d")
        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        sql = f'''SELECT * FROM public."QuarterlyEPS" where "Ext_Flag" is not null and "YearEnding" >= '{prev_4year}' and "YearEnding" <= '{qtr_now}';'''
        # sql = f'''SELECT * FROM public."QuarterlyEPS" where "Ext_Flag" is not null and "YearEnding" >= '{prev_4year}' and "YearEnding" <= '{today.strftime("%Y-%m-%d")}';'''
        quarterly_eps_year_back = sqlio.read_sql_query(sql, con = conn)	

        quarterly_eps_list['Sales'] =quarterly_eps_list['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        quarterly_eps_year_back['Sales'] = quarterly_eps_year_back['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        
        quarterly_eps_list = quarterly_eps_list.sort_values(by = 'YearEnding', ascending = True)


        # Calculating Q1 EPS Sales Growth
        for index, row in quarterly_eps_list.iterrows():
            
            prev_year_qtr = self.get_year_before_quarter(row['YearEnding'])
            #################
            eps_current = row['EPS']
            # EPS Value for previous year for current quarter
            eps_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]["EPS"]
            
            if len(eps_prev_list.index) > 1:
                    eps_prev = eps_prev_list[eps_prev_list.index[0]]
            else:
                eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan

            if (math.isnan(eps_prev)):
                eps_prev_list = quarterly_eps_list.loc[(quarterly_eps_list["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_list["YearEnding"] == prev_year_qtr)]["EPS"]
                if len(eps_prev_list.index) > 1:
                    eps_prev = eps_prev_list[eps_prev_list.index[0]]
                else:
                    eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan

                quarterly_eps_list.loc[index, 'Q1 EPS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            
            quarterly_eps_list.loc[index, 'Q1 EPS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan

            #################
            sales_current = row['Sales']
            # Sales Value for previous year for current quarter
            sales_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['Sales']
            
            if len(sales_prev_list.index) > 1:
                sales_prev = sales_prev_list[sales_prev_list.index[0]]
            else:
                sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan

            if (math.isnan(sales_prev)):
                sales_prev_list = quarterly_eps_list.loc[(quarterly_eps_list["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_list["YearEnding"] == prev_year_qtr)]['Sales']
                
                if len(sales_prev_list.index) > 1:
                    sales_prev = sales_prev_list[sales_prev_list.index[0]]
                else:
                    sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan
                
                quarterly_eps_list.loc[index, 'Q1 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev  !=0 else np.nan

            quarterly_eps_list.loc[index, 'Q1 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev  !=0 else np.nan


        # Calculating Q2 EPS Sales Growth
        for index, row in quarterly_eps_list.iterrows():

            prev_qtr = self.get_previous_quarter(row['YearEnding'])
            prev_year_qtr = self.get_year_before_quarter(prev_qtr)
            #################
            # EPS Value for previous year for previous quarter
            eps_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['EPS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            # EPS Value for previous quarter
            eps_current_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_qtr)]['EPS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan        
            
            quarterly_eps_list.loc[index, 'Q2 EPS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            quarterly_eps_list.loc[index , 'Q2 EPS'] = eps_current

            #################
            # Sales Value for previous year for previous quarter
            sales_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan
            # Sales Value for previous quarter
            sales_current_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_qtr)]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan 

            quarterly_eps_list.loc[index, 'Q2 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev !=0 else np.nan
            quarterly_eps_list.loc[index , 'Q2 Sales'] = sales_current

        self.export_table("2. EPS Sales Growth", quarterly_eps_list)

        return quarterly_eps_list



    def consolidated_quarterly_eps_sales_growth(self, quarterly_eps_list, today, conn):
        prev_4year = self.get_four_years_before_quarter(today).strftime("%Y-%m-%d")
        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        sql = f'''SELECT * FROM public."ConsolidatedQuarterlyEPS" where "Ext_Flag" is not null and "YearEnding" >= '{prev_4year}' and "YearEnding" <= '{qtr_now}';'''
        # sql = f'''SELECT * FROM public."ConsolidatedQuarterlyEPS" where "Ext_Flag" is not null and "YearEnding" >= '{prev_4year}' and "YearEnding" <= '{today.strftime("%Y-%m-%d")}';'''
        quarterly_eps_year_back = sqlio.read_sql_query(sql, con = conn)	

        quarterly_eps_list['Sales'] =quarterly_eps_list['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        quarterly_eps_year_back['Sales'] = quarterly_eps_year_back['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
        
        quarterly_eps_list = quarterly_eps_list.sort_values(by = 'YearEnding', ascending = True)


        # Calculating Q1 EPS Sales Growth
        for index, row in quarterly_eps_list.iterrows():
            
            prev_year_qtr = self.get_year_before_quarter(row['YearEnding'])
            #################
            eps_current = row['EPS']
            # EPS Value for previous year for current quarter
            eps_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]["EPS"]
            
            if len(eps_prev_list.index) > 1:
                    eps_prev = eps_prev_list[eps_prev_list.index[0]]
            else:
                eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan

            if (math.isnan(eps_prev)):
                eps_prev_list = quarterly_eps_list.loc[(quarterly_eps_list["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_list["YearEnding"] == prev_year_qtr)]["EPS"]
                if len(eps_prev_list.index) > 1:
                    eps_prev = eps_prev_list[eps_prev_list.index[0]]
                else:
                    eps_prev = eps_prev_list.item() if len(eps_prev_list.index) != 0 else np.nan

                quarterly_eps_list.loc[index, 'Q1 EPS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            
            quarterly_eps_list.loc[index, 'Q1 EPS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan

            #################
            sales_current = row['Sales']
            # Sales Value for previous year for current quarter
            sales_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['Sales']
            
            if len(sales_prev_list.index) > 1:
                sales_prev = sales_prev_list[sales_prev_list.index[0]]
            else:
                sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan

            if (math.isnan(sales_prev)):
                sales_prev_list = quarterly_eps_list.loc[(quarterly_eps_list["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_list["YearEnding"] == prev_year_qtr)]['Sales']
                
                if len(sales_prev_list.index) > 1:
                    sales_prev = sales_prev_list[sales_prev_list.index[0]]
                else:
                    sales_prev = sales_prev_list.item() if len(sales_prev_list.index) != 0 else np.nan
                
                quarterly_eps_list.loc[index, 'Q1 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev  !=0 else np.nan

            quarterly_eps_list.loc[index, 'Q1 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev  !=0 else np.nan


        # Calculating Q2 EPS Sales Growth
        for index, row in quarterly_eps_list.iterrows():

            prev_qtr = self.get_previous_quarter(row['YearEnding'])
            prev_year_qtr = self.get_year_before_quarter(prev_qtr)
            #################
            # EPS Value for previous year for previous quarter
            eps_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['EPS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            # EPS Value for previous quarter
            eps_current_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_qtr)]['EPS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan        
            
            quarterly_eps_list.loc[index, 'Q2 EPS Growth'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev  !=0 else np.nan
            quarterly_eps_list.loc[index , 'Q2 EPS'] = eps_current

            #################
            # Sales Value for previous year for previous quarter
            sales_prev_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan
            # Sales Value for previous quarter
            sales_current_list = quarterly_eps_year_back.loc[(quarterly_eps_year_back["CompanyCode"]==row['CompanyCode']) & (quarterly_eps_year_back["YearEnding"] == prev_qtr)]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan 

            quarterly_eps_list.loc[index, 'Q2 Sales Growth'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev !=0 else np.nan
            quarterly_eps_list.loc[index , 'Q2 Sales'] = sales_current

        return quarterly_eps_list



    def ttm_quarterly_list(self, today, conn):
    
        qtr_now = self.get_closest_quarter(today)

        prev_4year =  self.get_four_years_before_quarter(today)
        
        sql =   f'''SELECT QE.* from public."QuarterlyEPS" QE LEFT JOIN public."TTM" TTM on QE."CompanyCode"= TTM."CompanyCode" 
                and TTM."YearEnding" = QE."YearEnding"
                WHERE QE."YearEnding" <= '{qtr_now.strftime("%Y-%m-%d")}' 
                AND TTM."CompanyCode" is null 
                AND QE."Ext_Flag" is not null;'''
        
        quarterly_eps_list = sqlio.read_sql_query(sql, con = conn)

        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        qeps_back = f'''SELECT * FROM public."QuarterlyEPS" WHERE "YearEnding" >= '{prev_4year.strftime("%Y-%m-%d")}' and "YearEnding" <= '{qtr_now}';'''
        qrtlylist = sqlio.read_sql_query(qeps_back, con = conn)


        qrtlylist[qrtlylist.columns[4:16]] = qrtlylist[qrtlylist.columns[4:16]].replace(r'[?$,]', '', regex=True).astype(np.float32)
        
        ttm_calc = pd.DataFrame(columns=['CompanyCode', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',\
        'PAT', 'Equity',  'Reserves', 'Months', 'Quarter', 'EPS', 'NPM'])
        
        ttm_calc = pd.merge(ttm_calc, quarterly_eps_list[['CompanyCode', 'YearEnding']], left_on='CompanyCode', right_on='CompanyCode', how='right')


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

            ttm_calc.loc[index, 'EPS'] = ttm_calc.loc[index, 'PAT']/ttm_calc.loc[index, 'Equity'] if ttm_calc.loc[index, 'Equity'] != 0 else np.nan
            ttm_calc.loc[index, 'NPM'] = ttm_calc.loc[index, 'PAT']/ttm_calc.loc[index, 'Sales'] * 100 if ttm_calc.loc[index, 'Sales'] != 0 else np.nan

        ttm_calc = ttm_calc[['CompanyCode', 'YearEnding', 'Months', 'Quarter','Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',\
        'PAT', 'Equity', 'Reserves', 'EPS', 'NPM']]

        self.export_table("4. TTM", ttm_calc)

        return ttm_calc
    

    def consolidated_ttm_quarterly_list(self, today, conn):
        
        qtr_now = self.get_closest_quarter(today)

        prev_4year =  self.get_four_years_before_quarter(today)
        
        sql =   f'''SELECT QE.* from public."ConsolidatedQuarterlyEPS" QE LEFT JOIN public."ConsolidatedTTM" TTM on QE."CompanyCode"= TTM."CompanyCode" 
                    and TTM."YearEnding" = QE."YearEnding"
                    WHERE QE."YearEnding" <= '{qtr_now.strftime("%Y-%m-%d")}' 
                    AND TTM."CompanyCode" is null 
                    AND QE."Ext_Flag" is not null;'''

        quarterly_eps_list = sqlio.read_sql_query(sql, con = conn)

        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        qeps_back = f'''SELECT * FROM public."ConsolidatedQuarterlyEPS" WHERE "YearEnding" >= '{prev_4year.strftime("%Y-%m-%d")}' and "YearEnding" <= '{qtr_now}';'''
        # qeps_back = f'''SELECT * FROM public."ConsolidatedQuarterlyEPS" WHERE "YearEnding" >= '{prev_4year.strftime("%Y-%m-%d")}' and "YearEnding" <= '{today.strftime("%Y-%m-%d")}';'''
        qrtlylist = sqlio.read_sql_query(qeps_back, con = conn)


        qrtlylist[qrtlylist.columns[4:16]] = qrtlylist[qrtlylist.columns[4:16]].replace(r'[?$,]', '', regex=True).astype(np.float32)
        
        ttm_calc = pd.DataFrame(columns=['CompanyCode', 'Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',\
        'PAT', 'Equity',  'Reserves', 'Months', 'Quarter', 'EPS', 'NPM'])
        
        ttm_calc = pd.merge(ttm_calc, quarterly_eps_list[['CompanyCode', 'YearEnding']], left_on='CompanyCode', right_on='CompanyCode', how='right')

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

            ttm_calc.loc[index, 'EPS'] = ttm_calc.loc[index, 'PAT']/ttm_calc.loc[index, 'Equity'] if ttm_calc.loc[index, 'Equity'] != 0 else np.nan
            ttm_calc.loc[index, 'NPM'] = ttm_calc.loc[index, 'PAT']/ttm_calc.loc[index, 'Sales'] * 100 if ttm_calc.loc[index, 'Sales'] != 0 else np.nan

        ttm_calc = ttm_calc[['CompanyCode', 'YearEnding', 'Months', 'Quarter','Sales', 'Expenses', 'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',\
        'PAT', 'Equity', 'Reserves', 'EPS', 'NPM']]

        return ttm_calc
    

    def today_quarterly_eps(self, today, conn):
    
        print("\nCompiling quarterly EPS list", flush = True)
        c_quarterly_eps_list = self.set_daily_qtr_eps(today, conn)
        
        if not(c_quarterly_eps_list.empty):
            print("Calculating quarterly one two EPS Sales growth", flush = True)
            c_quarterly_eps_list = self.quarterly_eps_sales_growth(c_quarterly_eps_list,today, conn)
            
            print("Inserting Quarterly EPS Results", flush = True)
            self.insert_quarterly_eps_results(c_quarterly_eps_list, conn)

            print("Calculating TTM values", flush = True)
            c_ttm = self.ttm_quarterly_list(today, conn)
            self.insert_c_ttm(c_ttm, conn)
        
        else:
            print("Data not present for quarterly EPS list for ....Date: "+str(today))


    def consolidated_today_quarterly_eps(self, today, conn):

        print("\nCompiling quarterly EPS list", flush = True)
        c_quarterly_eps_list = self.consolidated_set_daily_qtr_eps(today, conn)
        
        if not(c_quarterly_eps_list.empty):
            print("Calculating quarterly one two EPS Sales growth", flush = True)
            c_quarterly_eps_list = self.consolidated_quarterly_eps_sales_growth(c_quarterly_eps_list,today, conn)
            
            print("Inserting Quarterly EPS Results", flush = True)
            self.insert_consolidated_quarterly_eps_results(c_quarterly_eps_list, conn)

            print("Calculating TTM values", flush = True)
            c_ttm = self.consolidated_ttm_quarterly_list(today, conn)
            self.insert_c_ttm(c_ttm, conn)
        
        else:
            print("Data not present for quarterly EPS list for ....Date: "+str(today))




    def get_eps_list(self, today, conn):
        btt_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
        next_month = today.month + 1 if today.month + 1 <= 12 else 1
        next_year = today.year if today.month + 1 <= 12 else today.year + 1
        btt_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")
        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        today = datetime.date(today.year, today.month, today.day).strftime("%Y-%m-%d")
        
        # Fetch data from both tables
        btt_df = sqlio.read_sql_query('SELECT * FROM public."BTTList" WHERE "BTTDate" >= %(btt_back)s AND "BTTDate" < %(btt_next)s;', con=conn, params={'btt_back': btt_back, 'btt_next': btt_next})
        eps_df = sqlio.read_sql_query('SELECT * FROM public."QuarterlyEPS" WHERE "YearEnding" <= \'' + qtr_now + '\';', con=conn)  
        
        # Filter rows in eps_df based on conditions
        eps_df = eps_df[eps_df['YearEnding'] == eps_df.groupby('CompanyCode')['YearEnding'].transform('max')]  
        
        # Merge DataFrames
        merged_df = pd.merge(btt_df, eps_df, on="CompanyCode", how="left")

        self.export_table("5. CHECK EPS List", merged_df)
        return merged_df


    def get_consolidated_eps_list(self, today, conn):
        btt_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
        next_month = today.month + 1 if today.month + 1 <= 12 else 1 
        next_year = today.year if today.month + 1 <= 12 else today.year + 1
        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")
        btt_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")

        today = datetime.date(today.year, today.month, today.day).strftime("%Y-%m-%d")

        btt_df = sqlio.read_sql_query('SELECT * FROM public."BTTList" WHERE "BTTDate" >= %(btt_back)s AND "BTTDate" < %(btt_next)s;', con=conn, params={'btt_back': btt_back, 'btt_next': btt_next})
        eps_df = sqlio.read_sql_query('SELECT * FROM public."ConsolidatedQuarterlyEPS" WHERE "YearEnding" <= \'' + qtr_now + '\';', con=conn)  
        
        # Filter rows in eps_df based on conditions
        eps_df = eps_df[eps_df['YearEnding'] == eps_df.groupby('CompanyCode')['YearEnding'].transform('max')] 
        merged_df = pd.merge(btt_df, eps_df, on="CompanyCode", how="left")
        
        return merged_df
    



    def ttm_eps_sales_growth(self, quarterly_eps_list, today, conn):
    
        four_year_back =  self.get_four_years_before_quarter(self.get_closest_quarter(today))
        five_year_back = self.get_year_before_quarter(four_year_back).strftime("%Y-%m-%d")
        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")

        sql = f'''SELECT * from public."TTM" WHERE "YearEnding" >='{five_year_back}' and "YearEnding" <= '{qtr_now}';'''
        ttm_backyear = pd.read_sql_query(sql, con = conn)

        ttm_backyear['Sales'] = ttm_backyear['Sales'].replace(r'[?$,]', '', regex=True).astype(float)

        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(columns = ['TTM1 EPS', 'TTM1 Sales', 'TTM2 EPS', 'TTM2 Sales', 'TTM3 EPS', 'TTM3 Sales'])], sort=False)


        for index, row in quarterly_eps_list.iterrows():

            ##TTM1

            prev_year_qtr = self.get_year_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['EPS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == row["YearEnding"])]['EPS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM1 EPS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == row["YearEnding"])]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM1 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan

            ttm_eps_val = quarterly_eps_list.loc[index, 'TTM1 EPS'] 
            ttm_eps_set_val1 = ( ((0.3)* ttm_eps_val) if not math.isnan(ttm_eps_val) else 0 ) 
            # quarterly_eps_list.loc[index, 'EPS Rating'] = quarterly_eps_list.loc[index, 'EPS Rating'] + ttm_eps_set_val

            ##TTM2

            prev_two_year_qtr = self.get_two_years_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['EPS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['EPS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM2 EPS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM2 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan

            ttm_eps_val = quarterly_eps_list.loc[index, 'TTM2 EPS'] 
            ttm_eps_set_val2 = ( ((0.2)* ttm_eps_val) if not math.isnan(ttm_eps_val) else 0 ) 
            # quarterly_eps_list.loc[index, 'EPS Rating'] = quarterly_eps_list.loc[index, 'EPS Rating'] + ttm_eps_set_val

            ##TTM3

            prev_three_year_qtr = self.get_three_years_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_three_year_qtr)]['EPS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['EPS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM3 EPS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_three_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM3 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan


            ttm_eps_val = (0.1) * quarterly_eps_list.loc[index, 'TTM3 EPS'] 
            ttm_eps_set_val3 = (ttm_eps_val if not math.isnan(ttm_eps_val)  else 0 ) 

            quarterly_eps_list.loc[index, 'EPS Rating'] = quarterly_eps_list.loc[index, 'EPS Rating'] + ttm_eps_set_val1 + ttm_eps_set_val2 + ttm_eps_set_val3
            

        return quarterly_eps_list



    def consolidated_ttm_eps_sales_growth(self, quarterly_eps_list, today, conn):

        four_year_back =  self.get_four_years_before_quarter(self.get_closest_quarter(today))
        five_year_back = self.get_year_before_quarter(four_year_back).strftime("%Y-%m-%d")
        qtr_now = self.get_closest_quarter(today).strftime("%Y-%m-%d")


        sql = f'''SELECT * from public."ConsolidatedTTM" WHERE "YearEnding" >='{five_year_back}' and "YearEnding" <= '{qtr_now}';'''
        ttm_backyear = pd.read_sql_query(sql, con = conn)

        ttm_backyear['Sales'] = ttm_backyear['Sales'].replace(r'[?$,]', '', regex=True).astype(float)

        quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(columns = ['TTM1 EPS', 'TTM1 Sales', 'TTM2 EPS', 'TTM2 Sales', 'TTM3 EPS', 'TTM3 Sales'])], sort=False)


        for index, row in quarterly_eps_list.iterrows():

            ##TTM1

            prev_year_qtr = self.get_year_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['EPS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == row["YearEnding"])]['EPS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM1 EPS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == row["YearEnding"])]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM1 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan

            ttm_eps_val = quarterly_eps_list.loc[index, 'TTM1 EPS'] 
            ttm_eps_set_val1 = ( ((0.3)* ttm_eps_val) if not math.isnan(ttm_eps_val) else 0 ) 
            # quarterly_eps_list.loc[index, 'EPS Rating'] = quarterly_eps_list.loc[index, 'EPS Rating'] + ttm_eps_set_val

            ##TTM2

            prev_two_year_qtr = self.get_two_years_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['EPS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['EPS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM2 EPS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_year_qtr)]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM2 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan

            ttm_eps_val = quarterly_eps_list.loc[index, 'TTM2 EPS'] 
            ttm_eps_set_val2 = ( ((0.2)* ttm_eps_val) if not math.isnan(ttm_eps_val) else 0 ) 
            # quarterly_eps_list.loc[index, 'EPS Rating'] = quarterly_eps_list.loc[index, 'EPS Rating'] + ttm_eps_set_val

            ##TTM3

            prev_three_year_qtr = self.get_three_years_before_quarter(row['YearEnding'])

            eps_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_three_year_qtr)]['EPS']
            eps_prev = eps_prev_list.item() if len(eps_prev_list.index) == 1 else np.nan
            
            eps_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['EPS']
            eps_current = eps_current_list.item() if len(eps_current_list.index) == 1 else np.nan

            quarterly_eps_list.loc[index, 'TTM3 EPS'] = ((eps_current-eps_prev)/abs(eps_prev))*100 if eps_prev != 0 else np.nan

            sales_prev_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_three_year_qtr)]['Sales']
            sales_prev = sales_prev_list.item() if len(sales_prev_list.index) == 1 else np.nan		
            
            sales_current_list = ttm_backyear.loc[(ttm_backyear["CompanyCode"]==row['CompanyCode']) & (ttm_backyear["YearEnding"] == prev_two_year_qtr)]['Sales']
            sales_current = sales_current_list.item() if len(sales_current_list.index) == 1 else np.nan		

            quarterly_eps_list.loc[index, 'TTM3 Sales'] = ((sales_current-sales_prev)/abs(sales_prev))*100 if sales_prev != 0 else np.nan


            ttm_eps_val = (0.1) * quarterly_eps_list.loc[index, 'TTM3 EPS'] 
            ttm_eps_set_val3 = (ttm_eps_val if not math.isnan(ttm_eps_val)  else 0 ) 

            quarterly_eps_list.loc[index, 'EPS Rating'] = quarterly_eps_list.loc[index, 'EPS Rating'] + ttm_eps_set_val1 + ttm_eps_set_val2 + ttm_eps_set_val3
            

        return quarterly_eps_list
    



    def eps_rating(self, quarterly_eps_list, today, conn):
        
        for index, row in quarterly_eps_list.iterrows():
            if(quarterly_eps_list.loc[index, 'Q1 EPS Growth'] is not None):
                Q1_EPS_Growth = (0.2) * quarterly_eps_list.loc[index, 'Q1 EPS Growth']
            else:
                Q1_EPS_Growth = np.nan
                
            if(quarterly_eps_list.loc[index, 'Q2 EPS Growth'] is not None):
                Q2_EPS_Growth = (0.2) * quarterly_eps_list.loc[index, 'Q2 EPS Growth']
            else:
                Q2_EPS_Growth = np.nan

            quarterly_eps_list.loc[index, 'EPS Rating'] = (Q1_EPS_Growth if not (math.isnan(Q1_EPS_Growth) or Q1_EPS_Growth == np.nan)  else 0) +\
                                                        (Q2_EPS_Growth if not(math.isnan(Q2_EPS_Growth) or Q2_EPS_Growth == np.nan) else 0)

        quarterly_eps_list = self.ttm_eps_sales_growth(quarterly_eps_list, today, conn)

        return quarterly_eps_list


    def consolidated_eps_rating(self, c_quarterly_eps_list, today, conn):
    
        for index, row in c_quarterly_eps_list.iterrows():
            if(c_quarterly_eps_list.loc[index, 'Q1 EPS Growth'] is not None):
                Q1_EPS_Growth = (0.2)*c_quarterly_eps_list.loc[index, 'Q1 EPS Growth']
            else:
                Q1_EPS_Growth = np.nan
                
            if(c_quarterly_eps_list.loc[index, 'Q2 EPS Growth'] is not None):
                Q2_EPS_Growth = (0.2)*c_quarterly_eps_list.loc[index, 'Q2 EPS Growth']
            else:
                Q2_EPS_Growth = np.nan

            c_quarterly_eps_list.loc[index, 'EPS Rating'] = (Q1_EPS_Growth if not (math.isnan(Q1_EPS_Growth) or Q1_EPS_Growth == np.nan)  else 0) +\
                                                        (Q2_EPS_Growth if not(math.isnan(Q2_EPS_Growth) or Q2_EPS_Growth == np.nan) else 0)

        c_quarterly_eps_list = self.consolidated_ttm_eps_sales_growth(c_quarterly_eps_list, today, conn)

        return c_quarterly_eps_list
    



    def stock_percentile_ranking(self, quarterly_eps_list):

        quarterly_eps_list['EPS Ranking'] = quarterly_eps_list['EPS Rating'].rank(ascending=False)
        quarterly_eps_list['EPS Ranking'] = ((len(quarterly_eps_list.index)-quarterly_eps_list['EPS Ranking']+1)/len(quarterly_eps_list.index))*100

        return quarterly_eps_list
    


    def insert_quarterly_eps_results(self, quarterly_eps_list, conn):

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
        'OPM', 'Tax', 'PATRAW', 'PAT', 'Equity', 'Reserves', 'EPS', 'NPM', 'Ext_Flag', 'Q1 EPS Growth', 'Q1 Sales Growth', 'Q2 EPS', 'Q2 EPS Growth', 'Q2 Sales', 'Q2 Sales Growth']]

        exportfilename = "QuarterlyEPSListExport.csv"
        exportfile = open(exportfilename,"w+")
        quarterly_eps_list.to_csv(exportfile, header=True, index=False, lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY "public"."QuarterlyEPS" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)

        updatequery = 'UPDATE public."QuarterlyEPS" SET "Ext_Flag" = true WHERE "Ext_Flag" IS NULL'                 
        cur.execute(updatequery)
        conn.commit()



    def insert_consolidated_quarterly_eps_results(self, quarterly_eps_list, conn):

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
        'OPM', 'Tax', 'PATRAW', 'PAT', 'Equity', 'Reserves', 'EPS', 'NPM', 'Ext_Flag', 'Q1 EPS Growth', 'Q1 Sales Growth', 'Q2 EPS', 'Q2 EPS Growth', 'Q2 Sales', 'Q2 Sales Growth']]

        exportfilename = "ConsolidatedQuarterlyEPSListExport.csv"
        exportfile = open(exportfilename,"w+")
        quarterly_eps_list.to_csv(exportfile, header=True, index=False, lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY "public"."ConsolidatedQuarterlyEPS" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f) 
            conn.commit()
            f.close()
        os.remove(exportfilename)

        updatequery = 'UPDATE public."ConsolidatedQuarterlyEPS" SET "Ext_Flag" = true WHERE "Ext_Flag" IS NULL'           
        cur.execute(updatequery)
        conn.commit()



    def insert_ttm(self, ttm, conn):

        cur = conn.cursor()

        # Column Sclicing
        ttm[ttm.columns[4:17]] = ttm[ttm.columns[4:17]].apply(pd.to_numeric, errors='coerce')

        ttm = ttm.round(2) # Change made on June 3rd (Recently)

        exportfilename = "ttm_export.csv"
        exportfile = open(exportfilename,"w+")
        ttm.to_csv(exportfile, header=True, index=False, lineterminator='\r')
        exportfile.close()
            
        copy_sql = """
                COPY "public"."TTM" FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)


    def insert_c_ttm(self, ttm, conn):

        cur = conn.cursor()

        # Column Sclicing
        ttm[ttm.columns[4:17]] = ttm[ttm.columns[4:17]].apply(pd.to_numeric, errors='coerce')

        ttm = ttm.round(2) # Change made on June 3rd (Recently)

        exportfilename = "consolidated_ttm_export.csv"
        exportfile = open(exportfilename,"w+")
        ttm.to_csv(exportfile, header=True, index=False, lineterminator='\r')
        exportfile.close()
            
        copy_sql = """
                COPY "public"."ConsolidatedTTM" FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)


    def insert_EPS(self, name,table, conn, cur, today):
        """ Insert EPS data into database

        Args:
            table = table with appropriate Column,

        Operation:
            insert the data into EPS Table.	
        """

        table["EPS Date"] = today.strftime("%Y-%m-%d")

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

        table = table.rename(columns = {'Sales' : 'Q1 Sales', 'EPS': 'Q1 EPS'})
        # Creating table
        table = table[['CompanyCode', 'NSECode', 'BSECode', 'CompanyName', 'ISIN', 'Months', 'Quarter', 'YearEnding', \
        'Q1 EPS', 'Q1 EPS Growth' , 'Q1 Sales','Q1 Sales Growth' , 'Q2 EPS', 'Q2 EPS Growth', 'Q2 Sales','Q2 Sales Growth', \
        'TTM1 EPS', 'TTM1 Sales', 'TTM2 EPS', 'TTM2 Sales', 'TTM3 EPS', 'TTM3 Sales', 'NPM', 'EPS Rating', 'EPS Ranking', 'EPS Date']]

        # print(table) EPS

        exportfilename = name+"_export.csv"
        exportfile = open(exportfilename,"w+")

        table.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()
        # self.export_table("End_quarter_result", table)
        # raise Exception("Break for testing")
        copy_sql = """
                COPY "Reports"."EPS" FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)


    def export_table(self, name,table):
        exportfilename = "saved/_"+name+"_export.csv"
        exportfile = open(exportfilename,"w")

        table.to_csv(exportfile, header=True, index=False, float_format="%2f", lineterminator='\r')
        exportfile.close()


    def current_eps_report(self, conn, cur, today):
        print("Initiating QuarterlyEPS & TTM Process.... Date: " + str(today), flush=True)

        # Define tasks for each thread
        def task_normal(today, conn):
            self.today_quarterly_eps(today, conn)
            quarterly_eps_list = self.get_eps_list(today, conn)
            return quarterly_eps_list

        def task_consolidated(today, conn):
            if today >= datetime.date(2021, 4, 1):
                self.consolidated_today_quarterly_eps(today, conn)
                c_quarterly_eps_list = self.get_consolidated_eps_list(today, conn)
                return c_quarterly_eps_list
            return None

        def task_rating(quarterly_eps_list, today, conn):
            quarterly_eps_list = self.eps_rating(quarterly_eps_list, today, conn)
            return quarterly_eps_list

        def task_consolidated_rating(c_quarterly_eps_list, today, conn):
            if today >= datetime.date(2021, 4, 1):
                c_quarterly_eps_list = self.consolidated_eps_rating(c_quarterly_eps_list, today, conn)
                return c_quarterly_eps_list
            return None
        

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_normal = executor.submit(task_normal, today, conn)
            future_consolidated = executor.submit(task_consolidated, today, conn)

            quarterly_eps_list = future_normal.result()
            c_quarterly_eps_list = future_consolidated.result()

        if not(quarterly_eps_list.empty):  
            quarterly_eps_list_null =  quarterly_eps_list[quarterly_eps_list['YearEnding'].isnull()]
            quarterly_eps_list =  quarterly_eps_list[quarterly_eps_list['YearEnding'].notnull()]

            
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_rating = executor.submit(task_rating, quarterly_eps_list, today, conn)
                future_consolidated_rating = executor.submit(task_consolidated_rating, quarterly_eps_list, today, conn)

                quarterly_eps_list = future_rating.result()
                c_quarterly_eps_list = future_consolidated_rating.result()

            if(today >= datetime.date(2021, 4, 1)):
                number_of_consolidated = 0
                for index, row in c_quarterly_eps_list.iterrows():

                    if(str(row["TTM3 EPS"]) != 'nan'):
                        quarterly_eps_list.drop(quarterly_eps_list.loc[quarterly_eps_list['CompanyCode']==row['CompanyCode']].index, inplace=True)
                        row['CompanyName'] += " (C)"

                        try:
                            quarterly_eps_list = quarterly_eps_list.append(row, ignore_index = True)
                        except AttributeError:
                            quarterly_eps_list = pd.concat([quarterly_eps_list, pd.DataFrame(row).transpose()], ignore_index=True)
                        number_of_consolidated += 1
                print("Number of entries from consolidated data: ", number_of_consolidated)


            print("Merging Null Set Back into list", flush = True)
            quarterly_eps_list = pd.concat([quarterly_eps_list, quarterly_eps_list_null], sort=True)
            print("Calculating Stock Percentile Ranking", flush = True)

            quarterly_eps_list = self.stock_percentile_ranking(quarterly_eps_list)

            print("Inserting into EPS..", flush = True)

            self.insert_EPS("EPS_" + today.strftime("%Y-%m-%d"), quarterly_eps_list, conn, cur,today)
        
        else:
            print("Data not present for QuarterlyEPS & TTM Process.... Date: "+str(today))




    def Generate_Daily_Report(self, curr_date, conn, cur):
        """Generating EPS reports"""

        today = curr_date
        
        self.current_eps_report(conn,cur, today)