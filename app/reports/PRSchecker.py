# Script to compile and insert daily PRS reports per each company on BTT List
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
import sys
import math
import utils.date_set as date_set


today = np.nan
today_date = np.nan
month_back = np.nan
year_back = np.nan


class PRS:
	"""
	Generating PRS data for current date
	Fetch the data from BTTList, OHLC and  PE table
	and calculate the NewLow, NewHigh, Value Average, and Rate of Change for
	current date.
 	"""

	def __init__(self):
		# Constuctor
		
		pass

	def set_date(self, today_in=None):
		"""Setting the date for today, month back and a year back"""

		global today
		global today_date
		global month_back
		global year_back

		if(today_in == None):
			today = datetime.date.today()
			today = datetime.datetime.combine(today,datetime.datetime.min.time())
				
		else:
			today = today_in

		today_date = today.strftime("%Y-%m-%d")
		month_back = (today+datetime.timedelta(-30)).strftime("%Y-%m-%d")
		year_back = (today+datetime.timedelta(-366)).strftime("%Y-%m-%d")
		print("year_back\n", year_back)
		print("Today date:",today)
		

	def fetch_btt_prs(self, curr_date, conn):
		"""Fetching the data from BTTList from first each month"""

		# today = datetime.datetime.now().date()
		today = curr_date
		# today = datetime.datetime.combine(today,datetime.datetime.min.time())

		# print("today Date for the fetch_btt_prs:",today)
		
		BTT_back = datetime.date(
			today.year, today.month, 1).strftime("%Y-%m-%d")

		next_month = today.month + 1 if today.month + 1 <= 12 else 1
		next_year = today.year if today.month + 1 <= 12 else today.year + 1
		BTT_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")

		btt_sql = 'select distinct on ("CompanyCode") "ISIN", "NSECode", "BSECode", "CompanyName", "CompanyCode"  from public."BTTList" \
				where "BTTDate" >= \''+BTT_back + '\' and "BTTDate" < \''+BTT_next+'\';'
		
		bttlist = pd.read_sql_query(btt_sql, con=conn)

		return bttlist



	def fetch_ohlc_prs(self, curr_date, bttlist, conn):
		""" Fetch the data from OHLC for current date

		Args:
			bttlist: bttlist data for first of each month.

		Return:
			Merge data of OHLC and bttlist.
		"""
		# print("today_date\n",today_date)
		# today = datetime.datetime.combine(curr_date,datetime.datetime.min.time())
		today_date = curr_date.strftime("%Y-%m-%d")
		
		sql = 'select * from public."OHLC" where "Date" = \''+today_date+'\' ;'
		ohlc_company = sqlio.read_sql_query(sql, conn)

		bttlist = pd.merge(bttlist, ohlc_company, on=['CompanyCode'], how='left')
		
		bttlist.rename(columns={'NSECode_x': 'NSECode','ISIN_x': 'ISIN', 'BSECode_x':'BSECode'}, inplace=True)
		columns_to_remove = ["NSECode_y","BSECode_y", "ISIN_y", "Company"]
		# bttlist = bttlist.drop(columns_to_remove, axis=1)
  
		# bttlist.rename(columns={'CompanyCode_x': 'CompanyCode','ISIN_x': 'ISIN','BSECode_x' : 'BSECode'}, inplace=True)
		# columns_to_remove = [ "BSECode_y","ISIN_y", "Company"]
		bttlist = bttlist.drop(columns_to_remove, axis=1)
		bttlist.Date.fillna(today_date, inplace=True)
		# print(bttlist.columns)
		return bttlist
  
		# columns = ['ISIN', 'NSECode', 'BSECode', 'CompanyName', 'CompanyCode', '52W High',
		# '52W Low', '52W High Date', '52W Low Date', '52W NewHigh',
		# '90D NewHigh', '30D NewHigh', '52W NewLow', '90D NewLow', '30D NewLow',
		# 'RR1', 'RR5', 'RR10', 'RR30', 'RR60', 'Change52W', 'Change90',
		# 'Change30', 'RR52W', 'RR90', 'RS52W', 'RS90', 'RS30', 'CombinedRS',
		# 'RR30_Replaced', 'RR60_Replaced', 'RR90_Replaced', 'RR52W_Replaced',
		# 'Off-High', 'Off-Low', 'Open', 'High', 'Low', 'Close', 'Date', 'Value',
		# 'Volume']
		# # print(len(columns))
		# merge_df = pd.DataFrame(columns=columns)
		# # print(type(merge_df))
		# # merge_row = pd.DataFrame()
		# for index, row in bttlist.iterrows():
		# 	# print(row['CompanyCode'])
		# 	coco = row['CompanyCode']
		# 	bse = row['BSECode']
		# 	nse = row['NSECode']
		# 	row_df = pd.DataFrame(row).transpose()
		# 	# print(row)
		# 	# print(type(row))

		# 	coco_ohlc = ohlc_company[ohlc_company['CompanyCode'] == coco]
		# 	# print(coco_ohlc)
		# 	nse_ohlc = ohlc_company[ohlc_company['NSECode'] == nse]
		# 	# print(nse_ohlc)
		# 	bse_ohlc = ohlc_company[ohlc_company['BSECode'] == bse]
		
		
		# 	# break
		# 	if not coco_ohlc.empty:
		# 		merge_row = pd.merge(row_df, coco_ohlc, on=['CompanyCode'], how='left')
		# 		merge_row.rename(columns={'CompanyCode_x': 'CompanyCode','ISIN_x': 'ISIN','NSECode_x' : 'NSECode', 'BSECode_x':'BSECode'}, inplace=True)
		# 		columns_to_remove = [ "NSECode_y","ISIN_y", "Company", "BSECode_y"]
		# 		merge_row = merge_row.drop(columns_to_remove, axis=1)
		# 		merge_df = pd.concat([merge_df, merge_row], ignore_index=True)
		# 	elif not nse_ohlc.empty:
		# 		merge_row = pd.merge(row_df, nse_ohlc, on=['NSECode'], how='left')
		# 		merge_row.rename(columns={'CompanyCode_x': 'CompanyCode','ISIN_x': 'ISIN', 'BSECode_x':'BSECode'}, inplace=True)
		# 		columns_to_remove = [ "CompanyCode_y","ISIN_y", "Company", "BSECode_y"]
		# 		merge_row = merge_row.drop(columns_to_remove, axis=1)
		# 		merge_df = pd.concat([merge_df, merge_row], ignore_index=True)

		# 	elif not bse_ohlc.empty:
		# 		merge_row = pd.merge(row_df, bse_ohlc, on=['BSECode'], how='left')
		# 		merge_row.rename(columns={'CompanyCode_x': 'CompanyCode','ISIN_x': 'ISIN', 'NSECode_x':'NSECode'}, inplace=True)
		# 		columns_to_remove = [ "CompanyCode_y","ISIN_y", "Company", "NSECode_y"]
		# 		merge_row = merge_row.drop(columns_to_remove, axis=1)
		# 		merge_df = pd.concat([merge_df, merge_row], ignore_index=True)
  
		# return merge_df



	def fetch_ohlc_prs_history(self, bttlist, conn):
		""" Fetching the history for OHLC Data

		Args:
			bttlist: data of BTTList ,

		Return:
			fetch the data from OHLC for current date,
			and merge it with data of  bttlist 
			to fetch the history
		"""

		sql = 'select * from public."OHLC" where "Date" = \''+today_date+'\';'
		ohlc_company = sqlio.read_sql_query(sql, conn)

		bttlist = pd.merge(bttlist, ohlc_company, left_on='CompanyCode', right_on='CompanyCode', how='left')
		bttlist.rename(columns={'NSECode_x': 'NSECode','BSECode_x': 'BSECode', 'ISIN_x': 'ISIN'}, inplace=True)
		columns_to_remove = ["ISIN_y", "BSECode_y", "Company"]
		bttlist = bttlist.drop(columns_to_remove, axis=1)
		bttlist.Date.fillna(today_date, inplace=True)

		# print("fetch_ohlc_prs_history\n",bttlist)

		return bttlist



	def fetch_highlow_prs(self, curr_date, bttlist, conn):
		""" Calculating the value of NewLow and NewHigh

		Args:
			bttlist: merge data of OHLC and BTTList.
		
		Return:
			The "NewLow" and "NewHigh" for the 52 week |90 days | 30 days
		 """
		today = curr_date
		today = datetime.datetime.combine(today,datetime.datetime.min.time())
		today_date = curr_date.strftime("%Y-%m-%d")
		month_back = (curr_date+datetime.timedelta(-30)).strftime("%Y-%m-%d")
		year_back = (curr_date+datetime.timedelta(-366)).strftime("%Y-%m-%d")

		sql = 'select * from public."OHLC" where "Date" < \'' + \
			today_date+'\' and "Date">=\''+year_back+'\';'
		# print(sql)
		ohlc_full = sqlio.read_sql_query(sql, conn)

		###############################################
		# 52 Week High
		###############################################

		idxs = ohlc_full.groupby(['CompanyCode'], as_index=True)['High'].idxmax()
		ohlc_full_high_comcode = ohlc_full.loc[idxs].copy()
		idxs = ohlc_full.groupby(['NSECode'], as_index=True)['High'].idxmax()
		ohlc_full_high_nse = ohlc_full.loc[idxs].copy()

		# '''
		for index, row in bttlist.iterrows():

			temp_high_nse = ohlc_full_high_nse.loc[(
			    ohlc_full_high_nse['NSECode'] == row['NSECode']), 'High']
			temp_date_nse = ohlc_full_high_nse.loc[(
			    ohlc_full_high_nse['NSECode'] == row['NSECode']), 'Date']

			temp_high_comcode = ohlc_full_high_comcode.loc[(
			    ohlc_full_high_comcode['CompanyCode'] == row['CompanyCode']), 'High']
			temp_date_comcode = ohlc_full_high_comcode.loc[(
			    ohlc_full_high_comcode['CompanyCode'] == row['CompanyCode']), 'Date']

			if not (temp_high_comcode.empty):
				high = temp_high_comcode.item()
				date = temp_date_comcode.item()

			else:
				if not (temp_high_nse.empty):
					high = temp_high_nse.item()
					date = temp_date_nse.item()
				else:
					high = np.nan
					date = np.nan

			bttlist.loc[index, '52W High'] = high if high > row['High'] else row['High']
			bttlist.loc[index, '52W High Date'] = date if high > row['High'] else row['Date']

			# print("bttlist for 52 high \n",bttlist)

		# '''

		###############################################
		# 52 Week Low
		###############################################
		idxs = ohlc_full.groupby(['CompanyCode'], as_index=True)['Low'].idxmin()
		ohlc_full_low_comcode = ohlc_full.loc[idxs].copy()
		idxs = ohlc_full.groupby(['NSECode'], as_index=True)['Low'].idxmin()
		ohlc_full_low_nse = ohlc_full.loc[idxs].copy()
		
		# '''
		for index, row in bttlist.iterrows():

			temp_low_nse = ohlc_full_low_nse.loc[(
			    ohlc_full_low_nse['NSECode'] == row['NSECode']), 'Low']
			temp_date_nse = ohlc_full_low_nse.loc[(
			    ohlc_full_low_nse['NSECode'] == row['NSECode']), 'Date']

			temp_low_comcode = ohlc_full_low_comcode.loc[(
			    ohlc_full_low_comcode['CompanyCode'] == row['CompanyCode']), 'Low']
			temp_date_comcode = ohlc_full_low_comcode.loc[(
			    ohlc_full_low_comcode['CompanyCode'] == row['CompanyCode']), 'Date']

			if not(temp_low_comcode.empty):

				low = temp_low_comcode.item()
				date = temp_date_comcode.item()

			else:
					if not(temp_low_nse.empty):
						low = temp_low_nse.item()
						date = temp_date_nse.item()
					else:
						low = np.nan
						date = np.nan

			bttlist.loc[index, '52W Low'] = low if low < row['Low'] else row['Low']
			bttlist.loc[index, '52W Low Date'] = date if low < row['Low'] else row['Date']  

			# print("bttlist 52W new low\n", bttlist )

		# '''

		###############################################
		# 52 Week NewHigh
		###############################################
		# set date for 3 month back

		threemonth_back = (today+datetime.timedelta(-90))
		ohlc_52 = ohlc_full.copy()
		ohlc_52["Date"] = pd.to_datetime(ohlc_52["Date"])

		mask = (ohlc_52['Date'] < today) & (ohlc_52['Date'] >= year_back)
		ohlc_52 = ohlc_52.loc[mask]
		ohlc_52.sort_values(by=['Date'], inplace=True, ascending=False)

		idxs = ohlc_full.groupby(['CompanyCode'], as_index=True)['High'].idxmax()
		ohlc_full_high_comcode = ohlc_52.loc[idxs].copy()
		idxs = ohlc_full.groupby(['NSECode'], as_index=True)['High'].idxmax()
		ohlc_full_high_nse = ohlc_52.loc[idxs].copy()

		# '''
		for index, row in bttlist.iterrows():

			temp_high_nse = ohlc_full_high_nse.loc[(ohlc_full_high_nse['NSECode']==row['NSECode']), 'High']
			temp_high_comcode =	ohlc_full_high_comcode.loc[(ohlc_full_high_comcode['CompanyCode'] == row['CompanyCode']), 'High']


			if not(temp_high_comcode.empty):

				W52High = temp_high_comcode.item()

			else:
				if not (temp_high_nse.empty):
					W52High = temp_high_nse.item()
				else:
					W52High = np.nan

			bttlist.loc[index, '52W NewHigh'] = "1" if row["High"] > W52High else "0"

			# print("bttlist 52W new high\n",bttlist)

		# '''

		###############################################
		# 52 Week NewLow
		###############################################
		idxs = ohlc_full.groupby(['CompanyCode'], as_index=True)['Low'].idxmin()
		ohlc_full_low_comcode = ohlc_full.loc[idxs].copy()

		idxs = ohlc_full.groupby(['NSECode'], as_index=True)['Low'].idxmin()
		ohlc_full_low_nse = ohlc_full.loc[idxs].copy()

		# '''
		for index, row in bttlist.iterrows():

			temp_low_nse = ohlc_full_low_nse.loc[(ohlc_full_low_nse['NSECode']==row['NSECode']), 'Low']
			temp_low_comcode =	ohlc_full_low_comcode.loc[(ohlc_full_low_comcode['CompanyCode'] == row['CompanyCode']), 'Low']

			if not(temp_low_comcode.empty):

				W52Low = temp_low_comcode.item()

			else:
				if not(temp_low_nse.empty):
					W52Low = temp_low_nse.item()
				else:
					W52Low = np.nan

			bttlist.loc[index, '52W NewLow'] = "1" if row["Low"] < W52Low else "0"

			# print("52 W New Low\n",bttlist)

		###############################################
		# 65(90) Day NewHigh
		###############################################


		threemonth_back = (today+datetime.timedelta(-90))
		ohlc_65 = ohlc_full.copy()
		ohlc_65["Date"] = pd.to_datetime(ohlc_65["Date"])

		mask = (ohlc_65['Date'] < today) & (ohlc_65['Date'] >= threemonth_back)
		ohlc_65 = ohlc_65.loc[mask]
		ohlc_65.sort_values(by=['Date'], inplace=True, ascending=False)

		# print(ohlc_65)

		idxs = ohlc_65.groupby(['CompanyCode'], as_index=True)['High'].idxmax()
		ohlc_65_high_comcode = ohlc_65.loc[idxs].copy()

		idxs = ohlc_65.groupby(['NSECode'], as_index=True)['High'].idxmax()
		ohlc_65_high_nse = ohlc_65.loc[idxs].copy()


		# '''
		for index, row in bttlist.iterrows():

			temp_high_nse = ohlc_65_high_nse.loc[(ohlc_65_high_nse['NSECode']==row['NSECode']), 'High']
			temp_high_comcode =	ohlc_65_high_comcode.loc[(ohlc_65_high_comcode['CompanyCode'] == row['CompanyCode']), 'High']

			# print("temp high comcode\n",temp_high_comcode)

			if not(temp_high_comcode.empty):

				T_High = temp_high_comcode.item()

			else:
				if not(temp_high_nse.empty):
					T_High = temp_high_nse.item()
				else:
					T_High = np.nan

			bttlist.loc[index, '90D NewHigh'] = "1" if row["High"] > T_High else "0"

			# print("65(90) Day NewHigh\n",bttlist)

		# '''

		###############################################
		# 65(90) Day NewLow
		###############################################
		idxs = ohlc_65.groupby(['CompanyCode'], as_index=True)['Low'].idxmin()
		ohlc_65D_low_comcode = ohlc_full.loc[idxs].copy()
		
		idxs = ohlc_65.groupby(['NSECode'], as_index=True)['Low'].idxmin()
		ohlc_65D_low_nse = ohlc_full.loc[idxs].copy()

		# '''
		for index, row in bttlist.iterrows():

			temp_low_nse = ohlc_65D_low_nse.loc[(ohlc_65D_low_nse['NSECode']==row['NSECode']), 'Low']
			temp_low_comcode =	ohlc_65D_low_comcode.loc[(ohlc_65D_low_comcode['CompanyCode'] == row['CompanyCode']), 'Low']

			if not(temp_low_comcode.empty):

				T_Low = temp_low_comcode.item()

			else:
				if not(temp_low_nse.empty):
					T_Low = temp_low_nse.item()
				else:
					T_Low = np.nan

			bttlist.loc[index, '90D NewLow'] = "1" if row["Low"] < T_Low else "0"

			# print("65(90) Day NewLow\n",bttlist)

		# '''

		# '''

		###############################################
		# 20 Day NewHigh
		###############################################

		# set the date for 30 days back
		month_back = (today+datetime.timedelta(-30))
		ohlc_20 = ohlc_full.copy()
		ohlc_20["Date"] = pd.to_datetime(ohlc_20["Date"])

		mask = ((ohlc_20['Date'] < today) & (ohlc_20['Date'] >= month_back))
		ohlc_20 = ohlc_20.loc[mask]
		ohlc_20.sort_values(by=['Date'], inplace=True, ascending=False)

		# print(ohlc_20)

		idxs = ohlc_20.groupby(['CompanyCode'], as_index=True)['High'].idxmax()
		ohlc_20_high_comcode = ohlc_20.loc[idxs].copy()

		idxs = ohlc_20.groupby(['NSECode'], as_index=True)['High'].idxmax()
		ohlc_20_high_nse = ohlc_20.loc[idxs].copy()

		# print(ohlc_full_high_nse)
		# print(ohlc_full_high_bse)

		# '''
		for index, row in bttlist.iterrows():

			T_High_nse = ohlc_20_high_nse.loc[(ohlc_20_high_nse['NSECode']==row['NSECode']), 'High']
			T_High_comcode = ohlc_20_high_comcode.loc[(ohlc_20_high_comcode['CompanyCode'] == row['CompanyCode']), 'High']

			if not(T_High_comcode.empty):

				T_High = T_High_comcode.item()

			else:
				if not(T_High_nse.empty):
					T_High = T_High_nse.item()
				else:
					T_High = np.nan

			bttlist.loc[index, '30D NewHigh'] = "1" if row["High"] > T_High else "0"

			# print("20 Day NewHigh\n",bttlist)

		# '''

		###############################################
		# 20 Day NewLow
		###############################################
		idxs = ohlc_20.groupby(['CompanyCode'], as_index=True)['Low'].idxmin()
		ohlc_20D_low_comcode = ohlc_full.loc[idxs].copy()

		idxs = ohlc_20.groupby(['NSECode'], as_index=True)['Low'].idxmin()
		ohlc_20D_low_nse = ohlc_full.loc[idxs].copy()

		# print(ohlc_20D_low_nse)

		# '''
		for index, row in bttlist.iterrows():

			T_Low_nse = ohlc_20D_low_nse.loc[(ohlc_20D_low_nse['NSECode']==row['NSECode']), 'Low']
			temp_low_comcode = ohlc_20D_low_comcode.loc[(ohlc_20D_low_comcode['CompanyCode'] == row['CompanyCode']), 'Low']

			if not(temp_low_comcode.empty):

				T_Low = temp_low_comcode.item()

			else:
				if not(T_Low_nse.empty):
					T_Low = T_Low_nse.item()
				else:
					temp_low_comcode = np.nan

			bttlist.loc[index, '30D NewLow'] = "1" if row["Low"] < T_Low else "0"

			# print("20 Day NewLow\n",T_Low_nse)

		# '''

		# print("bttlist\n",bttlist)

		return bttlist



	def value_average(self,curr_date,bttlist, conn):
		"""Calculating the Value Average 
			
			Args:
				bttlist: bttlist data of NewLow and NewHigh,
			
			Operation:
				fetch the data  from OHLC data for eightyday back data
				taking the head value of 50 from data and calculating the average value
				Value Average = 50D Vol / 50
			
			Return:
				Value Average of head 50 data based on NSECode and ComapnyCode.
		"""
		today = datetime.datetime.combine(curr_date,datetime.datetime.min.time())
		today_date=curr_date.strftime("%Y-%m-%d")
		eightyday_back = (curr_date+datetime.timedelta(-80)).strftime("%Y-%m-%d")

		sql = 'select * from public."OHLC" where "Date" <= \''+ \
			today_date+'\' and "Date">=\''+eightyday_back+'\';'
		ohlc_full = sqlio.read_sql_query(sql, conn)

		ohlc_full['Date'] = pd.to_datetime(ohlc_full['Date'])
		ohlc_full.sort_values(by=['Date'], inplace=True, ascending=False)

		for index, row in bttlist.iterrows():

			ohlc_full_val_nse = ohlc_full[ohlc_full['NSECode']==row['NSECode']]
			ohlc_full_val_comcode = ohlc_full[ohlc_full['CompanyCode'] == row['CompanyCode']]

			nse_idx = len(ohlc_full_val_comcode.index)
			nse_idx = len(ohlc_full_val_nse.index)

			if not((ohlc_full_val_comcode.empty) or ohlc_full_val_comcode['Value'].isnull().all() ):

				if(nse_idx >= 50):
					value_average = ohlc_full_val_comcode['Value'].head(50).agg('sum').item() / 50
				elif nse_idx > 0:
					value_average = ohlc_full_val_comcode['Value'].head(nse_idx).agg('sum').item() / nse_idx
				else:
					value_average = np.nan

			elif not( (ohlc_full_val_nse.empty) or ohlc_full_val_nse['Value'].isnull().all() ):

				if(nse_idx >= 50):
					value_average = ohlc_full_val_nse['Value'].head(50).agg('sum').item() / 50
				elif nse_idx > 0:
					value_average = ohlc_full_val_nse['Value'].head(nse_idx).agg('sum').item() / nse_idx
				else:
					value_average = np.nan

			else:
				value_average = np.nan


			bttlist.loc[index, 'Value Average'] = value_average
		# print("Value Average\n",bttlist)

		return bttlist



	def getclosestdate_ohlc(self,companylist,s_date):
		"""Fetching Closest date

		Args:
			companylist: data of rr and ohlc,
			s_date = back date for diffrent time interval
		
		Return:
			close value for given s_date, if null returns close 
			value for closest date according to the logic below.  
		"""

		# Workaround to remove timestamp and compare with puredate values transformed into datetime
		s_date = s_date.date()
		s_date = pd.Timestamp(s_date)

		mask_0 = companylist['Date'] == (s_date)
		mask_1 = companylist['Date'] == (s_date+datetime.timedelta(1))
		mask_b_1 = companylist['Date'] == (s_date+datetime.timedelta(-1))
		mask_b_2 = companylist['Date'] == (s_date+datetime.timedelta(-2))
		mask_b_3 = companylist['Date'] == (s_date+datetime.timedelta(-3))

		idx = len(companylist.index)

		if not (companylist.loc[mask_0].empty):
			# print(companylist[companylist['Date']==date]["Close"])
			return companylist.loc[mask_0, "Close"].iloc[0]
		else:
			if not (companylist.loc[mask_b_1].empty):
				# print("Not Empty at -1")
				return companylist.loc[mask_b_1, "Close"].iloc[0]
			else:
				if not (companylist.loc[mask_b_2].empty):
					# print("Not Empty at -2")
					return companylist.loc[mask_b_2, "Close"].iloc[0]
				else:
					if not (companylist[mask_1].empty):
						# print("Not Empty at 1")
						return companylist.loc[mask_1, "Close"].iloc[0]
					else:
						if not (companylist.loc[mask_b_3].empty):
							# print("Not Empty at -3")
							return companylist.loc[mask_b_3, "Close"].iloc[0]
						else:
							# print("Empty all all 4 indicies")
							return None



	def prs_rr(self,curr_date, bttlist, conn):
		"""Calculating Relative Rate of Change

			Args:
				bttlist: bttlist data of Value Average,

			Operation:
				Fetch the data from OHLC for yearodd back(-370 days),
				Calculating the Rate of Change (RR) for the 30days | 60days| 90days| 365days
			 	rate of change = current - previous / previous * 100,

			Return:
				Relative Rate of change.
			"""

		today = curr_date
		today = datetime.datetime.combine(today,datetime.datetime.min.time())
		today_date = curr_date.strftime("%Y-%m-%d")
		yearodd_back = (today+datetime.timedelta(-370)).strftime("%Y-%m-%d")
		# Extract the year component from today's date
		year = today.year
		# Check if today's year is a leap year
		if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
			# Increment Fyear_back by one day
			Fyear_back = today - pd.DateOffset(days=365)
		else:
			Fyear_back = today - pd.DateOffset(days=364)

		print("Fyear_back:", Fyear_back)

		F90days_back = (today+datetime.timedelta(-90))
		F60days_back = (today+datetime.timedelta(-60))
		F30days_back = (today+datetime.timedelta(-30))

		sql = 'select * from public."OHLC" where "Date" < \''+ \
			today_date+'\' and "Date">=\''+yearodd_back+'\';'
		# print(sql)
		ohlc_full = sqlio.read_sql_query(sql, conn)
		ohlc_full['Date'] = pd.to_datetime(ohlc_full['Date'], format='%Y.%m.%d')
		ohlc_full.sort_values(by=['Date'], inplace=True, ascending=False)

		for index, row in bttlist.iterrows():

			ohlc_compcode_RR = ohlc_full[ohlc_full['CompanyCode'] == row['CompanyCode']]
			ohlc_nse_RR = ohlc_full[ohlc_full['NSECode'] == row['NSECode']]		
			ohlc_compcode_RR_copy = ohlc_compcode_RR.copy()
			ohlc_nse_RR_copy = ohlc_nse_RR.copy()

			RR30_R = '0'
			RR60_R = '0'
			RR90_R = '0'
			RR52W_R = '0'


			if(np.isfinite(row['CompanyCode'])):

				# print("For: ", row['NSECode'])
				idx = len(ohlc_compcode_RR.index)
				idx = idx - 1
				# converting the datatype of row into int
				# comp_fixed_code = int(row['CompanyCode'])

				# calling the getclosestdate_ohlc
				thirty_close = self.getclosestdate_ohlc(ohlc_compcode_RR, F30days_back)
				sixy_close = self.getclosestdate_ohlc(ohlc_compcode_RR, F60days_back)
				ninty_close = self.getclosestdate_ohlc(ohlc_compcode_RR, F90days_back)
				year_close = self.getclosestdate_ohlc(ohlc_compcode_RR, Fyear_back)

				# ohlc_compcode_RR.index = pd.DatetimeIndex(ohlc_compcode_RR["Date"])
					# fetching the
				if (thirty_close is None and idx >= 0):
					date_index = abs(ohlc_compcode_RR_copy['Date'] - F30days_back).idxmin()
					thirty_close = ohlc_compcode_RR_copy.loc[date_index]["Close"].item()
					RR30_R = '1'

				if (sixy_close is None and idx >= 0):
					date_index = abs(ohlc_compcode_RR_copy['Date'] - F60days_back).idxmin()
					sixy_close = ohlc_compcode_RR_copy.loc[date_index]["Close"].item()
					RR60_R = '1'

				if (ninty_close is None and idx >= 0):
					date_index = abs(ohlc_compcode_RR_copy['Date'] - F90days_back).idxmin()
					ninty_close = ohlc_compcode_RR_copy.loc[date_index]["Close"].item()
					RR90_R = '1'

				if (year_close is None and idx >= 0):
					date_index = abs(ohlc_compcode_RR_copy['Date'] - Fyear_back).idxmin()
					year_close = ohlc_compcode_RR_copy.loc[date_index]["Close"].item()
					RR52W_R = '1'

				# calculating the Rate of Change (RR)
				RR1 = ((row["Close"]-ohlc_compcode_RR.iloc[0]["Close"])/ohlc_compcode_RR.iloc[0]["Close"])*100 if idx >= 0 else np.nan
				RR5 = ((row["Close"]-ohlc_compcode_RR.iloc[4]["Close"])/ \
						ohlc_compcode_RR.iloc[4]["Close"])*100 if idx >= 4 else np.nan
				RR10 = ((row["Close"]-ohlc_compcode_RR.iloc[9]["Close"])/ \
						ohlc_compcode_RR.iloc[9]["Close"])*100 if idx >= 9 else np.nan
				RR30 = ((row["Close"]-thirty_close)/thirty_close)* \
						100 if thirty_close is not None else np.nan
				RR60 = ((row["Close"]-sixy_close)/sixy_close)* \
						100 if sixy_close is not None else np.nan
				RR90 = ((row["Close"]-ninty_close)/ninty_close)* \
						100 if ninty_close is not None else np.nan
				RR52W = ((row["Close"]-year_close)/year_close)* \
							100 if year_close is not None else np.nan
				Change30 = (row["Close"]-thirty_close) if thirty_close is not None else np.nan
				Change90 = (row["Close"]-ninty_close) if sixy_close is not None else np.nan
				Change52W = (row["Close"]-year_close) if year_close is not None else np.nan

			else:

				# fetching the RR values for the company's where CompanyCode in none
				# Calculating RR based on NSECode

				idx = len(ohlc_nse_RR.index)
				idx = idx - 1
				# fetch the date for 30D back | 60D back | 90D back | 52 week back
				thirty_close = self.getclosestdate_ohlc(ohlc_nse_RR, F30days_back)
				sixy_close = self.getclosestdate_ohlc(ohlc_nse_RR, F60days_back)
				ninty_close = self.getclosestdate_ohlc(ohlc_nse_RR, F90days_back)
				year_close = self.getclosestdate_ohlc(ohlc_nse_RR, Fyear_back)

				# ohlc_nse_RR.index = pd.DatetimeIndex(ohlc_nse_RR["Date"])

				# fetch the index which hold highest value for various time period
				if (thirty_close is None and idx >= 0):
					date_index = abs(ohlc_nse_RR_copy['Date'] - F30days_back).idxmin()
					thirty_close = ohlc_nse_RR_copy.loc[date_index]["Close"].item()
					RR30_R = '1'

				if (sixy_close is None and idx >= 0):
					date_index = abs(ohlc_nse_RR_copy['Date'] - F60days_back).idxmin()
					sixy_close = ohlc_nse_RR_copy.loc[date_index]["Close"].item()
					RR60_R = '1'

				if (ninty_close is None and idx >= 0):
					date_index = abs(ohlc_nse_RR_copy['Date'] - F90days_back).idxmin()
					ninty_close = ohlc_nse_RR_copy.loc[date_index]["Close"].item()
					RR90_R = '1'

				if (year_close is None and idx >= 0):
					date_index = abs(ohlc_nse_RR_copy['Date'] - Fyear_back).idxmin()
					year_close = ohlc_nse_RR_copy.loc[date_index]["Close"].item()
					RR52W_R = '1'
				
				# calculate Rate of Change
				RR1 = ((row["Close"]-ohlc_nse_RR.iloc[0]["Close"])/ohlc_nse_RR.iloc[0]["Close"])*100 if idx >= 0 else np.nan
				RR5 = ((row["Close"]-ohlc_nse_RR.iloc[4]["Close"])/ohlc_nse_RR.iloc[4]["Close"])*100 if idx >= 4 else np.nan
				RR10 = ((row["Close"]-ohlc_nse_RR.iloc[9]["Close"])/ohlc_nse_RR.iloc[9]["Close"])*100 if idx >= 9 else np.nan
				RR30 = ((row["Close"]-thirty_close)/thirty_close)*100 if thirty_close is not None else np.nan
				RR60 = ((row["Close"]-sixy_close)/sixy_close)*100 if sixy_close is not None else np.nan
				RR90 = ((row["Close"]-ninty_close)/ninty_close)*100 if ninty_close is not None else np.nan
				RR52W = ((row["Close"]-year_close)/year_close)*100 if year_close is not None else np.nan
				Change30 = (row["Close"]-thirty_close) if thirty_close is not None else np.nan
				Change90 = (row["Close"]-ninty_close) if ninty_close is not None else np.nan
				Change52W = (row["Close"]-year_close) if year_close is not None else np.nan

			OffHigh = ((row["52W High"]-row["Close"])/row["52W High"])*100 if year_close is not None else np.nan
			OffLow = ((row["Close"]-row["52W Low"])/row["52W Low"])*100 if year_close is not None else np.nan

			bttlist.loc[index, 'RR1'] = RR1
			bttlist.loc[index, 'RR5'] = RR5
			bttlist.loc[index, 'RR10'] = RR10
			bttlist.loc[index, 'RR30'] = RR30
			bttlist.loc[index, 'RR30_Replaced'] = RR30_R		
			bttlist.loc[index, 'RR60'] = RR60
			bttlist.loc[index, 'RR60_Replaced'] = RR60_R				
			bttlist.loc[index, 'RR90'] = RR90
			bttlist.loc[index, 'RR90_Replaced'] = RR90_R				
			bttlist.loc[index, 'RR52W'] = RR52W
			bttlist.loc[index, 'RR52W_Replaced'] = RR52W_R				
			bttlist.loc[index, 'Change30'] = Change30
			bttlist.loc[index, 'Change90'] = Change90
			bttlist.loc[index, 'Change52W'] = Change52W
			bttlist.loc[index, 'Off-High'] = OffHigh
			bttlist.loc[index, 'Off-Low'] = OffLow

		bttlist['RS30'] = bttlist['RR30'].rank(ascending=False)
		bttlist['RS30'] = (
			(len(bttlist.index)-bttlist['RS30']+1)/len(bttlist.index))*100

		bttlist['RS90'] = bttlist['RR90'].rank(ascending=False)
		bttlist['RS90'] = (
			(len(bttlist.index)-bttlist['RS90']+1)/len(bttlist.index))*100

		bttlist['RS52W'] = bttlist['RR52W'].rank(ascending=False)
		bttlist['RS52W'] = (
			(len(bttlist.index)-bttlist['RS52W']+1)/len(bttlist.index))*100

		bttlist['CombinedRS'] = ((30/100)*bttlist['RS30']) + ((35/100)*bttlist['RS90']) + ((35/100)*bttlist['RS52W'])

		return bttlist



	def merge_pe_prs(self,bttlist, conn, date):
		""" Mergeing the data from bttlist and PE table,

			Args:
				bttlist = bttlist data of Relative Rate of change,

			Operation:
				Fetch the from PE Table Current date,

			Return:
				Merge Data of PE and bttlist. 
		"""

		pe_sql = 'SELECT * FROM public."PE" where "GenDate" = \''+str(date)+'\';'
		pe = sqlio.read_sql_query(pe_sql, con= conn)

		pe = pe[['CompanyCode', 'Market Cap Value', 'Market Cap Class',
			'Market Cap Rank', 'PE', 'PE High', 'PE High Date', 'PE Low', 'PE Low Date']]

		bttlist = pd.merge(bttlist, pe, left_on= 'CompanyCode', right_on = 'CompanyCode', how = 'left')

		# print("Merge pe and prs",bttlist)

		return bttlist



	def export_table(self,name, table):
		"""Exporting the CSV files"""

		exportfilename = name+"_export.csv"
		exportfile = open(exportfilename, "w")

		table.to_csv(exportfile, header=True, index=False,float_format="%.2f", lineterminator='\r')
		exportfile.close()

		# os.remove("exportfilename")
		# print("CSV file Exported",exportfile)


	def insert_nhnl(self,bttlist, conn):
		"""Insert NHNL data into Database,

			Args:
				bttlist: bttlist data of NewLow and NewHigh,

			Operation:
				Calculate NewLowNewHigh (NHNL) of 30D | 90D | 52W,
				NHNL = NewHigh - NewLow,
				Export the data NHNL Table.
		"""

		cur = conn.cursor()

		nhnl = pd.DataFrame(index=[0], columns=['Date','30DNH', '30DNL', '30DNHNL','90DNH', '90DNL', '90DNHNL','52WNH', '52WNL', '52WNHNL'])

		nhnl.iloc[0]['Date'] = today_date
		nhnl.iloc[0]['30DNH'] = len(bttlist[bttlist['30D NewHigh'] == '1'].index)
		nhnl.iloc[0]['30DNL'] = len(bttlist[bttlist['30D NewLow'] == '1'].index)
		nhnl.iloc[0]['30DNHNL'] = nhnl['30DNH'].item() - nhnl['30DNL'].item()

		nhnl.iloc[0]['90DNH'] = len(bttlist[bttlist['90D NewHigh'] == '1'].index)
		nhnl.iloc[0]['90DNL'] = len(bttlist[bttlist['90D NewLow'] == '1'].index)
		nhnl.iloc[0]['90DNHNL'] = nhnl['90DNH'].item() - nhnl['90DNL'].item()

		nhnl.iloc[0]['52WNH'] = len(bttlist[bttlist['52W NewHigh'] == '1'].index)
		nhnl.iloc[0]['52WNL'] = len(bttlist[bttlist['52W NewLow'] == '1'].index)
		nhnl.iloc[0]['52WNHNL'] = nhnl['52WNH'].item() - nhnl['52WNL'].item()

		self.export_table("NHNL", nhnl)

		copy_sql = """
			COPY "Reports"."NewHighNewLow" FROM stdin WITH CSV HEADER
			DELIMITER as ','
			"""
		with open("NHNL_export.csv", 'r') as f:
			cur.copy_expert(sql=copy_sql, file=f)
			conn.commit()

		os.remove("NHNL_export.csv")
		# print("NHNL file",nhnl)



	def insert_prs(self,bttlist, conn):
		"""Insert PRS data into database,

			Args:
				bttlist: PE and bttlist merge data,

			Operation:
				Export the data into "PRS_export.csv file"
				and insert data into PRS table.
		"""

		cur = conn.cursor()

		bttlist.Volume.fillna(0, inplace=True)
		bttlist.Volume = bttlist.Volume.astype(int)

		bttlist["BSECode"].fillna(-1, inplace=True)
		bttlist = bttlist.astype({"BSECode": int})
		bttlist = bttlist.astype({"BSECode": str})
		bttlist["BSECode"] = bttlist["BSECode"].replace('-1', np.nan)

		bttlist["Market Cap Rank"] = bttlist["Market Cap Rank"].fillna(-1)
		bttlist = bttlist.astype({"Market Cap Rank": int})
		bttlist = bttlist.astype({"Market Cap Rank": str})
		bttlist["Market Cap Rank"] = bttlist["Market Cap Rank"].replace('-1', np.nan)

		exportfilename = "PRS_export.csv"
		exportfile = open(exportfilename, "w")
		bttlist.to_csv(exportfile, header=True, index=False,float_format="%.2f", lineterminator='\r')
		exportfile.close()

		copy_sql = """
			COPY "Reports"."PRS" FROM stdin WITH CSV HEADER
			DELIMITER as ','
			"""

		with open(exportfilename, 'r') as f:
			cur.copy_expert(sql=copy_sql, file=f)
			conn.commit()
			f.close()
		os.remove(exportfilename)
		# print("PRS file Inserted", exportfile)



	def history_range_insert(self,Dates):
		"""Opening the history into the txt file named as "PRS_History_Log.txt" 
		 	and fetching the range for the history.
		"""

		stdout_backup = sys.stdout
		print('\n\n Starting History Insert for Date Range \n\n')

		for date_in in Dates:

			print('-------------------------------------------------------------------------------')
			print('Starting History PRS Insert for Date: ',date_in.strftime("%Y-%m-%d"))
			sys.stdout = open("PRS_History_Log.txt", "a")
			print('-------------------------------------------------------------------------------')
			print('Starting History PRS Insert for Date: ',date_in.strftime("%Y-%m-%d"))
			self.history_insert(date_in)
			print('-------------------------------------------------------------------------------')
			sys.stdout = stdout_backup
			print('Completed Insert')
			print('-------------------------------------------------------------------------------')



	def history_insert(self,today_in):
		"""Fetch the data of NewLow, NewHigh, 
			value Average, and Rate Of Change(RR),
			and get the history data. 
		"""

		conn = DB_Helper().db_connect()
		cur = conn.cursor()

		self.set_date(today_in)

		print("Fetching BTTList")
		bttlist = self.fetch_btt_prs(today,conn)

		# Creating the column without any data.
		bttlist = pd.concat([bttlist, pd.DataFrame(columns = ['52W High', '52W Low', '52W High Date','52W Low Date', '52W NewHigh', '90D NewHigh',\
																 '30D NewHigh', '52W NewLow', '90D NewLow', '30D NewLow', 'RR1', 'RR5', 'RR10', 'RR30', 'RR60', 'Change52W',\
																'Change90', 'Change30', 'RR52W', 'RR90', 'RS52W', 'RS90', 'RS30', 'CombinedRS', 'RR30_Replaced', 'RR60_Replaced', 'RR90_Replaced',\
																'RR52W_Replaced', 'Off-High', 'Off-Low'])], sort=False)

		print("Fetching OHLC Data")
		bttlist = self.fetch_ohlc_prs_history(bttlist, conn)

		if(bttlist['Close'].isnull().all()):
			print("No OHLC Data for this date")
			raise ValueError('OHLC data not found for date: '+today_in.strftime("%Y-%m-%d"))

		print("Calculating High/Low & Value Avg")
		bttlist = self.fetch_highlow_prs(bttlist, conn)
		bttlist = self.value_average(bttlist, conn)

		print("Calculating Relative Rate of change")
		bttlist = self.prs_rr(bttlist, conn)

		bttlist.round(2)

		bttlist = bttlist[['CompanyName', 'NSECode', 'BSECode', 'Open', 'High', 'Low', 'Close', 'Volume', 'Value', '52W High', '52W Low', '52W High Date',\
							'52W Low Date', '52W NewHigh', '90D NewHigh', '30D NewHigh', '52W NewLow', '90D NewLow', '30D NewLow', 'RR1', 'RR5', 'RR10', 'RR30', 'RR60', 'Change52W',\
							'Change90', 'Change30', 'RR52W', 'RR90', 'RS52W', 'RS90', 'RS30', 'CombinedRS', 'ISIN', 'Date', 'RR30_Replaced', 'RR60_Replaced', 'RR90_Replaced',\
							'RR52W_Replaced', 'Off-High', 'Off-Low', 'CompanyCode',  'Value Average']]

		print("Inserting into PRS Table")
		self.insert_prs(bttlist, conn)

		print("Calculating NHNL Totals")
		self.insert_nhnl(bttlist, conn)

		conn.close()

	# get list of stocks from BTT and invoke PRS generation for each stock



	def generate_prs_daily(self, curr_date,conn,cur):
		"""fetching the data of,
		BTTList, OHLC Data, High/Low & Value Avg, 
		Relative Rate of change and data of Merging with Price Earnings,
		to generate PRS for current date.
		"""
		

		self.set_date(curr_date)

		# date = PRS.set_date(self,today_in=None)

		print('-------------------------------------------------------------------------------')
		print("Fetching BTTList for Date: ", curr_date)
		bttlist = self.fetch_btt_prs(curr_date, conn)

		bttlist = pd.concat([bttlist, pd.DataFrame(columns = ['52W High', '52W Low', '52W High Date','52W Low Date', '52W NewHigh', '90D NewHigh',\
																'30D NewHigh', '52W NewLow', '90D NewLow', '30D NewLow', 'RR1', 'RR5', 'RR10', 'RR30', 'RR60', 'Change52W',\
																'Change90', 'Change30', 'RR52W', 'RR90', 'RS52W', 'RS90', 'RS30', 'CombinedRS', 'RR30_Replaced', 'RR60_Replaced', 'RR90_Replaced',\
																'RR52W_Replaced', 'Off-High', 'Off-Low'])], sort=False)

		print("Fetching OHLC Data")
		bttlist = self.fetch_ohlc_prs(curr_date, bttlist, conn)
		print(bttlist.head())

		if(bttlist['Close'].isnull().all()):
			print("No OHLC Data for this date")
			# raise ValueError('OHLC data not found for date:')
			print("there is no OHLC data, NO PRS for", curr_date)


		else:
			print("Calculating High/Low & Value Avg")
			bttlist = self.fetch_highlow_prs(curr_date, bttlist, conn)

			bttlist = self.value_average(curr_date, bttlist, conn)

			print("Calculating Relative Rate of change")
			bttlist = self.prs_rr(curr_date, bttlist, conn)

			print("Merging with Price Earnings")
			bttlist = self.merge_pe_prs(bttlist, conn, curr_date)
			# print("Duplicate \n", bttlist[bttlist.duplicated()])

			bttlist.round(2)

				# creating the column followed by CompanyName
			bttlist = bttlist[['CompanyName', 'NSECode', 'BSECode', 'Open', 'High', 'Low', 'Close', 'Volume', 'Value', '52W High', '52W Low', '52W High Date',\
								'52W Low Date', '52W NewHigh', '90D NewHigh', '30D NewHigh', '52W NewLow', '90D NewLow', '30D NewLow', 'RR1', 'RR5', 'RR10', 'RR30', 'RR60', 'Change52W',\
								'Change90', 'Change30', 'RR52W', 'RR90', 'RS52W', 'RS90', 'RS30', 'CombinedRS', 'ISIN', 'Date', 'RR30_Replaced', 'RR60_Replaced', 'RR90_Replaced',\
								'RR52W_Replaced', 'Off-High', 'Off-Low', 'CompanyCode',  'Value Average', 'Market Cap Value', 'Market Cap Class', 'Market Cap Rank', 'PE', \
								'PE High', 'PE High Date', 'PE Low', 'PE Low Date']]

			print("Inserting into PRS Table")
			self.insert_prs(bttlist, conn)

			print("Calculating NHNL Totals")
			self.insert_nhnl(bttlist, conn)

			print('-------------------------------------------------------------------------------')
		return bttlist

