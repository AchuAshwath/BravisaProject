#Script to compile RatiosMerge list and calculate SMR from it 
import datetime
from lib.btt_list import export_table
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
import utils.date_set as date_set


class SMR:
	""" Generating SMR for current date.
		Fetching the data from BTTList,RatiosBankingVI and RatiosNonBankingVI table
		to calculating the value for SMR Rank,NPM, ROE and sales.
	"""
	def __init__(self):
		pass
	
	def get_closest_quarter(self, target):
		"""Fetch the closest quarter from the current date"""
		
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

	def get_previous_quarter(self, target):
		"""Fetch the previous quarter from the current quarter"""

		curr_qrt = self.get_closest_quarter(target)
		curr_qrt_dec = datetime.date(curr_qrt.year, curr_qrt.month, (curr_qrt.day - 2))
		prev_qrt = self.get_closest_quarter(curr_qrt_dec)
		return prev_qrt

	def get_year_before_quarter(self, target):
		"""Fetch the last year quarter from closest quarter"""
		
		curr_qrt = self.get_closest_quarter(target)
		one_qtr_back = self.get_previous_quarter(curr_qrt)
		two_qtr_back = self.get_previous_quarter(one_qtr_back)
		three_qtr_back = self.get_previous_quarter(two_qtr_back)
		four_qtr_back = self.get_previous_quarter(three_qtr_back)
		return four_qtr_back

	def get_four_years_before_quarter(self, target):
		"""Fetch the quarter of four year back"""
		
		year_back = self.get_year_before_quarter(target)
		two_years_back = self.get_year_before_quarter(year_back)
		three_years_back = self.get_year_before_quarter(two_years_back)
		four_years_back = self.get_year_before_quarter(three_years_back)
		return four_years_back

	def compile_ratios_list_history(self, conn, date):
		""" Compiling ratios list history

		Operation:
			Fetch the data from RatiosBankingVI and
			RatiosNonBankingVI  for YearEnding data
			and concatenate the executed data.

		Return:
			Merge data of RatiosBankingVI, RatiosNonBankingVI
			for generating the history data.
		"""
		
		ratios_banking_list_init = pd.DataFrame(columns = ['CompanyCode', 'YearEnding', 'Months', 'FaceValue', 'ROE', 'Debt'])

		sql_ratios_banking = 'SELECT distinct on("CompanyCode") * FROM public."RatiosBankingVI" where "YearEnding" <= \'' +date.strftime("%Y-%m-%d")+ '\' \
							ORDER BY "CompanyCode", "YearEnding" DESC ;'					
		ratios_banking_list = sqlio.read_sql_query(sql_ratios_banking, con = conn)

		sql_ratios_nonbanking = 'SELECT distinct on("CompanyCode") * FROM public."RatiosNonBankingVI" where "YearEnding" <= \'' +date.strftime("%Y-%m-%d")+ '\' \
								ORDER BY "CompanyCode", "YearEnding" DESC ;'
		ratios_nonbanking_list = sqlio.read_sql_query(sql_ratios_nonbanking, con = conn)


		ratios_nonbanking_list = ratios_nonbanking_list.rename(columns = {'ROCE': 'ROE', 'DebtEquity': 'Debt'})
		ratios_nonbanking_list = ratios_nonbanking_list[['CompanyCode', 'YearEnding', 'Months', 'FaceValue', 'ROE', 'Debt']]
		
		for index, row in ratios_banking_list.iterrows():
			debt = 0
			ratios_banking_list.loc[index, 'Debt'] = debt
			
		ratios_banking_list = ratios_banking_list[['CompanyCode', 'YearEnding', 'Months', 'FaceValue', 'ROE', 'Debt']] \
							if len(ratios_banking_list.index !=0) else ratios_banking_list_init
		
		ratios_merge_list = pd.concat([ratios_banking_list, ratios_nonbanking_list], axis=0)

		return ratios_merge_list
 
	def compile_ratios_list_current(self, conn, date):
		""" Fetch the data for Ratios merge list

		Operation:
			Take the data from RatiosBankingVI,RatiosNonBankingVI and BTTList for max date
			and merge execured data, based  on company code.

		Return:
			Merge data of Ratios Banking, Ratios Non banking and BTTlist table. 
		"""

		ratios_banking_list_init =  pd.DataFrame(columns = ['CompanyCode', 'YearEnding', 'Months', 'FaceValue', 'ROE', 'Debt'])

		#For running backdated daily reports in case of bugs add the following to the above query
		#AND rb."ModifiedDate"<\''+str(date + datetime.timedelta(1))+'\'

		# sql_ratios_banking = 'SELECT DISTINCT ON("CompanyCode") * FROM public."RatiosBankingVI" rb \
		# 					WHERE rb."YearEnding" = (SELECT MAX("YearEnding") FROM public."RatiosBankingVI" WHERE "CompanyCode" = rb."CompanyCode" AND "YearEnding" <= DATE(\'{}\'));'.format(date)
		sql_ratios_banking = """SELECT DISTINCT ON (rb."CompanyCode") rb.*
							FROM public."RatiosBankingVI" rb
							WHERE (rb."CompanyCode", rb."YearEnding") IN (
								SELECT "CompanyCode", MAX("YearEnding") AS max_year
								FROM public."RatiosBankingVI"
								WHERE "YearEnding" <= %s
								GROUP BY "CompanyCode");"""
		ratios_banking_list = sqlio.read_sql_query(sql_ratios_banking, con = conn, params = [date])

		print("RatiosBanking :", len(ratios_banking_list.index))
		# sql_ratios_nonbanking = 'SELECT DISTINCT ON("CompanyCode") * FROM public."RatiosNonBankingVI" rnb \
		# 					WHERE rnb."YearEnding" = (SELECT MAX("YearEnding") FROM public."RatiosNonBankingVI" WHERE "CompanyCode" = rnb."CompanyCode" AND "YearEnding" <= DATE(\'{}\'));'.format(date)
		sql_ratios_nonbanking = """SELECT DISTINCT ON (rnb."CompanyCode") rnb.*
									FROM public."RatiosNonBankingVI" rnb
									WHERE (rnb."CompanyCode", rnb."YearEnding") IN (
										SELECT "CompanyCode", MAX("YearEnding") AS max_year
										FROM public."RatiosNonBankingVI"
										WHERE "YearEnding" <= %s
										GROUP BY "CompanyCode"
									);"""
		ratios_nonbanking_list = sqlio.read_sql_query(sql_ratios_nonbanking, con = conn, params = [date])
		print("RatiosNonBanking :", len(ratios_nonbanking_list.index))
		# '''
		# sql_ratios_banking = 'SELECT distinct on (rb."CompanyCode") rb.* FROM public."RatiosBankingVI" rb \
		# 						left join public."RatiosMergeList" rm \
		# 						on \
		# 						(rb."CompanyCode" = rm."CompanyCode" \
		# 						and rm."ROEYearEnding" = rb."YearEnding") \
		# 							WHERE ((rm."CompanyCode" is null OR \
		# 							(rm."ROEYearEnding" = (SELECT max("YearEnding") FROM public."RatiosBankingVI" rm2 WHERE  rm2."CompanyCode"=rb."CompanyCode") \
		# 							AND rm."TTMYearEnding" = (SELECT max("TTMYearEnding") FROM public."RatiosMergeList" rm3  WHERE rm3."CompanyCode"=rb."CompanyCode") \
		# 							AND rm."TTMYearEnding"  != (SELECT max("YearEnding") FROM public."TTM" ttm  WHERE ttm."CompanyCode"=rb."CompanyCode" \
		# 							AND ttm."YearEnding" <= \'' +date.strftime("%Y-%m-%d")+'\')))) \
		# 						ORDER BY rb."CompanyCode", rm."TTMYearEnding", rb."YearEnding" ;'

		# ratios_banking_list = sqlio.read_sql_query(sql_ratios_banking, con = conn)


		# sql_ratios_nonbanking = 'SELECT distinct on (rb."CompanyCode") rb.* FROM public."RatiosNonBankingVI" rb \
		# 						left join public."RatiosMergeList" rm \
		# 						on \
		# 						(rb."CompanyCode" = rm."CompanyCode" \
		# 						and rm."ROEYearEnding" = rb."YearEnding") \
		# 							WHERE ((rm."CompanyCode" is null OR \
		# 							(rm."ROEYearEnding" = (SELECT max("YearEnding") FROM public."RatiosMergeList" rm2 WHERE  rm2."CompanyCode"=rb."CompanyCode") \
		# 							AND rm."TTMYearEnding" = (SELECT max("TTMYearEnding") FROM public."RatiosMergeList" rm3  WHERE rm3."CompanyCode"=rb."CompanyCode") \
		# 							AND rm."TTMYearEnding"  != (SELECT max("YearEnding") FROM public."TTM" ttm  WHERE ttm."CompanyCode"=rb."CompanyCode" \
		# 							AND ttm."YearEnding" <= \'' +date.strftime("%Y-%m-%d")+'\')))) \
		# 						ORDER BY rb."CompanyCode", rm."TTMYearEnding", rb."YearEnding" ;'                                                                                                                                      
		
		# ratios_nonbanking_list = sqlio.read_sql_query(sql_ratios_nonbanking, con = conn)
		# '''

		ratios_nonbanking_list = ratios_nonbanking_list.rename(columns = {'ROCE': 'ROE', 'DebtEquity': 'Debt'})
		ratios_nonbanking_list = ratios_nonbanking_list[['CompanyCode', 'YearEnding', 'Months', 'FaceValue', 'ROE', 'Debt']]
		

		for index, row in ratios_banking_list.iterrows(): 
			debt = 0
			ratios_banking_list.loc[index, 'Debt'] = debt 
			
		ratios_banking_list = ratios_banking_list[['CompanyCode', 'YearEnding', 'Months', 'FaceValue', 'ROE', 'Debt']] \
							if len(ratios_banking_list.index !=0) else ratios_banking_list_init
		

		ratios_merge_list = pd.concat([ratios_banking_list, ratios_nonbanking_list], axis=0)

		# btt_sql = 'SELECT "CompanyCode" FROM public."BTTList" WHERE "BTTDate" = \
		# 			(SELECT MAX("BTTDate") FROM public."BTTList" WHERE "BTTDate" <= DATE(\'{}\'));'.format(str(date))
		btt_sql = """SELECT "CompanyCode"
					FROM public."BTTList"
					WHERE "BTTDate" = (
						SELECT MAX("BTTDate")
						FROM public."BTTList"
						WHERE "BTTDate" <= %s
					);"""

		btt =  sqlio.read_sql_query(btt_sql, con = conn, params = [date])
		print("BTTList :", len(btt.index))
		ratios_merge_list = pd.merge(btt, ratios_merge_list, left_on = 'CompanyCode', right_on = 'CompanyCode', how='left')
		print("Ratios Merge List: ", len(ratios_merge_list.index))
		return ratios_merge_list

	def compile_sales_npm_roe(self, ratios_merge_list, date, conn):
		""" calculating the value of sales and NPM

		Args:
			ratios_merge_list = Merge data of Ratios Banking, Ratios Non banking and BTTList.

		Operation:
			Fetch the data from TTM for max date for one year back quarter
			and calculate the value of TTMYearEnding, NPM and Sales Growth
			Sales Growth = (ttm sales curr - ttm sales prev / abs(ttm sales prev)*100.

		Return:
			Value of Sales Growth and NPM.
		"""
		
		one_year_back = self.get_year_before_quarter(date).strftime("%Y-%m-%d")
		four_years_back = self.get_four_years_before_quarter(date).strftime("%Y-%m-%d")
		# ????
		sql_ttm = """
					WITH max_year_ending AS (
					SELECT "CompanyCode", MAX("YearEnding") AS max_year
					FROM public."TTM"
					WHERE "YearEnding" <= %s
					GROUP BY "CompanyCode"
				)
				SELECT DISTINCT ON (ttm."CompanyCode") ttm.*
				FROM public."TTM" ttm
				JOIN max_year_ending m
					ON ttm."CompanyCode" = m."CompanyCode" AND ttm."YearEnding" = m.max_year;"""

		# sql_ttm = 'SELECT DISTINCT ON(ttm."CompanyCode") * FROM public."TTM" ttm \
		# 				WHERE ttm."YearEnding" = (SELECT MAX("YearEnding") FROM public."TTM" \
		# 						WHERE "CompanyCode" = ttm."CompanyCode" AND "YearEnding" <= DATE(\'{}\'));'.format(str(date))
		# sql_ttm = 'SELECT DISTINCT ON(ttm."CompanyCode") * FROM public."TTM" ttm \
		# 				WHERE ttm."YearEnding" <= DATE(\'{}\');'.format(str(date))
		ttm_list = sqlio.read_sql_query(sql_ttm, con = conn, params = [date])
		print("TTM: ", len(ttm_list.index))
		# self.export_table("00_02_ttm_list", ttm_list)
		# sql_ttm_year_back =  'SELECT * FROM public."TTM" \
		# 					WHERE "YearEnding" >= \''+four_years_back+'\' AND "YearEnding" <= \''+one_year_back+'\' ;'\
		sql_ttm_year_back = """
							SELECT * FROM public."TTM"
							WHERE "YearEnding" >= %s AND "YearEnding" <= %s
						"""
		ttm_year_back_list =  sqlio.read_sql_query(sql_ttm_year_back, con = conn, params = [four_years_back, one_year_back])
		# self.export_table("00_02_ttm_year_back_list", ttm_year_back_list)
		print("TTM Year Back: ", len(ttm_year_back_list.index))
		ttm_list['Sales'] =ttm_list['Sales'].replace(r'[?$,]', '', regex=True).astype(float)
		ttm_year_back_list['Sales'] =ttm_year_back_list['Sales'].replace(r'[?$,]', '', regex=True).astype(float)

		# f = open("00_02_03{}.txt".format(date), "a+")
		# f.write("\n\n------------------------------------------------------------------------------------------------\n\n")
		# f.write("\t\t FOR DATE: " + str(date) + "\n")
		print("Starting loop---")
		for index, row in ratios_merge_list.iterrows():
			# print(index)
			# print(ratios_merge_list.loc[(ratios_merge_list['CompanyCode']==14030078)])
			# print(ttm_list.loc[(ttm_list['CompanyCode']==14030078)]['YearEnding'])

			item_qtr_list = ttm_list.loc[(ttm_list['CompanyCode']==row['CompanyCode'])]["YearEnding"]
			item_qtr = item_qtr_list.item() if len(item_qtr_list.index) == 1 else "No Date"

			# print(item_qtr)

			prev_one_year = self.get_year_before_quarter(item_qtr) if item_qtr != "No Date" else np.nan

			npm_list = ttm_list.loc[(ttm_list['CompanyCode']==row['CompanyCode'])]['NPM']
			npm = npm_list.item() if len(npm_list.index) == 1 else np.nan
			
			ttm_sales_curr_list = ttm_list.loc[(ttm_list["CompanyCode"]==row['CompanyCode'])]['Sales']
			ttm_sales_curr = ttm_sales_curr_list.item() if len(ttm_sales_curr_list.index) == 1 else np.nan

			ttm_sales_prev_list = ttm_year_back_list.loc[(ttm_year_back_list['CompanyCode']==row['CompanyCode']) & (ttm_year_back_list['YearEnding']==prev_one_year)]['Sales']
			ttm_sales_prev = ttm_sales_prev_list.item() if len(ttm_sales_prev_list.index) == 1  else np.nan

			# f.write( "CompanyCode: "+ str(row['CompanyCode']) + 
			# 		"ttm_sales_curr_list: " + str(ttm_sales_curr_list.item() if len(ttm_sales_curr_list.index) == 1 else np.nan) +
			# 		"\nttm_sales_prev_list: " + str(ttm_sales_prev_list.item() if len(ttm_sales_prev_list.index) == 1  else np.nan) +
			# 		"\nttm_sales_curr-ttm_sales_prev: " + str((ttm_sales_curr-ttm_sales_prev)) +
			# 		"\nabs(ttm_sales_prev): "+ str(abs(ttm_sales_prev)) + 
			# 		"len(ttm_sales_prev_list.index): " + str(len(ttm_sales_prev_list.index)) + "\n\n\n")

			ratios_merge_list.loc[index, 'TTMYearEnding'] = item_qtr if item_qtr != "No Date" else np.nan
			ratios_merge_list.loc[index , 'NPM'] = npm
			ratios_merge_list.loc[index , 'Sales Growth'] = ((ttm_sales_curr-ttm_sales_prev)/abs(ttm_sales_prev))*100 if ttm_sales_prev != 0 else np.nan

		# f.close()

		return ratios_merge_list

	def merge_background_industry_info(self, ratios_merge_list, conn):
		""" Merging the BackgroundInfo and IndustryMapping

		Args:
			ratios_merge_list = data of Sales Growth and NPM,

		Operation:
			Fetch the data from BackgroundInfo and IndustryMapping
			and merge both the executed date based on IndustryCode

		Return:
			Merge data of BackgroundInfo, IndustryMapping and ratios merge list.
		"""
		
		background_info_sql ='select *  from public."BackgroundInfo";'
		background_info = pd.read_sql_query(background_info_sql, conn)
		ratios_merge_list = pd.merge(ratios_merge_list, background_info[['CompanyCode', 'CompanyName', 'IndustryCode']], left_on = 'CompanyCode', \
									right_on = 'CompanyCode', how = 'left')
		print("lenght of ratios_merge_list after backgroundinfo : ", len(ratios_merge_list.index))

		industry_info_sql = 'select *  from public."IndustryMapping";'
		industry_info = pd.read_sql_query(industry_info_sql, conn)
		ratios_merge_list = pd.merge(ratios_merge_list, industry_info[['IndustryCode', 'Industry']], left_on = 'IndustryCode', \
									right_on = 'IndustryCode', how = 'left')
		
		print("lenght of ratios_merge_list after industrymapping : ", len(ratios_merge_list.index))
		
		ratios_merge_list = ratios_merge_list.drop(["IndustryCode"], axis = 1)

		# get duplicates from ratios_merge_list
		duplicates = ratios_merge_list[ratios_merge_list.duplicated(['CompanyCode'], keep=False)]

		#drop duplicates from ratios_merge_list
		ratios_merge_list = ratios_merge_list.drop_duplicates(subset=['CompanyCode'], keep=False)
		# print("length of ratios_merge_list after dropping duplicates : ", len(ratios_merge_list.index))

		for index, row in duplicates.iterrows():
			company_code = row['CompanyCode']
			print("Company Code that has duplicate is : ", company_code)
			# company code values from duplicates
			duplicates_values = duplicates[duplicates['CompanyCode'] == company_code]
			# print("duplicates_values : ", duplicates_values)
			if (duplicates_values['Industry'] == 'Banks').all():
				# keep the first of the duplicates
				print("all are Banks")
				ratios_merge_list = ratios_merge_list._append(duplicates_values.iloc[0])
				# delete the second of the duplicates from duplicates
				# duplicates = duplicates.drop(duplicates_values.index[1])
				# print("length of ratios_merge_list after appending : ", len(ratios_merge_list.index))

		ratios_merge_list = ratios_merge_list.drop_duplicates(subset=['CompanyCode'], keep='first')
		

		return ratios_merge_list

	def insert_ratios_merge_list(self, ratios_merge_list, conn, cur, date):
		"""Insert the ratios merge data into database

		Args:
			ratios_merge_list = data of ratios list.

		Operation:
			Exporting the data into ratiosmerge_export.csv file
			and insert data into RatiosMergeList table.
		"""

		ratios_merge_list_init = pd.DataFrame(columns = ['CompanyCode', 'CompanyName', 'Industry', 'ROEYearEnding', 'Months', 'FaceValue', 'Sales Growth', 'NPM', \
												'ROE', 'Debt', 'TTMYearEnding' ,'GenDate'])

		ratios_merge_list['GenDate'] = date.strftime("%Y-%m-%d")

		ratios_merge_list["Months"].fillna(-1, inplace=True)
		ratios_merge_list = ratios_merge_list.astype({"Months": int})
		ratios_merge_list = ratios_merge_list.astype({"Months": str})
		ratios_merge_list["Months"] = ratios_merge_list["Months"].replace('-1', np.nan)
		# ratios_merge_list['FaceValue'] = ratios_merge_list['FaceValue'].str.replace(r'[^0-9.]', '', regex=True).astype(float)

		ratios_merge_list['FaceValue'] = ratios_merge_list['FaceValue'].replace(r'[?$,]', '', regex=True).astype(float)
		ratios_merge_list['ROE'] = ratios_merge_list['ROE'].replace(r'[?$,]', '', regex=True).astype(float)
		ratios_merge_list['Debt'] = ratios_merge_list['Debt'].replace(r'[?$,]', '', regex=True).astype(float)
		
		# Renaming column
		ratios_merge_list = ratios_merge_list.rename(columns = {'YearEnding' : 'ROEYearEnding'})

		ratios_merge_list = ratios_merge_list[['CompanyCode', 'CompanyName', 'Industry', 'ROEYearEnding', 'Months', 'FaceValue', 'Sales Growth', 'NPM', \
												'ROE', 'Debt', 'TTMYearEnding' ,'GenDate']] if len(ratios_merge_list.index != 0) else ratios_merge_list_init

		print("Inserting Ratios Merge List: ")

		exportfilename = "ratiosmerge_export.csv"
		exportfile = open(exportfilename,"w")
		ratios_merge_list.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r' )
		exportfile.close()
			
		copy_sql = """
				COPY "public"."RatiosMergeList" FROM stdin WITH CSV HEADER
				DELIMITER as ','
				"""
		with open(exportfilename, 'r') as f:
			cur.copy_expert(sql=copy_sql, file=f)
			conn.commit()
			f.close()
		# os.remove(exportfilename)

	def get_bttlist_stocks_history(self, date, conn):
		""" Fetch the bttlist stock history.

		Operation:
			Fetch the data from BTTList and RatiosMergeList for max date
			and merge both the executed data based on CompanyCode.

		Return:
			Merge data of BTTList and RatiosMergeList to generate SMR rank history.
		"""
		
		BTT_back = datetime.date(2018, 12, 1).strftime("%Y-%m-%d")
		BTT_next = datetime.date(2019, 1, 1).strftime("%Y-%m-%d")


		sql = \
		'SELECT BTT."ISIN", BTT."NSECode", BTT."BSECode",BTT."CompanyCode" AS "Company Code", RM.* \
		FROM public."BTTList" BTT 	\
		LEFT JOIN public."RatiosMergeList" RM \
			ON RM."CompanyCode" = BTT."CompanyCode" \
			AND RM."ROEYearEnding"= (SELECT max("ROEYearEnding") FROM public."RatiosMergeList"	WHERE "CompanyCode"=BTT."CompanyCode" \
									AND "ROEYearEnding" <= \'' +date.strftime("%Y-%m-%d")+ '\') \
			AND RM."TTMYearEnding"= (SELECT max("TTMYearEnding") FROM public."RatiosMergeList"  WHERE "CompanyCode"=BTT."CompanyCode" \
									AND "TTMYearEnding" <= \'' +date.strftime("%Y-%m-%d")+ '\') \
		WHERE "BTTDate">= \'' + BTT_back + '\' and "BTTDate"< \'' + BTT_next + '\'  ;'

		btt_smr_merge_list = sqlio.read_sql_query(sql, con = conn)
		btt_smr_merge_list = btt_smr_merge_list.drop(["CompanyCode"], axis = 1)
		btt_smr_merge_list = btt_smr_merge_list.rename(columns = {'Company Code' : 'CompanyCode'})

		return btt_smr_merge_list

	def get_bttlist_stocks_current(self, conn,today):
		""" Get the BTTList data for current date

		Operation:
			Fetch the data from BTTList and RatiosMergeList for first of each month,

		Return:
			BTTList to get list of btt stocks.
		"""
		
		BTT_back = datetime.date(today.year, today.month, 1).strftime("%Y-%m-%d")
		next_month = today.month + 1 if today.month + 1 <= 12 else 1 
		next_year = today.year if today.month + 1 <= 12 else today.year + 1
		BTT_next = datetime.date(next_year, next_month, 1).strftime("%Y-%m-%d")
		sql = """WITH max_gen_dates AS (
				SELECT
					"CompanyCode",
					MAX("GenDate") AS max_gen_date
				FROM
					public."RatiosMergeList"
				WHERE
					"GenDate" <= DATE(%s)
				GROUP BY
					"CompanyCode"
			)
			SELECT
				BTT."ISIN",
				BTT."NSECode",
				BTT."BSECode",
				BTT."CompanyCode" AS "Company Code",
				RM.*
			FROM
				public."BTTList" BTT
			LEFT JOIN
				public."RatiosMergeList" RM ON RM."CompanyCode" = BTT."CompanyCode"
			JOIN
				max_gen_dates MGD ON RM."CompanyCode" = MGD."CompanyCode" AND RM."GenDate" = MGD.max_gen_date
			WHERE
				"BTTDate" >= %s
				AND "BTTDate" < %s;"""


		btt_smr_merge_list = sqlio.read_sql_query(sql, con = conn, params = [today, BTT_back, BTT_next])

		btt_smr_merge_list = btt_smr_merge_list.drop(["CompanyCode"], axis = 1)
		btt_smr_merge_list = btt_smr_merge_list.rename(columns = {'Company Code' : 'CompanyCode'})

		return btt_smr_merge_list
	
	def npm_roe_sales_rank(self, btt_ratios_list, conn):
		""" calculate teh value of NPM, ROE and sales growth
		
		Args:
			btt_ratios_list = Data of BTTList and RatiosMergeList for first of the month.

		Return:
			Value of NPM, ROE and sales growth
		"""

		btt_ratios_list['NPM Rank'] = btt_ratios_list['NPM'].rank(ascending=False)
		btt_ratios_list['NPM Rank'] = ((len(btt_ratios_list.index)-btt_ratios_list['NPM Rank']+1)/len(btt_ratios_list.index))*100

		btt_ratios_list['ROE Rank'] = btt_ratios_list['ROE'].rank(ascending=False)
		btt_ratios_list['ROE Rank'] = ((len(btt_ratios_list.index)-btt_ratios_list['ROE Rank']+1)/len(btt_ratios_list.index))*100

		btt_ratios_list['Sales Growth Rank'] = btt_ratios_list['SalesGrowth'].rank(ascending=False)
		btt_ratios_list['Sales Growth Rank'] = ((len(btt_ratios_list.index)-btt_ratios_list['Sales Growth Rank']+1)/len(btt_ratios_list.index))*100

		return btt_ratios_list

	def smr_rank(self, btt_ratios_list, conn):
		"""  Calculating the value of SMR Rank

		Args:
			btt_ratios_list = data of NPM, ROE and sales growth,

		Operation:
			Fetch the grade where grade variation are 'A'= >=80 to <=100, 'B' = >=60 to <80 ,
			'C'= >=40 to <60 ,'D'= >=20 to <40 and 'E' >=0 to <20 and calculate SMR Grade and SMR Rank
			SMR Grade = Sales Grade + NPM Grade + ROE Grade.

		Return:
			SMR Rank.
		"""

		smr_grade_map = {'A' : 25, 'B' : 22.5, 'C' : 20, 'D' : 17.5, 'E' : 15}

		SMR_grade_values = [75,72.5,70,67.5,65,72.5,70,67.5,65,62.5,70,67.5,65,62.5,60,67.5,65,62.5,\
		60,57.5,65,62.5,60,57.5,55,72.5,70,67.5,65,62.5,70,67.5,65,62.5,60,67.5,65,62.5,60,57.5,65,\
		62.5,60,57.5,55,62.5,60,57.5,55,52.5,70,67.5,65,62.5,60,67.5,65,62.5,60,57.5,65,62.5,60,57.5,\
		55,62.5,60,57.5,55,52.5,60,57.5,55,52.5,50,67.5,65,62.5,60,57.5,65,62.5,60,57.5,55,62.5,60,57.5,\
		55,52.5,60,57.5,55,52.5,50,57.5,55,52.5,50,47.5,65,62.5,60,57.5,55,62.5,60,57.5,55,52.5,60,57.5,55,\
		52.5,50,57.5,55,52.5,50,47.5,55,52.5,50,47.5,45]

		SMR_rating = [100,97.6,92.7,84.7,72.6,97.6,92.7,84.7,72.6,58.1,92.7,84.7,72.6,58.1,42.7,84.7,72.6,\
		58.1,42.7,28.2,72.6,58.1,42.7,28.2,16.1,97.6,92.7,84.7,72.6,58.1,92.7,84.7,72.6,58.1,42.7,84.7,72.6,58.1,42.7,\
		28.2,72.6,58.1,42.7,28.2,16.1,58.1,42.7,28.2,16.1,8.1,92.7,84.7,72.6,58.1,42.7,84.7,72.6,58.1,42.7,28.2,72.6,58.1,\
		42.7,28.2,16.1,58.1,42.7,28.2,16.1,8.1,42.7,28.2,16.1,8.1,3.2,84.7,72.6,58.1,42.7,28.2,72.6,58.1,42.7,28.2,16.1,58.1,\
		42.7,28.2,16.1,8.1,42.7,28.2,16.1,8.1,3.2,28.2,16.1,8.1,3.2,0.8,72.6,58.1,42.7,28.2,16.1,58.1,42.7,28.2,16.1,8.1,42.7,\
		28.2,16.1,8.1,3.2,28.2,16.1,8.1,3.2,0.8,16.1,8.1,3.2,0.8,0]

		smr_rating_map = {SMR_grade_values[i]: SMR_rating[i] for i in range(len(SMR_grade_values))} 

		for index, row in btt_ratios_list.iterrows():

			if  (row['Sales Growth Rank'] >= 80 and  row['Sales Growth Rank'] <= 100):
				sales_gr = 'A'
				btt_ratios_list.loc[index, 'Sales Gr'] = sales_gr
			elif (row['Sales Growth Rank'] >= 60 and  row['Sales Growth Rank'] < 80):
				sales_gr = 'B'
				btt_ratios_list.loc[index, 'Sales Gr'] = sales_gr
			elif (row['Sales Growth Rank'] >= 40 and  row['Sales Growth Rank'] < 60):
				sales_gr = 'C'
				btt_ratios_list.loc[index, 'Sales Gr'] = sales_gr
			elif (row['Sales Growth Rank'] >= 20 and  row['Sales Growth Rank'] < 40):
				sales_gr = 'D'
				btt_ratios_list.loc[index, 'Sales Gr'] = sales_gr
			elif (row['Sales Growth Rank'] >= 0 and  row['Sales Growth Rank'] < 20):
				sales_gr = 'E'
				btt_ratios_list.loc[index, 'Sales Gr'] = sales_gr
			else:
				sales_gr = 'E'
				btt_ratios_list.loc[index, 'Sales Gr'] = sales_gr


			if (row['NPM Rank'] >= 80 and  row['NPM Rank'] <= 100):
				npm_gr = 'A'
				btt_ratios_list.loc[index, 'NPM Gr'] = npm_gr
			elif (row['NPM Rank'] >= 60 and  row['NPM Rank'] < 80):
				npm_gr = 'B'
				btt_ratios_list.loc[index, 'NPM Gr'] = npm_gr
			elif (row['NPM Rank'] >= 40 and  row['NPM Rank'] < 60):
				npm_gr = 'C'
				btt_ratios_list.loc[index, 'NPM Gr'] = npm_gr
			elif (row['NPM Rank'] >= 20 and  row['NPM Rank'] < 40):
				npm_gr = 'D'
				btt_ratios_list.loc[index, 'NPM Gr'] = npm_gr
			elif (row['NPM Rank'] >= 0 and  row['NPM Rank'] < 20):
				npm_gr = 'E'
				btt_ratios_list.loc[index, 'NPM Gr'] = npm_gr
			else:
				npm_gr = 'E'
				btt_ratios_list.loc[index, 'NPM Gr'] = npm_gr


			if (row['ROE Rank'] >= 80 and  row['ROE Rank'] <= 100):
				roe_gr = 'A'
				btt_ratios_list.loc[index, 'ROE Gr'] = roe_gr
			elif (row['ROE Rank'] >= 60 and  row['ROE Rank'] < 80):
				roe_gr = 'B'
				btt_ratios_list.loc[index, 'ROE Gr'] = roe_gr
			elif (row['ROE Rank'] >= 40 and  row['ROE Rank'] < 60):
				roe_gr = 'C'
				btt_ratios_list.loc[index, 'ROE Gr'] = roe_gr
			elif (row['ROE Rank'] >= 20 and  row['ROE Rank'] < 40):
				roe_gr = 'D'
				btt_ratios_list.loc[index, 'ROE Gr'] = roe_gr
			elif (row['ROE Rank'] >= 0 and  row['ROE Rank'] < 20):
				roe_gr = 'E'
				btt_ratios_list.loc[index, 'ROE Gr'] = roe_gr
			else:
				roe_gr = 'E' 
				btt_ratios_list.loc[index, 'ROE Gr'] = roe_gr


			btt_ratios_list.loc[index, 'SMRGrade'] = (sales_gr + npm_gr + roe_gr)

			btt_ratios_list.loc[index, 'SMR'] = smr_rating_map[smr_grade_map[sales_gr] + smr_grade_map[npm_gr] + smr_grade_map[roe_gr]]

		btt_ratios_list['SMR Rank'] = btt_ratios_list['SMR'].rank(ascending=False)
		btt_ratios_list['SMR Rank'] = ((len(btt_ratios_list.index)-btt_ratios_list['SMR Rank']+1)/len(btt_ratios_list.index))*100

		return btt_ratios_list

	def insert_smr(self, btt_ratios_list, conn, cur, date):
		""" Insert the data into Database

		Args:
			btt_ratios_list = calculated data of SMR.

		Operation:
			Export the data into smr_export.csv
			and insert into SMR Table.
		"""

		btt_ratios_list['SMRDate'] = date.strftime("%Y-%m-%d")
		
		btt_ratios_list["BSECode"] = btt_ratios_list["BSECode"].fillna(-1)
		btt_ratios_list = btt_ratios_list.astype({"BSECode": int})
		btt_ratios_list = btt_ratios_list.astype({"BSECode": str})
		btt_ratios_list["BSECode"] = btt_ratios_list["BSECode"].replace('-1', np.nan)


		btt_ratios_list["Months"] = btt_ratios_list["Months"].fillna(-1)
		btt_ratios_list = btt_ratios_list.astype({"Months": int})
		btt_ratios_list = btt_ratios_list.astype({"Months": str})
		btt_ratios_list["Months"] = btt_ratios_list["Months"].replace('-1', np.nan)

		btt_ratios_list = btt_ratios_list[['CompanyCode', 'NSECode', 'BSECode', 'Industry', 'ROEYearEnding', 'Months', 'TTMYearEnding', \
										'SalesGrowth', 'Sales Growth Rank', 'NPM', 'NPM Rank', 'ROE', 'ROE Rank', 'SMRGrade', 'SMR', \
										'SMR Rank', 'SMRDate', 'CompanyName', 'ISIN']]

	# for i in curr_qtr_date to next_qtr_date-1:
	# 			btt_ratios_list['SMRDate'] = i.strftime("%Y-%m-%d")


		exportfilename = "smr_export.csv"	
		exportfile = open(exportfilename,"w")
		btt_ratios_list.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
		exportfile.close()

		copy_sql = """
				COPY "Reports"."SMR" FROM stdin WITH CSV HEADER
				DELIMITER as ','
				"""
		with open(exportfilename, 'r') as f:
			cur.copy_expert(sql=copy_sql, file=f)
			conn.commit()
			f.close()

	def generate_ratios_list_history(self, date):
		""" Generate the Ratios list history

		Operation:
			Fetch the data from compile ratios list history,value of sales 
			gorwth , NPM and Merging background info.
			to generate the Ratios list history.
		"""
		
		conn = DB_Helper.db_connect()
		cur = conn.cursor()

		today = date

		print("Compiling List for date: ", date)

		print("Compiling list from RatiosBanking and NonBanking...")
		ratios_merge_list = self.compile_ratios_list_history(conn, date)

		print("Calculating sales growth and npm for ttm year...")
		ratios_merge_list = self.compile_sales_npm_roe(ratios_merge_list, date, conn)

		print("Merging background info to Ratios list")
		ratios_merge_list =  self.compile_ratios_list_history(ratios_merge_list, conn)

		print("Inserting values into Ratios merge list")
		self.insert_ratios_merge_list(ratios_merge_list, conn, cur, date)

		conn.close()

	def generate_ratios_list_current(self, conn,cur, today):
		""" Generate the ratios list for current date

		Operation:
			Fetching the data of ratios list for current date, Sales growth 
			and NPM value and BackgroundInfo.
			to generate Ratios list for current date.
		"""

		print("Compiling list from RatiosBanking and NonBanking...", flush = True)
		ratios_merge_list = self.compile_ratios_list_current(conn,today)
		# self.export_table("01_compile_ratios_list_current", ratios_merge_list)
		# print("lenght of compile_ratios_list_current: ", len(ratios_merge_list.index), flush = True)
		print("Calculating sales growth and npm for ttm year...", flush = True)
		ratios_merge_list = self.compile_sales_npm_roe(ratios_merge_list, today, conn)
		# self.export_table("02_compile_sales_npm_roe", ratios_merge_list)
		# print("lenght of compile_sales_npm_roe: ", len(ratios_merge_list.index), flush = True)
		print("Merging background info to Ratios list", flush = True)
		ratios_merge_list =  self.merge_background_industry_info(ratios_merge_list, conn)
		# # self.export_table("03_Background_Industry_Merge", ratios_merge_list)
		print("lenght of background_Industry: ", len(ratios_merge_list.index), flush = True)
		# print("Inserting values into Ratios merge list", flush = True)
		self.insert_ratios_merge_list(ratios_merge_list, conn, cur, today)

		return ratios_merge_list
	
	def generate_smr_history(self, date):
		""" Generate SMR history 

		Operation:
			fetch the data from BTT stocks,percentile values for NPM, 
			ROE and Sales Growth and SMR Rank.
			to generate the SMR history.
		"""

		conn = DB_Helper.db_connect()
		cur = conn.cursor()

		today = date

		print("Generating SMR for date: ", date)

		print("Fetching BTT stocks for SMR Ranking")
		btt_ratios_list = self.get_bttlist_stocks_history(date, conn)
		print("number of stocks:", len(btt_ratios_list))

		print("Calculating percentile values for NPM, ROE and Sales Growth")
		btt_ratios_list = self.npm_roe_sales_rank(btt_ratios_list, conn)

		print("Calculating SMR Rank")
		btt_ratios_list = self.smr_rank(btt_ratios_list, conn)

		print("Inserting into SMR..")
		self.insert_smr(btt_ratios_list, conn, cur, date)

		conn.close()

	def generate_smr_current(self, curr_date, conn, cur):
		""" Generate the SMR for current date

		Operation:
			Fetch the data from generate ratios list, bttlist stock,
			percentile rank of NPM, ROE and sales growth SMR Grade.
			to generate	SMR Rank for current date.
		"""

		today = curr_date
		# today = today.date()

		print("Compiling Ratios Merge List for today", today, flush = True)
		self.generate_ratios_list_current(conn, cur, today)
		print("Fetching BTT stocks for SMR Ranking", flush = True)
		btt_ratios_list = self.get_bttlist_stocks_current(conn,today)
		print("lenght of btt_ratios_list: ", len(btt_ratios_list.index), flush = True)
		# self.export_table("04_get_bttlist_stocks_current", btt_ratios_list)
		print("number of stocks:", len(btt_ratios_list), flush = True)

		if not(btt_ratios_list.empty):

			print("Calculating percentile values for NPM, ROE and Sales Growth")
			btt_ratios_list = self.npm_roe_sales_rank(btt_ratios_list, conn)
			print("lenght of npm_roe_sales_rank : ", len(btt_ratios_list.index), flush = True)
			# self.export_table("05_npm_roe_sales_rank", btt_ratios_list)

			print("Calculating SMR Rank")
			btt_ratios_list = self.smr_rank(btt_ratios_list, conn)
			# self.export_table("06_smr_rank", btt_ratios_list)
			print("lenght of smr_rank : ", len(btt_ratios_list.index), flush = True)
			print("Inserting into SMR...")
			self.insert_smr(btt_ratios_list, conn, cur, today)	
		
		else:
			print("Data not present for BTT stocks for SMR Ranking for ", today)

		return btt_ratios_list

	def history_daterange_ratioslist(self):
		""" Generate RatiosMergeList for history date range"""

		start_date = datetime.date(2019, 1, 1)
		end_date = datetime.date(2019, 5, 30)

		curr_date = self.get_closest_quarter(end_date)

		while(curr_date >= start_date):

			self.generate_ratios_list_history(curr_date)
			curr_date = self.get_previous_quarter(curr_date)

	def history_daterange_smr(self):
		"""Generate SMR for history for given date range """
		
		
		start_date = datetime.date(2019, 5, 1)
		end_date = datetime.date(2019, 5, 28)


		# curr_date = get_closest_quarter(start_date)

		while(end_date >= start_date):

			self.generate_smr_history(start_date)
			start_date = start_date +  datetime.timedelta(1)

	def export_table(self, name, table):
		exportfilename = "__SMR_"+name+".csv"
		exportfile = open(exportfilename,"w")

		table.to_csv(exportfile, header=True, index=False, float_format="%2f", lineterminator='\r')
		exportfile.close()