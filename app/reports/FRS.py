#Script to compile and insert daily and history Fund Ranking report per each company on BTT List
import datetime
from numpy.lib.npyio import _savez_compressed_dispatcher
import requests
import os.path
import os
import codecs
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


class FRS:
	""" Generating FRS Rank and NAV rank for current date
		by fetching the data from SchemeMaster,SchemePortfolioHeader,
		IndustryMapping and PE tables.
		and calculate the MF List, MF Rank and value of NAV.
	"""
	def __init__(self):
		pass

	def get_last_date_prev_month(self, target):
		""" Fetching the last day of previous month """

		first = target.replace(day=1)
		last_month = first - datetime.timedelta(days=1)
	
		return last_month

	def get_scheme_master_list(self, conn, date):
		""" Geting the List of Scheme Master

		Operation:
			Fetch the data from SchemeMaster and SchemePortfolioHeader table
			for SchemePlanCode 2066. And merge both the Executed data based on
			SchemeCode.

		Return:
			Merge data of SchemeMaster and SchemePortfolioHeader.
		"""

		scheme_master_sql = 'SELECT * FROM public."SchemeMaster" \
							WHERE "SchemePlanCode" = 2066 \
							AND "MainCategory" = \'' +'Equity'+ '\' \
							AND "SchemeTypeDescription" = \'' +'Open Ended'+ '\' \
							and "SchemeName" @@ to_tsquery( \'' +'!Direct & !Institutional'+ '\');'

		scheme_master_list = sqlio.read_sql_query(scheme_master_sql, con = conn)
		print("Length of scheme master list:", len(scheme_master_list))
		# print("SQL:", scheme_master_sql)


		scheme_aum_sql = 'SELECT distinct on("SchemeCode") * FROM public."SchemePortfolioHeader" \
						WHERE "SchemePlanCode" = 2066 \
						AND "HoldingDate" <= \'' +date.strftime("%Y-%m-%d")+ '\' \
						order by "SchemeCode", "HoldingDate" desc ;'
		
		scheme_aum_list = sqlio.read_sql_query(scheme_aum_sql, con = conn)

		scheme_mf_list = pd.merge(scheme_master_list, scheme_aum_list, left_on = 'SchemeCode', right_on = 'SchemeCode', how = 'left')
		# print("Number of schemes:", len(scheme_mf_list))
		
		return scheme_mf_list								 

	def merge_schememaster_schemeportfolio(self, scheme_mf_list, conn, date):
		""" Fetch the merge data from schememaster and schemeWiseportfolio

		Args:
			scheme_mf_list = Merge data of SchemeMaster and SchemePortfolioHeader.

		Operation:
			Fetch the data of scheme master list and SchemePortfolioHeader for
			SchemePlanCode 2066 for differnt holding_date. And merge both the
			data.

		Return:
			Merge data of scheme mf list and SchemePortfolioHeader.
		"""

		holding_date = self.get_last_date_prev_month(date)

		holding_date_1 = self.get_last_date_prev_month(holding_date)
		holding_date_2 = self.get_last_date_prev_month(holding_date_1)
		holding_date_3 = self.get_last_date_prev_month(holding_date_2)

		holding_date = holding_date.strftime("%Y-%m-%d")
		holding_date_1 = holding_date_1.strftime("%Y-%m-%d")
		holding_date_2 = holding_date_2.strftime("%Y-%m-%d")
		holding_date_3 = holding_date_3.strftime("%Y-%m-%d")


		scheme_wise_portfolio_sql = 'SELECT DISTINCT ON ("SchemeCode", "InvestedCompanyCode") * from public."SchemewisePortfolio" \
									WHERE "SchemePlanCode" = 2066 \
									AND "InstrumentName" = \'' +'Equity'+ '\' \
									AND "ModifiedDate" <= \'' +date.strftime("%Y-%m-%d")+ '\' \
									AND "HoldingDate" in (\''+holding_date+'\', \''+holding_date_1+'\', \''+holding_date_2+'\') \
									order by "SchemeCode",  "InvestedCompanyCode", "HoldingDate" desc;'
		# print("SQL:", scheme_wise_portfolio_sql)
							
		scheme_wise_portfolio_list = sqlio.read_sql_query(scheme_wise_portfolio_sql, con = conn)

		scheme_mf_list = pd.merge(scheme_mf_list, scheme_wise_portfolio_list, left_on = 'SchemeCode', \
									right_on = 'SchemeCode' , how = 'left')
		# print("Number of schemes:", len(scheme_mf_list))


		return scheme_mf_list

	def merge_background_industry_info(self, scheme_mf_list, conn):
		""" Merge the Background info data

		Args:
			scheme_mf_list = Merge data of scheme mf list and SchemePortfolioHeader.

		Operation:
			Fetch the data from IndustryMapping table and merge it with input data.

		Return:
			Merge data of scheme mf list and IndustryMapping.
		"""
		
		industry_info_sql = 'select *  from public."IndustryMapping";'
		industry_info = pd.read_sql_query(industry_info_sql, conn)
		
		scheme_mf_list = pd.merge(scheme_mf_list, industry_info[['IndustryCode', 'Industry']], left_on = 'IndustryCode', \
									right_on = 'IndustryCode', how = 'left')

		scheme_mf_list = scheme_mf_list.drop(["IndustryCode"], axis = 1)

		# print("Number of schemes:", len(scheme_mf_list))
		return scheme_mf_list

	def map_market_capitalisation(self, scheme_mf_list, conn, date):
		"""  Map market capitlisation

		Args:
			scheme_mf_list = Merge data of scheme mf list and IndustryMapping.

		Operation:
			Fetch data from PE table and get the Market Cap and Market Cap Value
			and calculating Market Cap Rank to map market capitalisation
			'Market Cap Rank' = 'Market Cap Value' * 100.
		
		Return:
			Value of market cap to map market capitalisation.
		"""

		pe_sql = 'SELECT DISTINCT ON("CompanyCode") * FROM public."PE" \
				WHERE "GenDate" <=  \'' +date.strftime("%Y-%m-%d")+ '\'	\
				ORDER BY "CompanyCode", "GenDate" DESC;'
		pe_list = sqlio.read_sql_query(pe_sql, con = conn)

		for index, row in scheme_mf_list.iterrows():

			market_cap_value_list = pe_list.loc[(pe_list["CompanyCode"]==row['InvestedCompanyCode'])]["Market Cap Value"]
			market_cap_value = market_cap_value_list.item() if len(market_cap_value_list.index) == 1 else np.nan

			market_cap_list = pe_list.loc[(pe_list["CompanyCode"] == row['InvestedCompanyCode'])]["Market Cap Class"]
			market_cap = market_cap_list.item() if len(market_cap_list.index) == 1 else np.nan

		
			scheme_mf_list.loc[index, 'Market Cap Value'] = market_cap_value
			scheme_mf_list.loc[index, 'Market Cap'] = market_cap


		scheme_mf_list['Market Cap Rank'] = scheme_mf_list['Market Cap Value'].rank(pct = True) * 100
		scheme_mf_list = scheme_mf_list.sort_values(by = 'Market Cap Value', ascending=False)


		return scheme_mf_list
		
	def insert_mutualfund_list(self, scheme_mf_list, conn, cur, date):
		""" Insert MF merge data into database

		Args:
			scheme_mf_list = data of market capitlisation

		Operation:
			export the data into schememerge_export.csv file
			and insert into MFMergeList Table.
		"""
		
		scheme_mf_list['GenDate'] = date.strftime("%Y-%m-%d")

		scheme_mf_list = scheme_mf_list.rename(columns = {'HoldingDate_y' : 'HoldingDate', 'TotalMarketValue' : 'AUM'})
		scheme_mf_list = scheme_mf_list.drop(["SchemePlanCode_x"], axis = 1)
		scheme_mf_list = scheme_mf_list.drop(["SchemePlanCode_y"], axis = 1)

		scheme_mf_list["SchemePlanCode"].fillna(-1, inplace=True)
		scheme_mf_list = scheme_mf_list.astype({"SchemePlanCode": int})
		scheme_mf_list = scheme_mf_list.astype({"SchemePlanCode": str})
		scheme_mf_list["SchemePlanCode"] = scheme_mf_list["SchemePlanCode"].replace('-1', np.nan)

		scheme_mf_list['AUM'] = scheme_mf_list['AUM'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_mf_list['Quantity'] = scheme_mf_list['Quantity'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_mf_list['Percentage'] = scheme_mf_list['Percentage'].replace(r'[?$, ]', '', regex=True).astype(float)

		scheme_mf_list = scheme_mf_list[['SchemeCode', 'SchemeName', 'SchemePlanCode' ,'SchemeCategoryDescription', 'MainCategory', 'SchemeTypeDescription', \
										'AUM', 'HoldingDate', 'InvestedCompanyCode', 'InvestedCompanyName', 'Industry', 'Quantity', 'Percentage', \
										'ISINCode', 'Market Cap', 'GenDate', 'Market Cap Rank']]



		exportfilename = "schememerge_export.csv"
		exportfile = open(exportfilename,"w",encoding="utf-8")
		scheme_mf_list.to_csv(exportfile, header=True, index=False, lineterminator='\r')
		exportfile.close()
		
		
		copy_sql = """
				COPY "public"."MFMergeList" FROM stdin WITH CSV HEADER
				DELIMITER as ','
				"""

		with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
			cur.copy_expert(sql=copy_sql, file=f)
			conn.commit()
			f.close()
		os.remove(exportfilename)

	def calc_mf_exposure(self, conn, date):
		""" Calculate MF exposure

		Operation: 
			Fetch the data from MFMergeList and ShareHilding table
			and calculate the MF exposure and MF Rank
			MF Exposure = (quantity/outstanding_shares)*100

		Return:
			Value of MF Rank.
		"""

		mf_rank_unique_sql = 'SELECT DISTINCT ON("InvestedCompanyCode") * FROM public."MFMergeList" \
							WHERE "GenDate" = \'' +date.strftime("%Y-%m-%d")+ '\' ; '
		
		mf_rank_unique_list = sqlio.read_sql_query(mf_rank_unique_sql, con = conn)		


		stock_qty_sql = 'SELECT \
						"InvestedCompanyCode", \
						SUM ("Quantity") AS qty_sum \
						FROM \
						public."MFMergeList" \
						WHERE "GenDate" = \'' +date.strftime("%Y-%m-%d")+ '\'	\
						GROUP BY \
						"InvestedCompanyCode";'

		stock_qty_list = sqlio.read_sql_query(stock_qty_sql, con = conn)



		shareholding_sql = 'SELECT distinct on("CompanyCode") * FROM public."ShareHolding" \
							WHERE "SHPDate" <= \'' +date.strftime("%Y-%m-%d")+ '\'  \
							order by "CompanyCode", "SHPDate" desc ;'
							
		shareholding_list = sqlio.read_sql_query(shareholding_sql, con = conn)	

		shareholding_list[shareholding_list.columns[2:36]] = shareholding_list[shareholding_list.columns[2:36]].replace(r'[?$, ]', '', regex=True).astype(float)


		print("Index Size for MF: ", len(mf_rank_unique_list.index))


		for index, row in mf_rank_unique_list.iterrows():

			outstanding_shares_list = shareholding_list.loc[(shareholding_list["CompanyCode"]==row['InvestedCompanyCode'])]["Total"]
			outstanding_shares = outstanding_shares_list.item() if len(outstanding_shares_list.index) == 1 else np.nan

			quantity_list = stock_qty_list.loc[(stock_qty_list["InvestedCompanyCode"]==row['InvestedCompanyCode'])]['qty_sum']
			quantity = quantity_list.item() if len(quantity_list.index) == 1 else np.nan


			mf_exposure = (quantity/outstanding_shares)*100 if outstanding_shares != np.nan and quantity != np.nan else np.nan
			# mf_exposure = (quantity/outstanding_shares)*100 if outstanding_shares != np.nan and outstanding_shares != 0 and quantity != np.nan and quantity != 0 else np.nan


			mf_rank_unique_list.loc[index, 'Quantity'] = quantity
			mf_rank_unique_list.loc[index, 'OutstandingShares'] = outstanding_shares
			mf_rank_unique_list.loc[index, 'MFExposure'] = mf_exposure

		mf_rank_unique_list['MF Rank'] = mf_rank_unique_list['MFExposure'].rank(ascending=False)
		mf_rank_unique_list['MF Rank'] = ((len(mf_rank_unique_list.index)-mf_rank_unique_list['MF Rank']+1)/len(mf_rank_unique_list.index))*100


		return mf_rank_unique_list

	def insert_mf_rank(self, mf_rank_list, conn, cur, date):
		""" Insert MF Rank data into database

		Args:
			mf_rank_list = data of calculated MF Rank.

		Operation:
			Export the data into "mf_rank_export.csv" file 
			and insert into FRS-MFRank Table.
		"""


		mf_rank_list['Date'] = date.strftime("%Y-%m-%d")

		mf_rank_list = mf_rank_list[[ 'InvestedCompanyCode', 'InvestedCompanyName' , 'Date', \
									'ISINCode',  'Quantity', 'OutstandingShares', 'MFExposure', 'MF Rank']]


		exportfilename = "mf_rank_export.csv"
		exportfile = open(exportfilename,"w",encoding="utf-8")
		mf_rank_list.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
		exportfile.close()
			
		
		copy_sql = """
				COPY "Reports"."FRS-MFRank" FROM stdin WITH CSV HEADER
				DELIMITER as ','
				"""
		with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
			cur.copy_expert(sql=copy_sql, file=f)
			conn.commit()
			f.close()
		os.remove(exportfilename)

	def scheme_master_nav_list(self, conn, date):
		""" Fetch the data for scheme master nav list

		Operation:
			Fetch the data from SchemeMaster and SchemePortfolioHeader
			for SchemePlanCode 2066 and merge both data based on SchemeCode.

		Return:
			Merge data of SchemeMaster and SchemePortfolioHeader.
		"""

		scheme_sql = 'SELECT * FROM public."SchemeMaster" \
							WHERE "SchemePlanCode" = 2066 \
							AND "SchemeTypeDescription" = \'' +'Open Ended'+ '\' \
							and "SchemeName" @@ to_tsquery( \'' +'!Direct & !Institutional'+ '\');'
		scheme_list = sqlio.read_sql_query(scheme_sql, con = conn)

		# self.export_table("01_SchemeMaster", scheme_list)
		scheme_aum_sql = 'SELECT distinct on("SchemeCode") * FROM public."SchemePortfolioHeader" \
						WHERE "SchemePlanCode" = 2066 \
						AND "HoldingDate" <= \'' +date.strftime("%Y-%m-%d")+ '\' \
						order by "SchemeCode", "HoldingDate" desc ;'
		
		scheme_aum_list = sqlio.read_sql_query(scheme_aum_sql, con = conn)
		# self.export_table("02_SchemePortfolioHeader", scheme_aum_list)

		
		scheme_master_list = pd.merge(scheme_list, scheme_aum_list, left_on = 'SchemeCode', \
									right_on = 'SchemeCode' , how = 'left')
		# self.export_table("03_merge_left", scheme_master_list)
		
		scheme_master_list = scheme_master_list.rename(columns = {'TotalMarketValue' : 'AUM'})


		return scheme_master_list

	def merge_scheme_nav_master(self, scheme_master_list, conn):
		""" Fetch the data of merge scheme nav master

		Args:
			scheme_master_list = Merge data of SchemeMaster and SchemePortfolioHeader.

		Operation:
			Fetch the data from SchemeNAVMaster for SchemePlanCode 2066.
			and merge it with input data.

		Return:
			merge data of scheme master list and SchemeNAVMaster.
		"""

		scheme_nav_master_sql = 'SELECT * FROM public."SchemeNAVMaster" \
								WHERE "SchemePlanCode" = 2066 ;'

		scheme_nav_master_list = sqlio.read_sql_query(scheme_nav_master_sql, con = conn)

		# self.export_table("11_SchemeNAVMaster", scheme_nav_master_list)
		scheme_nav_merge_list = pd.merge(scheme_master_list, scheme_nav_master_list, left_on = 'SchemeCode', \
									right_on = 'SchemeCode' , how = 'left')
		
		scheme_nav_merge_list = scheme_nav_merge_list[['SchemeCode', 'SecurityCode' , 'HoldingDate' ,'SchemeName', 'SchemeTypeDescription', 'SchemePlanCode_x', \
													'SchemePlanDescription_x', 'SchemeClassCode', 'SchemeClassDescription', \
													'SchemeCategoryCode',  'SchemeCategoryDescription', 'MainCategory', 'SubCategory', 'AUM']]

		scheme_nav_merge_list = scheme_nav_merge_list.rename(columns = {'SchemePlanCode_x' : 'SchemePlanCode'})											
		scheme_nav_merge_list = scheme_nav_merge_list.rename(columns = {'SchemePlanDescription_x' : 'SchemePlanDescription'})	

		# self.export_table("12_merge_03_11", scheme_nav_merge_list)
		return scheme_nav_merge_list

	def get_mf_category_mapping(self, scheme_nav_merge_list, conn):
		""" Get the data from MF Category Mapping

		Args:
			scheme_nav_merge_list = merge data of scheme master list and SchemeNAVMaster.

		Operation:
			Fetch the data from mf_category_mapping table and merge it with input data.

		Return:
			Merge data of scheme nav merge list and SchemeNAVMaster
			to get BTT MF Categories for schemes.
		"""

		sql = 'SELECT "scheme_code", "btt_scheme_code", "btt_scheme_category" FROM public.mf_category_mapping;'
		btt_mf_category = sqlio.read_sql_query(sql, con = conn)

		# self.export_table("21_mf_category_mapping", btt_mf_category)
		# self.export_table("02_scheme_nav_merge_list", scheme_nav_merge_list)

		scheme_nav_merge_list = pd.merge(btt_mf_category, scheme_nav_merge_list, left_on='scheme_code',
										right_on='SchemeCode', how='left')

		# self.export_table("22_merge_21_12", scheme_nav_merge_list)
		scheme_nav_merge_list = scheme_nav_merge_list[pd.notnull(scheme_nav_merge_list['btt_scheme_category'])]
		scheme_nav_merge_list = scheme_nav_merge_list.drop(['scheme_code'], axis = 1)

		return scheme_nav_merge_list

	def get_scheme_nav_current_prices(self, scheme_nav_merge_list, conn, date):
		""" Get scheme nav current price

		Args:
		scheme_nav_merge_list = Merge data of scheme nav merge list and SchemeNAVMaster.
		
		Operation:
			Fetch the data from SchemeNAVCurrentPrices and merge it with scheme nav merge list 
			and calculate the and NAV Current Price for Rank 1Day | 1 Week | 1 Month | 3 Month |
			6 Month | 9 Month | 1 Year | 2 Year | 3 Year | 5 Year.

		Return:
			Value of NAV rank.
		"""

		back_date = date + datetime.timedelta(-3)

		scheme_nav_sql = 'SELECT distinct on("SecurityCode") * FROM public."SchemeNAVCurrentPrices" \
							WHERE "DateTime" >= \''+str(back_date)+'\' \
							ORDER BY "SecurityCode", "DateTime" DESC ;'
		scheme_nav_prices_list = sqlio.read_sql_query(scheme_nav_sql, con = conn)

		# self.export_table("31_SchemeNAVCurrentPrices", scheme_nav_prices_list)
		scheme_nav_prices_list = pd.merge(scheme_nav_merge_list, scheme_nav_prices_list, left_on = 'SecurityCode', right_on = 'SecurityCode', 
										how = 'left')

		# self.export_table("32_merge_22_31", scheme_nav_prices_list)
		scheme_nav_prices_list = scheme_nav_prices_list.rename(columns = {'SchemeCode_x' : 'SchemeCode', 'SchemeName_x' : 'SchemeName', \
																	'SchemeCategoryDescription_x' : 'SchemeCategoryDescription', 'AUM_x' : 'AUM' })


		scheme_nav_prices_list['PercentageChange'] = scheme_nav_prices_list['PercentageChange'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_prices_list['Prev1WeekPer'] = scheme_nav_prices_list['Prev1WeekPer'].replace(r'[?$, ]','', regex=True).astype(float)
		scheme_nav_prices_list['Prev1MonthPer'] = scheme_nav_prices_list['Prev1MonthPer'].replace(r'[?$, ]','', regex=True).astype(float)
		scheme_nav_prices_list['Prev3MonthsPer'] = scheme_nav_prices_list['Prev3MonthsPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_prices_list['Prev6MonthsPer'] = scheme_nav_prices_list['Prev6MonthsPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_prices_list['Prev9MonthsPer'] = scheme_nav_prices_list['Prev9MonthsPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_prices_list['PrevYearPer'] = scheme_nav_prices_list['PrevYearPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_prices_list['Prev2YearCompPer'] = scheme_nav_prices_list['Prev2YearCompPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_prices_list['Prev3YearCompPer'] = scheme_nav_prices_list['Prev3YearCompPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_prices_list['Prev5YearCompPer'] = scheme_nav_prices_list['Prev5YearCompPer'].replace(r'[?$, ]', '', regex=True).astype(float)


		scheme_nav_prices_list['SchemeCategoryDescription'] = scheme_nav_prices_list['SchemeCategoryDescription'].str.replace(" ", '') 


		scheme_nav_prices_list['1 Day Rank'] = scheme_nav_prices_list.groupby(["btt_scheme_category"])["PercentageChange"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['1 Day Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['1 Day Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['1 Week Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["Prev1WeekPer"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['1 Week Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['1 Week Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['1 Month Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["Prev1MonthPer"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['1 Month Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['1 Month Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['3 Month Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["Prev3MonthsPer"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['3 Month Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['3 Month Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['6 Month Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["Prev6MonthsPer"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['6 Month Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['6 Month Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['9 Month Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["Prev9MonthsPer"].rank(ascending=True, pct=True)	* 100	 											
		# scheme_nav_prices_list['9 Month Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['9 Month Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['1 Year Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["PrevYearPer"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['1 Year Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['1 Year Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['2 Year Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["Prev2YearCompPer"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['2 Year Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['2 Year Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['3 Year Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["Prev3YearCompPer"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['3 Year Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['3 Year Rank']+1)/len(scheme_nav_prices_list.index))*100

		scheme_nav_prices_list['5 Year Rank'] = scheme_nav_prices_list.groupby("btt_scheme_category")["Prev5YearCompPer"].rank(ascending=True, pct=True) * 100
		# scheme_nav_prices_list['5 Year Rank'] = ((len(scheme_nav_prices_list.index)-scheme_nav_prices_list['5 Year Rank']+1)/len(scheme_nav_prices_list.index))*100


		scheme_nav_prices_list = scheme_nav_prices_list[['SchemeCode', 'SecurityCode', 'HoldingDate', 'SchemeName', 'SchemeCategoryDescription', \
														'AUM', 'PrevNAVAmount' ,'PercentageChange', '1 Day Rank', 'Prev1WeekPer', '1 Week Rank', 'Prev1MonthPer', '1 Month Rank', \
														'Prev3MonthsPer', '3 Month Rank', 'Prev6MonthsPer', '6 Month Rank', 'Prev9MonthsPer', '9 Month Rank', \
														'PrevYearPer', '1 Year Rank', 'Prev2YearCompPer', '2 Year Rank', 'Prev3YearCompPer', '3 Year Rank', \
														'Prev5YearCompPer', '5 Year Rank', 'btt_scheme_category', 'btt_scheme_code']]

		return scheme_nav_prices_list

		# else:
			# print("NAV Rank cannot be calculated for the current date")
			# raise ValueError('MF NAV data not found for date: '+str(date))


	def calc_scheme_rank(self, scheme_nav_list, conn, date):
		""" Fetch the value of Scheme rank

		Args:
			scheme_nav_list = Data list of NAV rank.
		
		Return:
			data scheme NAV Value, Scheme Rank
			scheme rank = scheme rank value * 100.
			scheme rank value = (0.6)* one year growth + (0.4)*three year growth.
		 """


		for index, row in scheme_nav_list.iterrows():

			one_year_growth_list = scheme_nav_list.loc[scheme_nav_list["SecurityCode"] == row['SecurityCode']]["PrevYearPer"]

			one_year_growth = np.nan
			if len(one_year_growth_list) == 1:
				one_year_growth = one_year_growth_list.item()
			elif len(one_year_growth_list) > 1:
				unique_values = one_year_growth_list.unique()
				if len(unique_values) == 1:
					one_year_growth = unique_values[0]
				else:
					raise Exception("one_year_growth_list has multiple unique values")
				
			
			three_year_growth_list = scheme_nav_list.loc[scheme_nav_list["SecurityCode"] == row['SecurityCode']]['Prev3YearCompPer']
			
			three_year_growth = np.nan
			if len(three_year_growth_list) == 1:
				three_year_growth = three_year_growth_list.item()
			elif len(three_year_growth_list) > 1:
				unique_values = three_year_growth_list.unique()
				if len(unique_values) == 1:
					three_year_growth = unique_values[0]
				else:
					raise Exception("three_year_growth_list has multiple unique values")
			scheme_nav_list.loc[index, 'Scheme Rank Value'] = (0.6 * one_year_growth + 0.4 * three_year_growth) if not np.isnan(one_year_growth) and not np.isnan(three_year_growth) else np.nan
		
		scheme_nav_list['Scheme Rank'] = scheme_nav_list.groupby("btt_scheme_category")["Scheme Rank Value"].rank(ascending=True, pct=True) * 100
		return scheme_nav_list
 

	def insert_scheme_nav_rank(self, scheme_nav_list, conn, cur, date):
		""" Insert the NAV Rank data into database

		Args:
			scheme_nav_list = data of NAV Rank.

		Operation:
			export the data into scheme_nav_rank_export.csv file and 
			insert into FRS-NAVRank table.
		 """


		scheme_nav_list['PrevNAVAmount'] = scheme_nav_list['PrevNAVAmount'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['PercentageChange'] = scheme_nav_list['PercentageChange'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['Prev1WeekPer'] = scheme_nav_list['Prev1WeekPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['Prev1MonthPer'] = scheme_nav_list['Prev1MonthPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['Prev3MonthsPer'] = scheme_nav_list['Prev3MonthsPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['Prev6MonthsPer'] = scheme_nav_list['Prev6MonthsPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['Prev9MonthsPer'] = scheme_nav_list['Prev9MonthsPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['PrevYearPer'] = scheme_nav_list['PrevYearPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['Prev2YearCompPer'] = scheme_nav_list['Prev2YearCompPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['Prev3YearCompPer'] = scheme_nav_list['Prev3YearCompPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['Prev5YearCompPer'] = scheme_nav_list['Prev5YearCompPer'].replace(r'[?$, ]', '', regex=True).astype(float)
		scheme_nav_list['AUM'] = scheme_nav_list['AUM'].replace(r'[?$, ]', '', regex=True).astype(float)


		scheme_nav_list['Date'] = date.strftime("%Y-%m-%d")


		scheme_nav_list = scheme_nav_list.rename(columns = {'PrevNAVAmount' : 'Current', 'PercentageChange' : '1 Day', 'Prev1WeekPer' : '1 Week', 'Prev1MonthPer' : '1 Month', \
															'Prev3MonthsPer' : '3 Month', 'Prev6MonthsPer' : '6 Month', 'Prev9MonthsPer' : '9 Month', \
															'PrevYearPer' : '1 Year', 'Prev2YearCompPer' : '2 Year', 'Prev3YearCompPer' : '3 Year', \
															'Prev5YearCompPer' : '5 Year'})

		scheme_nav_list = scheme_nav_list[['SchemeCode', 'SchemeName', 'Date', 'Current', '1 Day', '1 Day Rank', '1 Week', \
										'1 Week Rank', '1 Month', '1 Month Rank', '3 Month', '3 Month Rank', '6 Month', '6 Month Rank', '9 Month', \
										'9 Month Rank', '1 Year', '1 Year Rank', '2 Year', '2 Year Rank', '3 Year', '3 Year Rank', '5 Year', '5 Year Rank', \
										'AUM', 'Scheme Rank', 'btt_scheme_category', 'btt_scheme_code']]


								
		print("Inserting Scheme NAV List: ")

		
		exportfilename = "scheme_nav_rank_export.csv"
		exportfile = open(exportfilename,"w")
		scheme_nav_list.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
		exportfile.close()
			
		
		copy_sql = """
				COPY "Reports"."FRS-NAVRank" FROM stdin WITH CSV HEADER
				DELIMITER as ','
				"""
		with open(exportfilename, 'r') as f:
			cur.copy_expert(sql=copy_sql, file=f)
			conn.commit()
			f.close()
		os.remove(exportfilename)
		

		return scheme_nav_list
		
	def calc_average_scheme_category(self, scheme_nav_list, conn):
		""" Fetch the data of calculated average scheme category.

		Args: 
			scheme_nav_list = data of scheme NAV Value, Scheme Rank.

		Return:
			Average of range 1 Day | 1 Week | 1 Month |
			3 Month | 6 Month |9 Month | 1 Year | 2 Year | 3 Year | 5 Year.
		"""


		scheme_nav_avg_list = pd.DataFrame(columns = ['Date', '1 Day Avg', '1 Week Avg', '1 Month Avg', '3 Month Avg', \
														'6 Month Avg', '9 Month Avg', '1 Year Avg', '2 Year Avg', '3 Year Avg', '5 Year Avg' ])

		
		scheme_nav_avg_list['1 Day Avg'] = scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["1 Day"].mean()

		scheme_nav_avg_list['1 Week Avg'] = scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["1 Week"].mean()

		scheme_nav_avg_list['1 Month Avg'] = scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["1 Month"].mean()

		scheme_nav_avg_list['3 Month Avg'] = scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["3 Month"].mean()

		scheme_nav_avg_list['6 Month Avg'] =scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["6 Month"].mean()

		scheme_nav_avg_list['9 Month Avg'] = scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["9 Month"].mean()

		scheme_nav_avg_list['1 Year Avg'] = scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["1 Year"].mean()

		scheme_nav_avg_list['2 Year Avg'] = scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["2 Year"].mean()

		scheme_nav_avg_list['3 Year Avg'] = scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["3 Year"].mean()

		scheme_nav_avg_list['5 Year Avg'] =scheme_nav_list.groupby(['btt_scheme_category'], as_index=True)["5 Year"].mean()

		scheme_nav_avg_list = scheme_nav_avg_list.reset_index()

		return scheme_nav_avg_list

	def insert_scheme_nav_category_avg(self, scheme_nav_category_avg, conn, cur, date):
		""" Insert the scheme NAV Category average value into data base

		Args:
			scheme_nav_category_avg = data of NAV Category average.
		
		Operation:
			Export the data into scheme_nav_category_avg_export.csv file
			and Insert into FRS-NAVCategoryAvg table.
		"""

		scheme_nav_category_avg['Date'] = date.strftime("%Y-%m-%d")


		scheme_nav_category_avg = scheme_nav_category_avg[['btt_scheme_category', 'Date', '1 Day Avg', '1 Week Avg', '1 Month Avg', '3 Month Avg', \
														'6 Month Avg', '9 Month Avg', '1 Year Avg', '2 Year Avg', '3 Year Avg', '5 Year Avg']]
		
		scheme_nav_category_avg.dropna(how = 'any', inplace=True)							  
		
		
		print("Inserting Scheme NAV category average List: ")

		
		exportfilename = "scheme_nav_category_avg_export.csv"
		exportfile = open(exportfilename,"w")
		scheme_nav_category_avg.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
		exportfile.close()
			
		
		copy_sql = """
				COPY "Reports"."FRS-NAVCategoryAvg" FROM stdin WITH CSV HEADER
				DELIMITER as ','
				"""
		with open(exportfilename, 'r') as f:
			cur.copy_expert(sql=copy_sql, file=f)
			conn.commit()
			f.close()
		os.remove(exportfilename)
		
	def compile_mf_list(self, conn,cur,date):
		""" Compile the data of MF List

		Operation:
			Fetch the data from Scheme Master, schemewise portfolio,
			Indutry mapping and Market Cap, to calculate the 
			data of MF List.
		"""

		global today
		today = date


		print("Filtering schemes from Scheme Master")
		scheme_mf_list = self.get_scheme_master_list(conn, today)

		# print("Data \n",scheme_mf_list.head())

		if not(scheme_mf_list.empty):

			print("Merging filtered list with schemewise portfolio")
			scheme_mf_list =  self.merge_schememaster_schemeportfolio(scheme_mf_list, conn, today)

			print("Merging with Indutry mapping")
			scheme_mf_list = self.merge_background_industry_info(scheme_mf_list, conn)

			print("Calculating Market Cap")
			scheme_mf_list = self.map_market_capitalisation(scheme_mf_list, conn, today)

			print("Inserting into MFList")
			self.insert_mutualfund_list(scheme_mf_list, conn, cur, today)
		
		else:
			print("Data not present for Filtering schemes from Scheme Master for ...Date: ",today)

	def calc_mf_rank(self, conn,cur,date):
		""" Calculate MF rank

		Operation:
			Fetch the data from MF Exposure and Rank,
			and calculating the MF Rank.	
		"""

		global today
		today = date

		print("Calculating MF Exposure and Rank")
		mf_rank_list = self.calc_mf_exposure(conn, date)

		# print(mf_rank_list)

		print("Inserting into table")
		self.insert_mf_rank(mf_rank_list, conn, cur, date)

	def generate_history_mflist(self, conn, cur):
		"""Fetch the data of compile mf list
			and generate the history for MF list """

		start_date = datetime.date(2020, 8, 1)
		end_date = datetime.date(2020, 8, 31)

		while(end_date>=start_date):

			print("\nGenerating MF List for date:", start_date)
			self.compile_mf_list(conn,cur,start_date)
			print("Compiled MF merge list")

			start_date = start_date + datetime.timedelta(1)

	def generate_history_mfrank(self, conn, cur):
		"""Fetch the data of Completed MF Rank
			and generate history for MF rank 
		"""

		start_date = datetime.date(2020, 8, 1)
		end_date = datetime.date(2020, 8, 31)

		while(end_date>=start_date):

			print("\nGenerating MF Report for date:", start_date)		
			self.calc_mf_rank(conn,cur,start_date)
			print("Completed MF Rank")
			
			start_date = start_date + datetime.timedelta(1)

	def generate_current_mflist(self, conn,cur, curr_date):
		""" Generate MF List 
		
		Operation:
			Fetch the data compile MF List,
			and generate the MF list for current date.
		"""

		print("\nGenerating MF List for today:", curr_date)
		self.compile_mf_list(conn,cur,curr_date)
		print("Compiled MF merge list")

	def generate_current_mfrank(self, curr_date, conn,cur):
		""" Generate current mfrank,

		Operation:
			fetch the data of MFList and MF Rank for current date,
			and generate MF Rank for current date.
		"""

		self.generate_current_mflist(conn,cur, curr_date)
		self.calc_mf_rank(conn,cur,curr_date)

		print("Completed MF Rank generation")

	def export_table(self, name,table):
		exportfilename = "FRS_"+name+"_export.csv"
		exportfile = open(exportfilename,"w")

		table.to_csv(exportfile, header=True, index=False, float_format="%2f", lineterminator='\r')
		exportfile.close()

	def calc_nav_rank(self, conn,cur,date):
		""" Calculate value of NAV Rank
		
		Operation:
			Fetch the data from scheme master, SchemeNavMaster, BTT MF Categories,
			NAV current prices data, Scheme Rank and average of each group.
			to calculate NAV Rank.
		"""

		today = date

		print("Filtering schemes from scheme master")
		scheme_nav_list = self.scheme_master_nav_list(conn, today)
		# # self.export_table("1_scheme_master_nav_list", scheme_nav_list)

		print("Merging with SchemeNavMaster")
		scheme_nav_list = self.merge_scheme_nav_master(scheme_nav_list, conn)
		# # self.export_table("21_merge_scheme_nav_master", scheme_nav_list)

		print("Getting BTT MF Categories")
		scheme_nav_list = self.get_mf_category_mapping(scheme_nav_list, conn)
		# # self.export_table("22_get_mf_category_mapping", scheme_nav_list)

		print("Getting Scheme NAV current prices data")
		scheme_nav_list = self.get_scheme_nav_current_prices(scheme_nav_list, conn, today)
		# # self.export_table("3_get_scheme_nav_current_prices", scheme_nav_list)

		print("Calculating Scheme Rank")
		scheme_nav_list = self.calc_scheme_rank(scheme_nav_list, conn, today)
		# # self.export_table("4_calc_scheme_rank", scheme_nav_list)

		print("Inserting Scheme NAV Rank in table")
		scheme_nav_list = self.insert_scheme_nav_rank(scheme_nav_list, conn, cur, today)
		# # self.export_table("5_insert_scheme_nav_rank", scheme_nav_list)

		print("Calculating average of each group")
		scheme_nav_category_avg = self.calc_average_scheme_category(scheme_nav_list, conn)
		# # self.export_table("6_calc_average_scheme_category", scheme_nav_list)

		# raise Exception('CUT')
		print("Inserting NAV Category Average in table")
		self.insert_scheme_nav_category_avg(scheme_nav_category_avg, conn, cur, today)

	def generate_history_nav_rank(self, conn, cur):
		"""Fetch the data of NAV Rank and category average,
			to Generate NAV Rank History.
		"""

		start_date = datetime.date(2016, 1, 31)
		end_date = datetime.date(2019, 7, 12)

		while(end_date>=start_date):

			print("\nGenerating NAV Rank and category average for date:", start_date)	
			self.calc_nav_rank(conn,cur,start_date)
			print("Completed NAV Rank and category average")	

			start_date = start_date + datetime.timedelta(1)

	def generate_current_nav_rank(self, curr_date, conn,cur):
		"""Fetch the data of NAV Rank and category average  
			to generate history of NAV rank for current date.
		"""

		print("\nGenerating NAV Rank and category average for today:", curr_date)
		self.calc_nav_rank(conn,cur,curr_date)
		print("Completed NAV Rank and category average")

