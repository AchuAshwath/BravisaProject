#Script for compiling and inserting BTT-List

import datetime
from utils.db_helper import DB_Helper
from utils.check_helper import Check_Helper
import requests 
requests.adapters.DEFAULT_RETRIES = 5
import os
import csv
import pandas as pd
import psycopg2
import numpy as np
import pandas.io.sql as sqlio
import utils.date_set as date_set
import rootpath

# my_path = os.path.abspath(os.path.dirname(__file__))
my_path = os.getcwd()	#working directory
filepath = os.path.join(my_path, "index-files\\")


def dropby_marketcap(table_btt, conn, curr_date):
	#TODO: Drop stocks by market cap
	#get public.pe for all the companies in table_btt
	pe_sql = 'select * from public."PE" where "GenDate"<=\''+curr_date.strftime("%Y-%m-%d")+'\';'
	pe_table = sqlio.read_sql_query(pe_sql, conn)

	#filter CompanyCode from table_btt from pe_table and get all the pe_table values of table_btt companies
	pe_table = pe_table[pe_table['CompanyCode'].isin(table_btt['CompanyCode'])]

	#for matching CompanyCode, get the market cap from pe_table and drop the rows from table_btt	
	for index, row in table_btt.iterrows():
		company_code = row['CompanyCode']
		pe_row = pe_table[pe_table['CompanyCode']==company_code]
		# if market cap value is less than 2 crore, drop the row
		if not pe_row.empty and pe_row['Market Cap Value'].values[0] < 20000000:
			table_btt.drop(index, inplace=True)
	return table_btt


#Function to fetch NSE Index file and store it in provided file path
def fetch_nse_index():
	print("\n\nNSE Index Fetch invoked ....")

	# ## Headers necessary to access NSE website
	# headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8', 'Connection': 'keep-alive', 'Content-Type': 'application/zip', 'Referer': 'https://www.nseindia.com/products/content/derivatives/equities/archieve_fo.htm', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36', 'X-frame-options': 'SAMEORIGIN'}
	# cookies = {'pointer':'1','sym1':'KPIT', 'NSE-TEST-1':'1826627594.20480.0000'}
	# file_url = "https://www1.nseindia.com/content/indices/ind_nifty500list.csv"
	file_name = "ind_nifty500list.csv"

	# req = requests.get(file_url, headers=headers, cookies=cookies)
	# if req.status_code == 200:

	# 	print("Downloading File....")
	# 	path_to_store_file = open(filepath + file_name, "wb")
	# 	for chunk in req.iter_content(100000):
	# 		path_to_store_file.write(chunk)
	# 	path_to_store_file.close()
	# 	print("Completed")
	# else:
	# 	print ("Can't fetch the file: "+str(req.status_code))
	csv_file=filepath+file_name
	print(csv_file)
	if os.path.isfile(csv_file):
		table_nse = pd.read_csv(csv_file)
	else:
		print ("Can't fetch the file: ",file_name)

	# os.remove(csv_file)	
	
	return table_nse

#Function to fetch BSE Index file and store it in provided file path
def fetch_bse_index():

	print("\n\nBSE Index Fetch invoked ....")

		#Cookies, Params, Headers, Data necessary to access BSE website (invokes js function to download, curl'd from website)
	# '''cookies = {
	#     '_ga': 'GA1.2.1485200485.1543222416',
	#     '__gads': 'ID=a4e75adfbe7cde41:T=1543222417:S=ALNI_Ma82iq6VzNRRGTe5sDkfoTY84J9oA',
	#     '__auc': '272c8b5a1674f398d7ec5cccf2d',
	#     '__utmz': '253454874.1543407023.3.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)',
	#     '__utma': '253454874.1485200485.1543222416.1543670909.1543917949.6',
	#     '_gid': 'GA1.2.756563302.1545203860',
	# }

	# headers = {
	#     'Connection': 'keep-alive',
	#     'Cache-Control': 'max-age=0',
	#     'Origin': 'https://www.bseindia.com',
	#     'Upgrade-Insecure-Requests': '1',
	#     'Content-Type': 'application/x-www-form-urlencoded',
	#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
	#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
	#     'Referer': 'https://www1.bseindia.com/sensex/IndicesWatch_Weight.aspx?iname=BSE500&index_Code=17',
	#     'Accept-Encoding': 'gzip, deflate, br',
	#     'Accept-Language': 'en-US,en;q=0.9',
	# }

	# params = (
	#     ('iname', 'BSE500'),
	#     ('index_Code', '17'),
	# )

	# data = {
	#   '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$lnkDownload',
	#   '__EVENTARGUMENT': '',
	#   '__VIEWSTATE': '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',
	#   '__VIEWSTATEGENERATOR': '46F7555C',
	#   '__EVENTVALIDATION': '/wEdAA2+8JNm1gj03b+KOYSGmVL3a6tHl5dHwpE//d/Tlsvd5/ALACk6aQjbwRyDSy5RQ/SzYXYm8AXk2F0LBJNCd4MNkjlEcxz13fOOGXL6aHiyi3UYtTCy57ObpGZnb9HItR8/yYVwMVDgsp6RilsrFd/pxH0lxUdj+bF69gI10Osm4M7DQFpzO9kJRd5dT+igGB9lrjd4UV08ISMDJdsUiLUrK8f8P17breHYgV4iyKKhmRXw9aT0CYAUf7n/sglAnv9Kx/IejI3b2cmtSM7fsLcmVPN4DqnoVpKG5YmP2uUCPS29uhTIbsC4EuG61ygHo4w=',
	#   'ctl00$ContentPlaceHolder1$hdnCode': ''
	# }

	# # response = requests.post('https://www.bseindia.com/sensex/IndicesWatch_Weight.aspx', headers=headers, params=params, cookies=cookies, data=data)

	# #NB. Original query string below. It seems impossible to parse and
	# #reproduce query strings 100% accurately so the one below is given
	# #in case the reproduced version is not "correct".
	# # response = requests.post('https://www.bseindia.com/sensex/IndicesWatch_Weight.aspx', headers=headers, cookies=cookies, data=data)'''

	file_name = "BSE500_Index.csv"

	# '''req = requests.post('https://www.bseindia.com/sensex/IndicesWatch_weight.aspx', headers=headers, params=params, cookies=cookies, data=data)
	# if req.status_code == 200:
	# 	print("Downloading File....")
	# 	path_to_store_file = open(filepath + file_name, "wb")
	# 	for chunk in req.iter_content(100000):
	# 		path_to_store_file.write(chunk)
	# 	path_to_store_file.close()
	# 	print("Completed")
	# else:
	# 	print ("Can't fetch the file: "+str(req.status_code))

	# ## Manually run the following curl to verify url is active
	# ## curl 'https://www.bseindia.com/sensex/IndicesWatch_weight.aspx?iname=BSE500&index_Code=17' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' -H 'Origin: https://www.bseindia.com' -H 'Upgrade-Insecure-Requests: 1' -H 'Content-Type: application/x-www-form-urlencoded' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8' -H 'Referer: https://www.bseindia.com/sensexview/IndicesWatch_weight.aspx?iname=BSE500&index_Code=17' -H 'Accept-Encoding: gzip, deflate, br' -H 'Accept-Language: en-US,en;q=0.9' -H 'Cookie: _ga=GA1.2.1485200485.1543222416; __gads=ID=a4e75adfbe7cde41:T=1543222417:S=ALNI_Ma82iq6VzNRRGTe5sDkfoTY84J9oA; __auc=272c8b5a1674f398d7ec5cccf2d; __asc=d473fa051675a3749ef68dabafe; _gid=GA1.2.595705810.1543406832; gettabs=0; __utmc=253454874; ivtab2=0; ivtab1=0; __utma=253454874.1485200485.1543222416.1543406854.1543407023.3; __utmz=253454874.1543407023.3.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); ASP.NET_SessionId=popzzlrwckenpk45h34cmiqb; __utmb=253454874.13.10.1543407023; expandable=0c' --data '__EVENTTARGET=ctl00%24ContentPlaceHolder1%24lnkDownload&__EVENTARGUMENT=&__VIEWSTATE=%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%2B2MMo1Mygg&__VIEWSTATEGENERATOR=0BB80A06&__EVENTVALIDATION=%2FwEWDQKJ3uShAgL8np6XAwLl44LQAgKN%2BtDQCAKN%2BqzQCAKN%2BrjQCAKN%2BrTQCAKN%2BqDQCAKN%2BrzQCAKN%2BojQCAKN%2BoTQCALj8ODgCQKI2cKdDM5ba82BaGS0AZUHVJvjt1IGSGk%2F&myDestination=%23&WINDOW_NAMER=1&ctl00%24ContentPlaceHolder1%24hdnCode=' --compressed > BSE.csv
	# '''

	csv_file=filepath+file_name
	
	if os.path.isfile(csv_file):
		table_bse = pd.read_csv(csv_file)
	else:
		print ("Can't fetch the file: ",file_name)

	# os.remove(csv_file)	

	return table_bse


def generate_btt_list(table_nse, table_bse, conn, cur, curr_date):

	today_date = curr_date.strftime("%Y-%m-%d")
	#Merge NSE and BSE
	table_btt = pd.merge(table_nse, table_bse, left_on='ISIN Code', right_on='ISIN No.', how='outer')
	print("NSE COUNT: {} BSE COUNT: {}".format(len(table_nse.index), len(table_bse.index)))
	export_table("NSE", table_nse)
	export_table("BSE", table_bse)
	print("\t\t Table count right after merge: ", today_date, len(table_btt.index), flush=True)
	#Fill in Company and ISIN from BSE where null
	table_btt["Company Name"].fillna(table_btt["COMPANY"], inplace=True)
	table_btt["ISIN Code"].fillna(table_btt["ISIN No."], inplace=True)

	#Rename Columns
	table_btt.rename(columns = {'Company Name':'Company', 'ISIN Code':'ISIN', 'Symbol':'NSECode', 'Scrip Code':'BSECode', 'Series':'NSESeries'}, inplace = True)

	#Delete unwanted rows
	table_columns = ["Company", "ISIN", 'NSECode', 'NSESeries', 'BSECode']	
	csv_columns = list(table_btt.columns.values)
	columns_to_remove = [x for x in csv_columns if x not in table_columns]
	table_btt = table_btt.drop(columns_to_remove, axis=1)

	#Add Date and MFList (default false) columns
	table_btt["Date"] = today_date
	table_btt["MFList"] = False 

	#TODO: Populate MFList by infering data from FB. 

	#NSE Series and other unwanted colums are dropped
	table_btt = table_btt[['Company','ISIN','NSECode','BSECode', 'Date', 'MFList']]

	

	return table_btt

def ohlc_30day_check(table_btt, conn, curr_date):

	today_date = curr_date.strftime("%Y-%m-%d")
	month_back = (curr_date+datetime.timedelta(-30)).strftime("%Y-%m-%d")

	#Specific Case Condition: where ("NSECode"=\''+nse_code+'\' OR "BSECode"='+str(bse_code)+') AND 
	sql = 'select * from public."OHLC" where ("Date" <= \''+today_date+'\' and "Date" >= \''+month_back+'\');'
	ohlc_company = sqlio.read_sql_query(sql, conn)
	
	count = 0

	#Remove rows that don't have OHLC data
	for index,row in table_btt.iterrows():
		
		nse_code = row['NSECode'] 
		nse_code = nse_code	if not (nse_code=='' or nse_code==' ' or nse_code is np.nan) else "-3"
		bse_code = row['BSECode'] 
		bse_code = bse_code	if not np.isnan(bse_code) else -3
		bse_code = int(bse_code)
		
		
		if(ohlc_company[(ohlc_company['NSECode']==nse_code)].empty and ohlc_company[(ohlc_company['BSECode']==bse_code)].empty):
			print("Dropping: "+row['CompanyName']+"  "+row['ISIN'])
			table_btt.drop(index, inplace=True)
			count += 1

	print("\n\nDropped Table Count: ", count)

	return table_btt	


def get_backgroundtable(conn):

	background_info_sql ='select *  from public."BackgroundInfo";'
	background_info = pd.read_sql_query(background_info_sql,con=conn)

	#Fix NSE/BSE Codes in BGinfo file
	#background_info.loc[background_info['BSECode']<=0]['BSECode'] = -1
	replace_values = {np.nan : "-1", " " : "-1", "" : "-1" }
	background_info = background_info.replace({"NSECode": replace_values})
	replace_values = {np.nan : -1,  0: -1}	
	background_info = background_info.replace({"BSECode": replace_values})	
	# export_table("Background",background_info)

	return background_info

def merge_MFList(BTTList, conn, curr_date):

	month_back = (curr_date+datetime.timedelta(-30)).strftime("%Y-%m-%d")
	last_mfholding = (curr_date+datetime.timedelta(-60)).strftime("%Y-%m-%d")
	print("MONTH BACK: {} LAST_MFHOLDINGDate: {}".format(month_back, last_mfholding))
	# raise Exception("break for testing")
	MFList_sql ='select distinct on ("InvestedCompanyCode") "InvestedCompanyCode", "HoldingDate"  from public."SchemewisePortfolio" where "HoldingDate" >= \''+ last_mfholding + '\' and "HoldingDate" <= \''+month_back+'\' and "SchemePlanCode"=2066 and "InvestedCompanyCode">0 and "InvestedCompanyCode" is not null;'
	MFList = pd.read_sql_query(MFList_sql,con=conn)
	print("\t\t Total row count in MF List: ", len(MFList.index), flush = True)
	background_info = get_backgroundtable(conn)

	background_info = background_info[(background_info['CompanyCode'] != np.nan)]
	print("\t\t Total row count in background_info: ", len(background_info.index), flush = True)
	MFList = pd.merge(MFList, background_info[['CompanyName', 'BSECode', 'NSECode', 'CompanyCode', 'ISINCode']], left_on='InvestedCompanyCode', right_on='CompanyCode', how='left')

	MFList['MFList'] = True
	MFList = MFList.drop(['CompanyCode', 'InvestedCompanyCode'], axis=1)
	MFList.rename(columns = {'CompanyName':'Company', 'HoldingDate':'Date', 'ISINCode':'ISIN'}, inplace = True)	
	MFList = MFList[['Company','ISIN','NSECode','BSECode', 'Date', 'MFList']]
	MFList = MFList[MFList.ISIN.notnull()]
	print("\t\t Total row count in MF List background_info merge: ", len(MFList.index), flush = True)

	# export_table("MFList", MFList)

	#This is the full BTT list - MF + NSE500 + BSE500
	BTTList = pd.merge(BTTList, MFList, left_on='ISIN', right_on='ISIN', how='outer')
	print("\t\t Total row count in BTTList: (After All the merges) ", len(BTTList.index), flush = True)
	BTTList["Company_x"].fillna(BTTList["Company_y"], inplace=True)
	BTTList["NSECode_x"].fillna(BTTList["NSECode_y"], inplace=True)
	BTTList["BSECode_x"].fillna(BTTList["BSECode_y"], inplace=True)
	BTTList["Date_x"].fillna(BTTList["Date_y"], inplace=True)
	BTTList["MFList_y"].fillna(BTTList["MFList_x"], inplace=True)
	BTTList.rename(columns = {'Company_x':'Company', 'Date_x':'Date', 'NSECode_x':'NSECode', 'BSECode_x':'BSECode', 'MFList_y':'MFList'}, inplace = True)	
	
	table_columns = ["Company", "ISIN", 'NSECode', 'MFList', 'BSECode', 'Date']	
	csv_columns = list(BTTList.columns.values)
	columns_to_remove = [x for x in csv_columns if x not in table_columns]
	BTTList = BTTList.drop(columns_to_remove, axis=1)
	BTTList = BTTList[['Company','ISIN','NSECode','BSECode', 'Date', 'MFList']]
	BTTList = BTTList.replace({"NSECode": {"-1":np.nan}})

	return BTTList	

def get_BackgroundInfo(BTTList, conn):	

	background_info = get_backgroundtable(conn)

	background_info_nse = background_info[(background_info['NSECode']!=np.nan) & (background_info['NSECode']!=" ") & (background_info['NSECode']!="") & (background_info['NSECode']!="-1")]

	#Merge BackgroundInfo and BTTList
	BTTList = pd.merge(BTTList, background_info_nse[['CompanyName', 'BSECode', 'NSECode', 'CompanyCode']], left_on='NSECode', right_on='NSECode', how='left')
	BTTList["CompanyName"].fillna(BTTList["Company"], inplace=True)
	BTTList["BSECode_x"].fillna(BTTList["BSECode_y"], inplace=True)

	background_info_bse = background_info[(background_info['BSECode']!=np.nan) & (background_info['BSECode']>0)]	

	BTTList = pd.merge(BTTList, background_info_bse[['CompanyName', 'BSECode', 'NSECode', 'CompanyCode']], left_on='BSECode_x', right_on='BSECode', how='left')
	BTTList = BTTList.replace({"NSECode_y": {"-1":np.nan}})
	BTTList["NSECode_x"].fillna(BTTList["NSECode_y"], inplace=True)
	BTTList["CompanyCode_x"].fillna(BTTList["CompanyCode_y"], inplace=True)	
	# export_table("preclearn",BTTList)

	#Drop incorrect columns in favor of merged columns
	BTTList = BTTList.drop(['BSECode', 'Company'], axis=1)
	
	#Rename Columns
	BTTList.rename(columns = {'CompanyName_x':'CompanyName', 'NSECode_x':'NSECode', 'BSECode_x':'BSECode', 'CompanyCode_x':'CompanyCode'}, inplace = True)

	#Delete unwanted rows
	table_columns = ['CompanyName', 'ISIN', 'NSECode', 'BSECode', 'Date', 'MFList', 'CompanyCode']
	csv_columns = list(BTTList.columns.values)
	columns_to_remove = [x for x in csv_columns if x not in table_columns]
	BTTList = BTTList.drop(columns_to_remove, axis=1)
	BTTList = BTTList[['CompanyName','ISIN','NSECode','BSECode', 'Date', 'MFList', 'CompanyCode']]
	BTTList = BTTList.replace({"NSECode": {"-1" : np.nan}})
	BTTList = BTTList.replace({"BSECode": {-1 : np.nan}})

	return BTTList


def export_table(name,table):
	exportfilename = name+"_export.csv"
	exportfile = open(exportfilename,"w")

	table.to_csv(exportfile, header=True, index=False, float_format="%2f", lineterminator='\r')
	exportfile.close()


def insert_BTT(BTTList, conn, cur):

	BTTList = BTTList[['CompanyName','ISIN','NSECode','BSECode', 'Date', 'MFList', 'BTTDate','CompanyCode']]
	BTTList = BTTList.drop_duplicates(subset=['CompanyCode', 'BTTDate'])
	BTTList["BSECode"].fillna(-1, inplace=True)
	BTTList = BTTList.astype({"BSECode": int})
	BTTList = BTTList.astype({"BSECode": str})
	BTTList["BSECode"] = BTTList["BSECode"].replace('-1', np.nan)

	##
	BTTList = BTTList.dropna(subset=['CompanyCode'], inplace=False)
	BTTList = BTTList.dropna(subset=['BTTDate'], inplace=False)
	##
	export_table("BTTList", BTTList)

	copy_sql = """
           COPY public."BTTList" FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """

	with open("BTTList_export.csv", 'r') as f:
		cur.copy_expert(sql=copy_sql, file=f)
		conn.commit()
		
	# export_table("test_BTTList", BTTList)
	os.remove("BTTList_export.csv")
	
def main(curr_date):
	conn = DB_Helper().db_connect()
	cur = conn.cursor()

	print("\n\t\t BTT Fetch Service Started..........")
	Check_Helper().check_path(filepath)

	table_nse = fetch_nse_index()
	export_table("NSE_january", table_nse)
	table_bse = fetch_bse_index()
	export_table("BSE_january", table_bse)

	print("\nGenerating BTTList from NSE/BSE 500")
	BTTList = generate_btt_list(table_nse, table_bse, conn, cur, curr_date)
	# export_table("NSE_BSE500", BTTList)
	print("Total Row Count: ", len(BTTList.index))

	print("\nMerging MFList into BTTList")
	BTTList = merge_MFList(BTTList, conn, curr_date)
	export_table("MFList_january", BTTList)
	#this is the total collated list - check for total count
	print("Total Row Count: ", len(BTTList.index))

	print("\nAdding data from BackgroundInfo")
	#this is the companycode, bginfo merge function - may get dropped if stock is not tracked in FB
	BTTList = get_BackgroundInfo(BTTList,conn)
	# export_table("BackInfoMerge",BTTList)

	print("\nDropping Stocks without any OHLC data in 30 days")
	BTTList = ohlc_30day_check(BTTList, conn, curr_date)
	print("Total Row Count: ", len(BTTList.index))

	today_date = curr_date.strftime("%Y-%m-%d")

	BTTList['BTTDate'] = today_date

	BTTList = dropby_marketcap(BTTList, conn, curr_date)

	insert_BTT(BTTList, conn, cur)
	export_table("BTTList_january", BTTList)

	print("\n BTT Fetch Completed.")
	conn.close()
