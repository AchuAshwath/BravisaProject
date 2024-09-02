# Python script to calculate price earnings of OHLC data and capture high/low values for history and current
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


# get ohlc list for current date and merge with background to get company code
def get_ohlc_list(conn, date):

    ohlc_sql = 'SELECT * FROM public."OHLC" ohlc \
				where "Date" = \'' + date.strftime("%Y-%m-%d") + '\' \
				and "CompanyCode" is not null ;'
    
    pe_list = sqlio.read_sql_query(ohlc_sql, con=conn)

    return pe_list


# Calculate market cap for OHLC data and Price Earnings and rank them based on market cap value
def pe_calc(pe_list, conn, date):

    shareholding_sql = 'SELECT distinct on("CompanyCode") * FROM public."ShareHolding" \
						WHERE "SHPDate" <= \'' + date.strftime("%Y-%m-%d") + '\'  \
						order by "CompanyCode", "SHPDate" desc ;'
    
    shareholding_list = sqlio.read_sql_query(shareholding_sql, con=conn)

    ttm_sql = 'SELECT DISTINCT ON("CompanyCode") * FROM public."TTM" \
			   WHERE "YearEnding" <= \'' + date.strftime("%Y-%m-%d") + '\'  \
			   ORDER by "CompanyCode", "YearEnding" DESC ;'
    
    ttm_list = sqlio.read_sql_query(ttm_sql, con=conn)

    shareholding_list['Total'] = shareholding_list['Total'].replace(
        '[?$,]', '', regex=True).astype(float)

    for index, row in pe_list.iterrows():

        if (row['NSECode'] is None):

            ohlc_close_list = pe_list.loc[(
                pe_list["BSECode"] == row['BSECode'])]["Close"]
            ohlc_close = ohlc_close_list.item() if len(
                ohlc_close_list.index) == 1 else np.nan

        else:

            ohlc_close_list = pe_list.loc[(
                pe_list["NSECode"] == row['NSECode'])]["Close"]
            ohlc_close = ohlc_close_list.item() if len(
                ohlc_close_list.index) == 1 else np.nan
            
        os_shares_list = shareholding_list.loc[(
            shareholding_list["CompanyCode"] == row['CompanyCode'])]["Total"]
        os_shares = os_shares_list.item() if len(os_shares_list.index) == 1 else np.nan

        earnings_list = ttm_list.loc[(
            ttm_list["CompanyCode"] == row['CompanyCode'])]["PAT"]
        earnings = earnings_list.item() if len(earnings_list.index) == 1 else np.nan

        market_cap_value = ohlc_close * os_shares

        price_earnings = market_cap_value/earnings if earnings != 0 else np.nan

        pe_list.loc[index, 'Market Cap Value'] = market_cap_value
        pe_list.loc[index, 'PE'] = price_earnings

    pe_list['Market Cap Rank'] = pe_list['Market Cap Value'].rank(
        ascending=False)
    pe_list = pe_list.sort_values(by='Market Cap Value', ascending=False)

    for index, row in (pe_list.iloc[0:100]).iterrows():

        market_cap_class = "Large Cap"
        pe_list.loc[index, 'Market Cap Class'] = market_cap_class

    for index, row in (pe_list.iloc[100:250]).iterrows():

        market_cap_class = "Mid Cap"
        pe_list.loc[index, 'Market Cap Class'] = market_cap_class

    for index, row in (pe_list.iloc[250:]).iterrows():

        market_cap_class = "Small Cap"
        pe_list.loc[index, 'Market Cap Class'] = market_cap_class

    return pe_list


# Compare with PE backdate list to check if there is new PE high/low value
def pe_high_low(pe_list, conn, date):

    pe_back_sql = 'SELECT DISTINCT ON("CompanyCode") * from public."PE"	\
				   WHERE "GenDate" < \'' + date.strftime("%Y-%m-%d") + '\'	\
				   ORDER BY "CompanyCode", "GenDate" desc ;'

    pe_back_list = sqlio.read_sql_query(pe_back_sql, con=conn)

    # PE High
    for index, row in pe_list.iterrows():

        # PE for current OHLC
        pe_current_high_list = pe_list.loc[(
            pe_list["CompanyCode"] == row['CompanyCode'])]['PE']
        pe_current_high_date_list = pe_list.loc[(
            pe_list['CompanyCode'] == row['CompanyCode'])]['Date']

        pe_current_high = pe_current_high_list.item() if len(
            pe_current_high_list.index) == 1 else np.nan
        pe_current_high_date = pe_current_high_date_list.item() if len(
            pe_current_high_date_list.index) == 1 else np.nan

        # PE High for backdate
        pe_back_high_list = pe_back_list.loc[(
            pe_back_list["CompanyCode"] == row['CompanyCode'])]['PE High']
        pe_back_high_date_list = pe_back_list.loc[(
            pe_back_list['CompanyCode'] == row['CompanyCode'])]['PE High Date']

        pe_back_high = pe_back_high_list.item() if len(
            pe_back_high_list.index) == 1 else np.nan
        pe_back_high_date = pe_back_high_date_list.item() if len(
            pe_back_high_date_list.index) == 1 else np.nan

        # Compare current PE with backdate PE High value
        if(math.isnan(pe_back_high)):

            pe_list.loc[index, 'PE High'] = pe_current_high
            pe_list.loc[index, 'PE High Date'] = pe_current_high_date

        elif (pe_current_high > pe_back_high):

            pe_list.loc[index, 'PE High'] = pe_current_high
            pe_list.loc[index, 'PE High Date'] = pe_current_high_date

        else:

            pe_list.loc[index, 'PE High'] = pe_back_high
            pe_list.loc[index, 'PE High Date'] = pe_back_high_date

    # PE Low
    for index, row in pe_list.iterrows():

        # PE for current OHLC
        pe_current_low_list = pe_list.loc[(
            pe_list["CompanyCode"] == row['CompanyCode'])]['PE']
        pe_current_low_date_list = pe_list.loc[(
            pe_list['CompanyCode'] == row['CompanyCode'])]['Date']

        pe_current_low = pe_current_low_list.item() if len(
            pe_current_low_list.index) == 1 else np.nan
        pe_current_low_date = pe_current_low_date_list.item() if len(
            pe_current_low_date_list.index) == 1 else np.nan

        # PE Low for backdate
        pe_back_low_list = pe_back_list.loc[(
            pe_back_list["CompanyCode"] == row['CompanyCode'])]['PE Low']
        pe_back_low_date_list = pe_back_list.loc[(
            pe_back_list['CompanyCode'] == row['CompanyCode'])]['PE Low Date']

        pe_back_low = pe_back_low_list.item() if len(
            pe_back_low_list.index) == 1 else np.nan
        pe_back_low_date = pe_back_low_date_list.item() if len(
            pe_back_low_date_list.index) == 1 else np.nan

        # Compare current PE with backdate PE Low value
        if(math.isnan(pe_back_low)):

            pe_list.loc[index, 'PE Low'] = pe_current_low
            pe_list.loc[index, 'PE Low Date'] = pe_current_low_date

        elif (pe_current_low < pe_back_low):

            pe_list.loc[index, 'PE Low'] = pe_current_low
            pe_list.loc[index, 'PE Low Date'] = pe_current_low_date

        else:

            pe_list.loc[index, 'PE Low'] = pe_back_low
            pe_list.loc[index, 'PE Low Date'] = pe_back_low_date

    return pe_list


# Insert PE into the database
def pe_insert(pe_list, conn, cur, date):

    pe_list['GenDate'] = date.strftime("%Y-%m-%d")

    pe_list["BSECode"] = pe_list["BSECode"].fillna(-1)
    pe_list = pe_list.astype({"BSECode": int})
    pe_list = pe_list.astype({"BSECode": str})
    pe_list["BSECode"] = pe_list["BSECode"].replace('-1', np.nan)

    pe_list["Market Cap Rank"] = pe_list["Market Cap Rank"].fillna(-1)
    pe_list = pe_list.astype({"Market Cap Rank": int})
    pe_list = pe_list.astype({"Market Cap Rank": str})
    pe_list["Market Cap Rank"] = pe_list["Market Cap Rank"].replace(
        '-1', np.nan)

    pe_list = pe_list[['CompanyCode', 'NSECode', 'BSECode', 'ISIN', 'Market Cap Value', 'Market Cap Rank', 'Market Cap Class', 'PE', 'PE High',
                       'PE High Date', 'PE Low', 'PE Low Date', 'GenDate']]

    exportfilename = "PE_export.csv"
    exportfile = open(exportfilename, "w")
    pe_list.to_csv(exportfile, header=True, index=False, float_format="%.2f",lineterminator='\r')
    exportfile.close()

    copy_sql = """
		   COPY public."PE" FROM stdin WITH CSV HEADER
		   DELIMITER as ','
		   """

    with open(exportfilename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        f.close()
    os.remove(exportfilename)


# Generate PE for given date
def generate_pe(date):

    conn = DB_Helper().db_connect()
    cur = conn.cursor()

    global today
    today = date
    
    print("Getting OHLC list for Date:", today)
    pe_list = get_ohlc_list(conn, today)

    if not (pe_list.empty):
        print("Calculating PE for OHLC list")
        pe_list = pe_calc(pe_list, conn, today)
        print("PE high/low calc")
        pe_list = pe_high_low(pe_list, conn, today)
        print("Inserting PE")
        pe_insert(pe_list, conn, cur, today)
        print("Inserted PE for date: ", today)
        conn.close()
    else:
        print("OHLC empty for Date: ", today)
        conn.close()
    

# Run PE for history date range
def history_pe():

    start_date = datetime.date(2016, 1, 1)
    end_date = datetime.date(2019, 4, 16)

    while(end_date >= start_date):

        generate_pe(start_date)
        start_date = start_date + datetime.timedelta(1)


# Run PE for current date
def current_pe(curr_date):
    generate_pe(curr_date)
