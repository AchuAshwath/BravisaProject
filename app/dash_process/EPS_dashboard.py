import pandas as pd
import psycopg2
from reports.PRSchecker import PRS
from lib import PEchecker
from datetime import datetime
import warnings
import pandas.io.sql as sqlio
warnings.filterwarnings("ignore")
import re
import os 

def generate_eps_dashboard(curr_date, conn, cur):

        # curr_date = curr_date.date()    
        # Define the SQL query with CTE to find the maximum "YearEnding" for each "CompanyCode" that is less than or equal to the provided date
        QuarterlyEPS_sql = """
        WITH MaxYearEnding AS (
            SELECT "CompanyCode", MAX("YearEnding") AS max_year
            FROM "QuarterlyEPS"
            WHERE "YearEnding" <= %s
            GROUP BY "CompanyCode"
        )
        SELECT q.*
        FROM "QuarterlyEPS" q
        JOIN MaxYearEnding m
        ON q."CompanyCode" = m."CompanyCode" AND q."YearEnding" = m.max_year
        """

        # Fetch the data using sqlio.read_sql_query and pass the current date as a parameter
        QuarterlyEPS = sqlio.read_sql_query(QuarterlyEPS_sql, params=(curr_date,), con=conn)

        QuarterlyEPS = QuarterlyEPS.drop_duplicates()

        # Define the SQL query with CTE to find the maximum "YearEnding" for each "CompanyCode" that is less than or equal to the provided date
        ttm_sql = """
        WITH MaxYearEnding AS (
            SELECT "CompanyCode", MAX("YearEnding") AS max_year
            FROM "TTM"
            WHERE "YearEnding" <= %s
            GROUP BY "CompanyCode"
        )
        SELECT t.*
        FROM "TTM" t
        JOIN MaxYearEnding m
        ON t."CompanyCode" = m."CompanyCode" AND t."YearEnding" = m.max_year
        """

        # Fetch the data using sqlio.read_sql_query and pass the current date as a parameter
        ttm_list = sqlio.read_sql_query(ttm_sql, params=(curr_date,), con=conn)
        ttm_list = ttm_list.drop_duplicates()


        EPS_sql = """SELECT * FROM "Reports"."EPS"
                WHERE "EPSDate"= %s
        """
        EPS = sqlio.read_sql_query(EPS_sql,params=(curr_date,),  con = conn)
        EPS = EPS.drop_duplicates()

        # merging Quarterly and TTM
        EPS_dashboard = pd.merge(QuarterlyEPS, ttm_list, how = 'outer', on='CompanyCode' )

        # Compile the regular expression pattern for efficiency
        x = re.compile("_x")
        y = re.compile("_y")

        columns = []

        # Iterate over each string in the list
        for column in EPS_dashboard.columns:
            # Use re.search() to look for the pattern in the string
            if x.search(column):
                renamed_col = column[:-2]
                # If a match is found, add the string to the matches list
                columns.append(renamed_col)
            elif y.search(column):
                renamed_col = column[:-2]+'_C'
                columns.append(renamed_col)
            else:
                columns.append(column)

        EPS_dashboard.columns = columns

        #merging EPS reports 
        EPS_dashboard = pd.merge(EPS_dashboard, EPS, how='right', on='CompanyCode')

        x = re.compile("_x")
        y = re.compile("_y")

        columns = []

        # Iterate over each string in the list
        for column in EPS_dashboard.columns:
            # Use re.search() to look for the pattern in the string
            if x.search(column):
                renamed_col = column[:-2]
                # If a match is found, add the string to the matches list
                columns.append(renamed_col)
            elif y.search(column):
                renamed_col = column[:-2]+'_EPS'
                columns.append(renamed_col)
            else:
                columns.append(column)

        EPS_dashboard.columns = columns

        EPS_dashboard[[ 'Sales', 'Expenses',
        'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',
        'PATRAW', 'PAT', 'Equity', 'Reserves', 'Sales_C', 'Expenses_C', 'EBIDTA_C', 'Interest_C',
        'Depreciation_C', 'Extraordinary_C', 'OPM_C', 'Tax_C', 'PAT_C',
        'Equity_C', 'Reserves_C']] = EPS_dashboard[[ 'Sales', 'Expenses',
        'EBIDTA', 'Interest', 'Depreciation', 'Extraordinary', 'OPM', 'Tax',
        'PATRAW', 'PAT', 'Equity', 'Reserves', 'Sales_C', 'Expenses_C', 'EBIDTA_C', 'Interest_C',
        'Depreciation_C', 'Extraordinary_C', 'OPM_C', 'Tax_C', 'PAT_C',
        'Equity_C', 'Reserves_C']]/10000000

        exportfilename = "EPS_dash.csv"
        exportfile = open(exportfilename, "w")
        EPS_dashboard.to_csv(exportfile, header=True, index=False,float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
            COPY "dash_process"."EPS" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)
        print(curr_date, " finished")


    