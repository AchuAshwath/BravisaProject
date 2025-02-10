from flask import Flask, render_template, request, jsonify
import psycopg2
import pandas.io.sql as sqlio
import os
import warnings
from datetime import datetime, timedelta
import time
from utils.sanitize import san_in
from utils.db_helper import DB_Helper
from utils.report_delete import run_delete
from utils.scripts_helper import run_scripts_frompy
from lib.fb_insert import FB_Insert
import pandas.io.sql as sqlio
import webview
import threading
from datetime import datetime, timedelta
import os
import numpy as np
import pandas as pd
import warnings
import concurrent.futures
import time
import zipfile
from config import OHLC_FOLDER, INDEX_OHLC_FOLDER, INDEX_FILES_FOLDER, FB_FOLDER
from utils.logs import insert_logs
from flask import Flask, render_template, request, jsonify

# import blue prints
from routes.dash_reports import dash_reports
from routes.dash_display import dash_display
from routes.dash_summary import dash_summary
from routes.industrymapping import industry_mapping
from routes.uploadfile import uploadfile
# from flask_bootstrap import Bootstrap5


# Assuming necessary imports and configurations are done here
def check_files_presence(date, is_holiday):
    
    if date.weekday() in [5, 6] or is_holiday:
        print("Skipping file check for weekend:", date.strftime("%Y-%m-%d"))
        return True
    
    eqisin = date.strftime("%Y%m%d").upper()
    cmbhav = date.strftime("%Y%m%d").upper()

    # eqisin = date.strftime("%d%m%y")
    indall = date.strftime("%d%m%Y")

    print("OHLC:", OHLC_FOLDER)
    print("INDEX_OHLC:", INDEX_OHLC_FOLDER)
    print("INDEX_FILES:", INDEX_FILES_FOLDER)
    print("FB_FOLDER:", FB_FOLDER)  
     
    # Check if it's the start of the month
    if date.day == 1:
        # Check if all required files are present along with index files
        if (os.path.isfile(os.path.join(OHLC_FOLDER, "BhavCopy_BSE_CM_0_0_0_" + eqisin + "_F_0000.csv")) and
            os.path.isfile(os.path.join(OHLC_FOLDER, "BhavCopy_NSE_CM_0_0_0_"+ cmbhav +"_F_0000.csv" )) and
            os.path.isfile(os.path.join(INDEX_OHLC_FOLDER, "ind_close_all_" + indall + ".csv")) and
            os.path.isfile(os.path.join(INDEX_FILES_FOLDER, "ind_nifty500list.csv"))): #and
            #os.path.isfile(os.path.join(index_files_folder, "BSE500_Index.csv"))):
            return True
        else:
            print("Files not found on the first day of the month:", date.strftime("%Y-%m-%d"))
            return False
    else:
        # Check remaining files for other dates of the month
        if not(os.path.isfile(os.path.join(OHLC_FOLDER, "BhavCopy_BSE_CM_0_0_0_" + eqisin + "_F_0000.csv"))):
            print("BSE file not found for date:", date.strftime("%Y-%m-%d"))
        
        if not(os.path.isfile(os.path.join(OHLC_FOLDER, "BhavCopy_NSE_CM_0_0_0_"+ cmbhav +"_F_0000.csv"))):
            print("NSE file not found for date:", date.strftime("%Y-%m-%d"))
        
        if not(os.path.isfile(os.path.join(INDEX_OHLC_FOLDER, "ind_close_all_" + indall + ".csv"))):
            print("Index file not found for date:", date.strftime("%Y-%m-%d"))
        
        
        if (os.path.isfile(os.path.join(OHLC_FOLDER, "BhavCopy_BSE_CM_0_0_0_" + eqisin + "_F_0000.csv")) and
            os.path.isfile(os.path.join(OHLC_FOLDER, "BhavCopy_NSE_CM_0_0_0_"+ cmbhav +"_F_0000.csv" )) and
            os.path.isfile(os.path.join(INDEX_OHLC_FOLDER, "ind_close_all_" + indall + ".csv"))):
            return True
        else:
            print("Files not found for date:", date.strftime("%Y-%m-%d"))
            return False

app = Flask(__name__)

# bootstrap = Bootstrap5(app)
app.register_blueprint(dash_reports)
app.register_blueprint(dash_display)
app.register_blueprint(dash_summary)
app.register_blueprint(industry_mapping)
app.register_blueprint(uploadfile)

@app.route('/dash')
def dashboard():
    return render_template('dash.html')

    
@app.route('/')
def index():
    db = DB_Helper()
    conn = db.db_connect()

    cur = conn.cursor()

    last_insert_date_sql = """SELECT MAX("log_date") FROM "logs"."insert" """
    last_report_generation_date_sql = """SELECT MAX("log_date") FROM "logs"."report_generation" """

    last_insert_date = pd.read_sql_query(last_insert_date_sql, conn)
    last_report_generation_date = pd.read_sql_query(last_report_generation_date_sql, conn)

    last_insert_date = last_insert_date.values[0][0]
    last_report_generation_date = last_report_generation_date.values[0][0]
    
    return render_template('index.html', last_insert_date=last_insert_date, last_report_generation_date=last_report_generation_date)



@app.route('/process', methods=['POST'])
def process():
    warnings.filterwarnings("ignore", category=UserWarning)

    db = DB_Helper()
    # dbURL = db.engine()
    conn = db.db_connect()
    cur = conn.cursor()
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    main_menu = request.form.get('main_menu')
    submenu = request.form.get('submenu')
    is_holiday = request.form.get('is_holiday') == 'true'
    print(is_holiday)
    print(start_date, end_date)
    print(submenu)
    
    INDEX_MAPPING_sql = """select * from public."irs_index_mapping" """
    INDEX_MAPPING = pd.read_sql_query(INDEX_MAPPING_sql, conn)
    INDEX_MAPPING = INDEX_MAPPING.set_index('indexname')['indexmapping'].to_dict()
    
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as e:
        return jsonify({'message': 'Invalid date format. Please use YYYY-MM-DD.'})

    if main_menu == "insert_data":
        insert_data(start_date, end_date, conn, cur)
        return jsonify({'message': 'Insert data processing successful'})

    elif main_menu == "generate_report":
        start_time = time.time()  # Record the start time
        date_difference = end_date - start_date 
        num_days = date_difference.days + 1

        for i in range(num_days):
            current_date = start_date + timedelta(days=i)
            result = report_generation(current_date, current_date, is_holiday)
            if not result:
                return jsonify({'message': 'File not found for date: ' + current_date.strftime("%Y-%m-%d")})

        end_time = time.time()
        elapsed_time = end_time - start_time
        return jsonify({'message': 'Report generation successful', 'time_taken': elapsed_time})

    elif main_menu == "insert_generate":
        start_time = time.time()
        date_difference = end_date - start_date 
        num_days = date_difference.days + 1

        for i in range(num_days):
            current_date = start_date + timedelta(days=i)
            insert_data(current_date, current_date, conn, cur)
            result = report_generation(current_date, current_date, is_holiday)
            if not result:
                return jsonify({'message': 'File not found for date: ' + current_date.strftime("%Y-%m-%d")})

        end_time = time.time()
        elapsed_time = end_time - start_time
        return jsonify({'message': 'Insert and generate processing successful', 'time_taken': elapsed_time})

    elif main_menu == "delete_report":
        delete_data(start_date, end_date, submenu, conn, cur)
        return jsonify({'message': 'Generate report for delete processing successful'})
    elif main_menu == "download_txt":
        cwd = os.getcwd()
        #create txt_files folder if it does not exist
        txt_files_folder = os.path.join(cwd, "DownloadedFiles", "txt_files")
        os.makedirs(txt_files_folder, exist_ok=True)
        
        if submenu=="All files":
            # fetch NSE for start date and end date with TIMESTAMP as the date column
            nse_query = f"SELECT * FROM public.\"NSE\" WHERE \"TIMESTAMP\" >= '{start_date}' AND \"TIMESTAMP\" <= '{end_date}' ORDER BY \"TIMESTAMP\""
            nse_df = sqlio.read_sql_query(nse_query, conn)
            
            #fetch  BSE for start date and end date with TRADING_DATE as date column
            bse_query = f"SELECT * FROM public.\"BSE\" WHERE \"TRADING_DATE\" >= '{start_date}' AND \"TRADING_DATE\" <= '{end_date}' ORDER BY \"TRADING_DATE\""
            bse_df = sqlio.read_sql_query(bse_query, conn)
            
            # fetch IndexHistory for start date and end date with DATE as date column
            index_query = f"SELECT * FROM public.\"IndexHistory\" WHERE \"DATE\" >= '{start_date}' AND \"DATE\" <= '{end_date}' ORDER BY \"DATE\""
            index_df = sqlio.read_sql_query(index_query, conn)
            
            # fetch mf_ohlc for start date and end date with date as date column  
            mf_query = f"SELECT * FROM public.\"mf_ohlc\" WHERE \"date\" >= '{start_date}' AND \"date\" <= '{end_date}' ORDER BY \"date\""
            mf_df = sqlio.read_sql_query(mf_query, conn)
            
            nse_df = nse_df[['SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']]
            bse_df = bse_df[['SC_CODE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'NO_OF_SHRS']]
            
            index_df['TICKER'] = index_df['TICKER'].map(INDEX_MAPPING)
            index_df = index_df[['TICKER', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']]
            
            # download all the files into txt_files folder
            nse_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"NSE-{start_date}-{end_date}.txt")
            nse_df.to_csv(nse_output_file, sep=',', index=False, header=False)
            
            bse_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"BSE-{start_date}-{end_date}.txt")
            bse_df.to_csv(bse_output_file, sep=',', index=False, header=False)
            
            index_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"IRSOHLC-{start_date}-{end_date}.txt")
            index_df.to_csv(index_output_file, sep=',', index=False, header=False)
            
            mf_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"MFOHLC-{start_date}-{end_date}.txt")
            mf_df.to_csv(mf_output_file, sep=',', index=False, header=False)
                    
        elif submenu=="NSE":
            # fetch NSE for start date and end date with TIMESTAMP as the date column
            nse_query = f"SELECT * FROM public.\"NSE\" WHERE \"TIMESTAMP\" >= '{start_date}' AND \"TIMESTAMP\" <= '{end_date}' ORDER BY \"TIMESTAMP\""
            nse_df = sqlio.read_sql_query(nse_query, conn)
            nse_df = nse_df[['SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']]
            
            nse_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"NSE-{start_date}-{end_date}.txt")
            nse_df.to_csv(nse_output_file, sep=',', index=False, header=False)
        elif submenu=="BSE":
            #fetch  BSE for start date and end date with TRADING_DATE as date column
            bse_query = f"SELECT * FROM public.\"BSE\" WHERE \"TRADING_DATE\" >= '{start_date}' AND \"TRADING_DATE\" <= '{end_date}' ORDER BY \"TRADING_DATE\""
            bse_df = sqlio.read_sql_query(bse_query, conn)
            bse_df = bse_df[['SC_CODE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'NO_OF_SHRS']]
            
            bse_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"BSE-{start_date}-{end_date}.txt")
            bse_df.to_csv(bse_output_file, sep=',', index=False, header=False)
            
        elif submenu=="IRSOHLC":
            # fetch IndexHistory for start date and end date with DATE as date column
            index_query = f"SELECT * FROM public.\"IndexHistory\" WHERE \"DATE\" >= '{start_date}' AND \"DATE\" <= '{end_date}' ORDER BY \"DATE\""
            index_df = sqlio.read_sql_query(index_query, conn)
            index_df['TICKER'] = index_df['TICKER'].map(INDEX_MAPPING)
            index_df = index_df[['TICKER', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']]
            
            index_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"IRSOHLC-{start_date}-{end_date}.txt")
            index_df.to_csv(index_output_file, sep=',', index=False, header=False)
            
        elif submenu=="MFOHLC":
            # fetch mf_ohlc for start date and end date with date as date column
            mf_query = f"SELECT * FROM public.\"mf_ohlc\" WHERE \"date\" >= '{start_date}' AND \"date\" <= '{end_date}' ORDER BY \"date\""
            mf_df = sqlio.read_sql_query(mf_query, conn)
            
            mf_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"MFOHLC-{start_date}-{end_date}.txt")
            mf_df.to_csv(mf_output_file, sep=',', index=False, header=False)
        else:
            return jsonify({'message': 'Invalid submenu value'})
        
        return jsonify({'message': 'Download txt processing successful'})
        
    elif main_menu == "download":
        cwd = os.getcwd()
        print(cwd)
        report_mapping = {
            # "ERS": ('"Reports"."ERS"', '"ERSDate"'),
            # "Standalone ERS": ('"Reports"."STANDALONE_ERS"', '"ERSDate"'),
            # "Consolidated ERS": ('"Reports"."Consolidated_ERS"', '"ERSDate"'),
            "EPS": ('"Reports"."EPS"', '"EPSDate"'),
            "Standalone EPS": ('"Reports"."STANDALONE_EPS"', '"EPSDate"'),
            "Consolidated EPS": ('"Reports"."Consolidated_EPS"', '"EPSDate"'),
            "EERS": ('"Reports"."EERS"', '"EERSDate"'),
            "Standalone EERS": ('"Reports"."STANDALONE_EERS"', '"EERSDate"'),
            "Consolidated EERS": ('"Reports"."Consolidated_EERS"', '"EERSDate"'),
            "PRS": ('"Reports"."PRS"', '"Date"'),
            "SMR": ('"Reports"."SMR"', '"SMRDate"'),
            "IRS": ('"Reports"."IRS"', '"GenDate"'),
            "FRS-NAVCategoryAvg": ('"Reports"."FRS-NAVCategoryAvg"', '"Date"'),
            "CombinedRS": ('"Reports"."CombinedRS"', '"GenDate"'),
            "FRS-MFRank": ('"Reports"."FRS-MFRank"', '"Date"'),
            "FRS-NAVRank": ('"Reports"."FRS-NAVRank"', '"Date"')    
        }

        if submenu == "All Reports":
            selected_reports = report_mapping.keys()
            # print(selected_reports)
        else:
            print('Selected report is ', submenu)
            selected_reports = [submenu]
            

        for report in selected_reports:
            table, date_column = report_mapping[report]
            output_name= table.split('.')[1].replace('\"', '')
            # create reports folder if it does not exist
            os.makedirs(os.path.join(cwd, "DownloadedFiles", "reports"), exist_ok=True)
            output_file = os.path.join(cwd, "DownloadedFiles","reports",  f"{output_name}-{start_date}-{end_date}.csv")
            print(output_file)
            query = f"SELECT * FROM {table} WHERE {date_column} >= '{start_date}' AND {date_column} <= '{end_date}' ORDER BY {date_column}"
            df = sqlio.read_sql_query(query, con = conn)
            if df.empty:
                pass
            else:
                table = table.split('.')[1].replace('\"', '')
                download_csv(df, table, output_file, conn)
        return jsonify({'message': 'Download report processing successful'})

    else:
        return jsonify({'message': 'Invalid main_menu value'})

def insert_data(start_date, end_date, conn, cur):
    date_format = "%d%m%Y"
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() == 0:  # Monday
            prev_date = current_date - timedelta(days=2)  # Saturday
        else:
            prev_date = current_date - timedelta(days=1)  # Previous day
        start_time = time.time()
        print(current_date)
        fb3 = FB_Insert().fb_insert_03("FB" + prev_date.strftime(date_format) + "03", conn, cur)
        fb1 = FB_Insert().fb_insert_01("FB" + current_date.strftime(date_format) + "01", conn, cur)
        fb2 = FB_Insert().fb_insert_02("FB" + current_date.strftime(date_format) + "02", conn, cur)
        end_time = time.time()
        print(fb1, fb2, fb3)
        print("Inserted: ", current_date)
        if fb1 and fb2 and fb3:
            LOGS = {
                "log_date": current_date,
                "FB3": fb3,
                "FB1": fb1,
                "FB2": fb2,
                "log_time": datetime.now(),
                "runtime": end_time - start_time
            }
            insert_logs("insert", [LOGS], conn, cur)   
        else:
            print("Files not found for date:", current_date.strftime("%Y-%m-%d")) 
        current_date += timedelta(days=1)

def report_generation(startdate, enddate, is_holiday):
    date_difference = enddate - startdate
    num_days = date_difference.days + 1
    isPresent = True

    for i in range(num_days):
        current_date = startdate + timedelta(days=i)
        isPresent = check_files_presence(current_date, is_holiday)
        if not isPresent:
            break

    if isPresent:
        run_scripts_frompy(startdate, enddate, is_holiday)
        return True
    else:
        print("False, no file found")
        return False

def delete_data(startdate, enddate, delete_variable, conn, cur):
    if delete_variable == "Traditional Delete":
        run_delete(startdate, enddate)
    else:
        # need to change delete sequence
        report_delete_sql = {
            "ERS": 'DELETE FROM "Reports"."ERS" WHERE "ERSDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "ERSDate" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "Standalone ERS": 'DELETE FROM "Reports"."STANDALONE_ERS" WHERE "ERSDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "ERSDate" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "Consolidated ERS": 'DELETE FROM "Reports"."Consolidated_ERS" WHERE "ERSDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "ERSDate" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "EERS": 'DELETE FROM "Reports"."EERS" WHERE "EERSDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "EERSDate" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "Standalone EERS": 'DELETE FROM "Reports"."STANDALONE_EERS" WHERE "EERSDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "EERSDate" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "Consolidated EERS": 'DELETE FROM "Reports"."Consolidated_EERS" WHERE "EERSDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "EERSDate" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "PRS": 'DELETE FROM "Reports"."PRS" WHERE "Date" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "Date" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "SMR": 'DELETE FROM "Reports"."SMR" WHERE "SMRDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "SMRDate" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "IRS": 'DELETE FROM "Reports"."IRS" WHERE "GenDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "GenDate" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "FRS-NavRank": 'DELETE FROM "Reports"."FRS-NAVRank" WHERE "Date" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "Date" <= TO_DATE(%s, \'YYYY-MM-DD\');',
            "CombinedRS": 'DELETE FROM "Reports"."CombinedRS" WHERE "GenDate" >= TO_DATE(%s, \'YYYY-MM-DD\') AND "GenDate" <= TO_DATE(%s, \'YYYY-MM-DD\');'
        }

        if delete_variable in report_delete_sql:
            sql = report_delete_sql[delete_variable]
            data = (str(startdate), str(enddate))
            cur.execute(sql, data)
            conn.commit()
        else:
            raise ValueError("Invalid delete variable")

def download_csv(csv, report, filename, conn):
    dyn_list =  {'CompanyCode', 'NSECode', 'BSECode', 'CompanyName', 'ISIN'}
    columns_to_convert = {'EPS':['Q1 Sales', 'Q2 Sales'], 'ERS':['Q1 Sales', 'Q2 Sales'],'STANDALONE_ERS':['Q1 Sales', 'Q2 Sales'], 'STANDALONE_EPS':['Q1 Sales', 'Q2 Sales'], 'Consolidated_EPS':['Q1 Sales', 'Q2 Sales'],'Consolidated_ERS':['Q1 Sales', 'Q2 Sales'], 'EERS':['Q1 Sales', 'Q2 Sales'],'STANDALONE_EERS':['Q1 Sales', 'Q2 Sales'],'Consolidated_EERS':['Q1 Sales', 'Q2 Sales'], 'PRS':['Value', 'Value Average', 'Market Cap Value'], 'IRS':['OS', 'Volume'], "SMR": ('"Reports"."SMR"', '"SMRDate"'),
            "FRS-NAVCategoryAvg": [],
            "FRS-MFRank": [],
            "CombinedRS": [],
            "FRS-NAVRank": []}
    # loop through columns
    for col in csv.columns:
        # if the column is object type
        if csv[col].dtype == 'object':            
            dyn_list.add(col) 

        if col not in dyn_list:
            # convert decimal to 2 decimal places and handle NaN values
            csv[col] = csv[col].apply(lambda x: round(float(x), 2) if not pd.isnull(x) else x)
            # convert all column value into Cr if more than 8 digits

        if col in columns_to_convert[report]:
            csv[col] = csv[col].apply(lambda x: str(round(float(x/10000000), 2)) if x is not None and x > 10000000 else x)
            csv.rename(columns = {col: col + " Cr"}, inplace = True)
    print(report)
    if report=='IRS':
        print("the report that is being downloaded is IRS and the indexmapping is going to take place")
        # index_mapping = {'INDUSTRY-Automobiles': 'AUTO-I', 'INDUSTRY-Automobile Components': 'AUTOCOMP-I', 'INDUSTRY-Specialty Retail': 'SPECRETAI-I', 'INDUSTRY-Consumer Staples Distribution & Retail': 'CONSTAPDIST-I', 'INDUSTRY-Broadline Retail': 'BROADRETAI-I', 'INDUSTRY-Food Products': 'FOODPROD-I', 'INDUSTRY-Beverages': 'BEVERAGE-I', 'INDUSTRY-Building Products': 'BUILDPROD-I', 'INDUSTRY-Construction & Engineering': 'CONSENGINE-I', 'INDUSTRY-Chemicals': 'CHEMICAL-I', 'INDUSTRY-Textiles, Apparel & Luxury Goods': 'TEXAPPLUX-I', 'INDUSTRY-Containers & Packaging': 'CONTAINPKG-I', 'INDUSTRY-Oil, Gas & Consumable Fuels': 'ONGCONFUEL-I', 'INDUSTRY-Household Products': 'HOUSEPROD-I', 'INDUSTRY-Personal Care Products': 'PERCARPROD-I', 'INDUSTRY-Pharmaceuticals': 'PHARMAC-I', 'INDUSTRY-Health Care Equipment & Supplies': 'HLTCAEQSUPP-I', 'INDUSTRY-Tobacco': 'TOBACCO-I', 'INDUSTRY-Household Durables': 'HOUSEDUR-I', 'INDUSTRY-Technology Hardware, Storage & Peripherals': 'TECHARDSOFTPER-I', 'INDUSTRY-Software': 'SOFTWARE-I', 'INDUSTRY-Electronic Equipment, Instruments & Components': 'ELEEQINSCMP-I', 'INDUSTRY-Commercial Services & Supplies': 'COMSERVSUP-I', 'INDUSTRY-Electrical Equipment': 'ELECEQUIP-I', 'INDUSTRY-IT Services': 'ITSERV-I', 'INDUSTRY-Machinery': 'MACHINERY-I', 'INDUSTRY-Insurance': 'INSURAN-I', 'INDUSTRY-Banks': 'BANKS-I', 'INDUSTRY-Financial Services': 'FINSERV-I', 'INDUSTRY-Capital Markets': 'CAPIMKT-I', 'INDUSTRY-Metals & Mining': 'METAMINE-I', 'INDUSTRY-Energy Equipment & Services': 'ENEQUSERV-I', 'INDUSTRY-Electric Utilities': 'ELECUTIL-I', 'INDUSTRY-Diversified Telecommunication Services': 'DIVTELECOMSERV-I', 'INDUSTRY-Independent Power and Renewable Electricity Producers': 'INDPOWRENEL-I', 'INDUSTRY-Paper & Forest Products': 'PAPFORPROD-I', 'INDUSTRY-Health Care Providers & Services': 'HLTCAPROSERV-I', 'INDUSTRY-Marine Transportation': 'MARITRA-I', 'INDUSTRY-Ground Transportation': 'GRNDTRAN-SI', 'INDUSTRY-Air Freight & Logistics': 'AIRFRELOGIS-I', 'INDUSTRY-Hotels, Restaurants & Leisure': 'HOTRESLEIS-I', 'INDUSTRY-Transportation Infrastructure': 'TRANSINFRA-I', 'INDUSTRY-Interactive Media & Services': 'INTMEDSERV-I', 'INDUSTRY-Diversified Consumer Services': 'DIVERCONSERV-I', 'INDUSTRY-Real Estate Management & Development': 'RESMGMDEV-I', 'INDUSTRY-Entertainment': 'ENTERTAIN-I', 'INDUSTRY-Retail REITs': 'RETIALREIT-I', 'INDUSTRY-Media': 'MEDIA-I', 'INDUSTRY-Gas Utilities': 'GASUTIL-I', 'INDUSTRY-Leisure Products': 'LEISPROD-I', 'SUBINDUSTRY-Auto - LCVs/HCVs': 'AUTLCVHCV-SI', 'SUBINDUSTRY-Auto - Cars & Jeeps': 'AUTCARJEE-SI', 'SUBINDUSTRY-Motorcycle Manufacturers': 'MOTORMFG-SI', 'SUBINDUSTRY-Automotive Parts & Equipment': 'AUTPEQUIP-SI', 'SUBINDUSTRY-Tires & Rubber': 'TIRERUB-SI', 'SUBINDUSTRY-Carbon Black': 'CARBLCK-SI', 'SUBINDUSTRY-Other Specialty Retail': 'OTHSPECRETL-SI', 'SUBINDUSTRY-Retail - Departmental Stores': 'RETDEPTSTO-SI', 'SUBINDUSTRY-Retail - Apparel': 'RETAPPAREL-SI', 'SUBINDUSTRY-Retail - Apparel/Accessories': 'RETAPPARACC-SI', 'SUBINDUSTRY-Trading': 'TRADING-SI', 'SUBINDUSTRY-Plantations': 'PLANTATIO-SI', 'SUBINDUSTRY-Distillers & Vintners': 'DISTILVINTN-SI', 'SUBINDUSTRY-Aqua & Horticulture': 'AQUAHORTIC-SI', 'SUBINDUSTRY-Edible Oils': 'EDIBLEOIL-SI', 'SUBINDUSTRY-Agricultural Products & Services': 'AGRIPROSER-SI', 'SUBINDUSTRY-Soft Drinks & Non-alcoholic Beverages': 'SODRINALCBEV-SI', 'SUBINDUSTRY-Packaged Foods & Meats': 'PKGFOODMEAT-SI', 'SUBINDUSTRY-Sugar': 'SUGAR-SI', 'SUBINDUSTRY-Cement & Products': 'CEMPROD-SI', 'SUBINDUSTRY-Tiles & Granites': 'TILEGRANI-SI', 'SUBINDUSTRY-Decoratives & Laminates': 'DECOLAMIN-SI', 'SUBINDUSTRY-Paints': 'PAINTS-SI', 'SUBINDUSTRY-Building Products': 'BUILDPROD-SI', 'SUBINDUSTRY-Construction & Engineering': 'CONSENGINE-SI', 'SUBINDUSTRY-Commodity Chemicals': 'COMMOCHEM-SI', 'SUBINDUSTRY-Specialty Chemicals': 'SPECHEM-SI', 'SUBINDUSTRY-Fertilizers & Agricultural Chemicals': 'FERAGRICHM-SI', 'SUBINDUSTRY-Diversified Chemicals': 'DIVERCHEM-SI', 'SUBINDUSTRY-Textiles': 'TEXTILES-SI', 'SUBINDUSTRY-Paper & Plastic Packaging Products & Materials': 'PPLPGPROMAT-SI', 'SUBINDUSTRY-Oil & Gas Refining & Marketing': 'ONGREFMKT-SI', 'SUBINDUSTRY-Household Products': 'HOUSEPROD-SI', 'SUBINDUSTRY-Personal Care Products': 'PERCARPROD-SI', 'SUBINDUSTRY-Pharmaceuticals': 'PHARMAC-SI', 'SUBINDUSTRY-Apparel, Accessories & Luxury Goods': 'APPACCLUX-SI', 'SUBINDUSTRY-Health Care Supplies': 'HLTCARESUPP-SI', 'SUBINDUSTRY-Tobacco': 'TOBACCO-SI', 'SUBINDUSTRY-Household Appliances': 'HOUSEAPPLI-SI', 'SUBINDUSTRY-Technology Hardware, Storage & Peripherals': 'TECHARDSOFTPER-SI', 'SUBINDUSTRY-Systems Software': 'SYSOFT-SI', 'SUBINDUSTRY-Electronic Equipment & Instruments': 'ELECEQUINS-SI', 'SUBINDUSTRY-Office Services & Supplies': 'OFFSERVSUPP-SI', 'SUBINDUSTRY-Consumer Electronics': 'CONSELEC-SI', 'SUBINDUSTRY-Electrical Components & Equipment': 'ELECOMEQU-SI', 'SUBINDUSTRY-Health Care Equipment': 'HLTCAREQU-SI', 'SUBINDUSTRY-Application Software': 'APPLICSOFT-SI', 'SUBINDUSTRY-IT Consulting & Other Services': 'ITCONSOTHSV-SI', 'SUBINDUSTRY-Research & Consulting Services': 'RESCONSERV-SI', 'SUBINDUSTRY-Industrial Machinery & Supplies & Components': 'INDMCHSUPCOM-SI', 'SUBINDUSTRY-Finance - Life Insurance': 'FINLIFINS-SI', 'SUBINDUSTRY-Private Banks': 'PVTBNK-SI', 'SUBINDUSTRY-Finance - Term Lending': 'FINTERLEND-SI', 'SUBINDUSTRY-Finance - Mutual Funds': 'FINMF-SI', 'SUBINDUSTRY-Investment Trusts': 'INVTRUST-SI', 'SUBINDUSTRY-Finance - Housing': 'FINHSG-SI', 'SUBINDUSTRY-Finance & Investments': 'FININVTS-SI', 'SUBINDUSTRY-PSU Banks': 'PSUBNK-SI', 'SUBINDUSTRY-Finance - Non Life Insurance': 'FINNONLIFINS-SI', 'SUBINDUSTRY-Reinsurance': 'REINSURE-SI', 'SUBINDUSTRY-Financial Exchanges & Data': 'FINEXCHDATA-SI', 'SUBINDUSTRY-Bearings': 'BEARINGS-SI', 'SUBINDUSTRY-Ferro Alloys': 'FERALLO-SI', 'SUBINDUSTRY-Fasteners': 'FASTENERS-SI', 'SUBINDUSTRY-Industrial Gases': 'INDUSGAS-SI', 'SUBINDUSTRY-Metal, Glass & Plastic Containers': 'METGLAPLACON-SI', 'SUBINDUSTRY-Aluminum': 'ALUMINIUM-SI', 'SUBINDUSTRY-Precious Metals & Minerals': 'PRECMETMIN-SI', 'SUBINDUSTRY-Diversified Metals & Mining': 'DIVERMETMIN-SI', 'SUBINDUSTRY-Oil & Gas Drilling': 'ONGDRILL-SI', 'SUBINDUSTRY-Electric Utilities': 'ELECUTILI-SI', 'SUBINDUSTRY-Telecommunications - Equipment': 'TELECOMEQU-SI', 'SUBINDUSTRY-Heavy Electrical Equipment': 'HVYELECEQUIP-SI', 'SUBINDUSTRY-Renewable Electricity': 'RENEWELEC-SI', 'SUBINDUSTRY-Copper': 'COPPER-SI', 'SUBINDUSTRY-Integrated Telecommunication Services': 'INTGTELCOSVC-SI', 'SUBINDUSTRY-Infrastructure - General': 'INFRAGEN-SI', 'SUBINDUSTRY-Steel': 'STEEL-SI', 'SUBINDUSTRY-Paper Products': 'PAPERPROD-SI1', 'SUBINDUSTRY-Livestock - Hatcheries/Poultry': 'LIVHATCHPOUL-SI', 'SUBINDUSTRY-Health Care Facilities': 'HLTCAREFACIL-SI', 'SUBINDUSTRY-Marine Transportation': 'MARITRA-SI', 'SUBINDUSTRY-Transport - Road': 'TRAROAD-SI', 'SUBINDUSTRY-Transport - Air': 'TRAAIR-SI', 'SUBINDUSTRY-Hotels, Resorts & Cruise Lines': 'HOTRESCRUIS-SI', 'SUBINDUSTRY-Marine Ports & Services': 'MARIPORSV-SI', 'SUBINDUSTRY-Diversified Support Services': 'DIVERSUPSER-SI', 'SUBINDUSTRY-Interactive Media & Services': 'INTMEDSERV-SI', 'SUBINDUSTRY-Fire Protection Equipment': 'FIRPROTEQU-SI', 'SUBINDUSTRY-LPG Bottling/Distribution': 'LPGBOTDIST-SI', 'SUBINDUSTRY-Commercial Printing': 'COMMERPRINT-SI', 'SUBINDUSTRY-Agricultural & Farm Machinery': 'AGRIFARMCH-SI', 'SUBINDUSTRY-Diversified Financial Services': 'DIVERFINSER-SI', 'SUBINDUSTRY-E-Commerce - Retail': 'ECOMRET-SI', 'SUBINDUSTRY-Education Services': 'EDUSERV-SI', 'SUBINDUSTRY-Real Estate Development': 'REALSTATDEV-SI', 'SUBINDUSTRY-Waste Management': 'WASTEMAN-SI', 'SUBINDUSTRY-Multi-Sector Holdings': 'MULTSECTHLD-SI', 'SUBINDUSTRY-Fintech': 'FINTEC-SI', 'SUBINDUSTRY-Health Care Services': 'HLTCARESERV-SI', 'SUBINDUSTRY-Digital Entertainment': 'DIGIENTER-SI', 'SUBINDUSTRY-Leisure Facilities': 'LEISFACIL-SI', 'SUBINDUSTRY-Dairy': 'DAIRY-SI', 'SUBINDUSTRY-Marine Foods': 'MARIFOOD-SI', 'SUBINDUSTRY-Road Infrastructure': 'ROADINFRA-SI', 'SUBINDUSTRY-Retail REITs': 'RETAILREIT-SI', 'SUBINDUSTRY-Specialized Finance': 'SPECIFIN-SI', 'SUBINDUSTRY-Railway Wagons and Wans': 'RAILWAGWAN-SI', 'SUBINDUSTRY-Footwear': 'FOOTWEAR-SI', 'SUBINDUSTRY-Advertising': 'ADVERT-SI', 'SUBINDUSTRY-Construction Machinery & Heavy Transportation Equipment': 'CONMCHVYTRN-SI', 'SUBINDUSTRY-Home Furnishings': 'HOMFURNIS-SI', 'SUBINDUSTRY-Gas Utilities': 'GASUTIL-SI', 'SUBINDUSTRY-Insurance Brokers': 'INSURBROK-SI', 'SUBINDUSTRY-Leisure Products': 'LEISPROD-SI', 'SUBINDUSTRY-Micro Finance Institutions': 'MICROFININS-SI', 'SUBINDUSTRY-Oil & Gas Equipment & Services': 'ONGEQUSERV-SI', 'SUBINDUSTRY-Diversified Capital Markets': 'DIVERCAPMKT-SI', 'SECTOR-Consumer Discretionary': 'CONSDISC-S', 'SECTOR-Consumer Staples': 'CONSTAPL-S', 'SECTOR-Industrials': 'INDUSTRIAL-S', 'SECTOR-Materials': 'MATERIALS-S', 'SECTOR-Energy': 'ENERGY-S', 'SECTOR-Health Care': 'HLTHCARE-S', 'SECTOR-Information Technology': 'INFOTEC-S', 'SECTOR-Financials': 'FINANCIALS-S', 'SECTOR-Utilities': 'UTILITIES-S', 'SECTOR-Communication Services': 'COMMSERV-S', 'SECTOR-Real Estate': 'REALESTATE-S', 'SUBSECTOR-Automobiles & Components': 'AUTOCOMP-SS', 'SUBSECTOR-Consumer Discretionary Distribution & Retail': 'CONSDDRETA-SS', 'SUBSECTOR-Consumer Staples Distribution & Retail': 'CONSTAPDISRE-SS', 'SUBSECTOR-Food, Beverage & Tobacco': 'FOOBEVTOBA-SS', 'SUBSECTOR-Capital Goods': 'CAPIGOOD-SS', 'SUBSECTOR-Materials': 'MATERIAL-SS', 'SUBSECTOR-Consumer Durables & Apparel': 'CONSDURAPP-SS', 'SUBSECTOR-Energy': 'ENERGY-SS', 'SUBSECTOR-Household & Personal Products': 'HOUSPERS-SS', 'SUBSECTOR-Pharmaceuticals, Biotechnology & Life Sciences': 'PHARMBIOLIFS-SS', 'SUBSECTOR-Health Care Equipment & Services': 'HLTCAREQUSV-SS', 'SUBSECTOR-Technology Hardware & Equipment': 'TECHARDEQUI-SS', 'SUBSECTOR-Software & Services': 'SOFTSERV-SS', 'SUBSECTOR-Commercial & Professional Services': 'COMPRFSERV-SS', 'SUBSECTOR-Insurance': 'INSURANCE-SS', 'SUBSECTOR-Banks': 'BANKS-SS', 'SUBSECTOR-Financial Services': 'FINSERV-SS', 'SUBSECTOR-Utilities': 'UTILITIES-SS', 'SUBSECTOR-Telecommunication Services': 'TELECOSERV-SS', 'SUBSECTOR-Transportation': 'TRANSPORT-SS', 'SUBSECTOR-Consumer Services': 'CONSERV-SS', 'SUBSECTOR-Media & Entertainment': 'MEDIAENT-SS', 'SUBSECTOR-Real Estate Management & Development': 'RESMGMDEV-SS', 'SUBSECTOR-Equity Real Estate Investment Trusts (REITs)': 'EQREITS-SS'}
        # map the index_mapping to the IndexName column
    
        INDEX_MAPPING_sql = """select * from public."irs_index_mapping" """
        INDEX_MAPPING = pd.read_sql_query(INDEX_MAPPING_sql, conn)
        INDEX_MAPPING = INDEX_MAPPING.set_index('indexname')['indexmapping'].to_dict()
        csv['IndexName'] = csv['IndexName'].map(INDEX_MAPPING)
        
    try:        
        csv.to_csv(filename, index=False)
    except OSError:
        # create DownloadedFiles folder if it doesn't exist on the cwd
        os.makedirs('DownloadedFiles')
        csv.to_csv(filename, index=False)
    finally:
        print("File saved successfully")
        
    


def start_flask():
    app.run(host='127.0.0.1', port=5000)
    

if __name__ == "__main__":
    t = threading.Thread(target=start_flask)
    t.daemon = True
    t.start()

    webview.create_window("Bravisa Temple Tree", "http://127.0.0.1:5000", min_size=(950,800))
    webview.start()



    


