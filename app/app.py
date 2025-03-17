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
            nse_df['TIMESTAMP'] = pd.to_datetime(nse_df['TIMESTAMP'])

            #fetch  BSE for start date and end date with TRADING_DATE as date column
            bse_query = f"SELECT * FROM public.\"BSE\" WHERE \"TRADING_DATE\" >= '{start_date}' AND \"TRADING_DATE\" <= '{end_date}' ORDER BY \"TRADING_DATE\""
            bse_df = sqlio.read_sql_query(bse_query, conn)
            bse_df['TRADING_DATE'] = pd.to_datetime(bse_df['TRADING_DATE'])
            # remove decimal values from SC_CODE
            bse_df['SC_CODE'] = bse_df['SC_CODE'].astype(int)

            # fetch IndexHistory for start date and end date with DATE as date column
            index_query = f"SELECT * FROM public.\"IndexHistory\" WHERE \"DATE\" >= '{start_date}' AND \"DATE\" <= '{end_date}' ORDER BY \"DATE\""
            index_df = sqlio.read_sql_query(index_query, conn)
            index_df['DATE'] = pd.to_datetime(index_df['DATE'])

            # fetch mf_ohlc for start date and end date with date as date column  
            mf_query = f"SELECT * FROM public.\"mf_ohlc\" WHERE \"date\" >= '{start_date}' AND \"date\" <= '{end_date}' ORDER BY \"date\""
            mf_df = sqlio.read_sql_query(mf_query, conn)
            mf_df['date'] = pd.to_datetime(mf_df['date'])

            # arrange columns in the order required
            nse_df = nse_df[['SYMBOL','TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']]
            bse_df = bse_df[['SC_CODE','TRADING_DATE' , 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'NO_OF_SHRS']]
            mf_df = mf_df[['btt_scheme_code', 'date', 'open', 'high', 'low', 'close', 'volume']]
            
            index_df['TICKER'] = index_df['TICKER'].map(INDEX_MAPPING)
            index_df = index_df[['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']]
            
            # download all the files into txt_files folder
            
            nse_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"NSE-{start_date}-{end_date}.txt")
            nse_df = nse_df.dropna()
            # round off the values to 2 decimal places
            nse_df = nse_df.round(2)
            #format date column to be yyyymmdd without - and :
            nse_df['TIMESTAMP'] = nse_df['TIMESTAMP'].dt.strftime('%Y%m%d')
            nse_df.to_csv(nse_output_file, sep=',', index=False, header=False)
            
            bse_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"BSE-{start_date}-{end_date}.txt")
            # drop rows with null values
            bse_df = bse_df.dropna()
            bse_df = bse_df.round(2)
            bse_df['TRADING_DATE'] = bse_df['TRADING_DATE'].dt.strftime('%Y%m%d')
            bse_df.to_csv(bse_output_file, sep=',', index=False, header=False)
            
            index_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"IRSOHLC-{start_date}-{end_date}.txt")
            # drop rows with null values
            index_df = index_df.dropna()
            index_df = index_df.round(2)
            index_df['DATE'] = index_df['DATE'].dt.strftime('%Y%m%d')
            index_df.to_csv(index_output_file, sep=',', index=False, header=False)
            
            mf_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"MFOHLC-{start_date}-{end_date}.txt")
            # drop rows with null values
            mf_df = mf_df.dropna()
            mf_df = mf_df.round(2)
            mf_df['date'] = mf_df['date'].dt.strftime('%Y%m%d')
            mf_df.to_csv(mf_output_file, sep=',', index=False, header=False)
                    
        elif submenu=="NSE":
            # fetch NSE for start date and end date with TIMESTAMP as the date column
            nse_query = f"SELECT * FROM public.\"NSE\" WHERE \"TIMESTAMP\" >= '{start_date}' AND \"TIMESTAMP\" <= '{end_date}' ORDER BY \"TIMESTAMP\""
            nse_df = sqlio.read_sql_query(nse_query, conn)
            nse_df['TIMESTAMP'] = pd.to_datetime(nse_df['TIMESTAMP'])
            nse_df = nse_df[['SYMBOL','TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']]
            
            
            nse_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"NSE-{start_date}-{end_date}.txt")
            nse_df = nse_df.dropna()
            nse_df = nse_df.round(2)
            nse_df['TIMESTAMP'] = nse_df['TIMESTAMP'].dt.strftime('%Y%m%d')
            nse_df.to_csv(nse_output_file, sep=',', index=False, header=False)
        elif submenu=="BSE":
            #fetch  BSE for start date and end date with TRADING_DATE as date column
            bse_query = f"SELECT * FROM public.\"BSE\" WHERE \"TRADING_DATE\" >= '{start_date}' AND \"TRADING_DATE\" <= '{end_date}' ORDER BY \"TRADING_DATE\""
            bse_df = sqlio.read_sql_query(bse_query, conn)
            bse_df['TRADING_DATE'] = pd.to_datetime(bse_df['TRADING_DATE'])
            bse_df = bse_df[['SC_CODE','TRADING_DATE' , 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'NO_OF_SHRS']]
            
            bse_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"BSE-{start_date}-{end_date}.txt")
            bse_df = bse_df.dropna()
            bse_df = bse_df.round(2)
            bse_df['TRADING_DATE'] = bse_df['TRADING_DATE'].dt.strftime('%Y%m%d')
            bse_df.to_csv(bse_output_file, sep=',', index=False, header=False)
            
        elif submenu=="IRSOHLC":
            # fetch IndexHistory for start date and end date with DATE as date column
            index_query = f"SELECT * FROM public.\"IndexHistory\" WHERE \"DATE\" >= '{start_date}' AND \"DATE\" <= '{end_date}' ORDER BY \"DATE\""
            index_df = sqlio.read_sql_query(index_query, conn)
            index_df['DATE'] = pd.to_datetime(index_df['DATE'])
            index_df['TICKER'] = index_df['TICKER'].map(INDEX_MAPPING)
            index_df = index_df[['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']]
            
            index_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"IRSOHLC-{start_date}-{end_date}.txt")
            index_df = index_df.dropna()
            index_df = index_df.round(2)
            index_df['DATE'] = index_df['DATE'].dt.strftime('%Y%m%d')
            index_df.to_csv(index_output_file, sep=',', index=False, header=False)
            
        elif submenu=="MFOHLC":
            # fetch mf_ohlc for start date and end date with date as date column
            mf_query = f"SELECT * FROM public.\"mf_ohlc\" WHERE \"date\" >= '{start_date}' AND \"date\" <= '{end_date}' ORDER BY \"date\""
            mf_df = sqlio.read_sql_query(mf_query, conn)
            mf_df['date'] = pd.to_datetime(mf_df['date'])
            mf_df = mf_df[['btt_scheme_code', 'date', 'open', 'high', 'low', 'close', 'volume']]
            
            mf_output_file = os.path.join(cwd, "DownloadedFiles", "txt_files", f"MFOHLC-{start_date}-{end_date}.txt")
            mf_df = mf_df.dropna()
            mf_df = mf_df.round(2)
            mf_df['date'] = mf_df['date'].dt.strftime('%Y%m%d')
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
            "FRS-NAVRank": ('"Reports"."FRS-NAVRank"', '"Date"') ,
            "BTTIndex":('public."nse_index_change"', '"date"') ,
            "mf_ohlc":('public."mf_ohlc"','"date"'),
            "OHLC":('public."OHLC"','"Date"'),
            "IndexOHLC":('public."IndexOHLC"','"Date"'),
            "PE":('public."PE"','"GenDate"'),
            "nse_index_change":('public."nse_index_change"','"date"'),
            "stock_off_high": ('"dash_process"."stock_off_high"', '"date"'),
            "stock_off_low": ('"dash_process"."stock_off_low"', '"date"'),
            "stock_performance": ('"dash_process"."stock_performance"', '"date"'),
            "index_off_high": ('"dash_process"."index_off_high"', '"date"'),
            "index_off_low": ('"dash_process"."index_off_low"', '"date"'),
            "index_performance": ('"dash_process"."index_performance"', '"date"')
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
    dyn_list = {'CompanyCode', 'NSECode', 'BSECode', 'CompanyName', 'ISIN'}
    columns_to_convert = {
        'EPS': ['Q1 Sales', 'Q2 Sales'], 
        'ERS': ['Q1 Sales', 'Q2 Sales'],
        'STANDALONE_ERS': ['Q1 Sales', 'Q2 Sales'], 
        'STANDALONE_EPS': ['Q1 Sales', 'Q2 Sales'], 
        'Consolidated_EPS': ['Q1 Sales', 'Q2 Sales'],
        'Consolidated_ERS': ['Q1 Sales', 'Q2 Sales'],
        'EERS': ['Q1 Sales', 'Q2 Sales'],
        'STANDALONE_EERS': ['Q1 Sales', 'Q2 Sales'],
        'Consolidated_EERS': ['Q1 Sales', 'Q2 Sales'],
        'PRS': ['Value', 'Value Average', 'Market Cap Value'], 
        'IRS': ['OS', 'Volume'],
        'SMR': [],
        'FRS-NAVCategoryAvg': [],
        'CombinedRS': [],
        'FRS-MFRank': [],
        'FRS-NAVRank': [],
        'BTTIndex': [],
        'mf_ohlc': [],
        'OHLC': [],
        'IndexOHLC': [],
        'PE': [],
        'nse_index_change': [], 
        'stock_off_high': [],
        'stock_off_low': [],
        'stock_performance': [],
        'index_off_high': [],
        'index_off_low': [],
        'index_performance': []  
    }
    
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
    if report == 'IRS':
        print("the report that is being downloaded is IRS and the indexmapping is going to take place")
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

# import multiprocessing

# def start_webview():
#     webview.create_window("Bravisa Temple Tree", "http://127.0.0.1:5000", min_size=(950, 800))
#     webview.start()

# if __name__ == "__main__":
#     # Start the webview in a separate process
#     webview_process = multiprocessing.Process(target=start_webview)
#     webview_process.start()

#     # Start the Flask app in the main process
#     app.run(host='127.0.0.1', port=5000, debug=True, use_reloader=True)

#     # Ensure the webview process is terminated when the Flask app is closed
#     webview_process.join()


