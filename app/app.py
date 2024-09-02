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

from flask import Flask, render_template, request, jsonify
# Assuming necessary imports and configurations are done here
def check_files_presence(date, is_holiday):
    # Define the three file names
    ohlc_folder = 'D:\\Desktop Copy\\Braviza\\app\\OHLCFiles\\'
    index_ohlc_folder = 'D:\\Desktop Copy\\Braviza\\app\\IndexOHLCFiles\\'
    index_files_folder = 'D:\\Desktop Copy\\Braviza\\app\\index-files\\'
    
    if date.weekday() in [5, 6] or is_holiday:
        print("Skipping file check for weekend:", date.strftime("%Y-%m-%d"))
        return True
    
    eqisin = date.strftime("%Y%m%d").upper()
    cmbhav = date.strftime("%Y%m%d").upper()

    # eqisin = date.strftime("%d%m%y")
    indall = date.strftime("%d%m%Y")

    
    # Check if it's the start of the month
    if date.day == 1:
        # Check if all required files are present along with index files
        if (os.path.isfile(os.path.join(ohlc_folder, "BhavCopy_BSE_CM_0_0_0_" + eqisin + "_F_0000.csv")) and
            os.path.isfile(os.path.join(ohlc_folder, "BhavCopy_NSE_CM_0_0_0_"+ cmbhav +"_F_0000.csv" )) and
            os.path.isfile(os.path.join(index_ohlc_folder, "ind_close_all_" + indall + ".csv")) and
            os.path.isfile(os.path.join(index_files_folder, "ind_nifty500list.csv"))): #and
            #os.path.isfile(os.path.join(index_files_folder, "BSE500_Index.csv"))):
            return True
        else:
            return False
    else:
        # Check remaining files for other dates of the month
        if (os.path.isfile(os.path.join(ohlc_folder, "BhavCopy_BSE_CM_0_0_0_" + eqisin + "_F_0000.csv")) and
            os.path.isfile(os.path.join(ohlc_folder, "BhavCopy_NSE_CM_0_0_0_"+ cmbhav +"_F_0000.csv" )) and
            os.path.isfile(os.path.join(index_ohlc_folder, "ind_close_all_" + indall + ".csv"))):
            return True
        else:
            return False

app = Flask(__name__, root_path='D:\\Desktop Copy\\Braviza\\app\\')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/industrymap')
def industrymap():
    return render_template('industrymap.html')

@app.route('/add_industry_mapping', methods=['POST'])
def add_industry_mapping():
    warnings.filterwarnings("ignore", category=UserWarning)

    db = DB_Helper()
    conn = db.db_connect()
    cur = conn.cursor()

    industry_code = request.form.get('IndustryCode')
    industry_name = request.form.get('IndustryName')
    industry = request.form.get('Industry')
    code = request.form.get('Code')
    subsector = request.form.get('SubSector')
    subsector_code = request.form.get('SubSectorCode')
    sector = request.form.get('Sector')
    sector_code = request.form.get('SectorCode')
    subindustry = request.form.get('SubIndustry')
    subindustry_code = request.form.get('SubIndustryCode')

    IndustryIndexName =  'INDUSTRY-'+ industry
    SectorIndexName = 'SECTOR-'+ sector
    SubSectorIndexName = "SUBSECTOR-"+ subsector
    SubIndustryIndexName = 'SUBINDUSTRY-'+ subindustry 

    sql = """INSERT INTO public."IndustryMapping"(
        "IndustryCode", "IndustryName", "Industry", "Code", "SubSector", "SubSectorCode", "Sector", "SectorCode", \
        "SubIndustry", "SubIndustryCode", "IndustryIndexName", "SubSectorIndexName", "SectorIndexName", "SubIndustryIndexName")
        VALUES (%s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s,%s);"""

    data = (industry_code, industry_name, industry, code, subsector, subsector_code, sector, sector_code, \
        subindustry, subindustry_code, IndustryIndexName, SubSectorIndexName, SectorIndexName, SubIndustryIndexName)
    
    try:
        cur.execute(sql, data)
        conn.commit() 
        return jsonify({'message': 'New IndustryMapping data added successfully.'})
    except psycopg2.Error as e:
        print(e)
        conn.rollback()
        return jsonify({'message': 'Error inserting data into the table.'})  

@app.route('/process', methods=['POST'])
def process():
    warnings.filterwarnings("ignore", category=UserWarning)

    db = DB_Helper()
    dbURL = db.engine()
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
    
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as e:
        return jsonify({'message': 'Invalid date format. Please use YYYY-MM-DD.'})

    if main_menu == "insert_data":
        insert_data(start_date, end_date, dbURL, conn, cur)
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
            insert_data(current_date, current_date, dbURL, conn, cur)
            result = report_generation(current_date, current_date, is_holiday)
            if not result:
                return jsonify({'message': 'File not found for date: ' + current_date.strftime("%Y-%m-%d")})

        end_time = time.time()
        elapsed_time = end_time - start_time
        return jsonify({'message': 'Insert and generate processing successful', 'time_taken': elapsed_time})

    elif main_menu == "delete_report":
        delete_data(start_date, end_date, submenu, conn, cur)
        return jsonify({'message': 'Generate report for delete processing successful'})

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
            "FRS-NavRank": ('"Reports"."FRS-NAVRank"', '"Date"'),
            "CombinedRS": ('"Reports"."CombinedRS"', '"GenDate"')
        }

        if submenu == "All Reports":
            selected_reports = report_mapping.keys()
            print(selected_reports)
        else:
            selected_reports = [submenu]

        for report in selected_reports:
            table, date_column = report_mapping[report]
            output_name= table.split('.')[1].replace('\"', '')
            output_file = os.path.join(cwd, "DownloadedFiles", f"{output_name}-{start_date}-{end_date}.csv")
            print(output_file)
            query = f"SELECT * FROM {table} WHERE {date_column} >= '{start_date}' AND {date_column} <= '{end_date}' ORDER BY {date_column}"
            df = sqlio.read_sql_query(query, con = conn)
            if df.empty:
                pass
            else:
                table = table.split('.')[1].replace('\"', '')
                download_csv(df, table, output_file)
        return jsonify({'message': 'Download report processing successful'})

    else:
        return jsonify({'message': 'Invalid main_menu value'})

def insert_data(start_date, end_date, dbURL, conn, cur):
    date_format = "%d%m%Y"
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() == 0:  # Monday
            prev_date = current_date - timedelta(days=2)  # Saturday
        else:
            prev_date = current_date - timedelta(days=1)  # Previous day

        print(current_date)
        FB_Insert().fb_insert_03("FB" + prev_date.strftime(date_format) + "03", conn, cur)
        FB_Insert().fb_insert_01("FB" + current_date.strftime(date_format) + "01", conn, cur)
        FB_Insert().fb_insert_02("FB" + current_date.strftime(date_format) + "02", conn, cur)
        print("Inserted: ", current_date)
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

def download_csv(csv, report, filename):
    dyn_list =  {'CompanyCode', 'NSECode', 'BSECode', 'CompanyName', 'ISIN'}
    columns_to_convert = {'EPS':['Q1 Sales', 'Q2 Sales'], 'ERS':['Q1 Sales', 'Q2 Sales'],'STANDALONE_ERS':['Q1 Sales', 'Q2 Sales'], 'STANDALONE_EPS':['Q1 Sales', 'Q2 Sales'], 'Consolidated_EPS':['Q1 Sales', 'Q2 Sales'],'Consolidated_ERS':['Q1 Sales', 'Q2 Sales'], 'EERS':['Q1 Sales', 'Q2 Sales'],'STANDALONE_EERS':['Q1 Sales', 'Q2 Sales'],'Consolidated_EERS':['Q1 Sales', 'Q2 Sales'], 'PRS':['Value', 'Value Average', 'Market Cap Value'], 'IRS':['OS', 'Volume'], "SMR": ('"Reports"."SMR"', '"SMRDate"'),
            "FRS-NavRank": [],
            "CombinedRS": []}
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
        index_mapping = {'INDUSTRY-Automobiles': 'AUTO-I', 'INDUSTRY-Automobile Components': 'AUTOCOMP-I', 'INDUSTRY-Specialty Retail': 'SPECRETAI-I', 'INDUSTRY-Consumer Staples Distribution & Retail': 'CONSTAPDIST-I', 'INDUSTRY-Broadline Retail': 'BROADRETAI-I', 'INDUSTRY-Food Products': 'FOODPROD-I', 'INDUSTRY-Beverages': 'BEVERAGE-I', 'INDUSTRY-Building Products': 'BUILDPROD-I', 'INDUSTRY-Construction & Engineering': 'CONSENGINE-I', 'INDUSTRY-Chemicals': 'CHEMICAL-I', 'INDUSTRY-Textiles, Apparel & Luxury Goods': 'TEXAPPLUX-I', 'INDUSTRY-Containers & Packaging': 'CONTAINPKG-I', 'INDUSTRY-Oil, Gas & Consumable Fuels': 'ONGCONFUEL-I', 'INDUSTRY-Household Products': 'HOUSEPROD-I', 'INDUSTRY-Personal Care Products': 'PERCARPROD-I', 'INDUSTRY-Pharmaceuticals': 'PHARMAC-I', 'INDUSTRY-Health Care Equipment & Supplies': 'HLTCAEQSUPP-I', 'INDUSTRY-Tobacco': 'TOBACCO-I', 'INDUSTRY-Household Durables': 'HOUSEDUR-I', 'INDUSTRY-Technology Hardware, Storage & Peripherals': 'TECHARDSOFTPER-I', 'INDUSTRY-Software': 'SOFTWARE-I', 'INDUSTRY-Electronic Equipment, Instruments & Components': 'ELEEQINSCMP-I', 'INDUSTRY-Commercial Services & Supplies': 'COMSERVSUP-I', 'INDUSTRY-Electrical Equipment': 'ELECEQUIP-I', 'INDUSTRY-IT Services': 'ITSERV-I', 'INDUSTRY-Machinery': 'MACHINERY-I', 'INDUSTRY-Insurance': 'INSURAN-I', 'INDUSTRY-Banks': 'BANKS-I', 'INDUSTRY-Financial Services': 'FINSERV-I', 'INDUSTRY-Capital Markets': 'CAPIMKT-I', 'INDUSTRY-Metals & Mining': 'METAMINE-I', 'INDUSTRY-Energy Equipment & Services': 'ENEQUSERV-I', 'INDUSTRY-Electric Utilities': 'ELECUTIL-I', 'INDUSTRY-Diversified Telecommunication Services': 'DIVTELECOMSERV-I', 'INDUSTRY-Independent Power and Renewable Electricity Producers': 'INDPOWRENEL-I', 'INDUSTRY-Paper & Forest Products': 'PAPFORPROD-I', 'INDUSTRY-Health Care Providers & Services': 'HLTCAPROSERV-I', 'INDUSTRY-Marine Transportation': 'MARITRA-I', 'INDUSTRY-Ground Transportation': 'GRNDTRAN-SI', 'INDUSTRY-Air Freight & Logistics': 'AIRFRELOGIS-I', 'INDUSTRY-Hotels, Restaurants & Leisure': 'HOTRESLEIS-I', 'INDUSTRY-Transportation Infrastructure': 'TRANSINFRA-I', 'INDUSTRY-Interactive Media & Services': 'INTMEDSERV-I', 'INDUSTRY-Diversified Consumer Services': 'DIVERCONSERV-I', 'INDUSTRY-Real Estate Management & Development': 'RESMGMDEV-I', 'INDUSTRY-Entertainment': 'ENTERTAIN-I', 'INDUSTRY-Retail REITs': 'RETIALREIT-I', 'INDUSTRY-Media': 'MEDIA-I', 'INDUSTRY-Gas Utilities': 'GASUTIL-I', 'INDUSTRY-Leisure Products': 'LEISPROD-I', 'SUBINDUSTRY-Auto - LCVs/HCVs': 'AUTLCVHCV-SI', 'SUBINDUSTRY-Auto - Cars & Jeeps': 'AUTCARJEE-SI', 'SUBINDUSTRY-Motorcycle Manufacturers': 'MOTORMFG-SI', 'SUBINDUSTRY-Automotive Parts & Equipment': 'AUTPEQUIP-SI', 'SUBINDUSTRY-Tires & Rubber': 'TIRERUB-SI', 'SUBINDUSTRY-Carbon Black': 'CARBLCK-SI', 'SUBINDUSTRY-Other Specialty Retail': 'OTHSPECRETL-SI', 'SUBINDUSTRY-Retail - Departmental Stores': 'RETDEPTSTO-SI', 'SUBINDUSTRY-Retail - Apparel': 'RETAPPAREL-SI', 'SUBINDUSTRY-Retail - Apparel/Accessories': 'RETAPPARACC-SI', 'SUBINDUSTRY-Trading': 'TRADING-SI', 'SUBINDUSTRY-Plantations': 'PLANTATIO-SI', 'SUBINDUSTRY-Distillers & Vintners': 'DISTILVINTN-SI', 'SUBINDUSTRY-Aqua & Horticulture': 'AQUAHORTIC-SI', 'SUBINDUSTRY-Edible Oils': 'EDIBLEOIL-SI', 'SUBINDUSTRY-Agricultural Products & Services': 'AGRIPROSER-SI', 'SUBINDUSTRY-Soft Drinks & Non-alcoholic Beverages': 'SODRINALCBEV-SI', 'SUBINDUSTRY-Packaged Foods & Meats': 'PKGFOODMEAT-SI', 'SUBINDUSTRY-Sugar': 'SUGAR-SI', 'SUBINDUSTRY-Cement & Products': 'CEMPROD-SI', 'SUBINDUSTRY-Tiles & Granites': 'TILEGRANI-SI', 'SUBINDUSTRY-Decoratives & Laminates': 'DECOLAMIN-SI', 'SUBINDUSTRY-Paints': 'PAINTS-SI', 'SUBINDUSTRY-Building Products': 'BUILDPROD-SI', 'SUBINDUSTRY-Construction & Engineering': 'CONSENGINE-SI', 'SUBINDUSTRY-Commodity Chemicals': 'COMMOCHEM-SI', 'SUBINDUSTRY-Specialty Chemicals': 'SPECHEM-SI', 'SUBINDUSTRY-Fertilizers & Agricultural Chemicals': 'FERAGRICHM-SI', 'SUBINDUSTRY-Diversified Chemicals': 'DIVERCHEM-SI', 'SUBINDUSTRY-Textiles': 'TEXTILES-SI', 'SUBINDUSTRY-Paper & Plastic Packaging Products & Materials': 'PPLPGPROMAT-SI', 'SUBINDUSTRY-Oil & Gas Refining & Marketing': 'ONGREFMKT-SI', 'SUBINDUSTRY-Household Products': 'HOUSEPROD-SI', 'SUBINDUSTRY-Personal Care Products': 'PERCARPROD-SI', 'SUBINDUSTRY-Pharmaceuticals': 'PHARMAC-SI', 'SUBINDUSTRY-Apparel, Accessories & Luxury Goods': 'APPACCLUX-SI', 'SUBINDUSTRY-Health Care Supplies': 'HLTCARESUPP-SI', 'SUBINDUSTRY-Tobacco': 'TOBACCO-SI', 'SUBINDUSTRY-Household Appliances': 'HOUSEAPPLI-SI', 'SUBINDUSTRY-Technology Hardware, Storage & Peripherals': 'TECHARDSOFTPER-SI', 'SUBINDUSTRY-Systems Software': 'SYSOFT-SI', 'SUBINDUSTRY-Electronic Equipment & Instruments': 'ELECEQUINS-SI', 'SUBINDUSTRY-Office Services & Supplies': 'OFFSERVSUPP-SI', 'SUBINDUSTRY-Consumer Electronics': 'CONSELEC-SI', 'SUBINDUSTRY-Electrical Components & Equipment': 'ELECOMEQU-SI', 'SUBINDUSTRY-Health Care Equipment': 'HLTCAREQU-SI', 'SUBINDUSTRY-Application Software': 'APPLICSOFT-SI', 'SUBINDUSTRY-IT Consulting & Other Services': 'ITCONSOTHSV-SI', 'SUBINDUSTRY-Research & Consulting Services': 'RESCONSERV-SI', 'SUBINDUSTRY-Industrial Machinery & Supplies & Components': 'INDMCHSUPCOM-SI', 'SUBINDUSTRY-Finance - Life Insurance': 'FINLIFINS-SI', 'SUBINDUSTRY-Private Banks': 'PVTBNK-SI', 'SUBINDUSTRY-Finance - Term Lending': 'FINTERLEND-SI', 'SUBINDUSTRY-Finance - Mutual Funds': 'FINMF-SI', 'SUBINDUSTRY-Investment Trusts': 'INVTRUST-SI', 'SUBINDUSTRY-Finance - Housing': 'FINHSG-SI', 'SUBINDUSTRY-Finance & Investments': 'FININVTS-SI', 'SUBINDUSTRY-PSU Banks': 'PSUBNK-SI', 'SUBINDUSTRY-Finance - Non Life Insurance': 'FINNONLIFINS-SI', 'SUBINDUSTRY-Reinsurance': 'REINSURE-SI', 'SUBINDUSTRY-Financial Exchanges & Data': 'FINEXCHDATA-SI', 'SUBINDUSTRY-Bearings': 'BEARINGS-SI', 'SUBINDUSTRY-Ferro Alloys': 'FERALLO-SI', 'SUBINDUSTRY-Fasteners': 'FASTENERS-SI', 'SUBINDUSTRY-Industrial Gases': 'INDUSGAS-SI', 'SUBINDUSTRY-Metal, Glass & Plastic Containers': 'METGLAPLACON-SI', 'SUBINDUSTRY-Aluminum': 'ALUMINIUM-SI', 'SUBINDUSTRY-Precious Metals & Minerals': 'PRECMETMIN-SI', 'SUBINDUSTRY-Diversified Metals & Mining': 'DIVERMETMIN-SI', 'SUBINDUSTRY-Oil & Gas Drilling': 'ONGDRILL-SI', 'SUBINDUSTRY-Electric Utilities': 'ELECUTILI-SI', 'SUBINDUSTRY-Telecommunications - Equipment': 'TELECOMEQU-SI', 'SUBINDUSTRY-Heavy Electrical Equipment': 'HVYELECEQUIP-SI', 'SUBINDUSTRY-Renewable Electricity': 'RENEWELEC-SI', 'SUBINDUSTRY-Copper': 'COPPER-SI', 'SUBINDUSTRY-Integrated Telecommunication Services': 'INTGTELCOSVC-SI', 'SUBINDUSTRY-Infrastructure - General': 'INFRAGEN-SI', 'SUBINDUSTRY-Steel': 'STEEL-SI', 'SUBINDUSTRY-Paper Products': 'PAPERPROD-SI1', 'SUBINDUSTRY-Livestock - Hatcheries/Poultry': 'LIVHATCHPOUL-SI', 'SUBINDUSTRY-Health Care Facilities': 'HLTCAREFACIL-SI', 'SUBINDUSTRY-Marine Transportation': 'MARITRA-SI', 'SUBINDUSTRY-Transport - Road': 'TRAROAD-SI', 'SUBINDUSTRY-Transport - Air': 'TRAAIR-SI', 'SUBINDUSTRY-Hotels, Resorts & Cruise Lines': 'HOTRESCRUIS-SI', 'SUBINDUSTRY-Marine Ports & Services': 'MARIPORSV-SI', 'SUBINDUSTRY-Diversified Support Services': 'DIVERSUPSER-SI', 'SUBINDUSTRY-Interactive Media & Services': 'INTMEDSERV-SI', 'SUBINDUSTRY-Fire Protection Equipment': 'FIRPROTEQU-SI', 'SUBINDUSTRY-LPG Bottling/Distribution': 'LPGBOTDIST-SI', 'SUBINDUSTRY-Commercial Printing': 'COMMERPRINT-SI', 'SUBINDUSTRY-Agricultural & Farm Machinery': 'AGRIFARMCH-SI', 'SUBINDUSTRY-Diversified Financial Services': 'DIVERFINSER-SI', 'SUBINDUSTRY-E-Commerce - Retail': 'ECOMRET-SI', 'SUBINDUSTRY-Education Services': 'EDUSERV-SI', 'SUBINDUSTRY-Real Estate Development': 'REALSTATDEV-SI', 'SUBINDUSTRY-Waste Management': 'WASTEMAN-SI', 'SUBINDUSTRY-Multi-Sector Holdings': 'MULTSECTHLD-SI', 'SUBINDUSTRY-Fintech': 'FINTEC-SI', 'SUBINDUSTRY-Health Care Services': 'HLTCARESERV-SI', 'SUBINDUSTRY-Digital Entertainment': 'DIGIENTER-SI', 'SUBINDUSTRY-Leisure Facilities': 'LEISFACIL-SI', 'SUBINDUSTRY-Dairy': 'DAIRY-SI', 'SUBINDUSTRY-Marine Foods': 'MARIFOOD-SI', 'SUBINDUSTRY-Road Infrastructure': 'ROADINFRA-SI', 'SUBINDUSTRY-Retail REITs': 'RETAILREIT-SI', 'SUBINDUSTRY-Specialized Finance': 'SPECIFIN-SI', 'SUBINDUSTRY-Railway Wagons and Wans': 'RAILWAGWAN-SI', 'SUBINDUSTRY-Footwear': 'FOOTWEAR-SI', 'SUBINDUSTRY-Advertising': 'ADVERT-SI', 'SUBINDUSTRY-Construction Machinery & Heavy Transportation Equipment': 'CONMCHVYTRN-SI', 'SUBINDUSTRY-Home Furnishings': 'HOMFURNIS-SI', 'SUBINDUSTRY-Gas Utilities': 'GASUTIL-SI', 'SUBINDUSTRY-Insurance Brokers': 'INSURBROK-SI', 'SUBINDUSTRY-Leisure Products': 'LEISPROD-SI', 'SUBINDUSTRY-Micro Finance Institutions': 'MICROFININS-SI', 'SUBINDUSTRY-Oil & Gas Equipment & Services': 'ONGEQUSERV-SI', 'SUBINDUSTRY-Diversified Capital Markets': 'DIVERCAPMKT-SI', 'SECTOR-Consumer Discretionary': 'CONSDISC-S', 'SECTOR-Consumer Staples': 'CONSTAPL-S', 'SECTOR-Industrials': 'INDUSTRIAL-S', 'SECTOR-Materials': 'MATERIALS-S', 'SECTOR-Energy': 'ENERGY-S', 'SECTOR-Health Care': 'HLTHCARE-S', 'SECTOR-Information Technology': 'INFOTEC-S', 'SECTOR-Financials': 'FINANCIALS-S', 'SECTOR-Utilities': 'UTILITIES-S', 'SECTOR-Communication Services': 'COMMSERV-S', 'SECTOR-Real Estate': 'REALESTATE-S', 'SUBSECTOR-Automobiles & Components': 'AUTOCOMP-SS', 'SUBSECTOR-Consumer Discretionary Distribution & Retail': 'CONSDDRETA-SS', 'SUBSECTOR-Consumer Staples Distribution & Retail': 'CONSTAPDISRE-SS', 'SUBSECTOR-Food, Beverage & Tobacco': 'FOOBEVTOBA-SS', 'SUBSECTOR-Capital Goods': 'CAPIGOOD-SS', 'SUBSECTOR-Materials': 'MATERIAL-SS', 'SUBSECTOR-Consumer Durables & Apparel': 'CONSDURAPP-SS', 'SUBSECTOR-Energy': 'ENERGY-SS', 'SUBSECTOR-Household & Personal Products': 'HOUSPERS-SS', 'SUBSECTOR-Pharmaceuticals, Biotechnology & Life Sciences': 'PHARMBIOLIFS-SS', 'SUBSECTOR-Health Care Equipment & Services': 'HLTCAREQUSV-SS', 'SUBSECTOR-Technology Hardware & Equipment': 'TECHARDEQUI-SS', 'SUBSECTOR-Software & Services': 'SOFTSERV-SS', 'SUBSECTOR-Commercial & Professional Services': 'COMPRFSERV-SS', 'SUBSECTOR-Insurance': 'INSURANCE-SS', 'SUBSECTOR-Banks': 'BANKS-SS', 'SUBSECTOR-Financial Services': 'FINSERV-SS', 'SUBSECTOR-Utilities': 'UTILITIES-SS', 'SUBSECTOR-Telecommunication Services': 'TELECOSERV-SS', 'SUBSECTOR-Transportation': 'TRANSPORT-SS', 'SUBSECTOR-Consumer Services': 'CONSERV-SS', 'SUBSECTOR-Media & Entertainment': 'MEDIAENT-SS', 'SUBSECTOR-Real Estate Management & Development': 'RESMGMDEV-SS', 'SUBSECTOR-Equity Real Estate Investment Trusts (REITs)': 'EQREITS-SS'}
        # map the index_mapping to the IndexName column
        csv['IndexName'] = csv['IndexName'].map(index_mapping)
            
    csv.to_csv(filename, index=False)
    


def start_flask():
    app.run(host='127.0.0.1', port=5000)
    

if __name__ == "__main__":
    t = threading.Thread(target=start_flask)
    t.daemon = True
    t.start()

    webview.create_window("Bravisa Temple Tree", "http://127.0.0.1:5000", min_size=(800,800))
    webview.start()





# app = Flask(__name__)

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/industrymap')
# def industrymap():
#     return render_template('industrymap.html')



# @app.route('/add_industry_mapping', methods=['POST'])
# def add_industry_mapping():
#     warnings.filterwarnings("ignore", category=UserWarning)

#     db = DB_Helper()
#     conn = db.db_connect()
#     cur = conn.cursor()

#     industry_code = request.form.get('IndustryCode')
#     industry_name = request.form.get('IndustryName')
#     industry = request.form.get('Industry')
#     code = request.form.get('Code')
#     subsector = request.form.get('SubSector')
#     subsector_code = request.form.get('SubSectorCode')
#     sector = request.form.get('Sector')
#     sector_code = request.form.get('SectorCode')
#     subindustry = request.form.get('SubIndustry')
#     subindustry_code = request.form.get('SubIndustryCode')

#     IndustryIndexName =  'INDUSTRY-'+ industry
#     SectorIndexName = 'SECTOR-'+ sector
#     SubSectorIndexName = "SUBSECTOR-"+ subsector
#     SubIndustryIndexName = 'SUBINDUSTRY-'+ subindustry 

#     sql = """INSERT INTO public."IndustryMapping"(
# 	    "IndustryCode", "IndustryName", "Industry", "Code", "SubSector", "SubSectorCode", "Sector", "SectorCode", \
#         "SubIndustry", "SubIndustryCode", "IndustryIndexName", "SubSectorIndexName", "SectorIndexName", "SubIndustryIndexName")
# 	    VALUES (%s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s,%s);"""

#     data = (industry_code, industry_name, industry, code, subsector, subsector_code, sector, sector_code, \
#         subindustry, subindustry_code, IndustryIndexName, SubSectorIndexName, SectorIndexName, SubIndustryIndexName)
    
#     try:
#         cur.execute(sql, data)
#         conn.commit() 
#         return jsonify({'message': 'New IndustryMapping data added successfully.'})
#     except psycopg2.Error as e:
#         print(e)
#         conn.rollback()
#         return jsonify({'message': 'Error inserting data into the table.'})  




# @app.route('/process', methods=['POST'])

# def process():
#     warnings.filterwarnings("ignore", category=UserWarning)

#     db = DB_Helper()
#     dbURL = db.engine()
#     conn = db.db_connect()
#     cur = conn.cursor()
#     start_date = request.form.get('start_date')
#     end_date = request.form.get('end_date')
#     main_menu = request.form.get('main_menu')
#     submenu = request.form.get('submenu')
#     is_holiday = request.form.get('is_holiday') == 'true'
#     print(is_holiday)
#     print(start_date, end_date)
#     print(submenu)
        
#     # app.config['UPLOAD_FOLDER'] = 'uploads/'  # Folder to store uploaded files

#     # # Ensure the upload folder exists
#     # os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
#     start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
#     end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

#     # # Handle file upload
#     # if 'file' in request.files:
#     #     file = request.files['file']
#     #     if file.filename == '':
#     #         return jsonify({'message': 'No selected file'}), 400
        
#     #     if file:
#     #         filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
#     #         file.save(filename)
#     #         # Process the file as needed
#     #         # For example, you could read the file, parse it, and extract data

#     if main_menu == "insert_data":
#         insert_data(start_date, end_date, dbURL, conn, cur)
#         return jsonify({'message': 'Insert data processing successful'})

#     elif main_menu == "generate_report":
#         start_time = time.time() # Record the Start time
#         # Difference between end date and start date
#         date_difference = end_date - start_date 

#         # Number of days between start date and end date
#         num_days = date_difference.days + 1

#         # Generate individual dates between start date and end date
#         for i in range(num_days):
#             current_date = start_date + timedelta(days=i)
#             # report_generation(current_date, current_date,is_holiday)
#             result = report_generation(current_date, current_date, is_holiday)
#             if result:
#                 pass
#             else:
#                 return jsonify({'message': 'File not found'})

        

#     elif main_menu == "insert_generate":

#         start_time = time.time() # Record the Start time
#         # Difference between end date and start date
#         date_difference = end_date - start_date 

#         # Number of days between start date and end date
#         num_days = date_difference.days + 1

#         # Generate individual dates between start date and end date
#         for i in range(num_days):
#             current_date = start_date + timedelta(days=i)
#             insert_data(current_date, current_date, dbURL, conn, cur)
#             # report_generation(current_date, current_date,is_holiday)
#             result = report_generation(current_date, current_date, is_holiday)
#             if result:
#                 pass
#             else:
#                 return jsonify({'message': 'File not found'})
        
#         end_time = time.time()  # Record the end time
#         elapsed_time = end_time - start_time
        
#         return jsonify({'message': 'Insert and generate processing successful', 'time_taken': elapsed_time})


#     elif main_menu == "delete_report":
#         delete_data(start_date, end_date, submenu, conn, cur)
#         return jsonify({'message': 'Generate report for delete processing successful'})

    # elif main_menu == "download":
    #     cwd = os.getcwd()
    #     report_mapping = {
    #         "EPS": ('"Reports"."EPS"', '"EPSDate"'),
    #         "Standalone EPS": ('"Reports"."STANDALONE_EPS"', '"EPSDate"'),
    #         "Consolidated EPS": ('"Reports"."Consolidated_EPS"', '"EPSDate"'),
    #         "PRS": ('"Reports"."PRS"', '"Date"'),
    #         "SMR": ('"Reports"."SMR"', '"SMRDate"'),
    #         "IRS": ('"Reports"."IRS"', '"GenDate"'),
    #         "FRS-NavRank": ('"Reports"."FRS-NAVRank"', '"Date"'),
    #         "CombinedRS": ('"Reports"."CombinedRS"', '"GenDate"')
    #     }

    #     if submenu == "All Reports":
    #         selected_reports = report_mapping.keys()
    #     else:
    #         selected_reports = [submenu]

    #     for report in selected_reports:
    #         table, date_column = report_mapping[report]
    #         output_name= table.split('.')[1].replace('\"', '')
    #         output_file = os.path.join(cwd, "DownloadedFiles", f"{output_name}-{start_date}-{end_date}.csv")
    #         query = f"SELECT * FROM {table} WHERE {date_column} >= '{start_date}' AND {date_column} <= '{end_date}' ORDER BY {date_column}"
    #         df = sqlio.read_sql_query(query, con = conn)
    #         if df.empty:
    #             return jsonify({'message': 'No Data Found! Generate the Report.'})
    #         else:
    #             table = table.split('.')[1].replace('\"', '')
    #             download_csv(df, table, output_file)
    #             return jsonify({'message': 'Download report processing successful'})

    # else:
    #     return jsonify({'message': 'Invalid main_menu value'})





# from datetime import timedelta

# def insert_data(start_date, end_date, dbURL, conn, cur):
#     date_format = "%d%m%Y"
#     current_date = start_date
#     while current_date <= end_date:
#         if current_date.weekday() == 0:  # Monday
#             prev_date = current_date - timedelta(days=2)  # Saturday
#         else:
#             prev_date = current_date - timedelta(days=1)  # Previous day

#         print(current_date)
#         FB_Insert().fb_insert_03("FB" + prev_date.strftime(date_format) + "03", conn, cur)
#         FB_Insert().fb_insert_01("FB" + current_date.strftime(date_format) + "01", conn, cur)
#         FB_Insert().fb_insert_02("FB" + current_date.strftime(date_format) + "02", conn, cur)
#         print("Inserted: ", current_date)
#         current_date += timedelta(days=1)


# def report_generation(startdate, enddate, is_holiday):
#     date_difference = enddate - startdate 

#     # Number of days between start date and end date
#     num_days = date_difference.days + 1

#     isPresent = True
#     # Generate individual dates between start date and end date
#     for i in range(num_days):
#         current_date = startdate + timedelta(days=i)
#         isPresent = True
#         # isPresent = check_files_presence(current_date, is_holiday)
#         if not isPresent:
#             break
        
#     if isPresent:
#         run_scripts_frompy(startdate, enddate, is_holiday)
#         return True
#     else:
#         print("False no file found")
#         return False
    

# def delete_data(startdate, enddate, delete_variable, conn, cur):
#     if(delete_variable == "Traditional Delete"):
#         run_delete(startdate, enddate)

#     elif(delete_variable == "EPS"):
#         print("Deleting EPS reports")
#         sql =   """
#                     DELETE FROM "Reports"."EPS"
#                     WHERE "EPSDate" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "EPSDate" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()

#     elif(delete_variable == "Standalone EPS"):
#         print("Deleting EPS reports")
#         sql =   """
#                     DELETE FROM "Reports"."STANDALONE_EPS"
#                     WHERE "EPSDate" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "EPSDate" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()

#     elif(delete_variable == "Consolidated EPS"):
#         print("Deleting EPS reports")
#         sql =   """
#                     DELETE FROM "Reports"."Consolidated_EPS"
#                     WHERE "EPSDate" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "EPSDate" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()



#     elif(delete_variable == "PRS"):
#         print("Deleting PRS reports")
#         sql =   """
#                     DELETE FROM "Reports"."PRS"
#                     WHERE "Date" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "Date" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()

#     elif(delete_variable == "SMR"):
#         print("Deleting SMR reports")
#         sql =   """
#                     DELETE FROM "Reports"."SMR"
#                     WHERE "SMRDate" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "SMRDate" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()

#     elif(delete_variable == "IRS"):
#         print("Deleting IRS reports")
#         sql =   """
#                     DELETE FROM "Reports"."IRS"
#                     WHERE "GenDate" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "GenDate" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()

#     elif(delete_variable == "FRS-NavRank"):
#         print("Deleting FRS-NavRank reports")
#         sql =   """
#                     DELETE FROM "Reports"."FRS-NAVRank"
#                     WHERE "Date" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "Date" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()

#     elif(delete_variable == "CombinedRS"):
#         print("Deleting CombinedRS reports")
#         sql =   """
#                     DELETE FROM "Reports"."CombinedRS"
#                     WHERE "GenDate" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "GenDate" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()

#     elif(delete_variable == "BTT"):
#         print("Deleting BTT reports")
#         sql =   """
#                     DELETE FROM "public"."BTTList"
#                     WHERE "BTTDate" >= TO_DATE(%s, 'YYYY-MM-DD')
#                     AND "BTTDate" <= TO_DATE(%s, 'YYYY-MM-DD');
#                 """
#         cur.execute(sql, (str(startdate), str(enddate)))
#         conn.commit()



# def download_csv(csv, report, filename):
#     dyn_list =  {'CompanyCode', 'NSECode', 'BSECode', 'CompanyName', 'ISIN'}
#     columns_to_convert = {'EPS':['Q1 Sales', 'Q2 Sales'], 'STANDALONE_EPS':['Q1 Sales', 'Q2 Sales'], 'Consolidated_EPS':['Q1 Sales', 'Q2 Sales'], 'PRS':['Value', 'Value Average', 'Market Cap Value'], 'IRS':['OS', 'Volume']}
#     # loop through columns
#     for col in csv.columns:
#         # if the column is object type
#         if csv[col].dtype == 'object':            
#             dyn_list.add(col) 

#         if col not in dyn_list:
#             # convert decimal to 2 decimal places and handle NaN values
#             csv[col] = csv[col].apply(lambda x: round(float(x), 2) if not pd.isnull(x) else x)
#             # convert all column value into Cr if more than 8 digits
#         if col in columns_to_convert[report]:
#             csv[col] = csv[col].apply(lambda x: str(round(float(x/10000000), 2)) if x > 10000000 else x)
#             csv.rename(columns = {col: col + " Cr"}, inplace = True)

#     csv.to_csv(filename, index=False)
    


