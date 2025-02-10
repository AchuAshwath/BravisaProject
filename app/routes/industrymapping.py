from flask import Blueprint
from utils.db_helper import DB_Helper
import pandas as pd
import os
from flask import Flask, render_template, request, jsonify
import psycopg2
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Create Blueprint
industry_mapping = Blueprint('industry_mapping', __name__, template_folder="../templates")

db = DB_Helper()

@industry_mapping.route('/industrymap')
def industrymap():
    db = DB_Helper()
    conn = db.db_connect()
    cur = conn.cursor()
    
    industry_mapping_sql = 'SELECT * FROM public."IndustryMapping";'
    background_info_sql = 'SELECT * FROM public."BackgroundInfo";'

    # Fetch data into DataFrames
    industry_mapping_df = pd.read_sql_query(industry_mapping_sql, conn)
    background_info_df = pd.read_sql_query(background_info_sql, conn)


    # Get the list of CompanyCode where IndustryCode is missing in IndustryMapping
    missing_industry_codes = background_info_df[~background_info_df['IndustryCode'].isin(industry_mapping_df['IndustryCode'])]

    missing_industry_code_list = missing_industry_codes['IndustryCode'].unique().tolist()

    # Display the result
    print("IndustryCodes that are missing:", len(missing_industry_code_list))
    print("IndustryCodes that are missing:", missing_industry_code_list)
    
    # IndexNames
    df = pd.read_sql_query('SELECT * FROM irs_index_mapping', conn)

    # get df from IndustryMapping table
    df2 = industry_mapping_df

    # create a unique list of InustryIncexName, SubIndustryIndexName, SectorIndexName, SubSectorIndexName
    industry_index_names = list(df2['IndustryIndexName'].unique())
    sub_industry_index_names = list(df2['SubIndustryIndexName'].unique())
    sector_index_names = list(df2['SectorIndexName'].unique())
    sub_sector_index_names = list(df2['SubSectorIndexName'].unique())

    #combine all the unique index names
    all_index_names = industry_index_names + sub_industry_index_names + sector_index_names + sub_sector_index_names

    print("total index names : ",len(all_index_names))

    #find out the index names that are not in the irs_index_mapping table
    missing_index_names = []
    for index_name in all_index_names:
        if index_name not in list(df['indexname']):
            missing_index_names.append(index_name)

    print("missing index names : ",len(missing_index_names))
    print(missing_index_names)
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    
    return render_template('industrymap.html', missing_count = len(missing_industry_code_list), missing_codes = missing_industry_code_list, missing_index_names_count = len(missing_index_names), missing_index_names = missing_index_names)

@industry_mapping.route('/add_industry_mapping', methods=['POST'])
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
    
    
@industry_mapping.route('/add_index_mapping', methods=['POST'])
def add_index_mapping():
    warnings.filterwarnings("ignore", category=UserWarning)

    db = DB_Helper()
    conn = db.db_connect()
    cur = conn.cursor()

    selected_missing_index_name = request.form.get('selected_missing_index_name')
    index_name = request.form.get('index_name')
    
    sql = """INSERT INTO public."irs_index_mapping" (indexname, indexmapping)   VALUES (%s, %s);"""
    data = (selected_missing_index_name, index_name)
    try:
        print("inserting data into irs_index_mapping table")
        print(data)
        cur.execute(sql, data)
        conn.commit() 
        return jsonify({'message': 'New IndexMapping data added successfully.'})
    except psycopg2.Error as e:
        print(e)
        conn.rollback()
        return jsonify({'message': 'Error inserting data into the table.'})
    
    