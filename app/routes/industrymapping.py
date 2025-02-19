from flask import Blueprint
from utils.db_helper import DB_Helper
import pandas as pd
import os
from flask import Flask, render_template, request, jsonify
import psycopg2
import warnings
from datetime import datetime
warnings.filterwarnings("ignore", category=UserWarning)

DOWNLOADS_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")
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
    
    # get all distinct shemecodes from SchemeMaster
    scheme_master_sql = 'SELECT DISTINCT "SchemeCode" FROM public."SchemeMaster";'
    scheme_master_df = pd.read_sql_query(scheme_master_sql, conn)
    
    # get all schemecodes from mf_category_mapping
    mf_category_mapping_sql = 'SELECT DISTINCT "scheme_code" FROM public."mf_category_mapping";'
    mf_category_mapping_df = pd.read_sql_query(mf_category_mapping_sql, conn)
    
    # get all schemecodes from ignore_scheme_master
    ignore_scheme_master_sql = 'SELECT DISTINCT "scheme_code" FROM public."ignore_scheme_master";'
    ignore_scheme_master_df = pd.read_sql_query(ignore_scheme_master_sql, conn)
    
    # remove the schemecodes from scheme_master_df that are in mf_category_mapping_df and ignore_scheme_master_df
    scheme_master_df = scheme_master_df[~scheme_master_df['SchemeCode'].isin(mf_category_mapping_df['scheme_code'])]
    scheme_master_df = scheme_master_df[~scheme_master_df['SchemeCode'].isin(ignore_scheme_master_df['scheme_code'])]
    
    print("scheme_master_df : ", len(scheme_master_df))
    print(scheme_master_df)
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    
    return render_template('industrymap.html', missing_count = len(missing_industry_code_list), missing_index_names_count = len(missing_index_names), missing_index_names = missing_index_names, scheme_master_new_count = len(scheme_master_df))

@industry_mapping.route('/get_industry_list', methods=['GET'])
def get_industry_list():
    db = DB_Helper()
    conn = db.db_connect()
    cur = conn.cursor()
    
    industry_mapping_sql = 'SELECT * FROM public."IndustryMapping";'
    background_info_sql = 'SELECT * FROM public."BackgroundInfo";'

    industry_mapping_df = pd.read_sql_query(industry_mapping_sql, conn)
    background_info_df = pd.read_sql_query(background_info_sql, conn)
    
    missing_industry_codes = background_info_df[~background_info_df['IndustryCode'].isin(industry_mapping_df['IndustryCode'])]
    
    # create new dataframe which will be downloaded with same columns as IndustryMapping table
    missing_industry_df = pd.DataFrame(columns=industry_mapping_df.columns)
    
    # for every row in missing_industry_codes, get the IndustryCode and IndustryName from background_info_df and add it to missing_industry_df at the IndustryCode and IndustryName columns
    for index, row in missing_industry_codes.iterrows():
        new_row = pd.DataFrame({'IndustryCode': [row['IndustryCode']], 'IndustryName': [row['IndustryName']]})
        missing_industry_df = pd.concat([missing_industry_df, new_row], ignore_index=True)

    print(missing_industry_df)
    
    today_str = datetime.today().strftime('%Y-%m-%d')
    
    # Save the DataFrame as a CSV file
    missing_industry_df.to_csv(os.path.join(DOWNLOADS_FOLDER, f"missing_industry_list-{today_str}.csv"), index=False)
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    
    return jsonify({'message': 'Data downloaded successfully.'}) 

@industry_mapping.route('/get_scheme_list', methods=['GET'])
def get_scheme_list():
    db = DB_Helper()
    conn = db.db_connect()
    cur = conn.cursor()
    
    # get all distinct shemecodes from SchemeMaster
    scheme_master_sql = 'SELECT DISTINCT "SchemeCode" FROM public."SchemeMaster";'
    scheme_master_df = pd.read_sql_query(scheme_master_sql, conn)
    
    # get all schemecodes from mf_category_mapping
    mf_category_mapping_sql = 'SELECT DISTINCT "scheme_code" FROM public."mf_category_mapping";'
    mf_category_mapping_df = pd.read_sql_query(mf_category_mapping_sql, conn)
    
    # get all schemecodes from ignore_scheme_master
    ignore_scheme_master_sql = 'SELECT DISTINCT "scheme_code" FROM public."ignore_scheme_master";'
    ignore_scheme_master_df = pd.read_sql_query(ignore_scheme_master_sql, conn)
    
    # remove the schemecodes from scheme_master_df that are in mf_category_mapping_df and ignore_scheme_master_df
    scheme_master_df = scheme_master_df[~scheme_master_df['SchemeCode'].isin(mf_category_mapping_df['scheme_code'])]
    scheme_master_df = scheme_master_df[~scheme_master_df['SchemeCode'].isin(ignore_scheme_master_df['scheme_code'])]

    columns = ['scheme_code', 'scheme_name', 'scheme_category', 'date', 'btt_scheme_code', 'btt_scheme_category']
    missing_scheme_list = pd.DataFrame(columns=columns)
    
    for index, row in scheme_master_df.iterrows():
        new_row = pd.DataFrame({'scheme_code': [row['SchemeCode']], 'scheme_name': [''], 'scheme_category': [''], 'date': [''], 'btt_scheme_code': [''], 'btt_scheme_category': ['']})
        missing_scheme_list = pd.concat([missing_scheme_list, new_row], ignore_index=True)

    print(missing_scheme_list)
    
    # Get today's date as a string
    today_str = datetime.today().strftime('%Y-%m-%d')
    
    # Save the DataFrame as a CSV file
    missing_scheme_list.to_csv(os.path.join(DOWNLOADS_FOLDER, f"missing_scheme_list-{today_str}.csv"), index=False)
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    
    return jsonify({'message': 'Data downloaded successfully.'})

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
    


# ✅ Route for mf_category_mapping file
@industry_mapping.route('/add_missing_mf_category_mapping', methods=['POST'])
def upload_mf_category():
    db = DB_Helper()
    conn = db.db_connect()
    cur = conn.cursor()

    mf_category_file = request.files.get('missing_mf_category_mapping')

    if not mf_category_file:
        return jsonify({'message': 'No file uploaded'}), 400

    try:
        # Save the file
        mf_category_path = os.path.join(DOWNLOADS_FOLDER, mf_category_file.filename)
        mf_category_file.save(mf_category_path)
        mf_category_df = pd.read_csv(mf_category_path)
        print(mf_category_df.head())

        # Insert into mf_category_mapping table
        for _, row in mf_category_df.iterrows():
            sql = """INSERT INTO public."mf_category_mapping"(
                "scheme_code", "scheme_name", "scheme_category", "date", "btt_scheme_code", "btt_scheme_category") 
                VALUES (%s, %s, %s, %s, %s, %s);"""
            data = (
                row['scheme_code'], row['scheme_name'], row['scheme_category'], row['date'], row['btt_scheme_code'],
                row['btt_scheme_category']
            )
            print(data)
            cur.execute(sql, data)

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({'message': 'mf_category_mapping data inserted successfully.'}), 200

    except Exception as e:
        return jsonify({'message': str(e)}), 500

@industry_mapping.route('/add_missing_industry_mapping', methods=['POST'])
def upload_missing_industry_mapping():
    db = DB_Helper()
    conn = db.db_connect()
    cur = conn.cursor()

    missing_industry_file = request.files.get('missing_industryMapping')

    if not missing_industry_file:
        return jsonify({'message': 'No file uploaded'}), 400

    try:
        # Save the file
        missing_industry_path = os.path.join(DOWNLOADS_FOLDER, missing_industry_file.filename)
        missing_industry_file.save(missing_industry_path)
        missing_industry_df = pd.read_csv(missing_industry_path)
        print(missing_industry_df.head())

        # Insert into IndustryMapping table
        for _, row in missing_industry_df.iterrows():
            sql = """INSERT INTO public."IndustryMapping"(
                "IndustryCode", "IndustryName", "Industry", "Code", "SubSector", "SubSectorCode", "Sector", "SectorCode", \
                "SubIndustry", "SubIndustryCode", "IndustryIndexName", "SubSectorIndexName", "SectorIndexName", "SubIndustryIndexName")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s);"""  
                
            data = (row['IndustryCode'], row['IndustryName'], row['Industry'], row['Code'], row['SubSector'], row['SubSectorCode'], 
                    row['Sector'], row['SectorCode'], row['SubIndustry'], row['SubIndustryCode'], 'INDUSTRY-'+row['Industry'], 
                    'SUBSECTOR-'+row['SubSector'], 'SECTOR-'+row['Sector'], 'SUBINDUSTRY-'+row['SubIndustry'])
            print(data)
            
            cur.execute(sql, data)
            conn.commit()
            
        cur.close()
        
        return jsonify({'message': 'missing_industry_mapping data inserted successfully.'}), 200
    except Exception as e:
        return jsonify({'message': str(e)}), 500
    
@industry_mapping.route('/update_ignore_list', methods=['GET'])
def update_ignore_list():
    db = DB_Helper()
    conn = db.db_connect()
    cur = conn.cursor()
    
    # get all distinct shemecodes from SchemeMaster
    scheme_master_sql = 'SELECT DISTINCT "SchemeCode" FROM public."SchemeMaster";'
    scheme_master_df = pd.read_sql_query(scheme_master_sql, conn)
    
    # get all schemecodes from mf_category_mapping
    mf_category_mapping_sql = 'SELECT DISTINCT "scheme_code" FROM public."mf_category_mapping";'
    mf_category_mapping_df = pd.read_sql_query(mf_category_mapping_sql, conn)
    
    # get all schemecodes from ignore_scheme_master
    ignore_scheme_master_sql = 'SELECT DISTINCT "scheme_code" FROM public."ignore_scheme_master";'
    ignore_scheme_master_df = pd.read_sql_query(ignore_scheme_master_sql, conn)
    
    # remove the schemecodes from scheme_master_df that are in mf_category_mapping_df and ignore_scheme_master_df
    scheme_master_df = scheme_master_df[~scheme_master_df['SchemeCode'].isin(mf_category_mapping_df['scheme_code'])]
    scheme_master_df = scheme_master_df[~scheme_master_df['SchemeCode'].isin(ignore_scheme_master_df['scheme_code'])]
    
    print("scheme_master_df : ", len(scheme_master_df))
    print(scheme_master_df['SchemeCode'].tolist())
    
    # Insert into ignore_scheme_master table
    for _, row in scheme_master_df.iterrows():
        print(row['SchemeCode'])
        sql = """INSERT INTO public."ignore_scheme_master"("scheme_code") VALUES (%s);"""
        data = (row['SchemeCode'],)
        cur.execute(sql, data)
        conn.commit()
        
        print("Inserted")
        
    cur.close()
    conn.close()
        
    return jsonify({'message': 'Ignore list updated successfully.'}), 200
