from flask import Blueprint, render_template, request, jsonify, send_file
from utils.db_helper import DB_Helper
import pandas as pd
import os

# Create Blueprint
dash_display = Blueprint('dash_display', __name__, template_folder="../templates")
db = DB_Helper()

@dash_display.route('/dash-display')
def index():
    return render_template('dash-display.html')

@dash_display.route('/get_display')
def search():
    NSECode = request.args.get('NSECode')
    # Process the query as needed
    date = request.args.get('date')

    # connect to db
    conn = db.db_connect()
    cur = conn.cursor()

    #fetch data from SMR for date and the date column is SMRDate
    smr_sql = f""" SELECT * FROM "Reports"."SMR" WHERE "SMRDate" = '{date}' AND "NSESymbol" = '{NSECode}' """
    smr_data = pd.read_sql_query(smr_sql, conn)
    industry = smr_data['Industry'].values[0]

    # Fetch data from IRS for date
    irs_sql = f""" SELECT * FROM "Reports"."IRS" WHERE "GenDate" = '{date}' """
    irs_data = pd.read_sql_query(irs_sql, conn)
    industry_rank = irs_data[irs_data['Index'] == industry]['Rank'].values[0]
    
    # fetch PRS data from the PRS table where the date column is Date
    prs_sql = f""" SELECT * FROM "Reports"."PRS" WHERE "Date" = '{date}' AND "NSECode" = '{NSECode}' """
    prs_data = pd.read_sql_query(prs_sql, conn)

    #PRS Rank - from PRS RS 52W column
    prs_rank = prs_data['RS 52W'].values[0]

    #7. Change % - from PRS Change52WPercentage column.
    change = prs_data['Change52WPercentage'].values[0]

    # RS 30 Days - from PRS RS 30D column.
    rs_30d = prs_data['RS 30D'].values[0]

    # RS 90 Days - from PRS RS 90D column.
    rs_90d = prs_data['RS 90D'].values[0]

    #fetch data from EPS where the date column is EPSDate for the NSECode
    eps_sql = f""" SELECT * FROM "Reports"."EPS" WHERE "EPSDate" = '{date}' AND "NSECode" = '{NSECode}' """
    eps_data = pd.read_sql_query(eps_sql, conn)

    #ERS Rank - from EPS Ranking column.
    ers_rank = eps_data['Ranking'].values[0]


    # fetch data from EERS where the date column is EERSDate
    eers_sql = f""" SELECT * FROM "Reports"."EERS" WHERE "EERSDate" = '{date}' AND "NSECode" = '{NSECode}' """
    eers_data = pd.read_sql_query(eers_sql, conn)

    #EERS Rank - from EERS Ranking column.
    eers_rank = eers_data['Ranking'].values[0]

    # Define the columns to fetch
    columns = [
        "Q1 EPS", "Q1 EPS Growth", "Q1 Sales", "Q1 Sales Growth",
        "Q2 EPS", "Q2 EPS Growth", "Q2 Sales", "Q2 Sales Growth",
        "TTM1 EPS Growth", "TTM1 Sales Growth", "TTM2 EPS Growth",
        "TTM2 Sales Growth", "TTM3 EPS Growth", "TTM3 Sales Growth"
    ]
    
    # Create a comma-separated string of the columns
    columns_str = ', '.join([f'"{col}"' for col in columns])
    
    # Fetch data from STANDALONE_EPS where the date column is EPSDate for the NSECode
    standalone_eps_sql = f"""
        SELECT {columns_str}
        FROM "Reports"."STANDALONE_EPS"
        WHERE "EPSDate" = '{date}' AND "NSECode" = '{NSECode}'
    """
    standalone_eps_df = pd.read_sql_query(standalone_eps_sql, conn)

    #fetch "Q1 EERS", "Q1 EERS Growth", "Q2 EERS", "Q2 EERS Growth" from STANDALONE_EERS where the date column is EERSDate for the NSECode
    standalone_eers_sql = f"""
        SELECT "Q1 EERS", "Q1 EERS Growth", "Q2 EERS", "Q2 EERS Growth"
        FROM "Reports"."STANDALONE_EERS"
        WHERE "EERSDate" = '{date}' AND "NSECode" = '{NSECode}'
    """
    standalone_eers_df = pd.read_sql_query(standalone_eers_sql, conn)

    # fetch the data from Consolidated_EPS where the date column is EPSDate for the NSECode
    consolidated_eps_sql = f"""
        SELECT {columns_str}
        FROM "Reports"."Consolidated_EPS"
        WHERE "EPSDate" = '{date}' AND "NSECode" = '{NSECode}'
    """
    consolidated_eps_df = pd.read_sql_query(consolidated_eps_sql, conn)

    # fetch "Q1 EERS", "Q1 EERS Growth", "Q2 EERS", "Q2 EERS Growth" from Consolidated_EERS where the date column is EERSDate for the NSECode
    consolidated_eers_sql = f"""
        SELECT "Q1 EERS", "Q1 EERS Growth", "Q2 EERS", "Q2 EERS Growth"
        FROM "Reports"."Consolidated_EERS"
        WHERE "EERSDate" = '{date}' AND "NSECode" = '{NSECode}'
    """    
    consolidated_eers_df = pd.read_sql_query(consolidated_eers_sql, conn)

    # Create consolidated DataFrame with the first column being 'Quater' with values Q1, Q2, TTM1, TTM2, TTM3
    consolidated_df = pd.DataFrame({
        'Quater': ['Q1', 'Q2', 'TTM1', 'TTM2', 'TTM3'],
        'Cons EPS': [None] * 5,
        'Growth': [None] * 5,
        'Sale': [None] * 5,
        'Sale Growth': [None] * 5,
        'EERS': [None] * 5,
        'EERS Growth': [None] * 5
    })

    # Assign values to the consolidated DataFrame using .loc
    consolidated_df.loc[consolidated_df['Quater'] == 'Q1', 'Cons EPS'] = consolidated_eps_df['Q1 EPS'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q2', 'Cons EPS'] = consolidated_eps_df['Q2 EPS'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q1', 'Growth'] = consolidated_eps_df['Q1 EPS Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q2', 'Growth'] = consolidated_eps_df['Q2 EPS Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'TTM1', 'Growth'] = consolidated_eps_df['TTM1 EPS Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'TTM2', 'Growth'] = consolidated_eps_df['TTM2 EPS Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'TTM3', 'Growth'] = consolidated_eps_df['TTM3 EPS Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q1', 'Sale'] = consolidated_eps_df['Q1 Sales'].values[0]/10000000 if consolidated_eps_df['Q1 Sales'].values[0] != None else consolidated_eps_df['Q1 Sales'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q2', 'Sale'] = consolidated_eps_df['Q2 Sales'].values[0]/10000000 if consolidated_eps_df['Q2 Sales'].values[0] != None else consolidated_eps_df['Q2 Sales'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q1', 'Sale Growth'] = consolidated_eps_df['Q1 Sales Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q2', 'Sale Growth'] = consolidated_eps_df['Q2 Sales Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'TTM1', 'Sale Growth'] = consolidated_eps_df['TTM1 Sales Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'TTM2', 'Sale Growth'] = consolidated_eps_df['TTM2 Sales Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'TTM3', 'Sale Growth'] = consolidated_eps_df['TTM3 Sales Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q1', 'EERS'] = consolidated_eers_df['Q1 EERS'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q2', 'EERS'] = consolidated_eers_df['Q2 EERS'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q1', 'EERS Growth'] = consolidated_eers_df['Q1 EERS Growth'].values[0]
    consolidated_df.loc[consolidated_df['Quater'] == 'Q2', 'EERS Growth'] = consolidated_eers_df['Q2 EERS Growth'].values[0]


    # Create STANDALONE DataFrame with the first column being 'Quater' with values Q1, Q2, TTM1, TTM2, TTM3
    standalone_df = pd.DataFrame({
        'Quater': ['Q1', 'Q2', 'TTM1', 'TTM2', 'TTM3'],
        'Cons EPS': [None] * 5,
        'Growth': [None] * 5,
        'Sale': [None] * 5,
        'Sale Growth': [None] * 5,
        'EERS': [None] * 5,
        'EERS Growth': [None] * 5
    })

    # Assign values to the standalone DataFrame using .loc
    standalone_df.loc[standalone_df['Quater'] == 'Q1', 'Cons EPS'] = standalone_eps_df['Q1 EPS'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q2', 'Cons EPS'] = standalone_eps_df['Q2 EPS'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q1', 'Growth'] = standalone_eps_df['Q1 EPS Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q2', 'Growth'] = standalone_eps_df['Q2 EPS Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'TTM1', 'Growth'] = standalone_eps_df['TTM1 EPS Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'TTM2', 'Growth'] = standalone_eps_df['TTM2 EPS Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'TTM3', 'Growth'] = standalone_eps_df['TTM3 EPS Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q1', 'Sale'] = standalone_eps_df['Q1 Sales'].values[0]/10000000 if standalone_eps_df['Q1 Sales'].values[0] != None else standalone_eps_df['Q1 Sales'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q2', 'Sale'] = standalone_eps_df['Q2 Sales'].values[0]/10000000 if standalone_eps_df['Q2 Sales'].values[0] != None else standalone_eps_df['Q2 Sales'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q1', 'Sale Growth'] = standalone_eps_df['Q1 Sales Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q2', 'Sale Growth'] = standalone_eps_df['Q2 Sales Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'TTM1', 'Sale Growth'] = standalone_eps_df['TTM1 Sales Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'TTM2', 'Sale Growth'] = standalone_eps_df['TTM2 Sales Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'TTM3', 'Sale Growth'] = standalone_eps_df['TTM3 Sales Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q1', 'EERS'] = standalone_eers_df['Q1 EERS'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q2', 'EERS'] = standalone_eers_df['Q2 EERS'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q1', 'EERS Growth'] = standalone_eers_df['Q1 EERS Growth'].values[0]
    standalone_df.loc[standalone_df['Quater'] == 'Q2', 'EERS Growth'] = standalone_eers_df['Q2 EERS Growth'].values[0]


    # return with names and their values as f string
    return jsonify({
        'Industry': industry,
        'Industry Rank': round(industry_rank, 2) if isinstance(industry_rank, (int, float)) else industry_rank,
        'PRS Rank': round(prs_rank, 2) if isinstance(prs_rank, (int, float)) else prs_rank,
        'Change %': round(change, 2) if isinstance(change, (int, float)) else change,
        'RS 30 Days': round(rs_30d, 2) if isinstance(rs_30d, (int, float)) else rs_30d,
        'RS 90 Days': round(rs_90d, 2) if isinstance(rs_90d, (int, float)) else rs_90d,
        'ERS Rank': round(ers_rank, 2) if isinstance(ers_rank, (int, float)) else ers_rank,
        'EERS Rank': round(eers_rank, 2) if isinstance(eers_rank, (int, float)) else eers_rank,
        'Consolidated': [{key: round(val, 2) if isinstance(val, (int, float)) else val for key, val in row.items()} for row in consolidated_df.to_dict(orient='records')],
        'Standalone': [{key: round(val, 2) if isinstance(val, (int, float)) else val for key, val in row.items()} for row in standalone_df.to_dict(orient='records')]
    })
