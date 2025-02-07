from flask import Blueprint, render_template, request, jsonify, send_file
from utils.db_helper import DB_Helper
import pandas as pd
import os

# Create Blueprint
dash_summary = Blueprint('dash_summary', __name__, template_folder="../templates")
db = DB_Helper()

# Path to Downloads folder
DOWNLOADS_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")

@dash_summary.route('/dash-summary')
def summary_page():
    return render_template('dash-summary.html')

@dash_summary.route('/get_summary', methods=['GET'])
def get_summary():
    try:
        date = request.args.get('date')
        if not date:
            return jsonify({'error': 'Date is required'}), 400

        print(f"Fetching summary for date: {date}")  # Debugging Print

        conn = db.db_connect()

        # Fetch data from PRS
        prs_sql = f"""SELECT "NSECode", "Close", "Change52WPercentage", "RS 52W", "Market Cap Value" 
                      FROM "Reports"."PRS" WHERE "Date" = '{date}'"""
        prs_data = pd.read_sql_query(prs_sql, conn)

        if prs_data.empty:
            return jsonify({'error': 'No PRS data found for this date'}), 404

        # Fetch ranking from EPS
        eps_sql = f"""SELECT "NSECode", "Ranking" FROM "Reports"."EPS" WHERE "EPSDate" = '{date}'"""
        eps_data = pd.read_sql_query(eps_sql, conn)

        # Fetch ranking from EERS
        eers_sql = f"""SELECT "NSECode", "Ranking" FROM "Reports"."EERS" WHERE "EERSDate" = '{date}'"""
        eers_data = pd.read_sql_query(eers_sql, conn)

        # Fetch industry from SMR
        smr_sql = f"""SELECT "NSESymbol", "Industry" FROM "Reports"."SMR" WHERE "SMRDate" = '{date}'"""
        smr_data = pd.read_sql_query(smr_sql, conn)

        # Fetch rank from IRS
        irs_sql = f"""SELECT "Index", "Rank" FROM "Reports"."IRS" WHERE "GenDate" = '{date}'"""
        irs_data = pd.read_sql_query(irs_sql, conn)

        # Merge DataFrames
        summary_df = pd.merge(prs_data, eps_data, on='NSECode', how='inner')
        # drop rows with missing values in NSECode 
        summary_df = summary_df.dropna(subset=['NSECode'])

        summary_df = summary_df.merge(eers_data, on="NSECode", how="inner", suffixes=("", "_EERS"))

        summary_df = summary_df.merge(smr_data, left_on="NSECode", right_on="NSESymbol", how="inner")

        summary_df = summary_df.merge(irs_data, left_on="Industry", right_on="Index", how="inner")

        # Convert numerical values to two decimal places
        summary_df["Market Cap Value"] = summary_df["Market Cap Value"] / 10000000
        summary_df = summary_df.round(2)

        # Selecting necessary columns
        summary_df = summary_df[["NSECode", "Close", "Change52WPercentage", "RS 52W", 
                                 "Ranking", "Ranking_EERS", "Market Cap Value", "Industry", "Rank"]]

        # Renaming columns
        summary_df.columns = ["NSECode", "Close", "Change 52", "RS 52W", 
                              "EPS Rank", "EERS Rank", "Market Cap", "Industry", "IRS"]

        conn.close()

        # Save CSV to Downloads folder
        file_path = os.path.join(DOWNLOADS_FOLDER, f"Summary_{date}.csv")
        summary_df.to_csv(file_path, index=False)

        return jsonify({'message': 'CSV successfully generated!', 'file_path': file_path})

    except Exception as e:
        print(f"ERROR: {e}")  # Log Full Error
        return jsonify({'error': str(e)}), 500

# Route to Download CSV
@dash_summary.route('/download_summary', methods=['GET'])
def download_summary():
    try:
        date = request.args.get('date')
        if not date:
            return jsonify({'error': 'Date is required'}), 400

        file_path = os.path.join(DOWNLOADS_FOLDER, f"Summary_{date}.csv")

        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found! Generate summary first.'}), 404

        return send_file(file_path, as_attachment=True)

    except Exception as e:
        print(f"Download Error: {e}")
        return jsonify({'error': str(e)}), 500
