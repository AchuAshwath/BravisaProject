from flask import Blueprint, render_template, request, jsonify, send_file
from utils.db_helper import DB_Helper
import pandas as pd
import os

# Create Blueprint
dash_reports = Blueprint('dash_reports', __name__, template_folder="../templates")
db = DB_Helper()

# Route to render the dashboard reports page with schema list
@dash_reports.route('/dash-reports', methods=['GET'])
def dash_reports_page():
    try:
        conn = db.db_connect()
        cur = conn.cursor()

        # Fetch all schema names excluding system schemas
        schema_sql = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT LIKE 'pg_%' 
            AND schema_name != 'information_schema'
        """
        schema_list = pd.read_sql_query(schema_sql, conn)['schema_name'].tolist()

        cur.close()
        conn.close()

        # Check if a schema is selected
        selected_schema = request.args.get('schema')
        tables_list = []

        if selected_schema:
            conn = db.db_connect()
            cur = conn.cursor()

            # Fetch tables for the selected schema
            tables_sql = f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{selected_schema}'
            """
            tables_list = pd.read_sql_query(tables_sql, conn)['table_name'].tolist()

            cur.close()
            conn.close()

        return render_template('dash-reports.html', schemas=schema_list, selected_schema=selected_schema, tables=tables_list)

    except Exception as e:
        return render_template('dash-reports.html', schemas=[], tables=[], error=str(e))

# Route to fetch table preview
@dash_reports.route('/get_table_preview', methods=['GET'])
def get_table_preview():
    try:
        schema = request.args.get('schema', 'public')
        table = request.args.get('table')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        if not schema or not table or not start_date or not end_date:
            return render_template('dash-reports.html', preview_data=None, error="Missing input parameters.")

        conn = db.db_connect()
        query = f'SELECT * FROM "{schema}"."{table}" WHERE date_column BETWEEN \'{start_date}\' AND \'{end_date}\' LIMIT 10'
        df = pd.read_sql_query(query, conn)
        conn.close()

        preview_data = {
            "columns": df.columns.tolist(),
            "rows": df.values.tolist()
        }

        return render_template('dash-reports.html', preview_data=preview_data, selected_schema=schema, selected_table=table, start_date=start_date, end_date=end_date)

    except Exception as e:
        return render_template('dash-reports.html', preview_data=None, error=str(e))


# ✅ **API to Download CSV**
@dash_reports.route('/download_csv', methods=['GET'])
def download_csv():
    try:
        schema = request.args.get('schema', 'public')
        table = request.args.get('table')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        if not table:
            return "Missing table parameter", 400

        conn = db.db_connect()
        query = f'SELECT * FROM "{schema}"."{table}"'

        if start_date and end_date:
            query += f" WHERE date_column BETWEEN '{start_date}' AND '{end_date}'"

        df = pd.read_sql_query(query, conn)
        conn.close()

        # Save CSV
        csv_filename = f"{schema}_{table}.csv"
        df.to_csv(csv_filename, index=False)

        return send_file(csv_filename, as_attachment=True)

    except Exception as e:
        return str(e), 500
