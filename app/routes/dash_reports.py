from flask import Blueprint, render_template, request, jsonify, send_file
from utils.db_helper import DB_Helper
import pandas as pd
import os
# Path to Downloads folder
DOWNLOADS_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")

dash_reports = Blueprint('dash_reports', __name__, template_folder="../templates")
db = DB_Helper()

def download_csv_direct(schema, table, column, start_date, end_date):
    """ Generates CSV and saves it to the Downloads folder """
    try:
        conn = db.db_connect()
        sql = f"""SELECT * FROM "{schema}"."{table}" WHERE "{column}" BETWEEN '{start_date}' AND '{end_date}'"""
        df = pd.read_sql_query(sql, conn)

        # File path in Downloads folder
        file_path = os.path.join(DOWNLOADS_FOLDER, f"{table}_{start_date}-{end_date}.csv")
        df.to_csv(file_path, index=False)

        print(f"📌 CSV Generated: {file_path}")
        return f"✅ File successfully downloaded to {file_path}"

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return f"❌ Error generating CSV: {str(e)}"

@dash_reports.route('/dash-reports', methods=['GET', 'POST'])
def dash_reports_page():
    try:
        conn = db.db_connect()

        # Fetch schema names
        schema_sql = """
            SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name NOT LIKE 'pg_%' 
            AND schema_name != 'information_schema'
        """
        schema_list = pd.read_sql_query(schema_sql, conn)['schema_name'].tolist()
        message = None
        if request.method == "POST":
            schema = request.form.get('schema')
            table = request.form.get('table')
            column = request.form.get('column')
            start_date = request.form.get('start_date')
            end_date = request.form.get('end_date')
            action = request.form.get('action')  # 'preview' or 'download'

            print(f"📌 Received Request: {action}, Schema={schema}, Table={table}, Column={column}, Start={start_date}, End={end_date}")

            # Handle Download CSV Action
            if action == "download":
                message = download_csv_direct(schema, table, column, start_date, end_date)
                # notify user that file is downloaded


            # Fetch preview data
            preview_sql = f"""
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" BETWEEN '{start_date}' AND '{end_date}'
                LIMIT 10
            """
            preview_data = pd.read_sql_query(preview_sql, conn)

            return render_template("dash-reports.html", 
                schemas=schema_list,
                preview_data={"columns": preview_data.columns.tolist(), "rows": preview_data.values.tolist()},
                selected_schema=schema,
                selected_table=table,
                selected_column=column,
                start_date=start_date,
                end_date=end_date,
                message=message
            )

        return render_template("dash-reports.html", schemas=schema_list)

    except Exception as e:
        return render_template("dash-reports.html", schemas=[], error=str(e))


@dash_reports.route('/get_tables')
def get_tables():
    schema = request.args.get('schema')
    conn = db.db_connect()

    tables_sql = f"""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '{schema}'
    """
    tables = pd.read_sql_query(tables_sql, conn)['table_name'].tolist()
    return jsonify({"tables": tables})

@dash_reports.route('/get_columns')
def get_columns():
    schema = request.args.get('schema')
    table = request.args.get('table')
    conn = db.db_connect()

    columns_sql = f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        AND (data_type = 'date'
        OR data_type = 'timestamp without time zone')
    """
    columns = pd.read_sql_query(columns_sql, conn)['column_name'].tolist()
    return jsonify({"columns": columns})

@dash_reports.route('/download_csv')
def download_csv():
    schema = request.args.get('schema')
    table = request.args.get('table')
    start_date = request.args.get('start_date')
    column = request.args.get('column')
    end_date = request.args.get('end_date')

    conn = db.db_connect()
    sql = f"""SELECT * FROM "{schema}"."{table}" WHERE "{column}" BETWEEN '{start_date}' AND '{end_date}'"""
    df = pd.read_sql_query(sql, conn)

    file_path = os.path.join("downloads", f"{table}_report.csv")
    df.to_csv(file_path, index=False)
    return send_file(file_path, as_attachment=True)
