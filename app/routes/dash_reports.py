from flask import Blueprint, render_template, request, jsonify, send_file
from utils.db_helper import DB_Helper
import pandas as pd
import os
import traceback
# Path to Downloads folder
DOWNLOADS_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")

dash_reports = Blueprint('dash_reports', __name__, template_folder="../templates")
db = DB_Helper()

def download_csv_direct(schema, table, date_column, start_date, end_date, non_date_column=None, value=None):
    """ Generates CSV and saves it to the Downloads folder """
    try:
        conn = db.db_connect()
        
        if non_date_column and value:
            sql = f"""SELECT * FROM "{schema}"."{table}" WHERE "{date_column}" BETWEEN '{start_date}' AND '{end_date}' AND "{non_date_column}" = '{value}'"""
        else:
            sql = f"""SELECT * FROM "{schema}"."{table}" WHERE "{date_column}" BETWEEN '{start_date}' AND '{end_date}'"""
        
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
            check_box = request.form.get('enable_options')
            non_date_columns = request.form.get('input-select-1')
            value = request.form.get('Code')
            start_date = request.form.get('start_date')
            end_date = request.form.get('end_date')
            action = request.form.get('action')  # 'preview' or 'download'

            print(f"📌 Received Request: {action}, Schema={schema}, Table={table}, Column={column}, Start={start_date}, End={end_date}, check_box = {check_box}, non_date_column = {non_date_columns}, value = {value}")
            
            if non_date_columns == None:
                non_date_columns = []
            
            # Handle Download CSV Action
            if action == "download":
                if check_box == "on":
                    message = download_csv_direct(schema, table, column, start_date, end_date, non_date_columns, value)  
                    return render_template("dash-reports.html",
                        schemas=schema_list,
                        selected_schema=schema,
                        selected_table=table,
                        selected_column=column,
                        check_box=check_box,
                        value=value,
                        non_date_columns=non_date_columns, 
                        start_date=start_date,
                        end_date=end_date,
                        message=message
                    )
                    
                else:
                    message = download_csv_direct(schema, table, column, start_date, end_date)
                    return render_template("dash-reports.html",
                        schemas=schema_list,
                        selected_schema=schema,
                        selected_table=table,
                        selected_column=column,
                        non_date_columns=non_date_columns, 
                        start_date=start_date,
                        end_date=end_date,
                        message=message
                    )
                # notify user that file is downloaded


            if action == "preview":
                if check_box == "on":
                    preview_sql = f"""
                        SELECT * FROM "{schema}"."{table}"
                        WHERE "{column}" BETWEEN '{start_date}' AND '{end_date}' AND "{non_date_columns}" like '{value}'
                        LIMIT 10
                    """
                    preview_data = pd.read_sql_query(preview_sql, conn)
                    return render_template("dash-reports.html",
                        schemas=schema_list,
                        preview_data={"columns": preview_data.columns.tolist(), "rows": preview_data.values.tolist()},
                        selected_schema=schema,
                        selected_table=table,
                        selected_column=column,
                        check_box=check_box,
                        value=value,
                        non_date_columns=non_date_columns, 
                        start_date=start_date,
                        end_date=end_date,
                        message=message
                    )
                else:
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
                        non_date_columns=non_date_columns, 
                        start_date=start_date,
                        end_date=end_date,
                        message=message
                    )
        return render_template("dash-reports.html", schemas=schema_list)

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("traceback: ", traceback.format_exc())
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
    # print("tables: ", tables)
    return jsonify({"tables": tables})

@dash_reports.route('/get_columns')
def get_date_columns():
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
    # print("columns: ", columns)
    return jsonify({"columns": columns})

@dash_reports.route('/get_columns_non_date')
def get_columns_not_date():
    schema = request.args.get('schema')
    table = request.args.get('table')
    conn = db.db_connect()

    columns_sql = f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        AND data_type NOT IN ('date', 'timestamp without time zone')
    """
    non_date_columns = pd.read_sql_query(columns_sql, conn)['column_name'].tolist()
    # print("non_date columns: ", non_date_columns)
    return jsonify({"non_date_columns": non_date_columns})

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
