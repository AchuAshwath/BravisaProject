from psycopg2 import sql
from psycopg2.extras import execute_values

def insert_logs(table_name, data_list, conn, cur):
    if not data_list:
        return

    # Extract columns from the first dictionary
    columns = data_list[0].keys()
    
    # Create the SQL query dynamically
    insert_query = sql.SQL("INSERT INTO logs.{table} ({fields}) VALUES %s").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns))
    )
    
    # Convert list of dictionaries to list of tuples
    values = [tuple(data.values()) for data in data_list]
    
    # Execute the query with multiple rows
    execute_values(cur, insert_query, values)
    conn.commit()