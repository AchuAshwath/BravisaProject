# Helper Functions, used as common resource for BravisaPy

# Helper to check filepath exists, else attempts to create.
# insert the df into the table
import psycopg2.extras as extras

import psycopg2

class DB_Helper():
    """ Class helper is creating the connection with DataBase.
    """

    def __init__(self):
        pass

    def db_connect(self):
        """ Creating the connection with local database to access data
        """
        try:
            conn = psycopg2.connect(
                    host='localhost',   # IP address from PgAdmin or localhost
                    user='postgres',
                    password='edsols',  # the password you gave while setting up the database 
                    database='BravisaDB', # database Name
                    )
            return conn
        except Exception as e:
            print("Please provide the database connection details in the utils/db_helper.py file \nError: ", e)
            
    
    def insert_df(df, schema, tablename, conn, cur):
        # check if the table exists
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{tablename}')")
        if cur.fetchone()[0]:
            print(f"Table {tablename} exists")
        else:
            print(f"Table {tablename} does not exist")
            return
        
        # check if the df is empty
        if df.empty:
            print("Dataframe is empty")
            return
        
        # check if the columns in the df match the columns in the table
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tablename}'")
        columns = [column[0] for column in cur.fetchall()]
        if set(df.columns) != set(columns):
            print("Columns in dataframe do not match columns in table")
            return
        
        # Convert the DataFrame to list of tuples
        tuples = [tuple(x) for x in df.to_numpy()]
        
        # Generate the column names from the DataFrame
        cols = ','.join([f'"{col}"' for col in df.columns])
        
        # Generate the SQL query for inserting data
        query = f'INSERT INTO "{schema}"."{tablename}" ({cols}) VALUES %s'
        
        try:
            # Use execute_values to perform bulk insert
            extras.execute_values(cur, query, tuples)
            conn.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
            raise
        print("Data inserted successfully into ", tablename)
        


