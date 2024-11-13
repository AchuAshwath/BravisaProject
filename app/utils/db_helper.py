# Helper Functions, used as common resource for BravisaPy

# Helper to check filepath exists, else attempts to create.

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


