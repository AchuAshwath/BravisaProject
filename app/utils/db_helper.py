# Helper Functions, used as common resource for BravisaPy

# Helper to check filepath exists, else attempts to create.

import os
import datetime
import ftplib
import zipfile
from dateutil.relativedelta import relativedelta
import calendar
from pathlib import Path
import psycopg2
import datetime
from sqlalchemy.engine.url import make_url

import json
import sys
import rootpath

# To open the config.json file from the path
# if os.name == 'nt':
#     base_path = rootpath.detect()
#     path_configfile = str(base_path+"\config.json")
# else :
#     mainfolder_path = rootpath.detect() 
#     path_configfile = str(mainfolder_path+"/config.json")

# with open(path_configfile, 'r') as f:
#     config = json.load(f)

# # Local DB config
# localDBhost = config['DBLocal']['host']
# localDB = config['DBLocal']['DB']
# localDBuser = config['DBLocal']['DBuser']
# localDBPass = config['DBLocal']['DBPass']
# localDBPort = config['DBLocal']['DBPort']

# ## Bravisa system config
# prodDBhost = config['DBBravisa']['host']
# prodDB = config['DBBravisa']['DB']
# prodDBuser = config['DBBravisa']['DBuser']
# prodDBPass = config['DBBravisa']['DBPass']
# prodDBPort = config['DBBravisa']['DBPort']

class DB_Helper():
    """ Class helper is creating the connection with DataBase.
    """

    def __init__(self):
        pass

    def db_connect(self):
        """ Creating the connection with local database to access data
        """
        if os.name == 'nt':
            # add 'port=localDBPort' in conn and configure it in config.json file for local/prod port difference(if using cloud sql proxy)
            conn = psycopg2.connect(
                    host='localhost',   # IP address from PgAdmin or localhost
                    user='postgres',
                    password='edsols',  # the password you gave while setting up the database 
                    database='BravisaDB', # database Name
                    )
            return conn
        else:
            # Use this conn in case of connecting to localDB and using linux
            conn = psycopg2.connect(
                    host='localhost',
                    user='postgres',
                    password='edsols',
                    database='BravisaDB', 
                    )
            return conn

    def engine(self):
        url = "postgresql://postgres:edsols@localhost:5432/BravisaDB"

        # Extract the database name from the URL
        parsed_url = make_url(url)
        db_name = parsed_url.database
        print(f"Connection to database {db_name} successful.")
        return url

    def get_date(self):
        """ fetching the current date.
        """
        download_date = datetime.datetime.now()
        return download_date
