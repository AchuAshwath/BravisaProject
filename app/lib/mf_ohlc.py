# Script to get MF OHLC for everyday and insert it into the DB.
import datetime
import requests
import os.path
import os
import codecs
from zipfile import ZipFile
import csv
import psycopg2
import pandas as pd
import calendar
import numpy as np
import pandas.io.sql as sqlio
import time
import math
from datetime import timedelta
import utils.date_set as date_set


class MFOHLC():
    """ Class containing methods which get MF OHLC data from FRS and insert into the DB.
    """

    def __init__(self):

        self.start_date = datetime.date(2016, 1, 31)
        self.end_date = datetime.date.today() + datetime.timedelta(0)

    def get_frs_nav(self, conn, curr_date):
        """ Get NAV data from FRS-NAVRank table.

            Parameters: 
                conn: connection to the database. 

            Returns:
                frs_nav: Dataframe containing FRS-NAVRank data for current date.  
        """

        #query to get current day's data 
        sql = 'SELECT "Date", "Current", "AUM", "btt_scheme_code"\
               FROM "Reports"."FRS-NAVRank" \
               WHERE "Date" = \''+str(curr_date)+'\' and  "Current" is not null \
                and  "AUM" is not null and "btt_scheme_code" is not null \
                and btt_scheme_code != \''+str(0)+'\';'
        
        # Query to Get back Date data        
        # sql = 'SELECT "Date", "Current", "AUM", "btt_scheme_code"\
            #    FROM "Reports"."FRS-NAVRank" \
            #    WHERE "Date" between  \''+str(self.start_date)+'\' and \''+str(self.end_date)+'\' \
                # and  "Current" is not null and "AUM" is not null and "btt_scheme_code" is not null \
                # and btt_scheme_code != \''+str(0)+'\' ;'
    
        frs_nav = sqlio.read_sql_query(sql, con=conn)
        
        return frs_nav
    

    def set_mf_ohlc(self, frs_nav):
        """ Function to set OHLC data for MF from FRS NAV data.

            Parameters:
                frs_nav: dataframe containing NAV data

            Returns: 
                frs_nav: dataframe containing MF OHLC data. 
        """

        # Set open, high, low, close to the Current value from FRS
        frs_nav['open'] = frs_nav['Current']
        frs_nav['high'] = frs_nav['Current']
        frs_nav['low'] = frs_nav['Current']
        frs_nav['close'] = frs_nav['Current']
        frs_nav['volume'] = frs_nav['AUM']

        frs_nav = frs_nav[['btt_scheme_code','open', 'high',
                            'low', 'close', 'Date', 'volume']]

        return frs_nav

    
    def get_mf_averages(self, conn, curr_date):
        """ Function to get NAV averages data from FRS-NAVCategoryAvg table. 

            Parameters: 
                frs_nav: dataframe containing MF OHLC data.

            Returns: 
                frs_nav: MF OHLC data along with category average
        """

        sql = 'SELECT "Date", "1 Year Average", "btt_scheme_category" FROM "Reports"."FRS-NAVCategoryAvg" \
                           WHERE "Date" =  \''+str(curr_date)+'\';'
        
        # Query to Get backs Date data                  
        # sql = 'SELECT "Date", "1 Year Average", "btt_scheme_category" FROM "Reports"."FRS-NAVCategoryAvg" \
                        #    WHERE "Date"  between  \''+str(self.start_date)+'\' and \''+str(self.end_date)+'\' ;'
        frs_nav_cat = sqlio.read_sql_query(sql, con=conn)
        
        frs_nav_cat['open'] = frs_nav_cat['1 Year Average']
        frs_nav_cat['high'] = frs_nav_cat['1 Year Average']
        frs_nav_cat['low'] = frs_nav_cat['1 Year Average']
        frs_nav_cat['close'] = frs_nav_cat['1 Year Average']
        frs_nav_cat['volume'] = frs_nav_cat['1 Year Average']
        frs_nav_cat['btt_scheme_code'] = frs_nav_cat['btt_scheme_category']
        
        frs_nav_cat = frs_nav_cat[['btt_scheme_code', 'Date',\
                     'open', 'high', 'low', 'close','volume']]

        return frs_nav_cat
    
    def concat_navRank_navAvrg(self, frs_nav_cat, frs_nav):
        
        """ Function to concat row wise frs_nav rank and frs_nav_cat.
        
            paremters:
                frs_nav_cat : contains data from frs nav category table
                                for current date.
                                
                frs_nav : contains data from frs nav rank table for 
                            current date.
                            
            Returns : mf_ohlc 
            
        """
        mf_ohlc = pd.concat([frs_nav, frs_nav_cat], axis=0, sort=False)
        mf_ohlc = mf_ohlc.reset_index(drop=True)
        
        return mf_ohlc


    def insert_mf_ohlc(self, mf_ohlc, conn, cur):
        """ Insert MF OHLC into the DB.

            Parameters:
                mf_ohlc: dataframe containing MF OHLC data along with btt_scheme_code.
                conn: database connection
                cur: cursor object using the connection
        """

        mf_ohlc = mf_ohlc[['Date', 'btt_scheme_code' ,'open', 'high', 'low', 'close', 'volume']]

        exportfilename = "mf_ohlc.csv" 
        exportfile = open(exportfilename, "w")
        mf_ohlc.to_csv(exportfile, header=True, index=False, float_format="%.2f", lineterminator='\r')
        exportfile.close()

        copy_sql = """
                COPY public.mf_ohlc FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        with open(exportfilename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            f.close()
        os.remove(exportfilename)
        

    def gen_mf_ohlc_current(self, curr_date, conn, cur):
        """ Function to call the methods in class for MF OHLC process for current date.
        """

        print("Getting NAV data from FRS-NAVRank")
        frs_nav = self.get_frs_nav(conn, curr_date)
    
        if not(frs_nav.empty):

            print("Set MF OHLC from NAV data")
            frs_nav_ohlc = self.set_mf_ohlc(frs_nav)

            print("Getting category averages for schemes")
            frs_nav_cat_avrg = self.get_mf_averages(conn, curr_date)
            
            print("Frs_nav Rank and Frs_nav Average row wise ")
            mf_ohlc_concat = self.concat_navRank_navAvrg(frs_nav_cat_avrg, frs_nav_ohlc)

            print("Inserting into the DB")
            self.insert_mf_ohlc(mf_ohlc_concat, conn, cur)
        
        else:
            
            print("No FRS-NAV data for current date")
            # raise ValueError("Cannot get MF OHLC for current date: ", str(curr_date))
        
