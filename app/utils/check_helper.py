import zipfile
import os
import ftplib
import datetime
from ftplib import FTP
from dateutil.relativedelta import relativedelta
from pathlib import Path
import psycopg2
import calendar
import errno
from config import FB_FOLDER


class Check_Helper:
    """ Check Helper class is fetching the file
        path and file. 
    """

    def __init__(self):
        pass

    # Function to check if path exists:
    def check_path(self, pathname_tocheck):
        """ Function to check if path exists
            and this function is being use in
            btt_list.py, fb_insert_history.py,
            fb.py and index_ohlc.py, ohlc.py.
        """
        if not os.path.isdir(os.path.dirname(pathname_tocheck)):
            try:
                os.makedirs(os.path.dirname(pathname_tocheck))
            except OSError as exc:  # Guard against race condition
                print("Can't create folder")
                if exc.errno != errno.EEXIST:
                    raise
    
    def check_FBpath(self, pathname_tocheck):
        """ Function to check if path exists
            and this function is being use in
            fb.py.
        """
        # check if the folder with pathname_tocheck exists in the FB_FOLDER
        if not os.path.isdir(os.path.join(FB_FOLDER, pathname_tocheck)):
            # if not throw an error
            print(pathname_tocheck, "Folder does not exist")
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), pathname_tocheck)
        
            
        

    def check_file(self, file_path, filename_tocheck):
        """ Function to check if FB history file exists
            and this function is being use 
            in fb_insert_history.py.
        """
        my_path = os.path.abspath(os.path.dirname(__file__))
        zip_to_check = file_path + filename_tocheck
        # print(zip_to_check)
        # print(zipfile.is_zipfile(zip_to_check))
        return zipfile.is_zipfile(zip_to_check)
