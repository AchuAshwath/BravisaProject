import zipfile
import os
import ftplib
import datetime
import psycopg2
import calendar
import json
from ftplib import FTP
from dateutil.relativedelta import relativedelta
from pathlib import Path
from utils.check_helper import Check_Helper
import utils.date_set as date_set


# with open('config.json', 'r') as f:
#     config = json.load(f)

# my_path = os.path.abspath(os.path.dirname(__file__))
# file_path = os.path.join(my_path, "fb-files\\")


class FB_Helper():
    """ Class FB Helper which fetch the file
        of FB data.
    """

    def __init__(self):
        pass

    def get_fb_history_name_01(self, date):
        """ This function is use to get fb 01 history file 
            this function is being use in fb_insert_history.py.
        """
        history_date = date.strftime("%d%m%Y")
        fb_history_name = 'FB' + history_date + '01'
        return fb_history_name

    def get_fb_history_name_02(self, date):
        """ This function is use to get fb 02 history file
            this function is being use in fb_insert_history.py.
        """
        history_date = date.strftime("%d%m%Y")
        fb_history_name = 'FB' + history_date + '02'
        return fb_history_name

    def get_fb_history_name_03(self, date):
        """ This function is use to get fb 03 history file
            this function is being use in fb_insert_history.py.
        """
        history_date = date.strftime("%d%m%Y")
        fb_history_name = 'FB' + history_date + '03'
        return fb_history_name


    # Functions to get FB folder name
    def get_fb_name_one(self, curr_date):
        """ Get the file name of FB01 for downloaded date. 
        """
        fb_name_one = 'FB' + curr_date.strftime("%d%m%Y") + '01'
        return fb_name_one

    def get_fb_name_two(self, curr_date):
        """ Get the file name of FB02 for downloaded date.
        """
        fb_name_two = 'FB' + curr_date.strftime("%d%m%Y") + '02'
        return fb_name_two

    def get_fb_name_three(self, prev_date):
        """ Get the file name of FB02 for downloaded date.
        """
        fb_name_three = 'FB' + prev_date.strftime("%d%m%Y") + '03'
        return fb_name_three
