from flask import Blueprint
from utils.db_helper import DB_Helper
import pandas as pd
import os
from flask import Flask, render_template, request, jsonify
import psycopg2
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Create Blueprint
scheme_code = Blueprint('scheme_code', __name__, template_folder="../templates")

db = DB_Helper()


    