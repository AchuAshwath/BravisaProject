#Python service to insert data into DB
import os 
import csv 
import codecs
import datetime
from utils.db_helper import DB_Helper
from utils.check_helper import Check_Helper
import psycopg2
import pandas as pd
import numpy as np
import shutil
import time
from sqlalchemy import create_engine
from io import StringIO
from psycopg2 import sql, extras
import io
import calendar
import math
import rootpath



if os.name == 'nt':
	# change it to the directory of the app folder
	my_path = os.getcwd()

	file_path = os.path.join(my_path, "FBFiles\\")
	print("FB File path: ", file_path, flush = True)
else:
	my_path = os.path.abspath(os.path.dirname(__file__))
	file_path = os.path.join(my_path, "fb-files/")
	print("File path: ", file_path, flush = True)

class FB_Insert:
	""" Contains methods to export all the public file data 
		into database and insering into there respective tables.
		fetching the data from csv files executing the data 
		and exporting and insering the data into there respective table.
	"""

	def __init__(self):
		self.engine = create_engine('postgresql+psycopg2://postgres:edsols@localhost:5432/BravisaDB')
		self.alchemy_conn = self.engine.raw_connection()
		self.alchemy_cur = self.alchemy_conn.cursor()
		#self.logfile = open("fb_status_files\\log_file.txt", "a")
		#self.logfile.write("\t\tLOG FILE - INNER LOOP STATUS\n\n")
		pass

	def insert_annual_meetings(self, conn,cur,fbname):
		""" Inserting Annual General Meetimg data into database.

		Operation:
			Set the path and fetch the data from AnnualGeneralMeeting.csv.
			and delete the data based on key column,CompanyCode, DateOfAnnouncement and Purpose, 
			and Export the executed data into AnnualGeneralMeetingExport.csv file 
			and insert into AnnualGeneralMeeting Table. 
		"""
		print("File path: ", file_path, flush = True)
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'AnnualGeneralMeeting.csv'
		file_to_check = file_path + fb_name + '\\' + 'AnnualGeneralMeeting.csv'

		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'AnnualGeneralMeeting.csv'
			fb_csv_file = file_to_check
			

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')
					
				#Update Logic - Deletes based on key columns 
				print("Executing Delete Logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'DateOfAnnouncement', 'Purpose'], as_index=False).count()
				# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
				# 			"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				# self.logfile.flush()
				# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
				# 			"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				# self.logfile.flush()
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					
					sql = 'DELETE FROM public."AnnualGeneralMeeting" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "DateOfAnnouncement"=\'' + str(row['DateOfAnnouncement'])+'\' AND "Purpose"=\''+row['Purpose']+'\';'   
					
					cur.execute(sql)
					conn.commit()
					
				#Inserts all new data
				print("Inserting Data into AnnualGeneralMeeting", flush = True)
				exportfilename = "AnnualGeneralMeetingExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
					
				copy_sql = """
						COPY "public"."AnnualGeneralMeeting" FROM stdin WITH CSV HEADER
						DELIMITER as ','
						"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_annual_meetings():", e, flush=True)
				conn.rollback()
		else:
			print("File not found: " + fb_csv_file, flush = True)


	def insert_auditors(self, conn, cur, fbname):
		""" Insert the Auditors data into database

		Operation:
			Set the path for csv file, fetch the data from Auditors.csv.
			and delete the data based on key column CompanyCode, and 
			export the executed data into 'AuditorExport.csv' file and insert into Auditors Table.
		"""
		print("name of file fbname ",fbname, flush = True)

		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'Auditors.csv'

		# print("Aditors file data", fb_csv_file, flush = True)

		file_to_check = file_path + fb_name + '\\' + 'Auditors.csv'
		
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Auditors.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python', encoding='utf-8')

			# If UTF-8 fails, try reading with ANSI encoding
			except UnicodeDecodeError:
				table = pd.read_csv(fb_csv_file, engine='python', encoding='ansi')



				# print("fb_csv_file\n",fb_csv_file, flush = True)

				# print("table\n",table.head(10), flush = True)

				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['Companycode'], as_index=False).count()
				#self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							#"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				#self.logfile.flush()
				# print("table_to_delete\n",table_to_delete.head(10), flush = True)

				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."Auditors"  WHERE "Companycode" =\'' +str(row['Companycode']) + '\' ;'
					# print("row['Companycode']\n",row['Companycode'], flush = True)
					cur.execute(sql)
					conn.commit()
				
				print("Inserting Data into Auditors", flush = True)
				exportfilename = "AuditorsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Auditors" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			
			except Exception as e:
				print("Error in insert_auditors()", e, flush = True)
				conn.rollback()
		else:
			print("File not found: " + fb_csv_file, flush = True)


	def insert_backgroundinfo(self, conn, cur, fbname):
		"""
		Insert the Backgroundinfo data into the database.
		
		Operation:
			Set the path for CSV file, fetch the data from 'BackgroundInfo.csv' file.
			Delete the data based on the key column CompanyCode,
			export the executed data into 'BackgroundInfoExport.csv' file and insert into BackgroundInfo Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = os.path.join(fb_csv_path, 'BackgroundInfo.csv')
		
		file_to_check = os.path.join(file_path, fb_name, 'BackgroundInfo.csv')
		
		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'BackgroundInfo.csv'
			fb_csv_file = file_to_check
			
		if os.path.isfile(file_to_check):
			try:
				try:
					table = pd.read_csv(fb_csv_file, encoding='utf-8')
				except UnicodeDecodeError:
					table = pd.read_csv(fb_csv_file, encoding='latin1')
				
				# Update Logic - Deletes based on key columns
				print("Executing delete logic", flush=True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
				
				for index, row in table_to_delete.iterrows():
					sql = 'DELETE FROM public."BackgroundInfo" WHERE "CompanyCode" = \'' + str(row['CompanyCode']) + '\';'
					cur.execute(sql)
					conn.commit()
					
				# Convert 'BSECode' data to Int and handle missing values
				print("Converting BSECode data to Int")
				table['BSECode'] = pd.to_numeric(table['BSECode'], errors='coerce', downcast='integer')
				table['BSECode'] = table['BSECode'].fillna(-1).astype(int)  # Replace -1 with any default value
				
				print("Inserting Data into BackgroundInfo", flush=True)
				exportfilename = "BackgroundInfoExport.csv"
				table.to_csv(exportfilename, header=True, index=False, lineterminator='\r')
				
				# Use COPY for bulk insert
				copy_sql = """
					COPY "public"."BackgroundInfo" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
				
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_backgroundinfo():", e, flush=True)
				conn.rollback()
		else:
			print("File not found:", fb_csv_file, flush=True)

			
	def insert_bankers(self, conn, cur, fbname):
		"""Insert the Bankers data into the database.
		
		Operation:
        	Set the path for the CSV file, and fetch the data from Bankers.csv file.
        	Delete the data based on key column CompanyCode,
        	export the executed data into 'BankersExport.csv' file and insert into Bankers Table.
    	"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = fb_csv_path + 'Bankers.csv'

		file_to_check = file_path + fb_name + '\\' + 'Bankers.csv'

		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'Bankers.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				# Read CSV file with 'ISO-8859-1' encoding to handle special characters
				table = pd.read_csv(fb_csv_file, encoding='ISO-8859-1')

				# Update Logic - Deletes based on key columns
				print("Executing delete logic", flush=True)
				table_to_delete = table.groupby(['Companycode'], as_index=False).count()

				for index, row in table_to_delete.iterrows():
					sql = 'DELETE FROM public."Bankers"  WHERE "Companycode" = \'' + str(row['Companycode']) + '\' ;'
					cur.execute(sql)
					conn.commit()

				print("Inserting Data into Bankers", flush=True)
				exportfilename = "BankersExport.csv"
				table.to_csv(exportfilename, header=True, index=False, lineterminator='\r')

				# Use COPY for bulk insert
				copy_sql = """
					COPY "public"."Bankers" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()

				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_bankers():", e, flush=True)
				conn.rollback()
		else:
			print("File not found: " + fb_csv_file, flush=True)

	# def insert_bankers(self, conn, cur, fbname):
	# 	""" Insert the Bankers data into database

	# 	Operation:
	# 		Set the path for csv file, and fetch the data from Bankers.csv file.
	# 		and delete the data based on key column CompanyCode, and 
	# 		export the executed data into 'BankersExport.csv' file and insert into Bankers Table.
	# 	"""
	# 	fb_name = fbname
	# 	fb_csv_path = os.path.join(file_path, fb_name +'\\') 
	# 	fb_csv_file = fb_csv_path + 'Bankers.csv'
		
	# 	file_to_check = file_path + fb_name + '\\' + 'Bankers.csv'
				
	# 	if(fbname == 'intermediate_insert'):
			
	# 		file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Bankers.csv'
	# 		fb_csv_file = file_to_check

	# 	if os.path.isfile(file_to_check):
	# 		try:
	# 			table = pd.read_csv(fb_csv_file, engine='python')
	# 			#Update Logic - Deletes based on key columns 
	# 			print("Executing delete logic", flush = True)
	# 			table_to_delete = table.groupby(['Companycode'], as_index=False).count()
	# 			'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
	# 						"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
	# 			self.logfile.flush()'''
	# 			for index,row in table_to_delete.iterrows():
	# 				# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
	# 				# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
	# 				# self.logfile.flush()
	# 				sql = 'DELETE FROM public."Bankers"  WHERE "Companycode" =\'' +str(row['Companycode']) + '\' ;'
	# 				cur.execute(sql)
	# 				conn.commit()
				

	# 			print("Inserting Data into Bankers", flush = True)
	# 			exportfilename = "BankersExport.csv"
	# 			exportfile = open(exportfilename,"w",encoding='utf-8')
	# 			table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
	# 			exportfile.close()
				
	# 			copy_sql = """
	# 				COPY "public"."Bankers" FROM stdin WITH CSV HEADER
	# 				DELIMITER as ','
	# 				"""
	# 			with open(exportfilename, 'r') as f:
	# 				cur.copy_expert(sql=copy_sql, file=f)
	# 				conn.commit()
	# 				f.close()
	# 			os.remove(exportfilename)
	# 		except:
	# 			print("Error in insert_bankers()", flush = True)
	# 			conn.rollback()
	# 	else:
	# 		print("File not found: " + fb_csv_file, flush = True)
			

	def insert_boardmeetings(self, conn, cur, fbname):
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = os.path.join(fb_csv_path, 'BoardMeetings.csv')

		file_to_check = os.path.join(file_path, fb_name, 'BoardMeetings.csv')

		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'BoardMeetings.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				# Attempt to read with 'utf-8', fallback to 'latin1' on decode error
				try:
					table = pd.read_csv(fb_csv_file, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)
				except UnicodeDecodeError:
					table = pd.read_csv(fb_csv_file, encoding='latin1', quoting=csv.QUOTE_MINIMAL)
				
				# Handle newlines in DataFrame
				table.replace('\n', ' ', regex=True, inplace=True)

				# Update Logic - Bulk delete based on key columns using a temporary table
				print("Executing delete logic", flush=True)

				# Create a temporary table
				temp_table_name = 'temp_board_meetings_delete'
				cur.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
				cur.execute(f'CREATE TEMP TABLE {temp_table_name} ( "CompanyCode" INT, "BoardMeetDate" DATE )')
				conn.commit()

				# Insert data into the temporary table
				for index, row in table.iterrows():
					cur.execute(f'INSERT INTO {temp_table_name} VALUES (%s, %s)', (row["CompanyCode"], row["BoardMeetDate"]))
				conn.commit()

				# Perform the delete operation
				delete_sql = f'DELETE FROM public."BoardMeetings" WHERE ("CompanyCode", "BoardMeetDate") IN (SELECT "CompanyCode", "BoardMeetDate" FROM {temp_table_name});'
				cur.execute(delete_sql)
				conn.commit()

				print("Inserting Data into BoardMeetings", flush=True)
				export_buffer = StringIO()
				table.to_csv(export_buffer, header=True, index=False, lineterminator='\r', encoding='utf-8')

				# Use COPY for bulk insert
				export_buffer.seek(0)
				copy_sql = """
                    COPY "public"."BoardMeetings" FROM stdin WITH CSV HEADER QUOTE '"'
                    DELIMITER as ','
                """
				cur.copy_expert(sql=copy_sql, file=export_buffer)
				conn.commit()

			except Exception as e:
				print("Error in insert_boardmeetings():", e, flush=True)
				conn.rollback()
			finally:
				# Drop the temporary table
				cur.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
		else:
			print("File not found:", fb_csv_file, flush=True)



	def insert_bonus(self, conn, cur, fbname):
		""" Insert the Bonus data into database

		Operation:
			Set the path for csv file, and fetch the data from Bonus.csv file.
			delete the data based on key column,CompanyCode and DateOfAnnouncement
			and export data into 'BonusExport.csv' file and insert into Bonus Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\')	
		fb_csv_file = fb_csv_path + 'Bonus.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'Bonus.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Bonus.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'DateOfAnnouncement'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = sql = 'DELETE FROM public."Bonus" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "DateOfAnnouncement"=\'' + str(row['DateOfAnnouncement'])+'\';'   
					cur.execute(sql)
					conn.commit()
				
				print("Inserting Data into Bonus", flush = True)
				exportfilename = "BonusExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Bonus" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_bonus()", flush = True)
				conn.rollback()
		else:
			print("File not found: " + fb_csv_file, flush = True)
			
			
			
	def insert_capitalstructure(self, conn, cur, fbname):
		"""
		Insert the Capital Structure data into the database.

		Operation:
			Set the path for CSV file, fetch the data from CapitalStructure.csv file.
			Delete the data based on key columns, companycode, FromYear, and ToYear.
			Export data into 'CapitalStructureExport.csv' file and insert into CapitalStructure Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = os.path.join(fb_csv_path, 'CapitalStructure.csv')

		file_to_check = os.path.join(file_path, fb_name, 'CapitalStructure.csv')

		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'CapitalStructure.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				# Specify encoding when reading CSV file
				table = pd.read_csv(fb_csv_file, engine='python', encoding='latin1')

				# Replace empty strings with NaN in 'TotalArrears'
				table['TotalArrears'] = table['TotalArrears'].replace(' ', np.nan)

				# Update Logic - Deletes based on key columns
				print("Executing delete logic", flush=True)
				table_to_delete = table.groupby(['companycode', 'FromYear', 'ToYear'], as_index=False).count()

				for index, row in table_to_delete.iterrows():
					sql = f'DELETE FROM public."CapitalStructure" WHERE "companycode"=\'{row["companycode"]}\' AND "FromYear"=\'{row["FromYear"]}\' AND "ToYear"=\'{row["ToYear"]}\''
					cur.execute(sql)
					conn.commit()

				print("Inserting Data into CapitalStructure", flush=True)
				exportfilename = "CapitalStructureExport.csv"
				
				# Specify encoding and errors parameter when writing to CSV
				table.to_csv(exportfilename, header=True, index=False, lineterminator='\r', encoding='utf-8', errors='ignore')

				# Use COPY for bulk insert
				copy_sql = """
					COPY "public"."CapitalStructure" FROM stdin WITH CSV HEADER
					DELIMITER as ','
				"""
				with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()

				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_capitalstructure():", e, flush=True)
				conn.rollback()
		else:
			print("File not found:", fb_csv_file, flush=True)



	def insert_companymaster(self, conn, cur, fbname):
		""" Insert the Company Master data into database

		Operation:
			Set the path for csv file, and fetch the data from CompanyMaster.csv file.
			delete the data based on key column CompanyCode, and export data into 
			'CompanyMasterExport.csv' file and insert into CompanyMaster Table.
		"""
		fb_name = fbname 
		fb_csv_path = os.path.join(file_path, fb_name +'\\')	
		fb_csv_file = fb_csv_path + 'CompanyMaster.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'CompanyMaster.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'CompanyMaster.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."CompanyMaster" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\';'	
					cur.execute(sql)
					conn.commit()
				
				print("Inserting Data into CompanyMaster", flush = True)
				exportfilename = "CompanyMasterExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."CompanyMaster" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_companymaster()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_companynamechange(self, conn, cur, fbname):
		""" Insert the Company Name Change data into database

		Operation:
			Set the path for csv file, and fetch the data from CompanyNameChange.csv file.
			delete the data based on key column, CompanyCode and EffectiveDate
			and export executed data into 'CompanyNameChangeExport.csv' file 
			and insert into CompanyNameChange Table.
		"""
		fb_name = fbname 
		fb_csv_path = os.path.join(file_path, fb_name +'\\')	
		fb_csv_file = fb_csv_path + 'CompanyNameChange.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'CompanyNameChange.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'CompanyNameChange.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'EffectiveDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = sql = 'DELETE FROM public."CompanyNameChange" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "EffectiveDate"=\'' + str(row['EffectiveDate'])+'\';'   
					
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into CompanyNameChange", flush = True)
				exportfilename = "BonusExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."CompanyNameChange" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_companynamechange()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_consolidatedhalfyearlyresults(self, conn, cur, fbname):
		""" Insert the Consolidated Half Yearly Results data into database

		Operation:
			Set the path for csv file, and fetch the data from ConsolidatedHalfYearlyResults.csv file.
			delete the data based on key column,CompanyCode, YearEnding and Half,
			and export executed data into 'ConsolidatedHalfYearlyResultsExport.csv' file 
			and insert into ConsolidatedHalfYearlyResults Table.
		"""
		fb_name = fbname 
		fb_csv_path = os.path.join(file_path, fb_name +'\\')	
		fb_csv_file = fb_csv_path + 'ConsolidatedHalfyearlyResults.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'ConsolidatedHalfyearlyResults.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'ConsolidatedHalfyearlyResults.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding', 'Half'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."ConsolidatedHalfYearlyResults" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "YearEnding"=\'' + str(row['YearEnding'])+'\' AND "Half"=\''+str(row['Half'])+'\';'   
					
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into ConsolidatedHalfYearlyResults", flush = True)
				exportfilename = "ConsolidatedHalfYearlyResultsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."ConsolidatedHalfYearlyResults" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			
			except:
				print("Error in insert_consolidatedhalfyearlyresults()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_consolidatedninemonthsresults(self, conn, cur, fbname):
		""" Insert the Consolidated Nine Month Results data into database

		Operation:
			Set the path for csv file, and fetch the data from ConsolidatedNineMonthResults.csv file.
			delete the data based on key column,CompanyCode and YearEnding,
			and export executed data into 'ConsolidatedNineMonthResults.csv' file 
			and insert into ConsolidatedNineMonthResults Table.
		"""
		fb_name = fbname 
		fb_csv_path = os.path.join(file_path, fb_name +'\\')	
		fb_csv_file = fb_csv_path + 'ConsolidatedNinemonthsResults.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'ConsolidatedNinemonthsResults.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'ConsolidatedNinemonthsResults.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."ConsolidatedNineMonthsResults" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "YearEnding"=\'' + str(row['YearEnding'])+'\';'   
					
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into ConsolidatedNineMonthsResults", flush = True)
				exportfilename = "ConsolidatedNineMonthsResultsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."ConsolidatedNineMonthsResults" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			
			except:
				print("Error in insert_consolidatedninemonthsresults()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_consolidatedquarterlyresults(self, conn, cur, fbname):
		""" Insert the Consolidated Quarterly Results data into database

		Operation:
			Set the path for csv file, and fetch the data from ConsolidatedQuarterlyResults.csv file.
			delete the data based on key column, CompanyCode and YearEnding
			export executed data into 'ConsolidatedQuarterlyResultsExport.csv' file 
			and insert into ConsolidatedQuarterlyResults Table. 
		"""
		fb_name = fbname 
		fb_csv_path = os.path.join(file_path, fb_name +'\\')	
		fb_csv_file = fb_csv_path + 'ConsolidatedQuarterlyResults.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'ConsolidatedQuarterlyResults.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'ConsolidatedQuarterlyResults.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."ConsolidatedQuarterlyResults" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "YearEnding"=\'' + str(row['YearEnding'])+'\';'   
					
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into ConsolidatedQuarterlyResults", flush = True)
				exportfilename = "ConsolidatedQuarterlyResultsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."ConsolidatedQuarterlyResults" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			
			except:
				print("Error in insert_consolidatedquarterlyresults()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_currentdata(self, conn, cur,fbname ):
		""" Insert the Current Date data into database

		Operation:
			Set the path for csv file, and fetch the data from “CurrentData.csv file.
			delete the data based on key column, Companycode and export data into 
			'CurrentDataExport.csv' file and insert into CurrentData Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'CurrentData.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'CurrentData.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'CurrentData.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				table[table.columns[3:40]] = table[table.columns[3:40]].replace(' ', np.nan)
				table['Months'] = table['Months'].fillna(0).astype(int)

				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['Companycode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."CurrentData" WHERE "Companycode" =\'' +str(row['Companycode'])   + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into CurrentData", flush = True)
				exportfilename = "CurrentDataExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."CurrentData" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			
			except:
				print("Error in insert_currentdata()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_dividend(self, conn, cur,fbname):
		""" Insert the Dividend data into database

		Operation:
			Set the path for csv file, and fetch the data from Dividend.csv file.
			delete the data based on key column,CompanyCode and DateOfAnnouncement
			and export data into 'DividendExport.csv' file and insert into Dividend Table.
		"""

		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\')  
		fb_csv_file = fb_csv_path + 'Dividend.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'Dividend.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Dividend.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'DateOfAnnouncement'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = sql = 'DELETE FROM public."Dividend" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "DateOfAnnouncement"=\'' + str(row['DateOfAnnouncement'])+'\';'   
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into Dividend", flush = True)
				exportfilename = "DividendExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Dividend" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			
			except:
				print("Error in insert_dividend()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_financebanking(self, conn, cur,fbname):
		""" Insert the Finance Banking data into database

		Operation:
			Set the path for csv file, and fetch the data from FinanceBanking.csv file.
			delete the data based on key column,Companycode and YearEnding,
			and export data into 'FinanceBankingExport.csv' file 
			and insert into FinanceBanking Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'FinanceBankingVI.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'FinanceBankingVI.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'FinanceBankingVI.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['Companycode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."FinanceBankingVI" WHERE "Companycode"=\'' +str(row['Companycode'])+ '\' AND "YearEnding"=\'' + str(row['YearEnding'])+'\';'   
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into FinanceBankingVI", flush = True)
				exportfilename = "FinanceBankingVIExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."FinanceBankingVI" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_financebanking()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_financeconsolidatedbanking(self, conn, cur,fbname):
		""" Insert the Finance Consolidated BankingVI data into database

		Operation:
			Set the path for csv file, and fetch the data from FinanceConsolidatedBankingVI.csv file.
			delete the data based on key column,Companycode and YearEnding
			and export data into 'FinanceConsolidatedBankingVIsExport.csv' file 
			and insert into FinanceConsolidatedBankingVI Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\')  
		fb_csv_file = fb_csv_path + 'FinanceConsolidatedBankingVI.csv'
		

		file_to_check = file_path + fb_name + '\\' + 'FinanceConsolidatedBankingVI.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'FinanceConsolidatedBankingVI.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['Companycode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."FinanceConsolidatedBankingVI" WHERE "Companycode"=\'' +str(row['Companycode'])+ '\' AND "YearEnding"=\'' + str(row['YearEnding'])+'\';'   
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into FinanceConsolidatedBankingVI", flush = True)
				exportfilename = "FinanceConsolidatedBankingVIExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."FinanceConsolidatedBankingVI" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_financeconsolidatedbanking()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_financeconsolidatednonbanking(self, conn, cur,fbname):
		""" Insert the Finance Consolidated NonBankingVI data into database

		Operation:
			Set the path for csv file, and fetch the data from FinanceConsolidatedNonBankingVI.csv file.
			delete the data based on key column,Companycode and YearEnding
			and export data into 'FinanceConsolidatedNonBankingVIExport.csv' file 
			and insert into FinanceConsolidatedNonBankingVI Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\')  
		fb_csv_file = fb_csv_path + 'FinanceConsolidatedNonBankingVI.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'FinanceConsolidatedNonBankingVI.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'FinanceConsolidatedNonBankingVI.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['Companycode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."FinanceConsolidatedNonBankingVI" WHERE "Companycode"=\'' +str(row['Companycode'])+ '\' AND "YearEnding"=\'' + str(row['YearEnding'])+'\';'   
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into FinanceConsolidatedNonBankingVI", flush = True)
				exportfilename = "FinanceConsolidatedNonBankingVIExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."FinanceConsolidatedNonBankingVI" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_financeconsolidatednonbanking()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_financenonbanking(self, conn, cur,fbname):
		""" Insert the Finance NonBankingVI data into database

		Operation:
			Set the path for csv file, and fetch the data from FinanceNonBankingVI.csv file.
			delete the data based on key column,Companycode and YearEnding
			and export executed data into 'FinanceNonBankingVIExport.csv' file 
			and insert into FinanceNonBankingVI Table.		
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'FinanceNonBankingVI.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'FinanceNonBankingVI.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'FinanceNonBankingVI.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['Companycode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."FinanceNonBankingVI" WHERE "Companycode"=\'' +str(row['Companycode'])+ '\' AND "YearEnding"=\'' + str(row['YearEnding'])+'\';'   
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into FinanceNonBankingVI", flush = True)
				exportfilename = "FinanceNonBankingVIExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."FinanceNonBankingVI" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_financenonbanking()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_fundmaster(self, conn, cur,fbname ):
		""" Insert the Fund Master data into database

		Operation:
			Set the path for csv file, and fetch the data from FundMaster.csv file.
			delete the data based on key column,CompanyCode, and export executed data into
			'FundMastersExport.csv' file and insert into FundMaster Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\')  
		fb_csv_file = fb_csv_path + 'FundMaster.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'FundMaster.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'FundMaster.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				'''
				table.loc[table['MFSetUpDate'] == '""', 'MFSetUpDate'] = ""
				table.loc[table['AMCIncDate'] == '""', 'AMCIncDate'] = ""
				'''

				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."FundMaster" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into FundMaster", flush = True)
				exportfilename = "FundMasterExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."FundMaster" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_fundmaster()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_halfyearlyresults(self, conn, cur,fbname ):
		""" Insert the Half yearly Results data into database

		Operation:
			Set the path for csv file, and fetch the data from HalfyearlyResults.csv file.
			delete the data based on key column CompanyCode,YearEnding and Half  
			and export the executed data into 'HalfyearlyResultsExport.csv' file 
			and insert into HalfyearlyResults Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'HalfyearlyResults.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'HalfyearlyResults.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'HalfyearlyResults.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding', 'Half'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."HalfYearlyResults" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "YearEnding"=\'' + str(row['YearEnding'])+'\' AND "Half"=\'' + str(row['Half'])+'\';'   
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into HalfYearlyResults", flush = True)
				exportfilename = "HalfYearlyResultsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."HalfYearlyResults" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_halfyearlyresults()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_individualholding(self, conn, cur,fbname ):
		""" Insert the Individual Holding data into database

		Operation:
			Set the path for csv file, and fetch the data from IndividualHolding.csv file.
			delete the data based on key column CompanyCode, ShareHoldingDate
			and export data into 'IndividualHoldingExport.csv' file 
			and insert into IndividualHolding Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'IndividualHolding.csv'
		

		file_to_check = file_path + fb_name + '\\' + 'IndividualHolding.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'IndividualHolding.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				#table.loc[table['ShareHoldingDate'] == '""', 'ShareHoldingDate'] = ""

				
				#Update Logic - Deletes based on key columns 
				# print("Executing delete logic", flush = True)
				# table_to_delete = table.groupby(['CompanyCode', 'ShareHoldingDate'], as_index=False).count()
				# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
				# 			"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				# self.logfile.flush()
				# for index,row in table_to_delete.iterrows():
				# 	# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
				# 	# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				# 	# self.logfile.flush()
				# 	sql = 'DELETE FROM public."IndividualHolding"  WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "ShareHoldingDate"=\'' + str(row['ShareHoldingDate'])+'\' ;'
				# 	##print(sql, flush = True)
				# 	cur.execute(sql)
				# 	conn.commit()
				

				print("Inserting Data into IndividualHolding", flush = True)
				exportfilename = "IndividualHoldingExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."IndividualHolding" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:       
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_individualholding()",e , flush = True)
				conn.rollback()


		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_industrymaster(self, conn, cur,fbname ):
		""" Insert the Industry Master data into database

		Operation:
			Set the path for csv file, and fetch the data from IndustryMaster.csv file.
			delete the data based on key column IndustryCode, 
			and export executed data into 'IndustryMasterExport.csv' file 
			and insert into IndustryMaster Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\')  
		fb_csv_file = fb_csv_path + 'IndustryMaster.csv'
		

		file_to_check = file_path + fb_name + '\\' + 'IndustryMaster.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'IndustryMaster.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['IndustryCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."IndustryMaster" WHERE "IndustryCode" =\'' +str(row['IndustryCode']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into IndustryMaster", flush = True)
				exportfilename = "IndustryMasterExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."IndustryMaster" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_industrymaster()", flush = True)
				conn.rollback()


		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_keyexecutives(self, conn, cur, fbname):
		"""
		Insert the Key Executives data into the database.

		Operation:
			Set the path for csv file, and fetch the data from KeyExecutives.csv file.
			Delete the data based on key column Companycode, and export executed data into
			'KeyExecutivesExport.csv' file and insert into KeyExecutives Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = os.path.join(fb_csv_path, 'KeyExecutives.csv')

		file_to_check = os.path.join(file_path, fb_name, 'KeyExecutives.csv')

		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'KeyExecutives.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				# Try reading with UTF-8 encoding
				try:
					table = pd.read_csv(fb_csv_file, engine='python', encoding='utf-8')

				# If UTF-8 fails, try reading with ANSI encoding
				except UnicodeDecodeError:
					table = pd.read_csv(fb_csv_file, engine='python', encoding='ansi')

				# Update Logic - Deletes based on key columns
				print("Executing delete logic", flush=True)
				table_to_delete = table.groupby(['Companycode'], as_index=False).count()

				for index, row in table_to_delete.iterrows():
					sql = f'DELETE FROM public."KeyExecutives" WHERE "Companycode"=\'{row["Companycode"]}\' ;'
					cur.execute(sql)
					conn.commit()

				print("Inserting Data into KeyExecutives", flush=True)
				exportfilename = "KeyExecutivesExport.csv"
				
				# Use UTF-8 encoding when writing to CSV
				table.to_csv(exportfilename, header=True, index=False, lineterminator='\r', encoding='utf-8', errors='ignore')

				# Use COPY for bulk insert
				copy_sql = """
					COPY "public"."KeyExecutives" FROM stdin WITH CSV HEADER
					DELIMITER as ','
				"""
				with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()

				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_keyexecutives():", e, flush=True)
				conn.rollback()
		else:
			print("File not found:", fb_csv_file, flush=True)

				
	def insert_locations(self, conn, cur, fbname):
		"""
		Insert the Locations data into the database.

		Operation:
			Set the path for csv file, and fetch the data from Locations.csv file.
			Delete the data based on key column CompanyCode, and export executed data into 
			'LocationsExport.csv' file and insert into Locations Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = fb_csv_path + 'Locations.csv'

		file_to_check = file_path + fb_name + '\\' + 'Locations.csv'

		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'Locations.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				# Read CSV file with 'ISO-8859-1' encoding to handle special characters
				table = pd.read_csv(fb_csv_file, encoding='ISO-8859-1')

				# Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush=True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()

				for index, row in table_to_delete.iterrows():
					sql = 'DELETE FROM public."Locations" WHERE "CompanyCode" = \'' + str(row['CompanyCode']) + '\' ;'
					cur.execute(sql)
					conn.commit()

				print("Inserting Data into Locations", flush=True)
				exportfilename = "LocationsExport.csv"
				table.to_csv(exportfilename, header=True, index=False, lineterminator='\r', encoding='utf-8-sig')

				# Use COPY for bulk insert
				copy_sql = """
					COPY "public"."Locations" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r", encoding='utf-8-sig', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()

				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_locations():", e, flush=True)
				conn.rollback()
		else:
			print("File not found: " + fb_csv_file, flush=True)


	# def insert_locations(self, conn, cur,fbname ):
	# 	""" Insert the Locations data into database

	# 	Operation:
	# 		Set the path for csv file, and fetch the data from Locations.csv file.
	# 		delete the data based on key column CompanyCode, and export executed data into 
	# 		'LocationsExport.csv' file and insert into Locations Table.
	# 	"""
	# 	fb_name = fbname
	# 	fb_csv_path = os.path.join(file_path, fb_name +'\\') 
	# 	fb_csv_file = fb_csv_path + 'Locations.csv'

	# 	file_to_check = file_path + fb_name + '\\' + 'Locations.csv'
				
	# 	if(fbname == 'intermediate_insert'):
			
	# 		file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Locations.csv'
	# 		fb_csv_file = file_to_check

	# 	if os.path.isfile(file_to_check):
	# 		try:
	# 			table = pd.read_csv(fb_csv_file, engine='python')

				
	# 			#Update Logic - Deletes based on key columns 
	# 			print("Executing delete logic", flush = True)
	# 			table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
	# 			'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
	# 						"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
	# 			self.logfile.flush()'''
	# 			for index,row in table_to_delete.iterrows():
	# 				# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
	# 				# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
	# 				# self.logfile.flush()
	# 				sql = 'DELETE FROM public."Locations" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' ;'
	# 				##print(sql, flush = True)
	# 				cur.execute(sql)
	# 				conn.commit()
				

	# 			print("Inserting Data into Locations", flush = True)
	# 			exportfilename = "LocationsExport.csv"
	# 			exportfile = open(exportfilename,"w", encoding="utf-8")
	# 			table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
	# 			exportfile.close()
				
	# 			copy_sql = """
	# 				COPY "public"."Locations" FROM stdin WITH CSV HEADER
	# 				DELIMITER as ','
	# 				"""
	# 			with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:       
	# 				cur.copy_expert(sql=copy_sql, file=f)
	# 				conn.commit()
	# 				f.close()
	# 			os.remove(exportfilename)
	# 		except Exception as e:
	# 			print("Error in insert_locations()", e, flush = True)
	# 			conn.rollback()

	# 	else:
	# 		print("File not found: " + fb_csv_file, flush = True)

	def insert_mfinvestments(self, conn, cur,fbname ):
		""" Insert the MF Investments data into database

		Operation:
			Set the path for csv file, and fetch the data from MFInvestments.csv file.
			delete the data based on key column MFDate and InstrumentType,
			and export executed data into 'MFInvestmentsExport.csv' file 
			and insert into MFInvestments Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'MFInvestments.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'MFInvestments.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'MFInvestments.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['MFDate', 'InstrumentType'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."MFInvestments" WHERE "MFDate" =\'' +str(row['MFDate'])+ '\' AND "InstrumentType"=\'' + (row['InstrumentType']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into MFInvestments", flush = True)
				exportfilename = "MFInvestmentsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."MFInvestments" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_mfinvestments()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_managementteam(self, conn, cur, fbname ):
		""" Insert the Management Team data into database

		Operation:
			Set the path for csv file, and fetch the data from ManagementTeam.csv file.
			delete the data based on key column Companycode, and export executed data into 
			'ManagementTeamsExport.csv' file and insert into ManagementTeam Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'ManagementTeam.csv'
		file_to_check = file_path + fb_name + '\\' + 'ManagementTeam.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'ManagementTeam.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				try:
					table = pd.read_csv(fb_csv_file, engine='python',encoding='latin1')
				except UnicodeDecodeError:
					table = pd.read_csv(fb_csv_file, engine='python',encoding='utf-8')
				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['Companycode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."ManagementTeam"  WHERE "Companycode" =\'' +str(row['Companycode']) + '\' ;'
					#print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into ManagementTeam", flush = True)
				exportfilename = "ManagementTeamExport.csv"
				exportfile = open(exportfilename,"w", encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."ManagementTeam" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:       
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_managementteam()",e, flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_ninemonthsresults(self, conn, cur,fbname ):
		""" Insert the Nine months Results data into database

		Operation:
			Set the path for csv file, and fetch the data from NinemonthsResults.csv file.
			delete the data based on key column CompanyCode and YearEnding
			and export executed data into 'NinemonthsResultsExport.csv' file 
			and insert into NinemonthsResults Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'NinemonthsResults.csv'
		file_to_check = file_path + fb_name + '\\' + 'NinemonthsResults.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'NinemonthsResults.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."NinemonthsResults" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into NinemonthsResults", flush = True)
				exportfilename = "NinemonthsResultsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."NinemonthsResults" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_ninemonthsresults()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_products(self, conn, cur,fbname ):
		""" Insert the Products data into database

		Operation:
			Set the path for csv file, and fetch the data from Products.csv file.
			delete the data based on key column CompanyCode and YearEnding 
			and export executed data into 'ProductsExport.csv' file 
			and insert into Products Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'Products.csv'
		file_to_check = file_path + fb_name + '\\' + 'Products.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Products.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."Products"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\' ;'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into Products", flush = True)
				exportfilename = "ProductsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Products" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename,"r", encoding = 'utf-8', errors = 'ignore' ) as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_products()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_quarterlyresults(self, conn, cur,fbname ):
		""" Insert the Quarterly Results data into database

		Operation:
			Set the path for csv file, and fetch the data from QuarterlyResults.csv file.
			delete the data from QuarterlyEPS, TTM and QuarterlyResults tables based on 
			key column CompanyCode and YearEnding and export executed data into
			'QuarterlyResultsExport.csv' file and insert into QuarterlyResults Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'QuarterlyResults.csv'
		file_to_check = file_path + fb_name + '\\' + 'QuarterlyResults.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'QuarterlyResults.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')
				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()

					sql_eps = 'DELETE FROM public."QuarterlyEPS" WHERE "CompanyCode" =\'' +str(row['CompanyCode'])  + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\';'
					sql_ttm = 'DELETE FROM public."TTM" WHERE "CompanyCode" =\'' +str(row['CompanyCode'])  + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\';'
					sql = 'DELETE FROM public."QuarterlyResults" WHERE "CompanyCode" =\'' +str(row['CompanyCode'])  + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\';'
					
					cur.execute(sql_eps)
					cur.execute(sql_ttm)
					cur.execute(sql)

					conn.commit()
				

				print("Inserting Data into QuarterlyResults", flush = True)
				exportfilename = "QuarterlyResultsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."QuarterlyResults" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_quarterlyresults()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_ratiosbanking(self, conn, cur,fbname):
		""" Insert the Ratios BankingVI data into database

		Operation:
			Set the path for csv file, and fetch the data from RatiosBankingVI.csv file.
			delete the data from RatiosMergeList and RatiosBankingVI table based on 
			key column CompanyCode and YearEnding. export executed data into 
			'RatiosBankingVIExport.csv' file and insert into RatiosBankingVI Table.
		"""

		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'RatiosBankingVI.csv'
		file_to_check = file_path + fb_name + '\\' + 'RatiosBankingVI.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'RatiosBankingVI.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				table['YearEnding'] = pd.to_datetime(table['YearEnding'], errors = 'ignore')
				table['YearEnding'] = table['YearEnding'].apply(lambda dt: dt.strftime('%Y-%m-%d')if not pd.isnull(dt) else '')

				table['ModifiedDate'] = pd.to_datetime(table['ModifiedDate'], errors = 'ignore')
				table['ModifiedDate'] = table['ModifiedDate'].apply(lambda dt: dt.strftime('%Y-%m-%d')if not pd.isnull(dt) else '')


				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					
					sql_ratiosmerge_banking = 'DELETE FROM public."RatiosMergeList" WHERE "CompanyCode" =\'' +str(row['CompanyCode'])   + '\' AND "ROEYearEnding"=\'' + str(row['YearEnding']) + '\';'
					sql = 'DELETE FROM public."RatiosBankingVI" WHERE "CompanyCode" =\'' +str(row['CompanyCode'])   + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\';'
					
					cur.execute(sql_ratiosmerge_banking)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into RatiosBankingVI", flush = True)
				exportfilename = "RatiosBankingVIExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."RatiosBankingVI" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_ratiosbanking()", flush = True)
				conn.rollback()
		
		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_ratiosconsolidatednonbanking(self, conn, cur,fbname ):
		""" Insert the Ratios Consolidated NonBankingVI data into database

		Operation:
			Set the path for csv file, and fetch the data from RatiosConsolidatedNonBankingVI.csv file.
			delete the data based on key column CompanyCode and YearEnding, 
			export executed data into 'RatiosConsolidatedNonBankingVIExport.csv' file 
			and insert into RatiosConsolidatedNonBankingVI Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'RatiosConslidatedNonBankingVI.csv'
		file_to_check = file_path + fb_name + '\\' + 'RatiosConslidatedNonBankingVI.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'RatiosConslidatedNonBankingVI.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."RatiosConslidatedNonBankingVI" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into RatiosConslidatedNonBankingVI", flush = True)
				exportfilename = "RatiosConslidatedNonBankingVIExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."RatiosConslidatedNonBankingVI" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_ratiosconsolidatednonbanking()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_ratiosconsolidatedbanking(self, conn, cur,fbname ):
		""" Insert the Ratios Consolidated BankingVI data into database

		Operation:
			Set the path for csv file, and fetch the data from RatiosConsolidatedBankingVI.csv file.
			delete the data based on key column CompanyCode and YearEnding, export executed data into 
			'RatiosConsolidatedBankingVIExport.csv' file and insert into RatiosConsolidatedBankingVI Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'RatiosConsolidatedBankingVI.csv'
		file_to_check = file_path + fb_name + '\\' + 'RatiosConsolidatedBankingVI.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'RatiosConsolidatedBankingVI.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."RatiosConsolidatedBankingVI" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				
				
				print("Inserting Data into RatiosConsolidatedBankingVI", flush = True)
				exportfilename = "RatiosConslidatedBankingVIExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."RatiosConsolidatedBankingVI" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_ratiosconsolidatedbanking()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_ratiosnonbanking(self, conn, cur,fbname ):
		""" Insert the Ratios NonBankingVI data into database

		Operation:
			Set the path for csv file, and fetch the data from RatiosNonBankingVI.csv file.
			delete the data from RatiosMergeList and RatiosNonBankingVI table based on
			key column CompanyCode and YearEnding, export executed data into 
			'RatiosNonBankingVIsExport.csv' file and insert into RatiosNonBankingVI Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'RatiosNonBankingVI.csv'
		file_to_check = file_path + fb_name + '\\' + 'RatiosNonBankingVI.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'RatiosNonBankingVI.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				table['YearEnding'] = pd.to_datetime(table['YearEnding'], errors = 'ignore')
				table['YearEnding'] = table['YearEnding'].apply(lambda dt: dt.strftime('%Y-%m-%d')if not pd.isnull(dt) else '')

				table['ModifiedDate'] = pd.to_datetime(table['ModifiedDate'], errors = 'ignore')
				table['ModifiedDate'] = table['ModifiedDate'].apply(lambda dt: dt.strftime('%Y-%m-%d')if not pd.isnull(dt) else '')

				#print(table.dtypes, flush = True)
				table['EnterpriseValue'] = pd.to_numeric(table['EnterpriseValue'], errors='coerce')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()

					sql_ratiosmerge_nonbanking = 'DELETE FROM public."RatiosMergeList" WHERE "CompanyCode" =\'' +str(row['CompanyCode'])   + '\' AND "ROEYearEnding"=\'' + str(row['YearEnding']) + '\';'
					sql = 'DELETE FROM public."RatiosNonBankingVI" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\';'
					
					cur.execute(sql_ratiosmerge_nonbanking)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into RatiosNonBankingVI", flush = True)
				exportfilename = "RatiosNonBankingVIExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."RatiosNonBankingVI" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_ratiosnonbanking()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_rawmaterials(self, conn, cur,fbname ):
		""" Insert the Raw Material data into database

		Operation:
			Set the path for csv file, and fetch the data from RawMaterials.csv file.
			delete the data based on key column CompanyCode and YearEnding, export executed 
			data into 'RawMaterialsExport.csv' file and insert into RawMaterials Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'RawMaterials.csv'
		file_to_check = file_path + fb_name + '\\' + 'RawMaterials.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'RawMaterials.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'YearEnding'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."RawMaterials"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' AND "YearEnding"=\'' + str(row['YearEnding']) + '\' ;'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into RawMaterials", flush = True)
				exportfilename = "RawMaterialsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."RawMaterials" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_rawmaterials()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_registrars(self, conn, cur,fbname ):
		""" Insert the Registrars data into database

		Operation:
			Set the path for csv file, and fetch the data from Registrars.csv file.
			delete the data based on key column CompanyCode, and export executed data into 
			'RegistrarsExport.csv' file and insert into Registrars Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'Registrars.csv'
		file_to_check = file_path + fb_name + '\\' + 'Registrars.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Registrars.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				# Try reading with 'utf-8' encoding
				table = pd.read_csv(fb_csv_file, engine='python', encoding='utf-8')
			except UnicodeDecodeError:
				# If 'utf-8' fails, try reading with 'latin1' encoding
				table = pd.read_csv(fb_csv_file, engine='python', encoding='latin1')

				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."Registrars" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' ;'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into Registrars", flush = True)
				exportfilename = "RegistrarsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Registrars" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_registrars()",e, flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_rights(self, conn, cur,fbname ):
		""" Insert the Rights data into database

		Operation:
			Set the path for csv file, and fetch the data from Rights.csv file.
			delete the data based on key column CompanyCode and DateOfAnnouncement,
			export executed data into 'RightsExport.csv' file and insert into Rights Table.		
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'Rights.csv'
		file_to_check = file_path + fb_name + '\\' + 'Rights.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Rights.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				'''
				table.loc[table['RecordDate'] == '""', 'RecordDate'] = ""
				table.loc[table['BookClosureStartDate'] == '""', 'BookClosureStartDate'] = ""
				table.loc[table['BookClosureEndDate'] == '""', 'BookClosureEndDate'] = ""
				table.loc[table['XRDate'] == '""', 'XRDate'] = ""
				'''
				
				#print(table.dtypes, flush = True)

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'DateOfAnnouncement'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."Rights" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' AND "DateOfAnnouncement" =\'' + str(row['DateOfAnnouncement']) + '\' ;'
					#print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				
				
				print("Inserting Data into Rights", flush = True)
				exportfilename = "RightsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Rights" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_rights()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemeboardofamc(self, conn, cur,fbname):
		""" Insert the Scheme Boardo FAMC data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeBoardoFAMC.csv file.
			delete the data based on key column CompanyCode, and export executed data into
			'SchemeBoardoFAMCExport.csv' file and insert into SchemeBoardoFAMC Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'SchemeBoardOfAMC.csv'
		file_to_check = file_path + fb_name + '\\' + 'SchemeBoardOfAMC.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeBoardOfAMC.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeBoardOfAMC"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' ;'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeBoardOfAMC", flush = True)
				exportfilename = "SchemeBoardOfAMCExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeBoardOfAMC" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemeboardofamc()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemeboardoftrustees(self, conn, cur,fbname ):
		""" Insert the Scheme Board Of Trustees data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeBoardOfTrustees.csv file.
			delete the data based on key column CompanyCode, and export executed data into 
			'SchemeBoardOfTrusteesExport.csv' file and insert into SchemeBoardOfTrustees Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'SchemeBoardOfTrustees.csv'
		file_to_check = file_path + fb_name + '\\' + 'SchemeBoardOfTrustees.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeBoardOfTrustees.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeBoardOfTrustees"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' ;'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeBoardOfTrustees", flush = True)
				exportfilename = "SchemeBoardOfTrusteesExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeBoardOfTrustees" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemeboardoftrustees()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemebonusdetails(self, conn, cur,fbname ):
		""" Insert the Scheme Bonus Details data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeBonusDetails.csv file.
			delete the data based on key column SchemeCode, SchemePlanCode and BonusDate,
			export executed data into 'SchemeBonusDetailsExport.csv' file and 
			insert into SchemeBonusDetails Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'SchemeBonusDetails.csv'
		file_to_check = file_path + fb_name + '\\' + 'SchemeBonusDetails.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeBonusDetails.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode', 'BonusDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeBonusDetails" WHERE "SchemeCode" =\'' +str(row['SchemeCode']) + '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode']) + '\' AND "BonusDate"=\'' + str(row['BonusDate']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeBonusDetails", flush = True)
				exportfilename = "SchemeBonusDetailsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeBonusDetails" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemebonusdetails()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemecategorydetails(self, conn, cur,fbname ):
		""" Insert the Scheme Category Details data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeCategoryDetails.csv file.
			delete the data based on key column SchemeClassCode, and export executed data into 
			'SchemeCategoryDetailssExport.csv' file and insert into SchemeCategoryDetails Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'SchemeCategoryDetails.csv'
		file_to_check = file_path + fb_name + '\\' + 'SchemeCategoryDetails.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeCategoryDetails.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SchemeClassCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeCategoryDetails" WHERE "SchemeClassCode" =\'' +str(row['SchemeClassCode']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeCategoryDetails", flush = True)
				exportfilename = "SchemeCategoryDetailsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeCategoryDetails" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemecategorydetails()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemecorporatedividenddetails(self, conn, cur,fbname ): 
		""" Insert the Scheme Corporate Dividend Details data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeCorporateDividendDetails.csv file.
			delete the data based on key column SchemeCode, SchemePlanCode and DividendDate,
			export executed data into 'SchemeCorporateDividendDetailsExport.csv' file 
			and insert into SchemeCorporateDividendDetails Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemeCorporateDividendDetails.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemeCorporateDividendDetails.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeCorporateDividendDetails.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')
				table["DividendPerUnit"] = table["DividendPerUnit"].replace(' ', np.nan)
				#print(table.loc[table["DividendPerUnit"]==" "], flush = True)

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode', 'DividendDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeCorporateDividendDetails" WHERE "SchemeCode" =\'' +str(row['SchemeCode']) + '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode']) + '\' AND "DividendDate"=\'' + str(row['DividendDate']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeCorporateDividendDetails", flush = True)
				exportfilename = "SchemeCorporateDividendDetailsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()


				copy_sql = """
					COPY "public"."SchemeCorporateDividendDetails" FROM stdin WITH CSV HEADER
					DELIMITER as ',' 
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()	
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemecorporatedividenddetails()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)
			
			
	def insert_schemedividenddetails(self, conn, cur, fbname):
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = fb_csv_path + 'SchemeDividendDetails.csv'
		file_to_check = file_path + fb_name + '\\' + 'SchemeDividendDetails.csv'

		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'SchemeDividendDetails.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				# Convert numeric columns to Python types
				numeric_columns = ['DividendPerUnit']
				for col in numeric_columns:
					table[col] = pd.to_numeric(table[col], errors='coerce', downcast='float')

				# Replace spaces with NaN in 'DividendPerUnit' after conversion
				table["DividendPerUnit"] = table["DividendPerUnit"].replace(' ', np.nan)

				# Create a temporary table
				temp_table_name = 'temp_scheme_dividend_details_delete'
				cur.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
				cur.execute(f'CREATE TEMP TABLE {temp_table_name} AS SELECT * FROM public."SchemeDividendDetails" LIMIT 0')

				# Insert data into the temporary table
				insert_sql = f'INSERT INTO {temp_table_name} VALUES %s'
				insert_values = [tuple(row) for row in table.itertuples(index=False, name=None)]
				extras.execute_values(cur, insert_sql, insert_values, page_size=1000)

				# Perform the delete operation
				delete_sql = f'DELETE FROM public."SchemeDividendDetails" WHERE ("SchemeCode", "SchemePlanCode", "DividendDate") IN (SELECT "SchemeCode", "SchemePlanCode", "DividendDate" FROM {temp_table_name});'
				cur.execute(delete_sql)
				conn.commit()

				print("Inserting Data into SchemeDividendDetails", flush=True)
				export_buffer = StringIO()
				table.to_csv(export_buffer, header=True, index=False, lineterminator='\r', encoding='utf-8')

				# Use COPY for bulk insert
				export_buffer.seek(0)
				copy_sql = """
					COPY "public"."SchemeDividendDetails" FROM stdin WITH CSV HEADER
					DELIMITER as ','
				"""
				cur.copy_expert(sql=copy_sql, file=export_buffer)
				conn.commit()

			except Exception as e:
				print("Error in insert_schemedividenddetails():", e, flush=True)
				conn.rollback()
			finally:
				# Drop the temporary table
				cur.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
		else:
			print("File not found: " + fb_csv_file, flush=True)



	# def insert_schemedividenddetails(self, conn, cur,fbname ): 
	# 	""" Insert the Scheme Dividend Details data into database

	# 	Operation:
	# 		Set the path for csv file, and fetch the data from SchemeDividendDetails.csv file.
	# 		delete the data based on key column SchemeCode, SchemePlanCode and DividendDate,
	# 		and export executed data into 'SchemeDividendDetailsExport.csv' file 
	# 		and insert into SchemeDividendDetails Table.	
	# 	"""
	# 	fb_name = fbname
	# 	fb_csv_path = os.path.join(file_path, fb_name +'\\') 
	# 	fb_csv_file = fb_csv_path + 'SchemeDividendDetails.csv'
		
	# 	file_to_check = file_path + fb_name + '\\' + 'SchemeDividendDetails.csv'
				
	# 	if(fbname == 'intermediate_insert'):
			
	# 		file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeDividendDetails.csv'
	# 		fb_csv_file = file_to_check

	# 	if os.path.isfile(file_to_check):
	# 		try:
	# 			table = pd.read_csv(fb_csv_file, engine='python')
	# 			table["DividendPerUnit"] = table["DividendPerUnit"].replace(' ', np.nan)
				
	# 			#Update Logic - Deletes based on key columns
	# 			table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode', 'DividendDate'], as_index=False).count()       
	# 			'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
	# 						"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
	# 			self.logfile.flush()'''
	# 			for index,row in table_to_delete.iterrows():
	# 				# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
	# 				# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
	# 				# self.logfile.flush()        
	# 				sql = 'DELETE FROM public."SchemeDividendDetails" WHERE "SchemeCode" =\'' +str(row['SchemeCode']) + '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode']) + '\' AND "DividendDate"=\'' + str(row['DividendDate']) + '\' ;'      
	# 				##print(sql, flush = True)        
	# 				cur.execute(sql)        
	# 				conn.commit()
				
	# 			print("Inserting Data into SchemeDividendDetails", flush = True)
	# 			exportfilename = "SchemeDividendDetailsExport.csv"
	# 			exportfile = open(exportfilename,"w",encoding='utf-8')
	# 			table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
	# 			exportfile.close()
				
	# 			copy_sql = """
	# 				COPY "public"."SchemeDividendDetails" FROM stdin WITH CSV HEADER
	# 				DELIMITER as ','
	# 				"""
	# 			with open(exportfilename, 'r') as f:
	# 				cur.copy_expert(sql=copy_sql, file=f)
	# 				conn.commit()
	# 				f.close()
	# 			os.remove(exportfilename)
	# 		except:
	# 			print("Error in insert_schemedividenddetails()", flush = True)
	# 			conn.rollback()

	# 	else:
	# 		print("File not found: " + fb_csv_file, flush = True)

	def insert_schemefundmanagerprofile(self, conn, cur,fbname ): 
		""" Insert the Scheme Fund Manager Profile data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeFundManagerProfile.csv file.
			delete the data based on key column SchemeCode and SchemePlanCode,
			export executed data into  'SchemeFundManagerProfileExport.csv' file 
			and insert into SchemeFundManagerProfile Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemeFundManagerProfile.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemeFundManagerProfile.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeFundManagerProfile.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				try:
					table = pd.read_csv(fb_csv_file, engine='python',encoding='utf-8')

				except UnicodeDecodeError:

					table = pd.read_csv(fb_csv_file, engine='python',encoding='latin1')

				
				table["DeleteFlag"] = table["DeleteFlag"].fillna(-1)
				table["DeleteFlag"] = table["DeleteFlag"].astype(bool)

				table["DeleteFlag"] = table["DeleteFlag"].replace("0", False)
				table["DeleteFlag"] = table["DeleteFlag"].replace("0.0", False)

				table["DeleteFlag"] = table["DeleteFlag"].replace("1", True)
				table["DeleteFlag"] = table["DeleteFlag"].replace("1.0", True)

				table["DeleteFlag"] = table["DeleteFlag"].replace(' ', np.nan)
				table["DeleteFlag"] = table["DeleteFlag"].replace(-1, np.nan)
				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeFundManagerProfile" WHERE "SchemeCode" =\'' +str(row['SchemeCode']) + '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeFundManagerProfile", flush = True)
				exportfilename = "SchemeFundManagerProfileExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeFundManagerProfile" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_schemefundmanagerprofile()",e, flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemelocations(self, conn, cur,fbname ): 
		""" Insert the Scheme Locations data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeLocations.csv file.
			delete the data based on key column EntityCode, and export executed data into 
			'SchemeLocationsExport.csv' file and insert into SchemeLocations Table.
		""" 
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemeLocations.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemeLocations.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeLocations.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				try:
					table = pd.read_csv(fb_csv_file, engine='python',encoding='utf-8')

				except UnicodeDecodeError:
					table = pd.read_csv(fb_csv_file, engine='python', encoding='latin1')
		
				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['EntityCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeLocations"  WHERE "EntityCode" =\'' +str(row['EntityCode']) + '\'  ;'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeLocations", flush = True)
				exportfilename = "SchemeLocationsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeLocations" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_schemelocations()",e, flush = True)
				conn.rollback()
		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schememaster(self, conn, cur, fbname):
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = fb_csv_path + 'SchemeMaster.csv'

		file_to_check = os.path.join(file_path, fb_name, 'SchemeMaster.csv')

		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'SchemeMaster.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				# Try reading with 'utf-8' encoding
				table = pd.read_csv(fb_csv_file, engine='python', encoding='utf-8')
			except UnicodeDecodeError:
				# If 'utf-8' fails, try reading with 'latin1' encoding
				table = pd.read_csv(fb_csv_file, engine='python', encoding='latin1')

			table["SchemeCategoryCode"] = table["SchemeCategoryCode"].replace(" ", None)
			table["SchemeCategoryCode"] = table["SchemeCategoryCode"].replace(' ', np.nan)

			try:
				# Update Logic - Deletes based on key columns
				print("Executing delete logic", flush=True)
				table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode'], as_index=False).count()

				for index, row in table_to_delete.iterrows():
					sql = 'DELETE FROM public."SchemeMaster" WHERE "SchemeCode" =\'' + str(row['SchemeCode']) + '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode']) + '\';'
					cur.execute(sql)
					conn.commit()

				print("Inserting Data into SchemeMaster", flush=True)
				exportfilename = "SchemeMasterExport.csv"
				exportfile = open(exportfilename, "w", encoding='utf-8', errors='ignore')

				table['ExitLoad_WithSlab_Desc'] = table['ExitLoad_WithSlab_Desc'].replace("\n", " ")
				table.to_csv(exportfile, header=True, index=False, lineterminator='\n', encoding='utf-8', errors='ignore')
				exportfile.close()

				print("Added data to csv", flush=True)
				tabletwo = pd.read_csv(exportfilename, encoding='latin1')

				copy_sql = """
					COPY "public"."SchemeMaster" FROM stdin WITH CSV HEADER
					DELIMITER as ','
				"""
				with open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
				print("Added data to csv", flush=True)
			except Exception as e:
				print(str(e), flush=True)
				print("Error in insert_schememaster", e, flush=True)
				conn.rollback()
			finally:
				try:
					# Clean up: Close and remove the temporary CSV file
					exportfile.close()
					os.remove(exportfilename)
				except Exception as e:
					print("Error while removing the temporary file:", e, flush=True)
		else:
			print("File not found:", fb_csv_file, flush=True)


	def insert_schemenavcurrentprices(self, conn, cur,fbname):
		""" Insert the Scheme NAV Current Prices data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeNAVCurrentPrices.csv file.
			delete the data based on key column SecurityCode, and export executed data into 
			'SchemeNAVCurrentPricesExport.csv' file and insert into SchemeNAVCurrentPrices Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemeNAVCurrentPrices.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemeNAVCurrentPrices.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeNAVCurrentPrices.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			
			try:
				table = pd.read_csv(fb_csv_file, engine='python')
				
				print("Inserting Data into SchemeNAVCurrentPrices", flush = True)
				exportfilename = "SchemeNAVCurrentPrices.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeNAVCurrentPrices" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemenavcurrentprices() ", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemenavdetails(self, conn, cur,fbname ): 
		""" Insert the Scheme NAV Details data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeNAVDetails.csv file.
			delete the data based on key column SecurityCode and NAVDate, 
			export executed data into 'SchemeNAVDetailsExport.csv' file 
			and insert into SchemeNAVDetails Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemeNAVDetails.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemeNAVDetails.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeNAVDetails.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			
			try:
				table = pd.read_csv(fb_csv_file, engine='python')
				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SecurityCode', 'NAVDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''

				# Copy dataframe to a table
				table_to_delete.head(0).to_sql('temp_scheme_nav_details', self.engine, if_exists='replace',index=False) #drops old table and creates new empty table
				output = io.StringIO()
				table_to_delete.to_csv(output, sep='\t', header=False, index=False)
				output.seek(0)
				contents = output.getvalue()
				self.alchemy_cur.copy_from(output, 'temp_scheme_nav_details', null="") # null values become ''
				self.alchemy_conn.commit()

				# Delete values in public."SchemeNAVDetails" using that table
				sql = 'DELETE FROM public."SchemeNAVDetails" USING public."temp_scheme_nav_details" \
						WHERE "SchemeNAVDetails"."SecurityCode" = "temp_scheme_nav_details"."SecurityCode"::varchar \
						AND "SchemeNAVDetails"."NAVDate" = "temp_scheme_nav_details"."NAVDate"::date'
						
				cur.execute(sql)
				conn.commit()

				# Old Delete Logic
				# for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 			"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\n")
					# self.logfile.flush()
				# 	print("Index: ", index)
				# 	sql = 'DELETE FROM public."SchemeNAVDetails" WHERE "SecurityCode" =\'' +str(row['SecurityCode']) + '\' AND "NAVDate"=\'' + str(row['NAVDate']) + '\';'
				# 	print(sql, flush = True)
				# 	cur.execute(sql)
				# 	conn.commit()
				

				print("Inserting Data into SchemeNAVDetails", flush = True)
				exportfilename = "SchemeNAVDetailsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeNAVDetails" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_schemenavdetails() ", e,  flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemenavmaster(self, conn, cur,fbname ): 
		""" Insert the Scheme NAV Master data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeNAVMaster.csv file.
			delete the data based on key column SecurityCode, and export executed data into 
			'SchemeNAVMasterExport.csv' file and insert into SchemeNAVMaster Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemeNAVMaster.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemeNAVMaster.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeNAVMaster.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				table["IssueDate"] = table["IssueDate"].replace(' ', np.nan)
				table["ExpiryDate"] = table["ExpiryDate"].replace(' ', np.nan)
				table["BenchMarkIndex"] = table["BenchMarkIndex"].replace(' ', np.nan)
				#print(table.loc[table["IssueDate"]==' '], flush = True)

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SecurityCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeNAVMaster" WHERE "SecurityCode" =\'' +str(row['SecurityCode']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeNAVMaster", flush = True)
				exportfilename = "SchemeNAVMasterExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeNAVMaster" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemenavmaster()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemeportfolioheader(self, conn, cur,fbname ): 
		""" Insert the Scheme Portfolio Header data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemePortfolioHeader.csv file.
			delete the data based on key column SchemeCode, SchemePlanCode and HoldingDate,
			export executed data into 'SchemePortfolioHeaderExport.csv' file 
			and insert into SchemePortfolioHeader Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemePortfolioHeader.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemePortfolioHeader.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemePortfolioHeader.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode', 'HoldingDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemePortfolioHeader" WHERE "SchemeCode" =\'' +str(row['SchemeCode']) + '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode']) + '\' AND "HoldingDate"=\'' + str(row['HoldingDate']) +'\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemePortfolioHeader", flush = True)
				exportfilename = "SchemePortfolioHeaderExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemePortfolioHeader" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemeportfolioheader()", flush = True)
				conn.rollback()
		
		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemeregistrars(self, conn, cur,fbname ):
		""" Insert the Scheme Registrars data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeRegistrars.csv file.
			delete the data based on key column CompanyCode, and export executed data into 
			'SchemeRegistrarsExport.csv' file and insert into SchemeRegistrars Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemeRegistrars.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemeRegistrars.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeRegistrars.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeRegistrars"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeRegistrars", flush = True)
				exportfilename = "SchemeRegistrarsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeRegistrars" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemeregistrars()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemesplitdetails(self, conn, cur,fbname ): 
		""" Insert the Scheme Split Details data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeSplitDetails.csv file.
			delete the data based on key column SchemeCode, SchemePlanCode and SplitsDate,
			export executed data into 'SchemeSplitDetailsExport.csv' file 
			and insert into SchemeSplitDetails Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'SchemeSplitsDetails.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'SchemeSplitsDetails.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeSplitsDetails.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')
				
				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode', 'SplitsDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeSplitsDetails" WHERE "SchemeCode" =\'' +str(row['SchemeCode']) + '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode']) + '\' AND "SplitsDate"=\'' + str(row['SplitsDate']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()

				print("Inserting Data into SchemeSplitsDetails", flush = True)
				exportfilename = "SchemeSplitsDetailsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeSplitsDetails" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemesplitdetails()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_schemetaxbenefits(self, conn, cur,fbname ):
		""" Insert the Scheme Tax Benefits data into database

		Operation:
			Set the path for csv file, and fetch the data from SchemeTaxBenefits.csv file.
			delete the data based on key column SchemeCode and SchemePlanCode, 
			export executed data into 'SchemeTaxBenefitsExport.csv' file 
			and insert into SchemeTaxBenefits Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		
		fb_csv_file = fb_csv_path + 'SchemeTaxBenefit.csv'
		file_to_check = file_path + fb_name + '\\' + 'SchemeTaxBenefit.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemeTaxBenefit.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				#print(table.dtypes, flush = True)

				#table['SchemePlanCode'] = table['SchemePlanCode'].astype(int)

				#print(table.dtypes, flush = True)

				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."SchemeTaxBenefit" WHERE "SchemeCode" =\'' +str(row['SchemeCode']) + '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode']) +  '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into SchemeTaxBenefit", flush = True)
				exportfilename = "SchemeTaxBenefitExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."SchemeTaxBenefit" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_schemetaxbenefits()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)
			
			
	def insert_schemewiseportfolio(self, conn, cur, fbname):
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name + '\\')
		fb_csv_file = os.path.join(fb_csv_path, 'SchemewisePortfolio.csv')

		file_to_check = os.path.join(file_path, fb_name, 'SchemewisePortfolio.csv')

		if fbname == 'intermediate_insert':
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData' + '\\' + 'SchemewisePortfolio.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python', encoding='latin1')

				# Fix for a newline present in the column InvestedCompanyName
				table["InvestedCompanyName"] = table["InvestedCompanyName"].replace("\n", " ", regex=True)

				table["InvestedCompanyCode"] = table["InvestedCompanyCode"].replace(" ", None)
				table["InvestedCompanyCode"] = table["InvestedCompanyCode"].replace(' ', pd.NA)
				table["IndustryCode"] = table["IndustryCode"].replace(' ', pd.NA)
				table["IndustryCode"] = table["IndustryCode"].replace(" ", None)
				table["IndustryCode"] = table['IndustryCode'].fillna(0).astype(int)

				# Update Logic - Deletes based on key columns
				print("Executing delete logic", flush=True)

				table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode', 'HoldingDate'], as_index=False).count()

				# Copy dataframe to a table
				table_to_delete.to_sql('temp_schemewiseportfolio', self.engine, if_exists='replace', index=False)

				# Delete values in public."SchemewisePortfolio" using that table
				sql = """
						DELETE FROM public."SchemewisePortfolio"
						USING public."temp_schemewiseportfolio"
						WHERE "SchemewisePortfolio"."SchemeCode" = CAST("temp_schemewiseportfolio"."SchemeCode" AS double precision)
						AND "SchemewisePortfolio"."SchemePlanCode" = "temp_schemewiseportfolio"."SchemePlanCode"::integer
						AND "SchemewisePortfolio"."HoldingDate" = "temp_schemewiseportfolio"."HoldingDate"::date
					"""
				cur.execute(sql)
				conn.commit()

				print("Inserting Data into SchemewisePortfolio", flush=True)
				exportfilename = os.path.join(fb_csv_path, "SchemewisePortfolioExport.csv")
				table.to_csv(exportfilename, header=True, index=False, lineterminator='\r')

				copy_sql = """
						COPY "public"."SchemewisePortfolio" FROM stdin WITH CSV HEADER
						DELIMITER as ','
					"""
				with open(exportfilename, "r", encoding='utf-8') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()

				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_schemewiseportfolio():", e, flush=True)
				conn.rollback()

		else:
			print("File not found:", fb_csv_file, flush=True)



	# def insert_schemewiseportfolio(self, conn, cur,fbname ):
	# 	""" Insert the Scheme wise Portfolio data into database

	# 	Operation:
	# 		Set the path for csv file, and fetch the data from SchemewisePortfolio.csv file.
	# 		and delete the data based on key column SchemeCode, SchemePlanCode and HoldingDate,  
	# 		export executed data into 'SchemewisePortfolioExport.csv' file 
	# 		and insert into SchemewisePortfolio Table.
	# 	"""
	# 	fb_name = fbname
	# 	fb_csv_path = os.path.join(file_path, fb_name +'\\') 
	# 	fb_csv_file = fb_csv_path + 'SchemewisePortfolio.csv'
		
	# 	file_to_check = file_path + fb_name + '\\' + 'SchemewisePortfolio.csv'

	# 	if(fbname == 'intermediate_insert'):
			
	# 		file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'SchemewisePortfolio.csv'
	# 		fb_csv_file = file_to_check

	# 	if os.path.isfile(file_to_check):
	# 		try:
	# 			table = pd.read_csv(fb_csv_file, engine='python')

	# 			# Fix for a newline present in the column InvestedCompanyName
	# 			for rownum, val in table.loc[:, "InvestedCompanyName"].iteritems():
	# 				# print(rownum, val)
	# 				newval = str(val).replace("\n", " ")
	# 				if val != newval:
	# 					table.loc[rownum, "InvestedCompanyName"]  = newval

	# 			table["InvestedCompanyCode"] = table["InvestedCompanyCode"].replace(" ", None)
	# 			table["InvestedCompanyCode"] = table["InvestedCompanyCode"].replace(' ', np.nan)
	# 			table["IndustryCode"] = table["IndustryCode"].replace(' ', np.nan)
	# 			table["IndustryCode"] = table["IndustryCode"].replace(" ", None)

	# 			table["IndustryCode"] = table['IndustryCode'].fillna(0).astype(int)

	# 			'''
	# 			table.loc[table['IndustryCode'] == "", 'IndustryCode'] = ""
	# 			table.loc[table['InvestedCompanyCode'] == "", 'InvestedCompanyCode'] = ""
	# 			table['IndustryCode'] = table['IndustryCode'].str.strip()
	# 			table['InvestedCompanyCode'] = table['InvestedCompanyCode'].str.strip()
	# 			'''
				
	# 			#Update Logic - Deletes based on key columns 
	# 			print("Executing delete logic", flush = True)
	# 			table_to_delete = table.groupby(['SchemeCode', 'SchemePlanCode', 'HoldingDate'], as_index=False).count()
	# 			'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
	# 						"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
	# 			self.logfile.flush()'''
				
	# 			# Copy dataframe to a table
	# 			table_to_delete.head(0).to_sql('temp_schemewiseportfolio', self.engine, if_exists='replace',index=False) #drops old table and creates new empty table
	# 			output = io.StringIO()
	# 			table_to_delete.to_csv(output, sep='\t', header=False, index=False)
	# 			output.seek(0)
	# 			contents = output.getvalue()
	# 			self.alchemy_cur.copy_from(output, 'temp_schemewiseportfolio', null="") # null values become ''
	# 			self.alchemy_conn.commit()

	# 			# Delete values in public."SchemewisePortfolio" using that table
	# 			sql = 'DELETE FROM public."SchemewisePortfolio" USING public."temp_schemewiseportfolio" \
	# 					WHERE "SchemewisePortfolio"."SchemeCode" = "temp_schemewiseportfolio"."SchemeCode"::double precision \
	# 					AND "SchemewisePortfolio"."SchemePlanCode" = "temp_schemewiseportfolio"."SchemePlanCode"::integer \
	# 					AND "SchemewisePortfolio"."HoldingDate" = "temp_schemewiseportfolio"."HoldingDate"::date'
	# 			cur.execute(sql)
	# 			conn.commit()


	# 			# Old Delete logic
	# 			# print("length of table: ", table_to_delete.shape[0], flush = True)
	# 			# for index,row in table_to_delete.iterrows():
	# 			# 	self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
	# 			# 			"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\n")
	# 			# 	self.logfile.flush()
	# 			# 	sql = 'DELETE FROM public."SchemewisePortfolio"  WHERE "SchemeCode"=\'' +str(row['SchemeCode'])+ '\' AND "SchemePlanCode"=\'' + str(row['SchemePlanCode'])+'\' AND "HoldingDate"=\'' + str(row['HoldingDate'])+'\';'
	# 			# 	##print(sql, flush = True)
	# 			# 	cur.execute(sql)
	# 			# 	conn.commit()
				
	# 			print("Inserting Data into SchemewisePortfolio", flush = True)
	# 			exportfilename = "SchemewisePortfolioExport.csv"
	# 			exportfile = open(exportfilename,"w",encoding='utf-8')
	# 			table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
	# 			exportfile.close()
				
	# 			copy_sql = """
	# 				COPY "public"."SchemewisePortfolio" FROM stdin WITH CSV HEADER
	# 				DELIMITER as ','
	# 				"""
	# 			with codecs.open(exportfilename, "r",encoding='utf-8', errors='ignore') as f:
	# 				cur.copy_expert(sql=copy_sql, file=f)
	# 				conn.commit()
	# 				f.close()
	# 			os.remove(exportfilename)
	# 		except Exception as e:
	# 			print("Error in insert_schemewiseportfolio()", e, flush = True)
	# 			conn.rollback()

	# 	else:
	# 		print("File not found: " + fb_csv_file, flush = True)

	def insert_shareholding(self, conn, cur,fbname ):
		""" Insert the ShareHolding data into database

		Operation:
			Set the path for csv file, and fetch the data from ShareHolding.csv file.
			delete the data based on key column CompanyCode and SHPDate,  
			Export data into 'ShareHoldingExport.csv' file and insert into ShareHolding Table.
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'ShareHolding.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'ShareHolding.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'ShareHolding.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				table['SHPDate'] = pd.to_datetime(table['SHPDate'], errors = 'ignore')
				table['SHPDate'] = table['SHPDate'].apply(lambda dt: dt.strftime('%Y-%m-%d')if not pd.isnull(dt) else '')

				table['ModifiedDate'] = pd.to_datetime(table['ModifiedDate'], errors = 'ignore')
				table['ModifiedDate'] = table['ModifiedDate'].apply(lambda dt: dt.strftime('%Y-%m-%d')if not pd.isnull(dt) else '')
				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'SHPDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = 'DELETE FROM public."ShareHolding" WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' AND  "SHPDate"=\'' + str(row['SHPDate']) + '\';'
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into ShareHolding", flush = True)
				exportfilename = "ShareHoldingExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."ShareHolding" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_shareholding()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_splits(self, conn, cur,fbname ):
		""" Insert the Splits data into database

		Operation:
			Set the path for csv file, and fetch the data from Splits.csv file.
			delete the data based on key column CompanyCode and DateOfAnnouncement,
			Export data into 'SplitsExport.csv' file and insert into Splits Table. 
		"""
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'Splits.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'Splits.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Splits.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

				'''
				table.loc[table['RecordDate'] == '""', 'RecordDate'] = ""
				table.loc[table['BookClosureStartDate'] == '""', 'BookClosureStartDate'] = ""
				table.loc[table['BookClosureEndDate'] == '""', 'BookClosureEndDate'] = ""
				table.loc[table['XSDate'] == '""', 'XSDate'] = ""
				'''

				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'DateOfAnnouncement'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = sql = 'DELETE FROM public."Splits" WHERE "CompanyCode"=\'' +str(row['CompanyCode'])+ '\' AND "DateOfAnnouncement"=\'' + str(row['DateOfAnnouncement'])+'\';'   
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into Splits", flush = True)
				exportfilename = "SplitsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Splits" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with open(exportfilename, 'r') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_splits()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_subsidiaries(self, conn, cur,fbname ): 
		""" Insert the Subsidiaries data into database

		Operation:
			Set the path for csv file, and fetch the data from Subsidiaries.csv file.
			delete the data based on key column CompanyCode, and Export data into 
			'SubsidiariesExport.csv' file and insert into Subsidiaries Table.
		""" 
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'Subsidiaries.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'Subsidiaries.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Subsidiaries.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

					
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					sql = sql = 'DELETE FROM public."Subsidiaries"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' ;'   
					##print(sql, flush = True)
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into Subsidiaries", flush = True)
				exportfilename = "SubsidiariesExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Subsidiaries" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_subsidiaries()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_pledgeshares(self, conn, cur, fbname):
		""" Insert the Pledge Shares data into database

		Operation:
			Set the path for csv file, and fetch the data from PledgeShares.csv file.
			delete the data based on key column CompanyCode, and Export data into 
			'PledgeSharesExport.csv' file and insert into PledgeShares Table.
		"""
		
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'PledgeShares.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'PledgeShares.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'PledgeShares.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				table = pd.read_csv(fb_csv_file, engine='python')

					
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'SHPDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					
					sql = 'DELETE FROM public."PledgeShares"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' \
								AND "SHPDate"=\'' + str(row['SHPDate'])+'\';'     
					
					##print(sql, flush = True)
					
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into PledgeShares", flush = True)
				
				exportfilename = "PledgeSharesExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."PledgeShares" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except:
				print("Error in insert_pledgeshares()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_bulkdeals(self, conn, cur, fbname):
		""" Insert the Bulk Deals data into database

		Operation:
			Set the path for csv file, and fetch the data from BulkDeals.csv file.
			delete the data based on key column CompanyCode, Exchange and DealDate, 
			Export data into 'BulkDealsExport.csv' file and insert into BulkDeals Table.	
		"""
		
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'BulkDeals.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'BulkDeals.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'BulkDeals.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):
			try:
				try:
					table = pd.read_csv(fb_csv_file, encoding='utf-8',engine='python')
				except UnicodeDecodeError:
					table = pd.read_csv(fb_csv_file, engine='python', encoding='latin1')

					
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'Exchange', 'DealDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()
					
					sql =  'DELETE FROM public."BulkDeals"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' \
								AND "Exchange"=\'' + str(row['Exchange'])+'\' AND "DealDate"=\'' + str(row['DealDate'])+'\';'     
					
					##print(sql, flush = True)
					
					cur.execute(sql)
					conn.commit()
				

				print("Inserting Data into BulkDeals", flush = True)
				
				exportfilename = "BulkDealsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."BulkDeals" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)
			except Exception as e:
				print("Error in insert_bulkdeals()", e,flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def insert_blockdeals(self, conn, cur, fbname):
		""" Insert the Blockdeals data into database

		Operation:
			Set the path for csv file, and fetch the data from Blockdeals.csv file.
			delete the data based on key column CompanyCode, Exchange and DealDate, 
			Export data into 'BlockdealsExport.csv' file and insert into Blockdeals Table.
		"""
		
		fb_name = fbname
		fb_csv_path = os.path.join(file_path, fb_name +'\\') 
		fb_csv_file = fb_csv_path + 'Blockdeals.csv'
		
		file_to_check = file_path + fb_name + '\\' + 'Blockdeals.csv'
				
		if(fbname == 'intermediate_insert'):
			
			file_to_check = 'C:\\Users\\dsram\\BravisaLocalDeploy\\BravisaFiles\\MissingData'+ '\\' + 'Blockdeals.csv'
			fb_csv_file = file_to_check

		if os.path.isfile(file_to_check):

			try:
				table = pd.read_csv(fb_csv_file, engine='python')
				
				#Update Logic - Deletes based on key columns 
				print("Executing delete logic", flush = True)
				table_to_delete = table.groupby(['CompanyCode', 'Exchange', 'DealDate'], as_index=False).count()
				'''self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
							"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
				self.logfile.flush()'''
				for index,row in table_to_delete.iterrows():
					# self.logfile.write("\tIn function: " + str(fb_csv_file.split("\\")[-1].split(".")[0]) + 
					# 		"\t\tTotal Loops:" + str(int(table_to_delete.shape[0])) + "\t\tFor file: " + str(fbname) + "\n")
					# self.logfile.flush()

					sql =  'DELETE FROM public."Blockdeals"  WHERE "CompanyCode" =\'' +str(row['CompanyCode']) + '\' \
								AND "Exchange"=\'' + str(row['Exchange'])+'\' AND "DealDate"=\'' + str(row['DealDate'])+'\';'     
					
					cur.execute(sql)
					conn.commit()
				
				print("Inserting Data into Blockdeals", flush = True)
				
				exportfilename = "BlockdealsExport.csv"
				exportfile = open(exportfilename,"w",encoding='utf-8')
				table.to_csv(exportfile, header=True, index=False, lineterminator='\r')
				exportfile.close()
				
				copy_sql = """
					COPY "public"."Blockdeals" FROM stdin WITH CSV HEADER
					DELIMITER as ','
					"""
				with codecs.open(exportfilename, "r", encoding='utf-8', errors='ignore') as f:
					cur.copy_expert(sql=copy_sql, file=f)
					conn.commit()
					f.close()
				os.remove(exportfilename)

			except:				
				print("Error in insert_blockdeals()", flush = True)
				conn.rollback()

		else:
			print("File not found: " + fb_csv_file, flush = True)

	def fb_insert_01(self, fbname,conn, cur):
		""" Inserting the FB 01 data into database,
			Checking the path and exporing csv file 
			executig the data and inserting the data 
			into there respective tables data
		"""
		
		print("\n\t\t Starting FB01 Insert ", flush = True)
		Check_Helper().check_path(file_path)
		date = fbname[2:-2]
		print(int(date[0:2]), int(date[2:4]), int(date[4:]))
		date = datetime.date(int(date[4:]), int(date[2:4]), int(date[0:2]))
		#if(date.strftime("%A")=="Monday"):
			#self.logfile = open("fb_status_files\\month-" + str(date.month) + "_week-" + str(int(math.ceil(date.day/7))) + ".txt", "a")
			#self.logfile.write("\t\tLOG FILE - INNER LOOP STATUS\n\n")
			
			#######
		self.insert_annual_meetings(conn,cur,fbname)
		self.insert_auditors(conn, cur, fbname)
		self.insert_backgroundinfo(conn, cur, fbname)
		self.insert_bankers(conn, cur, fbname)
		self.insert_boardmeetings(conn, cur, fbname)
		self.insert_bonus(conn, cur, fbname)
		self.insert_capitalstructure(conn, cur, fbname)
		self.insert_companymaster(conn, cur, fbname)
		self.insert_companynamechange(conn, cur, fbname)
		self.insert_consolidatedhalfyearlyresults(conn, cur, fbname)
		self.insert_consolidatedninemonthsresults(conn, cur, fbname)
		self.insert_consolidatedquarterlyresults(conn, cur, fbname)
		self.insert_currentdata(conn, cur, fbname)
		self.insert_dividend(conn, cur, fbname)
		self.insert_financebanking(conn, cur, fbname)
		self.insert_financeconsolidatedbanking(conn, cur, fbname)
		self.insert_financeconsolidatednonbanking(conn, cur, fbname)
		self.insert_financenonbanking(conn, cur, fbname)
		self.insert_fundmaster(conn, cur, fbname)
		self.insert_halfyearlyresults(conn, cur, fbname)
		self.insert_individualholding(conn, cur, fbname)
		self.insert_industrymaster(conn, cur, fbname)
		self.insert_keyexecutives(conn, cur, fbname)
		self.insert_locations(conn, cur, fbname)
		self.insert_mfinvestments(conn, cur, fbname)
		self.insert_managementteam(conn, cur, fbname)
		self.insert_ninemonthsresults(conn, cur, fbname)
		self.insert_products(conn, cur, fbname)
		self.insert_quarterlyresults(conn, cur, fbname)
		self.insert_ratiosbanking(conn, cur, fbname)
		self.insert_ratiosconsolidatednonbanking(conn, cur, fbname)
		self.insert_ratiosconsolidatedbanking(conn, cur, fbname)
		self.insert_ratiosnonbanking(conn, cur, fbname)
		self.insert_rawmaterials(conn, cur, fbname)
		self.insert_registrars(conn, cur, fbname)
		self.insert_rights(conn, cur, fbname)
		self.insert_schemeboardofamc(conn, cur, fbname)
		self.insert_schemeboardoftrustees(conn, cur, fbname)
		self.insert_schemebonusdetails(conn, cur, fbname)
		self.insert_schemecategorydetails(conn, cur, fbname)
		self.insert_schemecorporatedividenddetails(conn, cur, fbname)
		self.insert_schemedividenddetails(conn, cur, fbname)
		self.insert_schemefundmanagerprofile(conn, cur, fbname)
		self.insert_schemelocations(conn, cur, fbname)
		self.insert_schememaster(conn, cur, fbname)
		self.insert_schemenavcurrentprices(conn, cur, fbname)
		self.insert_schemenavdetails(conn, cur, fbname)
		self.insert_schemenavmaster(conn, cur, fbname)
		self.insert_schemeportfolioheader(conn, cur, fbname)
		self.insert_schemeregistrars(conn, cur, fbname)
		self.insert_schemesplitdetails(conn, cur, fbname)
		self.insert_schemetaxbenefits(conn, cur, fbname)
		self.insert_schemewiseportfolio(conn, cur, fbname)
		self.insert_shareholding(conn, cur, fbname)
		self.insert_splits(conn, cur, fbname)
		self.insert_subsidiaries(conn, cur, fbname)
		self.insert_pledgeshares(conn, cur, fbname)
		self.insert_bulkdeals(conn, cur, fbname)
		self.insert_blockdeals(conn, cur, fbname)

		print("Inserted FB01 files to Database", flush = True)

		## For remove the folder with CSV files along of date. 
		# fb_csv_path = os.path.join(file_path, fbname)
		# shutil.rmtree(fb_csv_path)


	def fb_insert_02(self, fbname,conn,cur):
		"""Inserting the FB 02 data into database,
		   Checking the path and exporing csv file 
		   executig the data and inserting the data 
		   into there respective tables data
		"""
		
		print("\n\t\t Starting FB02 Insert ", flush = True)
		
		Check_Helper().check_path(file_path)
		self.insert_annual_meetings(conn,cur,fbname)
		self.insert_auditors(conn, cur, fbname)
		self.insert_backgroundinfo(conn, cur, fbname)
		self.insert_bankers(conn, cur, fbname)
		self.insert_boardmeetings(conn, cur, fbname)
		self.insert_bonus(conn, cur, fbname)
		self.insert_capitalstructure(conn, cur, fbname)
		self.insert_companymaster(conn, cur, fbname)
		self.insert_companynamechange(conn, cur, fbname)
		self.insert_consolidatedhalfyearlyresults(conn, cur, fbname)
		self.insert_consolidatedninemonthsresults(conn, cur, fbname)
		self.insert_consolidatedquarterlyresults(conn, cur, fbname)
		self.insert_currentdata(conn, cur, fbname)
		self.insert_dividend(conn, cur, fbname)
		self.insert_financebanking(conn, cur, fbname)
		self.insert_financeconsolidatedbanking(conn, cur, fbname)
		self.insert_financeconsolidatednonbanking(conn, cur, fbname)
		self.insert_financenonbanking(conn, cur, fbname)
		self.insert_fundmaster(conn, cur, fbname)
		self.insert_halfyearlyresults(conn, cur, fbname)
		self.insert_individualholding(conn, cur, fbname)
		self.insert_industrymaster(conn, cur, fbname)
		self.insert_keyexecutives(conn, cur, fbname)
		self.insert_locations(conn, cur, fbname)
		self.insert_mfinvestments(conn, cur, fbname)
		self.insert_managementteam(conn, cur, fbname)
		self.insert_ninemonthsresults(conn, cur, fbname)
		self.insert_products(conn, cur, fbname)
		self.insert_quarterlyresults(conn, cur, fbname)
		self.insert_ratiosbanking(conn, cur, fbname)
		self.insert_ratiosconsolidatednonbanking(conn, cur, fbname)
		self.insert_ratiosconsolidatedbanking(conn, cur, fbname)
		self.insert_ratiosnonbanking(conn, cur, fbname)
		self.insert_rawmaterials(conn, cur, fbname)
		self.insert_registrars(conn, cur, fbname)
		self.insert_rights(conn, cur, fbname)
		self.insert_schemeboardofamc(conn, cur, fbname)
		self.insert_schemeboardoftrustees(conn, cur, fbname)
		self.insert_schemebonusdetails(conn, cur, fbname)
		self.insert_schemecategorydetails(conn, cur, fbname)
		self.insert_schemecorporatedividenddetails(conn, cur, fbname)
		self.insert_schemedividenddetails(conn, cur, fbname)
		self.insert_schemefundmanagerprofile(conn, cur, fbname)
		self.insert_schemelocations(conn, cur, fbname)
		self.insert_schememaster(conn, cur, fbname)
		self.insert_schemenavcurrentprices(conn, cur, fbname)
		self.insert_schemenavdetails(conn, cur, fbname)
		self.insert_schemenavmaster(conn, cur, fbname)
		self.insert_schemeportfolioheader(conn, cur, fbname)
		self.insert_schemeregistrars(conn, cur, fbname)
		self.insert_schemesplitdetails(conn, cur, fbname)
		self.insert_schemetaxbenefits(conn, cur, fbname)
		self.insert_schemewiseportfolio(conn, cur, fbname)
		self.insert_shareholding(conn, cur, fbname)
		self.insert_splits(conn, cur, fbname)
		self.insert_subsidiaries(conn, cur, fbname)
		self.insert_pledgeshares(conn, cur, fbname)
		self.insert_bulkdeals(conn, cur, fbname)
		self.insert_blockdeals(conn, cur, fbname)

		print("Inserted FB02 files to Database", flush = True)

		## For remove the folder with CSV files along of date. 
		# fb_csv_path = os.path.join(file_path, fbname)
		# shutil.rmtree(fb_csv_path)

		
	def fb_insert_03(self, fbname,conn,cur):
		"""Inserting the FB 03 data into database,
		   Checking the path and exporing csv file 
		   executig the data and inserting the data 
		   into there respective tables data
		"""
		
		print("\n\t\t Starting FB03 Insert ", flush = True)
		# print("Date: ", fbname[2:-2])
		# date = fbname[2:-2]
		# print(int(date[0:2]), int(date[2:4]), int(date[4:]))
		# date = datetime.date(int(date[4:]), int(date[2:4]), int(date[0:2]))
		# print("Date: ", date, (date.strftime("%A")=="Monday"), "Week number: ", "month-" + str(date.month) + "_week-" + str(int(math.ceil(date.day/7))))
		# raise Exception("Intentional Break")
		Check_Helper().check_path(file_path)
		self.insert_annual_meetings(conn,cur,fbname)
		self.insert_auditors(conn, cur, fbname)
		self.insert_backgroundinfo(conn, cur, fbname)
		self.insert_bankers(conn, cur, fbname)
		self.insert_boardmeetings(conn, cur, fbname)
		self.insert_bonus(conn, cur, fbname)
		self.insert_capitalstructure(conn, cur, fbname)
		self.insert_companymaster(conn, cur, fbname)
		self.insert_companynamechange(conn, cur, fbname)
		self.insert_consolidatedhalfyearlyresults(conn, cur, fbname)
		self.insert_consolidatedninemonthsresults(conn, cur, fbname)
		self.insert_consolidatedquarterlyresults(conn, cur, fbname)
		self.insert_currentdata(conn, cur, fbname)
		self.insert_dividend(conn, cur, fbname)
		self.insert_financebanking(conn, cur, fbname)
		self.insert_financeconsolidatedbanking(conn, cur, fbname)
		self.insert_financeconsolidatednonbanking(conn, cur, fbname)
		self.insert_financenonbanking(conn, cur, fbname)
		self.insert_fundmaster(conn, cur, fbname)
		self.insert_halfyearlyresults(conn, cur, fbname)
		self.insert_individualholding(conn, cur, fbname)
		self.insert_industrymaster(conn, cur, fbname)
		self.insert_keyexecutives(conn, cur, fbname)
		self.insert_locations(conn, cur, fbname)
		self.insert_mfinvestments(conn, cur, fbname)
		self.insert_managementteam(conn, cur, fbname)
		self.insert_ninemonthsresults(conn, cur, fbname)
		self.insert_products(conn, cur, fbname)
		self.insert_quarterlyresults(conn, cur, fbname)
		self.insert_ratiosbanking(conn, cur, fbname)
		self.insert_ratiosconsolidatednonbanking(conn, cur, fbname)
		self.insert_ratiosconsolidatedbanking(conn, cur, fbname)
		self.insert_ratiosnonbanking(conn, cur, fbname)
		self.insert_rawmaterials(conn, cur, fbname)
		self.insert_registrars(conn, cur, fbname)
		self.insert_rights(conn, cur, fbname)
		self.insert_schemeboardofamc(conn, cur, fbname)
		self.insert_schemeboardoftrustees(conn, cur, fbname)
		self.insert_schemebonusdetails(conn, cur, fbname)
		self.insert_schemecategorydetails(conn, cur, fbname)
		self.insert_schemecorporatedividenddetails(conn, cur, fbname)
		self.insert_schemedividenddetails(conn, cur, fbname)
		self.insert_schemefundmanagerprofile(conn, cur, fbname)
		self.insert_schemelocations(conn, cur, fbname)
		self.insert_schememaster(conn, cur, fbname)
		self.insert_schemenavcurrentprices(conn, cur, fbname)
		self.insert_schemenavdetails(conn, cur, fbname)
		self.insert_schemenavmaster(conn, cur, fbname)
		self.insert_schemeportfolioheader(conn, cur, fbname)
		self.insert_schemeregistrars(conn, cur, fbname)
		self.insert_schemesplitdetails(conn, cur, fbname)
		self.insert_schemetaxbenefits(conn, cur, fbname)
		self.insert_schemewiseportfolio(conn, cur, fbname)
		self.insert_shareholding(conn, cur, fbname)
		self.insert_splits(conn, cur, fbname)
		self.insert_subsidiaries(conn, cur, fbname)
		self.insert_pledgeshares(conn, cur, fbname)
		self.insert_bulkdeals(conn, cur, fbname)
		self.insert_blockdeals(conn, cur, fbname)

		print("Inserted FB03 files to Database", flush = True)

		## For remove the folder with CSV files along of date. 
		# fb_csv_path = os.path.join(file_path, fbname)
		# shutil.rmtree(fb_csv_path)
		
	def intermediate_insert(self,conn,cur):

		print("From ", file_path, flush=True)

		file_path = file_path

		self.insert_annual_meetings(conn,cur,'intermediate_insert')
		self.insert_auditors(conn, cur, 'intermediate_insert')
		self.insert_backgroundinfo(conn, cur, 'intermediate_insert')
		self.insert_bankers(conn, cur, 'intermediate_insert')
		self.insert_boardmeetings(conn, cur, 'intermediate_insert')
		self.insert_bonus(conn, cur, 'intermediate_insert')
		self.insert_capitalstructure(conn, cur, 'intermediate_insert')
		self.insert_companymaster(conn, cur, 'intermediate_insert')
		self.insert_companynamechange(conn, cur, 'intermediate_insert')
		self.insert_consolidatedhalfyearlyresults(conn, cur, 'intermediate_insert')
		self.insert_consolidatedninemonthsresults(conn, cur, 'intermediate_insert')
		self.insert_consolidatedquarterlyresults(conn, cur, 'intermediate_insert')
		self.insert_currentdata(conn, cur, 'intermediate_insert')
		self.insert_dividend(conn, cur, 'intermediate_insert')
		self.insert_financebanking(conn, cur, 'intermediate_insert')
		self.insert_financeconsolidatedbanking(conn, cur, 'intermediate_insert')
		self.insert_financeconsolidatednonbanking(conn, cur, 'intermediate_insert')
		self.insert_financenonbanking(conn, cur, 'intermediate_insert')
		self.insert_fundmaster(conn, cur, 'intermediate_insert')
		self.insert_halfyearlyresults(conn, cur, 'intermediate_insert')
		self.insert_individualholding(conn, cur, 'intermediate_insert')
		self.insert_industrymaster(conn, cur, 'intermediate_insert')
		self.insert_keyexecutives(conn, cur, 'intermediate_insert')
		self.insert_locations(conn, cur, 'intermediate_insert')
		self.insert_mfinvestments(conn, cur, 'intermediate_insert')
		self.insert_managementteam(conn, cur, 'intermediate_insert')
		self.insert_ninemonthsresults(conn, cur, 'intermediate_insert')
		self.insert_products(conn, cur, 'intermediate_insert')
		self.insert_quarterlyresults(conn, cur, 'intermediate_insert')
		self.insert_ratiosbanking(conn, cur, 'intermediate_insert')
		self.insert_ratiosconsolidatednonbanking(conn, cur, 'intermediate_insert')
		self.insert_ratiosconsolidatedbanking(conn, cur, 'intermediate_insert')
		self.insert_ratiosnonbanking(conn, cur, 'intermediate_insert')
		self.insert_rawmaterials(conn, cur, 'intermediate_insert')
		self.insert_registrars(conn, cur, 'intermediate_insert')
		self.insert_rights(conn, cur, 'intermediate_insert')
		self.insert_schemeboardofamc(conn, cur, 'intermediate_insert')
		self.insert_schemeboardoftrustees(conn, cur, 'intermediate_insert')
		self.insert_schemebonusdetails(conn, cur, 'intermediate_insert')
		self.insert_schemecategorydetails(conn, cur, 'intermediate_insert')
		self.insert_schemecorporatedividenddetails(conn, cur, 'intermediate_insert')
		self.insert_schemedividenddetails(conn, cur, 'intermediate_insert')
		self.insert_schemefundmanagerprofile(conn, cur, 'intermediate_insert')
		self.insert_schemelocations(conn, cur, 'intermediate_insert')
		self.insert_schememaster(conn, cur, 'intermediate_insert')
		self.insert_schemenavcurrentprices(conn, cur, 'intermediate_insert')
		self.insert_schemenavdetails(conn, cur, 'intermediate_insert')
		self.insert_schemenavmaster(conn, cur, 'intermediate_insert')
		self.insert_schemeportfolioheader(conn, cur, 'intermediate_insert')
		self.insert_schemeregistrars(conn, cur, 'intermediate_insert')
		self.insert_schemesplitdetails(conn, cur, 'intermediate_insert')
		self.insert_schemetaxbenefits(conn, cur, 'intermediate_insert')
		self.insert_schemewiseportfolio(conn, cur, 'intermediate_insert')
		self.insert_shareholding(conn, cur, 'intermediate_insert')
		self.insert_splits(conn, cur, 'intermediate_insert')
		self.insert_subsidiaries(conn, cur, 'intermediate_insert')
		self.insert_pledgeshares(conn, cur, 'intermediate_insert')
		self.insert_bulkdeals(conn, cur, 'intermediate_insert')
		self.insert_blockdeals(conn, cur, 'intermediate_insert')