def delete_EPS(conn, cur, start_date, end_date):
	try:

		eps_sql = """DELETE FROM "Reports"."EPS"
					WHERE "EPSDate" <= %s AND "EPSDate" >= %s""" 
		cur.execute(eps_sql, (end_date, start_date))		
		conn.commit()
		print("Deleted EPS data")	
		STANDALONE_EPS_sql = """ DELETE FROM "Reports"."STANDALONE_EPS"
					WHERE "EPSDate" <= %s AND "EPSDate" >= %s"""
		cur.execute(STANDALONE_EPS_sql, (end_date, start_date))
		conn.commit()	
		print("Deleted STANDALONE_EPS data")
		Consolidated_EPS_sql = """ DELETE FROM "Reports"."Consolidated_EPS"
					WHERE "EPSDate" <= %s AND "EPSDate" >= %s"""
		cur.execute(Consolidated_EPS_sql, (end_date, start_date))
		conn.commit()
		print("Deleted Consolidated_EPS data")
		QaurterlyEPS_sql = """ DELETE FROM public."QuarterlyEPS"
					WHERE "YearEnding" >= %s AND "YearEnding" <= %s"""
		cur.execute(QaurterlyEPS_sql, (start_date, end_date))
		conn.commit()
		print("Deleted QuarterlyEPS data")
		ConsolidatedQuarterlyEPS_sql = """ DELETE FROM public."ConsolidatedQuarterlyEPS"
					WHERE "YearEnding" >= %s AND "YearEnding" <= %s"""
		cur.execute(ConsolidatedQuarterlyEPS_sql, (start_date,end_date))
		conn.commit()
		print("Deleted ConsolidatedQuarterlyEPS data")
		TTMsql = """ DELETE FROM public."TTM"
					WHERE "YearEnding" >= %s AND "YearEnding" <= %s"""
		cur.execute(TTMsql, (start_date, end_date))
		conn.commit()	
		print("Deleted TTM data")
		ConsolidatedTTMsql = """ DELETE FROM public."ConsolidatedTTM"
					WHERE "YearEnding" >= %s AND "YearEnding" <= %s"""
		cur.execute(ConsolidatedTTMsql, (start_date, end_date))
		conn.commit()
		print("Deleted ConsolidatedTTM data")	

	except Exception as e:
		print(f"Error deleting EPS data: {e}")
		import traceback; traceback.print_exc()	
  
def delete_smr(conn,cur, start_date, end_date):
    try:
        SMRsql = """ DELETE FROM "Reports"."SMR"
                    WHERE "SMRDate" >= %s AND "SMRDate" <= %s"""
        cur.execute(SMRsql, (start_date, end_date))
        conn.commit()
        print("Deleted SMR data")
        RatiosMergeListsql = """ DELETE FROM public."RatiosMergeList"
                    WHERE "GenDate" >= %s AND "GenDate" <= %s"""
        cur.execute(RatiosMergeListsql, (start_date, end_date))
        conn.commit()
    except Exception as e:
        print(f"Error deleting SMR data: {e}")
        import traceback; traceback.print_exc()
        
def delPRS(start_date, end_date, conn, cur):
    try:
        sql_PRS = """DELETE FROM "Reports"."PRS" 
                    WHERE "Date" >= %s AND "Date" <= %s"""
        cur.execute(sql_PRS, (start_date, end_date))
        conn.commit()
        print("Deleted the records from PRS table for the date range: ", start_date, " to ", end_date)
        
        sql_PE = """DELETE FROM "PE" 
                    WHERE "GenDate" >= %s AND "GenDate" <= %s"""
        cur.execute(sql_PE, (start_date, end_date))
        conn.commit()
        print("Deleted the records from PE table for the date range: ", start_date, " to ", end_date)
        
        sql_NHNL = """DELETE FROM "Reports"."NewHighNewLow" 
                    WHERE "Date" >= %s AND "Date" <= %s"""
        cur.execute(sql_NHNL, (start_date, end_date))
        conn.commit()
        print("Deleted the records from NHNL table for the date range: ", start_date, " to ", end_date)
    except Exception as e: 
        print(f"Error deleting PRS data: {e}")
        import traceback; traceback.print_exc()
        
def delIRS(curr_date, conn, cur):
    try:
        sql_IndustryList = """delete from "IndustryList" 
                            where "GenDate" = %s"""
        # execute the query with curr_date
        cur.execute(sql_IndustryList, (curr_date,))
        conn.commit()
        print("Deleted the records from IndustryList table for the date: ", curr_date)
        sql_SectorDivisor = """delete from "SectorDivisor"
                                where "Date" = %s"""
        cur.execute(sql_SectorDivisor, (curr_date,))
        conn.commit()
        print("Deleted the records from SectorDivisor table for the date: ", curr_date)
        sql_SubSecotDivisor = """delete from "SubSectorDivisor" 
                                where "Date" = %s"""
        cur.execute(sql_SubSecotDivisor, (curr_date,))
        conn.commit()
        print("Deleted the records from SubSectorDivisor table for the date: ", curr_date)
        sql_IndustryDivisor = """delete from "IndustryDivisor" 
                                where "Date" = %s"""
        cur.execute(sql_IndustryDivisor, (curr_date,))
        conn.commit()
        print("Deleted the records from IndustryDivisor table for the date: ", curr_date)
        sql_SubIndustryDivisor = """delete from "SubIndustryDivisor" 
                                    where "Date" = %s"""
        cur.execute(sql_SubIndustryDivisor, (curr_date,))
        conn.commit()
        print("Deleted the records from SubIndustryDivisor table for the date: ", curr_date)
        sql_SectorIndexList = """delete from "SectorIndexList" 
                                where "GenDate" = %s"""
        cur.execute(sql_SectorIndexList, (curr_date,))
        conn.commit()
        print("Deleted the records from SectorIndexList table for the date: ", curr_date)
        sql_SubSectorIndexList = """delete from "SubSectorIndexList" 
                                    where "GenDate" = %s"""
        cur.execute(sql_SubSectorIndexList, (curr_date,))
        conn.commit()
        print("Deleted the records from SubSectorIndexList table for the date: ", curr_date)
        sql_IndustryIndexList = """delete from "IndustryIndexList" 
                                    where "GenDate" = %s"""
        cur.execute(sql_IndustryIndexList, (curr_date,))
        conn.commit()
        print("Deleted the records from IndustryIndexList table for the date: ", curr_date)
        sql_SubIndustryIndexList = """delete from "SubIndustryIndexList" 
                                        where "GenDate" = %s""" 
        cur.execute(sql_SubIndustryIndexList, (curr_date,))
        conn.commit()
        print("Deleted the records from SubIndustryIndexList table for the date: ", curr_date)
        sql_IRS = """delete from "Reports"."IRS" 
                    where "GenDate" = %s"""
        cur.execute(sql_IRS, (curr_date,))
        conn.commit()
        print("Deleted the records from IRS table for the date: ", curr_date)
        sql_IndexHistory = """delete from "IndexHistory"  
                            where "DATE" = %s """
        cur.execute(sql_IndexHistory, (curr_date,))
        conn.commit() 
        print("Deleted the records from IndexHistory table for the date: ", curr_date)
    except Exception as e:
        print(f"Error deleting IRS data: {e}")
        import traceback; traceback.print_exc()
            
    def delete_OHLC(conn, cur, curr_date, end_date):
        try:
            ohlc_sql = f"""DELETE FROM public."OHLC" WHERE "Date" >= '{curr_date}' AND "Date" <= '{end_date}'"""
            cur.execute(ohlc_sql)
            conn.commit()
            print("Deleted OHLC data")
        except Exception as e:
            print(f"Error deleting OHLC data: {e}")

        try:
            nse_sql = f"""DELETE FROM public."NSE" WHERE "TIMESTAMP" >= '{curr_date}' AND "TIMESTAMP" <= '{end_date}'"""
            cur.execute(nse_sql)
            conn.commit()
            print("Deleted NSE data")
        except Exception as e:
            print(f"Error deleting NSE data: {e}")

        try:
            bse_sql = f"""DELETE FROM public."BSE" WHERE "TRADING_DATE" >= '{curr_date}' AND "TRADING_DATE" <= '{end_date}'"""
            cur.execute(bse_sql)
            conn.commit()
            print("Deleted BSE data")
        except Exception as e:
            print(f"Error deleting BSE data: {e}")