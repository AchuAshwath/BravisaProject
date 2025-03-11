from pandas.io import sql as sqlio
import pandas as pd
import numpy as np


def calc_industry_type_divisor( master_list, industry_type_divisor, industry_type, conn, date):
    """ Calculating the sector divisor.

    Args:
        master_list = Data of industryList for max date,
        industry_type_divisor = Data of group sector divisor.

    Operation:
        Fetch the data from IndexHistory, divisor_table, and IndustryList table
        merge the executed data and calculate the value for ff_close_current,
        divisor_current, os_current.

    Return:
         Data of sector divisor.
    """
    index_name_column = f"{industry_type}IndexName"
    divisor_table = f"{industry_type}Divisor"
        
    indexhistory_sql  = f'''SELECT DISTINCT ON("TICKER") * FROM public."IndexHistory" \
                            WHERE "DATE" < '{date}' \
                            ORDER by "TICKER","DATE" desc ;'''
    indexhistory = sqlio.read_sql_query(indexhistory_sql, con = conn)



    divisor_backdate_sql = f'''SELECT DISTINCT ON("IndexName") * FROM public."{divisor_table}" \
                        WHERE "Date" < '{date}' \
                        ORDER BY "IndexName", "Date" DESC; '''
    divisor_backdate = sqlio.read_sql_query(divisor_backdate_sql, con = conn)

    sql = f'''SELECT "{index_name_column}", "Sector", \
                SUM("FF_Open") AS ff_open_sum, \
                SUM("MCap_Open") AS mcap_open_sum, \
                SUM("OS") AS os_sum  \
                from public."IndustryList" \
                WHERE "GenDate" = '{date}' AND "{index_name_column}" is not null\
                GROUP BY "{index_name_column}", "Sector" ;'''
    industry_type_divisor_list = sqlio.read_sql_query(sql, con = conn)


    Sector = master_list[index_name_column]

    sector_prevclose = indexhistory.loc[indexhistory["TICKER"].isin(Sector)]
    sector_prevclose = sector_prevclose.rename(columns = {"TICKER":"IndexName"})

    prev_industry_list_sql = f"""
                              SELECT DISTINCT ON("CompanyCode") * 
                              FROM public."IndustryList"
                              WHERE "GenDate" = (
                                  SELECT MAX("GenDate") 
                                  FROM public."IndustryList" 
                                  WHERE "GenDate" < '{date}'
                              )
                              ORDER BY "CompanyCode", "GenDate" DESC;
                              """
    prev_industry_list = sqlio.read_sql_query(prev_industry_list_sql, con = conn)

    current_industry_list_sql = f'''SELECT DISTINCT ON("CompanyCode") * FROM public."IndustryList" 
                        WHERE "GenDate" = '{date}'  \
                        ORDER by "CompanyCode", "GenDate" desc ;'''
    current_industry_list = sqlio.read_sql_query(current_industry_list_sql, con = conn)

    merge_industry_type_divisor = pd.merge(industry_type_divisor, sector_prevclose, on="IndexName", how="left")


    for index, row in merge_industry_type_divisor.iterrows():

      os_prev_list = divisor_backdate.loc[divisor_backdate['IndexName']==row['IndexName']]['OS']
      os_prev = os_prev_list.item() if len(os_prev_list.index) == 1 else np.nan

      os_current_list = industry_type_divisor_list.loc[industry_type_divisor_list[index_name_column] == row['IndexName']]['os_sum']
      os_current = os_current_list.item() if len(os_current_list.index) == 1 else np.nan

      divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['Divisor']
      divisor_back = divisor_back_list.item() if len(divisor_back_list.index) == 1 else np.nan
      
      mcap_divisor_back_list = divisor_backdate.loc[divisor_backdate['IndexName'] == row['IndexName']]['MCap_Divisor'] #
      mcap_divisor_back = mcap_divisor_back_list.item() if len(mcap_divisor_back_list.index) == 1 else np.nan   

      prev_close_list = indexhistory.loc[indexhistory['TICKER'] == row['IndexName']]['OPEN']
      prev_close = prev_close_list.item() if len(prev_close_list.index) == 1 else np.nan

      # print(row['IndexName'])

      if(os_prev == os_current):
        # print("OS previous is equal to OS current")

        ff_close_prev_list = merge_industry_type_divisor.loc[merge_industry_type_divisor['IndexName'] == row['IndexName']]['SumFF_Open']
        ff_close_prev = ff_close_prev_list.item() if len(ff_close_prev_list.index) == 1 else np.nan

        mcap_open_prev_list = merge_industry_type_divisor.loc[merge_industry_type_divisor['IndexName'] == row['IndexName']]['SumMCap_Open']
        mcap_open_prev = mcap_open_prev_list.item() if len(mcap_open_prev_list.index) == 1 else np.nan

        divisor_current = divisor_back
        mcap_divisor_current = mcap_divisor_back 

        # print("FF_Open_Sum: ", ff_close_prev)
        # print("Mcap_Divisor: ",mcap_divisor_current)
        # print("Divisor: ", divisor_current)

        merge_industry_type_divisor.loc[index, 'IndexValue'] = ff_close_prev
        merge_industry_type_divisor.loc[index, 'SumMCap_Open'] = mcap_open_prev
        merge_industry_type_divisor.loc[index, 'MCapDivisor'] = mcap_divisor_current
        merge_industry_type_divisor.loc[index, 'Divisor'] = divisor_current
        merge_industry_type_divisor.loc[index, 'OS'] = os_prev

      else:
        # print("OS previous is not equal to OS current")
        # print("OS previous: ", os_prev)
        # print("OS current: ", os_current)

        ff_close_current_list = industry_type_divisor_list.loc[industry_type_divisor_list[index_name_column] == row['IndexName']]['ff_open_sum']
        ff_close_current = ff_close_current_list.item() if len(ff_close_current_list.index) == 1 else np.nan

        mcap_open_current_list = industry_type_divisor_list.loc[industry_type_divisor_list[index_name_column] == row['IndexName']]['mcap_open_sum']
        mcap_open_current = mcap_open_current_list.item() if len(mcap_open_current_list.index) == 1 else np.nan

        current_company_count = merge_industry_type_divisor.loc[merge_industry_type_divisor['IndexName'] == row['IndexName']]['Count'].values[0]
        # print("current_company_count", current_company_count)
        prev_company_count = len(prev_industry_list.loc[prev_industry_list[index_name_column] == row['IndexName']])

        if prev_close is not np.nan:

          prev_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['CLOSE']

          prev_mcap_close_index = indexhistory[indexhistory['TICKER'] == row['IndexName']]['MCap_CLOSE']   #

          current_companies = current_industry_list.loc[current_industry_list[index_name_column] == row['IndexName']]
          # print("current_company_count: ", current_company_count)
          
          # prev_companies = prev_industry_list.loc[prev_industry_list['IndustryIndexName'] == row['IndexName']]
          # print("prev_company_count: ", prev_company_count)
          current_companies_prev_list = prev_industry_list.loc[prev_industry_list['CompanyCode'].isin(current_companies['CompanyCode'])]
          # print("Companies from current list on previous list :", len(current_companies_prev_list))
            
          new_companies = current_companies[~current_companies['CompanyCode'].isin(current_companies_prev_list['CompanyCode'])]
          # print("new_companies: ", len(new_companies))
          
          
          if(len(current_companies_prev_list)==len(current_companies)):
            print("only OS has changed, no new companies added")
            
          elif(len(current_companies_prev_list)<len(current_companies)):
            print("new companies added")
            
          elif(len(current_companies_prev_list)>len(current_companies)):
            print("companies removed")
          
          merged_companies = pd.merge(current_companies_prev_list, current_companies, on='CompanyCode', suffixes=('_prev', '_current'))

          prev_companies_with_same_OS = merged_companies[merged_companies['OS_prev'] == merged_companies['OS_current']]
          # print("prev_companies_with_same_OS: ", len(prev_companies_with_same_OS))
          # print(row['IndexName'])
          # print(prev_companies_with_same_OS.columns)
          prev_index_name = f'{industry_type}IndexName_prev'
          prev_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS[prev_index_name] == row['IndexName']]['FF_Close_prev'].sum() 
          prev_mcap_close_sum = prev_companies_with_same_OS[prev_companies_with_same_OS[prev_index_name] == row['IndexName']]['MCap_Close_prev'].sum()
          # prev_close_sum = prev_industry_list.loc[prev_industry_list['SubIndustryIndexName'] == row['IndexName']]['FF_Close'].sum()
          # print("prev_close_sum: ", prev_close_sum) 
          # print("prev_mcap_close_sum: ", prev_mcap_close_sum)   
          
          prev_companies_with_different_OS = merged_companies[merged_companies['OS_prev'] != merged_companies['OS_current']]
          # print("prev_companies_with_different_OS: ", len(prev_companies_with_different_OS))
          
          prev_companies_with_different_OS['prev_close_sum_for_diff_OS'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['FreeFloat_current'] * prev_companies_with_different_OS['PrevClose_current']
          changed_prev_close_sum = prev_companies_with_different_OS['prev_close_sum_for_diff_OS'].sum()
          prev_companies_with_different_OS['prev_MCap_Close'] = prev_companies_with_different_OS['OS_current'] * prev_companies_with_different_OS['PrevClose_current']
          changed_prev_mcap_close_sum = prev_companies_with_different_OS['prev_MCap_Close'].sum()
           
          # print("changed_prev_close_sum: ", changed_prev_close_sum)
          # print("changed_prev_mcap_close_sum: ", changed_prev_mcap_close_sum)
          
          new_companies['prev_FF_Close'] = new_companies['OS'] * new_companies['FreeFloat'] * new_companies['PrevClose']
          addition_to_prev_close_sum = new_companies['prev_FF_Close'].sum()
          # print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)

          new_companies['prev_MCap_Close'] = new_companies['OS'] * new_companies['PrevClose']
          addition_to_prev_mcap_close_sum = new_companies['prev_MCap_Close'].sum()
          # print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)


          if not prev_close_index.empty:
              prev_close_index = prev_close_index.iloc[0]
              prev_mcap_close_index = prev_mcap_close_index.iloc[0]
          else:
              prev_close_index = np.nan
              prev_mcap_close_index = np.nan

          divisor = (prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum) / prev_close_index
          mcap_divisor = (prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum) / prev_mcap_close_index
          # print()
          # print("prev_close_sum: ", prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum)
          # print("addition_to_prev_close_sum: ", addition_to_prev_close_sum)
          # print("prev_close_index: ", prev_close_index)
          # print("divisor: ", divisor)
          # print("prev_mcap_close_sum: ", prev_mcap_close_sum)
          # print("addition_to_prev_mcap_close_sum: ", addition_to_prev_mcap_close_sum)
          # print("prev_mcap_close_index: ", prev_mcap_close_index)
          # print("mcap_divisor : ", mcap_divisor)

          merge_industry_type_divisor.loc[index, 'IndexValue'] = prev_close_sum + addition_to_prev_close_sum + changed_prev_close_sum
          merge_industry_type_divisor.loc[index, 'MCap_Open_sum'] = prev_mcap_close_sum + addition_to_prev_mcap_close_sum + changed_prev_mcap_close_sum
          merge_industry_type_divisor.loc[index, 'MCapDivisor'] = mcap_divisor
          merge_industry_type_divisor.loc[index, 'Divisor'] = divisor
          merge_industry_type_divisor.loc[index, 'OS'] = os_current

        else:
            divisor_current = ff_close_current / 1000
            MCapdivisor_current = mcap_open_current / 1000
            # print("Divisor: ", divisor_current)
            # print("FF_Open_sum: ", ff_close_current)
            # print("Mcap_Open_sum: ", mcap_open_current)
            # print("MCapDivisor: ", MCapdivisor_current)
            merge_industry_type_divisor.loc[index, 'MCap_Open_sum'] = mcap_open_current
            merge_industry_type_divisor.loc[index, 'Divisor'] = divisor_current
            merge_industry_type_divisor.loc[index, 'IndexValue'] = ff_close_current
            merge_industry_type_divisor.loc[index, 'MCapDivisor'] = MCapdivisor_current
            merge_industry_type_divisor.loc[index, 'OS'] = os_current




    return merge_industry_type_divisor
