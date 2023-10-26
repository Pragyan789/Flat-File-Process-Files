import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date

def backfill_fn(dq_backfills_data_path,sql_backfills_path):
    #Data source downloaded from DQ Dashboard:
    try:
        dq_backfills_df = pd.read_excel(dq_backfills_data_path)
    except:
        print("Enter correct file path for DQ Backfills")

    #Data source extracted from SQL database:
    try:
        sql_backfills_extract_df = pd.read_excel(sql_backfills_path)
    except:
        print("Enter correct file path for SQL Backfills Extract")
    dq_backfills_df = dq_backfills_df.drop(0)                 #dropping blank row
    #Resetting index
    dq_backfills_df = dq_backfills_df.reset_index()
    dq_backfills_df = dq_backfills_df.drop('index', axis=1)

    #Following columns need to be compared from both dataframes:
    # 'FIRST_NAME','LAST_NAME','ADDRESS','CITY_NM','ST','ORIG_ZIP','NDC_NBR','PRODUCT_INFO','QTY_DISPENSED','MOST_RECENT_SHIP_DATE','BRANCH_DEA_ID','ACCT_DEA_ID'
    # 'Account First Name', 'Account Last Name', 'Account Address 1', 'Account City', 'Account State', 'Account Orig Zip', 'NDC Number', 'Product Info', 'Quantity Dispensed', 'Most Recent Ship Date', '_BRANCH_DEA_ID', 'Account ID1 Value'

    dq_backfills_df["Validation"] = None

    for j in range(0,len(dq_backfills_df)):
        if dq_backfills_df['Account First Name'][j].lower() == sql_backfills_extract_df['FIRST_NAME'][j].lower() and dq_backfills_df['Account Last Name'][j].lower() == sql_backfills_extract_df['LAST_NAME'][j].lower() and str(dq_backfills_df['Account Address 1'][j]).lower() in str(sql_backfills_extract_df['ADDRESS'][j]).lower() and dq_backfills_df['Account City'][j].lower() == sql_backfills_extract_df['CITY_NM'][j].lower() and dq_backfills_df['Account State'][j].lower() == sql_backfills_extract_df['ST'][j].lower() and int(dq_backfills_df['Account Orig Zip'][j]) == int(sql_backfills_extract_df['ORIG_ZIP'][j]) and int(dq_backfills_df['NDC Number'][j]) == int(sql_backfills_extract_df['NDC_NBR'][j]) and str(sql_backfills_extract_df['PRODUCT_INFO'][j]).lower() in (dq_backfills_df['Product Info'][j]).lower() and int(dq_backfills_df['Quantity Dispensed'][j]) == int(sql_backfills_extract_df['QTY_DISPENSED'][j]) and dq_backfills_df['Most Recent Ship Date'][j].date() == sql_backfills_extract_df['MOST_RECENT_SHIP_DATE'][j].date() and str(dq_backfills_df['_BRANCH_DEA_ID'][j]).lower() == str(sql_backfills_extract_df['BRANCH_DEA_ID'][j]).lower() and int(dq_backfills_df['Account ID1 Value'][j]) == int(sql_backfills_extract_df['ACCT_DEA_ID'][j]):
            dq_backfills_df["Validation"][j] = "Duplicate"
        else:
            dq_backfills_df["Validation"][j] = "Pass"

    result_list = []
    #Calculating number of flags which are valid
    unique_flags = dq_backfills_df['Validation'].count() - dq_backfills_df[dq_backfills_df["Validation"] == "Duplicate"]['Validation'].count()
    
    #
    unique_ndcs = dq_backfills_df.query("Validation == 'Pass'")['NDC Number'].nunique()

    result_list.append(unique_flags)
    result_list.append(unique_ndcs)

    return result_list





# import pandas as pd
# import numpy as np

# from datetime import datetime
# from datetime import date

# #def backfill_fn(dq_backfills_data_path,sql_backfills_path):

# dq_backfills_data_path = r"C:\Users\pragyan.agrawal\Downloads\Pharmacare Backfills New Month.xlsx"
# sql_backfills_path = r"C:\Users\pragyan.agrawal\Downloads\Pharmacare Backfills SQL.xlsx"

# #Data source downloaded from DQ Dashboard:
# try:
#     dq_backfills_df = pd.read_excel(dq_backfills_data_path)
# except:
#     print("Enter correct file path for DQ Backfills")

# #Data source extracted from SQL database:
# try:
#     sql_backfills_extract_df = pd.read_excel(sql_backfills_path)
# except:
#     print("Enter correct file path for SQL Backfills Extract")


# dq_backfills_df = dq_backfills_df.drop(0)                 #dropping blank row
# #Resetting index
# dq_backfills_df = dq_backfills_df.reset_index()
# dq_backfills_df = dq_backfills_df.drop('index', axis=1)

# #Following columns need to be compared from both dataframes:
# # 'FIRST_NAME','LAST_NAME','ADDRESS','CITY_NM','ST','ORIG_ZIP','NDC_NBR','PRODUCT_INFO','QTY_DISPENSED','MOST_RECENT_SHIP_DATE','BRANCH_DEA_ID','ACCT_DEA_ID'
# # 'Account First Name', 'Account Last Name', 'Account Address 1', 'Account City', 'Account State', 'Account Orig Zip', 'NDC Number', 'Product Info', 'Quantity Dispensed', 'Most Recent Ship Date', '_BRANCH_DEA_ID', 'Account ID1 Value'

# #dq_backfills_df = dq_backfills_df.rename(columns={'Account First Name' : 'FIRST_NAME', 'Account Last Name' : 'LAST_NAME', 'Account Address 1' : 'ADDRESS', 'Account City' : 'CITY_NM', 'Account State' : 'ST', 'Account Orig Zip' : 'ORIG_ZIP', 'NDC Number' : 'NDC_NBR', 'Product Info' : 'PRODUCT_INFO', 'Quantity Dispensed' : 'QTY_DISPENSED', 'Most Recent Ship Date' : 'MOST_RECENT_SHIP_DATE', '_BRANCH_DEA_ID' : 'BRANCH_DEA_ID', 'Account ID1 Value' : 'ACCT_DEA_ID'})
# dq_backfills_df["Validation"] = None

# dq_backfills_df["Full_Name"] = dq_backfills_df['Account First Name']+ " " +dq_backfills_df['Account Last Name']
# sql_backfills_extract_df["Full_Name"] = sql_backfills_extract_df['FIRST_NAME']+ " " +sql_backfills_extract_df['LAST_NAME']

# #dq_backfills_df['Full_Name']+'_'+(dq_backfills_df.groupby('Full_Name').cumcount()).astype(str)
# sql_backfills_extract_df['Full_Name_Unique'] = sql_backfills_extract_df['Full_Name']+'_'+(sql_backfills_extract_df.groupby('Full_Name').cumcount()).astype(str)

# for iterable in range(0,len(dq_backfills_df)):
#     locs = []
#     if dq_backfills_df["Full_Name"].iloc[iterable] in list(sql_backfills_extract_df["Full_Name"]):
#         #TBC
#         idx = list(sql_backfills_extract_df[sql_backfills_extract_df["Full_Name"] == dq_backfills_df["Full_Name"].iloc[iterable]].index)[0]
#         print(idx,sql_backfills_extract_df["Full_Name"].iloc[idx])
        
#     if dq_backfills_df['Account First Name'][iterable].lower() == sql_backfills_extract_df['FIRST_NAME'][idx].lower() and dq_backfills_df['Account Last Name'][iterable].lower() == sql_backfills_extract_df['LAST_NAME'][idx].lower() and str(dq_backfills_df['Account Address 1'][iterable]).lower() in str(sql_backfills_extract_df['ADDRESS'][idx]).lower() and dq_backfills_df['Account City'][iterable].lower() == sql_backfills_extract_df['CITY_NM'][idx].lower() and dq_backfills_df['Account State'][iterable].lower() == sql_backfills_extract_df['ST'][idx].lower() and int(dq_backfills_df['Account Orig Zip'][iterable]) == int(sql_backfills_extract_df['ORIG_ZIP'][idx]) and int(dq_backfills_df['NDC Number'][iterable]) == int(sql_backfills_extract_df['NDC_NBR'][idx]) and str(sql_backfills_extract_df['PRODUCT_INFO'][idx].split(" ")[0]).lower() in (dq_backfills_df['Product Info'][iterable]).lower() and int(dq_backfills_df['Quantity Dispensed'][iterable]) == int(sql_backfills_extract_df['QTY_DISPENSED'][idx]) and dq_backfills_df['Most Recent Ship Date'][iterable].date() == sql_backfills_extract_df['MOST_RECENT_SHIP_DATE'][idx].date() and str(dq_backfills_df['_BRANCH_DEA_ID'][iterable]).lower() == str(sql_backfills_extract_df['BRANCH_DEA_ID'][idx]).lower(): #and dq_backfills_df['Account ID1 Value'][iterable] == sql_backfills_extract_df['ACCT_DEA_ID'][idx]:
#         dq_backfills_df["Validation"][iterable] = "Duplicate"
#     else:
#         dq_backfills_df["Validation"][iterable] = "Pass"

# # dq_backfills_df.to_csv(r'C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\out.csv')
# # sql_backfills_extract_df.to_csv(r'C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\out2.csv')

# # result_list = []
# # #Calculating number of flags which are valid
# # unique_flags = dq_backfills_df['Validation'].count() - dq_backfills_df[dq_backfills_df["Validation"] == "Duplicate"]['Validation'].count()
# # print(unique_flags)
# # #
# # unique_ndcs = dq_backfills_df.query("Validation == 'Pass'")['NDC Number'].nunique()
# # print(unique_ndcs)

# # result_list.append(unique_flags)
# # result_list.append(unique_ndcs)

# # return result_list