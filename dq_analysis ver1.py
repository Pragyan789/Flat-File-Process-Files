import pandas as pd
import numpy as np
from openpyxl import load_workbook

import warnings
warnings.filterwarnings('ignore')

from datetime import datetime
from datetime import date

import dq_branch_analysis
import dq_qty_min_max_analysis
import dq_zip_code_analysis
import dq_unknown_roche_ndc
import dq_uom_inconsistencies
#import dq_backfill_analysis

#IMP: Code inside ***/*** block will need to be modified if source file format is changed
#***

df = pd.read_excel(r"C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\Flat File Process Files\Input Paths.xlsx")
df = df.set_index('Variable Name')
df = df.fillna("")

main_file_path = list(df.loc['main_file_path'])[0]
new_month_dq_file_path = list(df.loc['new_month_dq_file_path'])[0]
supplier_names_file_path = list(df.loc['supplier_names_file_path'])[0]
tpc_file_path = list(df.loc['tpc_file_path'])[0]
branch_report_file_path = list(df.loc['branch_report_file_path'])[0]
current_month_branch_dq_file_path = list(df.loc['current_month_branch_dq_file_path'])[0]
previous_month_branch_dq_file_path = list(df.loc['previous_month_branch_dq_file_path'])[0]
txn_count_file_path = list(df.loc['txn_count_file_path'])[0]
dq_qty_min_file_path = list(df.loc['dq_qty_min_file_path'])[0]
dq_qty_max_file_path = list(df.loc['dq_qty_max_file_path'])[0]
dq_zip_code_file_path = list(df.loc['dq_zip_code_file_path'])[0]
dq_unknown_roche_ndc_file_path = list(df.loc['dq_unknown_roche_ndc_file_path'])[0]
dq_config_file_path = list(df.loc['dq_config_file_path'])[0]
dq_uom_inconsistencies_file_path = list(df.loc['dq_uom_inconsistencies_file_path'])[0]
raw_data_file_path = list(df.loc['raw_data_file_path'])[0]
ndc_factoring_values_file_path = list(df.loc['ndc_factoring_values_file_path'])[0]
output_path = list(df.loc['output_path'])[0]

#Source File Paths; to be modified according to user
# main_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD_SEP23.xlsx"
# new_month_dq_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD DQ.xlsx"
# supplier_names_file_path = r"C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\Flat File Process Files\Supplier Names TPC.xlsx"
# tpc_file_path = r"C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\Flat File Process Files\TPC File.xlsx"

# #Branch file paths
# branch_report_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD Branch.xlsx"
# current_month_branch_dq_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD current month branch.xlsx"
# previous_month_branch_dq_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD old month branch.xlsx"

# #Qty MIN/MAX file paths
# txn_count_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD Txn Count.xlsx"
# dq_qty_min_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD QTY_MIN DQ.xlsx"
# dq_qty_max_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD QTY_MAX DQ.xlsx"

# #Zip Code file path
# dq_zip_code_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD ZIP.xlsx"

# #Unknown Roche NDC file path
# dq_unknown_roche_ndc_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD Unknown NDC.xlsx"
# dq_config_file_path = r"C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\Flat File Process Files\DQ Config File.xlsx"

# #UOM Inconsistencies file path
# dq_uom_inconsistencies_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD UOM Inconsistenies DQ.xlsx"
# raw_data_file_path = r"C:\Users\pragyan.agrawal\Downloads\CIBD Raw data.xlsx"
# ndc_factoring_values_file_path = r"C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\Flat File Process Files\NDC Factoring Values.xlsx"


### *not needed* Backfill paths
# dq_backfills_data_path = r"C:\Users\pragyan.agrawal\Downloads\Pharmacare Backfills New Month.xlsx"
# sql_backfills_path = r"C:\Users\pragyan.agrawal\Downloads\Pharmacare Backfills SQL.xlsx"


#Below code to extract File_ID from TPC File based on Supplier name
#Two source files are necessary, one for mapping to names of suppliers exactly as present in TPC File, and one TPC File itself
#===
supplier_name = list(df.loc['supplier_name'])[0]

supplier_names_df = None
tpc_df = None
df = None
data_month_dq_df = None
df_dq = None

try:
    supplier_names_df = pd.read_excel(supplier_names_file_path)
except:
    print("Please enter correct path for Supplier names file")

supplier_names_df = supplier_names_df.set_index('File Name')
supplier_name = supplier_names_df[supplier_names_df.columns[0]][supplier_name.lower()]    #extracting supplier name as present in TPC File
supplier_category = supplier_names_df['Category'][supplier_name.lower()]

try:
    tpc_df = pd.read_excel(tpc_file_path)
except:
    print("Please enter correct path for TPC File")

try:
    file_id = int(tpc_df[(tpc_df["Sender Name"] == supplier_name) & (tpc_df["Received File Status"] == "CL Completed")]["Received File ID"])
except:
    file_id = ''
#===

try:
    df = pd.read_excel(main_file_path)
except:
    print('Path for Main file Incorrect/Missing')

#Extracting DQ Table only from first sheet
row_num = [0]
try:
    #Hardcoded value
    row_num = df[df[df.columns[0]] == 'Validation Rule Description History'].index
except:
    row_num[0] = 1

try:
    df = pd.read_excel(main_file_path,skiprows = row_num[0]+1)
except:
    print('Path for New Month DQ file Incorrect/Missing')

df['Validation Rule Description History'][0] = 'Month'

#hardcoded value
last_column_dq = np.argwhere(df.values=='Comments')[0][1]

#slicing dataframe with last row and last column
df_dq = df.iloc[:,:last_column_dq+1]
df_dq = df_dq[:21]

df_dq = df_dq.set_index(df_dq.columns[0])

df_dq = df_dq.fillna(0)

#***

#creating dataframe for New Data month DQ data
data_month_dq_df = pd.read_excel(new_month_dq_file_path, header=None, index_col=0)
data_month_dq_df = data_month_dq_df.fillna(0)

#Inserting New Data month Column into Main DQ table
#Import File ID code to be written
df_dq.insert(len(df_dq.columns)-1,f'File ID : {file_id}',data_month_dq_df[2])    #Column is inserted according to corresponding index values

#Assigning current month and year to a string
data_month = (date.today() - pd.offsets.DateOffset(months=1))
data_month_words = data_month.month_name(locale = 'English')
data_month_year = data_month.year
data_month_str = data_month_words+" "+str(data_month_year)

#Assigning string to column name
last_column_dq = np.argwhere(df_dq.values=='Comments')[0][1]

df_dq[df_dq.columns[last_column_dq-1]][0] = data_month_str

#Dropping comments column
df_comments = pd.DataFrame(df_dq[df_dq.columns[-1]])
df_dq = df_dq.drop(df_dq.columns[-1], axis = 1)

#Creating dictionary for storing Parameter Name and their index position
dq_indexes_dict = {}
count = 1
for i in df_dq.index[1:]:
    dq_indexes_dict[i] = count
    count += 1

#FOR SP Files:

def comment_generation():
    # function_call_list = []

    df_dq_copy = df_dq.copy()                                          #creating deep copy of df_dq
    df_dq_copy['Variance'] = None
    df_dq_copy['Comment Formation'] = None

    #Creating dictionary for storing threshold limits for each parameter
    param_dq_threshold_vals_dict = {}
    threshold_list = [3,3,10,3,3,3,3,3,10,10,10,0,0,3,0,0,3,3,3,0]
    ws_threshold_list = [3,3,3,3,3,3,3,3,3,3,3,0,0,3,0,0,3,3,2,0]
    
    for i,j in enumerate(df_dq.index[1:]):
        if supplier_category.lower() in ['sp']:
            param_dq_threshold_vals_dict[j] = threshold_list[i]
        elif supplier_category.lower() in ['w']:
            param_dq_threshold_vals_dict[j] = ws_threshold_list[i]

    #Comment content variables
    threshold_check = ''
    trend_check = ''

    #Comment formation main loop
    for i in df_dq.index[1:]:                                          #Parameter index starting from 1, 'i' is parameter
        param_value_dict = {}

        #Flags where threshold does not matter, even 1 flag is to be checked
        if dq_indexes_dict[i] in [1]:                                     #BACKFILLS ANALYSIS
            if df_dq[df_dq.columns[-1]][i] != 0:
                current_month_value = int(df_dq[df_dq.columns[-1]][i][df_dq[df_dq.columns[-1]][i].find("(")+1:df_dq[df_dq.columns[-1]][i].find("/")])
                
                df_dq_copy['Comment Formation'][i] = str(current_month_value) + " Backfills present, run designated SQL query to find removable duplicates."
            
        elif dq_indexes_dict[i] in [15]:                                  #QTY MIN ANALYSIS
            if df_dq[df_dq.columns[-1]][i] != 0:
                current_month_value = int(df_dq[df_dq.columns[-1]][i][df_dq[df_dq.columns[-1]][i].find("(")+1:df_dq[df_dq.columns[-1]][i].find("/")])

                comment_qty_min = str(current_month_value) + " flag(s) reported for: "
                comment_qty_min_list = dq_qty_min_max_analysis.qty_min_analysis(txn_count_file_path, dq_qty_min_file_path)
                for comment in comment_qty_min_list:
                    comment_qty_min += comment
                df_dq_copy['Comment Formation'][i] = comment_qty_min
        
        elif dq_indexes_dict[i] in [16]:                                  #QTY MAX ANALYSIS
            if df_dq[df_dq.columns[-1]][i] != 0:
                current_month_value = int(df_dq[df_dq.columns[-1]][i][df_dq[df_dq.columns[-1]][i].find("(")+1:df_dq[df_dq.columns[-1]][i].find("/")])

                comment_qty_max = str(current_month_value) + " flag(s) reported for: "
                comment_qty_max_list = dq_qty_min_max_analysis.qty_max_analysis(txn_count_file_path, dq_qty_max_file_path)
                for comment in comment_qty_max_list:
                    comment_qty_max += comment + ", "
                df_dq_copy['Comment Formation'][i] = comment_qty_max
        
        elif dq_indexes_dict[i] in [20]:                                   #UOM INCONSISTENCIES ANALYSIS
            if df_dq[df_dq.columns[-1]][i] != 0:
                current_month_value = int(df_dq[df_dq.columns[-1]][i][df_dq[df_dq.columns[-1]][i].find("(")+1:df_dq[df_dq.columns[-1]][i].find("/")])

                comments_uom_inc_list = dq_uom_inconsistencies.dq_uom_inconsistencies_analysis(dq_uom_inconsistencies_file_path,raw_data_file_path,ndc_factoring_values_file_path)
                comment_uom_inc = ""

                for comment_uom in comments_uom_inc_list:
                    comment_uom_inc += comment_uom + ", "
                df_dq_copy['Comment Formation'][i] = comment_uom_inc

# OLD BACKFILLS ANALYSIS METHOD:
# records = []
# try:
#     records = dq_backfill_analysis.backfill_fn(dq_backfills_data_path,sql_backfills_path)
#     df_dq_copy['Comment Formation'][i] = str(records[0]) + ' valid backfills present, for ' + str(records[1]) + ' unique NDCs'
#     print(records)
# except:
#     print("Backfills Function not run properly")

# if df_dq[df_dq.columns[-1]][i] != 0:
#     function_call_list.append(dq_indexes_dict[i])          #appending index to list to call them later using this index
    
    #     elif df_dq[df_dq.columns[-1]][i] != 0:
    #         if dq_indexes_dict[i] in [4]:
    #             dq_branch_analysis(df_dq) 
        

        #Entering Condition for Recurring flags
        
        elif df_dq[df_dq.columns[-1]][i] != 0 and df_dq[df_dq.columns[-2]][i] != 0:
            if supplier_category.lower() in ['sp']:
                current_month_value = int(df_dq[df_dq.columns[-1]][i][df_dq[df_dq.columns[-1]][i].find("(")+1:df_dq[df_dq.columns[-1]][i].find("/")])     #Extracting integer value from string of current month
                previous_month_value = int(df_dq[df_dq.columns[-2]][i][df_dq[df_dq.columns[-2]][i].find("(")+1:df_dq[df_dq.columns[-2]][i].find("/")])
                #Calculating variance
                calculated_variance = (1 - (min(current_month_value,previous_month_value)/max(current_month_value,previous_month_value)))*100
                df_dq_copy['Variance'][i] = calculated_variance
                
                #Finding Trending/Close value
                for col in df_dq.columns[:-1]:
                    #Assigning month to each month values for a parameter and storing it as keys in dictionary, 0 if not present
                    if df_dq[col][i]==0:
                        param_value_dict[df_dq[col]['Month']] = 0
                    else:
                        param_value_dict[df_dq[col]['Month']]=int(df_dq[col][i][df_dq[col][i].find("(")+1:df_dq[col][i].find("/")])
                
                if calculated_variance <= param_dq_threshold_vals_dict[i]:
                    threshold_check = 'Within' 
                else:
                    threshold_check = 'Over'
                
                #Trend/close check
                match_index = None
                trend_val = None
                trend_flag = False
                for iterable, val in enumerate(param_value_dict.values()):
                    if current_month_value == val:
                        match_index = iterable                             #index of trending value in dictionary, gets updated to the most recent value
                        trend_val = val                                    #Value itself
                        trend_flag = True
                if trend_flag == True:
                    match_list = [list(param_value_dict.keys())[match_index]]   #fetches month corresponding to the index stored in 'match_index'
                    #comment generation:
                    comment = threshold_check + ' ' + str(param_dq_threshold_vals_dict[i]) + '% threshold, trending with ' + str(match_list[0]) + '(' + str(current_month_value) + ').'
                else:
                    last_15_values_list = list(param_value_dict.values())[-16:]
                    variance = 1
                    close_index = None
                    close_val = None
                    close_flag = False
                    for iterable, val in enumerate(last_15_values_list):                 #Analyzing upto last 15 months
                        if val > 0:                                                      #Getting minimum variance value wrt last 15 months
                            if variance >= abs((current_month_value/val)-1):
                                variance = abs((current_month_value/val)-1)
                                close_index = iterable
                                close_val = val
                                close_flag = True
                    close_list = [list(param_value_dict.keys())[-16:][close_index]]
                    #comment generation:
                    comment = threshold_check + ' ' + str(param_dq_threshold_vals_dict[i]) + '% threshold, close in # of flags with ' + str(close_list[0]) + '(' + str(close_val) + ').'
                
            elif supplier_category.lower() in ['w']:
                # Extracting percentages in case of Wholesalers/SDs
                ws_percentage_current_month = int(df_dq[df_dq.columns[-1]][i][:df_dq[df_dq.columns[-1]][i].find("%")])
                ws_percentage_previous_month = int(df_dq[df_dq.columns[-2]][i][:df_dq[df_dq.columns[-2]][i].find("%")])
                
                current_month_value = int(df_dq[df_dq.columns[-1]][i][df_dq[df_dq.columns[-1]][i].find("(")+1:df_dq[df_dq.columns[-1]][i].find("/")])
                previous_month_value = int(df_dq[df_dq.columns[-2]][i][df_dq[df_dq.columns[-2]][i].find("(")+1:df_dq[df_dq.columns[-2]][i].find("/")])
                
                #Calculating variance
                calculated_variance_ws = abs(ws_percentage_current_month - ws_percentage_previous_month)
                df_dq_copy['Variance'][i] = calculated_variance_ws
                
                #Finding Trending/Close value
                for col in df_dq.columns[:-1]:
                    #Assigning month to each month values for a parameter and storing it as keys in dictionary, 0 if not present
                    if df_dq[col][i]==0:
                        param_value_dict[df_dq[col]['Month']] = 0
                    else:
                        param_value_dict[df_dq[col]['Month']]=int(df_dq[col][i][df_dq[col][i].find("(")+1:df_dq[col][i].find("/")])
                
                if calculated_variance_ws <= param_dq_threshold_vals_dict[i]:
                    threshold_check = 'Within' 
                else:
                    threshold_check = 'Over'
                
                #Trend/close check
                match_index = None
                trend_val = None
                trend_flag = False
                for iterable, val in enumerate(param_value_dict.values()):
                    if current_month_value == val:
                        match_index = iterable                             #index of trending value in dictionary, gets updated to the most recent value
                        trend_val = val                                    #Value itself
                        trend_flag = True
                if trend_flag == True:
                    match_list = [list(param_value_dict.keys())[match_index]]   #fetches month corresponding to the index stored in 'match_index'
                    #comment generation:
                    comment = threshold_check + ' ' + str(param_dq_threshold_vals_dict[i]) + '% threshold, trending with ' + str(match_list[0]) + '(' + str(current_month_value) + ').'
                else:
                    last_15_values_list = list(param_value_dict.values())[-16:]
                    variance = 1
                    close_index = None
                    close_val = None
                    close_flag = False
                    for iterable, val in enumerate(last_15_values_list):                 #Analyzing upto last 15 months
                        if val > 0:                                                      #Getting minimum variance value wrt last 15 months
                            if variance >= abs((current_month_value/val)-1):
                                variance = abs((current_month_value/val)-1)
                                close_index = iterable
                                close_val = val
                                close_flag = True
                    close_list = [list(param_value_dict.keys())[-16:][close_index]]
                    #comment generation:
                    comment = threshold_check + ' ' + str(param_dq_threshold_vals_dict[i]) + '% threshold, close in # of flags with ' + str(close_list[0]) + '(' + str(close_val) + ').'

            df_dq_copy['Comment Formation'][i] = comment
            
            if dq_indexes_dict[i] == 6:
                df_dq_copy['Comment Formation'][i] += " Matching with BO(" + str(current_month_value) + "). Pass."
            
            if dq_indexes_dict[i] == 18:
                zip_code_comment = dq_zip_code_analysis.zip_code_analysis(dq_zip_code_file_path)
                df_dq_copy['Comment Formation'][i] += " " + str(current_month_value) + " flags reported " + zip_code_comment + ", observed in past. Pass."
            
            if dq_indexes_dict[i] == 19:
                unknown_roche_ndc_comment = dq_unknown_roche_ndc.dq_unknown_roche_analysis(dq_config_file_path,dq_unknown_roche_ndc_file_path)
                df_dq_copy['Comment Formation'][i] += " " + str(current_month_value) + " flags reported across " + unknown_roche_ndc_comment

            if dq_indexes_dict[i] == 4:           #Branch analysis
                branch_comment = dq_branch_analysis.dq_non_trending_branch_analysis(branch_report_file_path,current_month_branch_dq_file_path,previous_month_branch_dq_file_path, output_path)
                df_dq_copy['Comment Formation'][i] += " " + branch_comment

            elif df_dq[df_dq.columns[-1]][i] != 0 and df_dq[df_dq.columns[-2]][i] == 0 and dq_indexes_dict[i] not in [1,15,16,18,19,20]:
                current_month_value = int(df_dq[df_dq.columns[-1]][i][df_dq[df_dq.columns[-1]][i].find("(")+1:df_dq[df_dq.columns[-1]][i].find("/")])
                
                df_dq_copy['Comment Formation'][i] = 'Trend Break, ' + str(current_month_value) + ' flag(s) reported'
    
    with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df_dq_copy.to_excel(writer, sheet_name=supplier_name + " DQ")

comment_generation()