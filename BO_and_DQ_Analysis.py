#Takes two data sources: 
# Combined Ins File
# Combined Outs File
# and outputs results to another file
#
#SAP Analysis to be implemented
#Implement Name Filter through google sheets
#This code assumes that Ins and Outs data is to be analyzed for 13 months
#code will not work for files such as 'Qualitas Pharmacy' which has only 4 months in BO, can include in future scope for improvement

#IMP - Find a way to set SAP filters without using hardcoded values.

#INs NDC data type - int64
#OUTs NDC data type - str

#IMP: Need to include more exception handling

import pandas as pd
import numpy as np
import os
from openpyxl import load_workbook

import warnings
warnings.filterwarnings('ignore')

from datetime import datetime
from datetime import date

import DQ_Analysis_Main
#File that contains all functions
import bo_analysis_functions
import dq_branch_analysis

df = None
reference_list_df = None
df_outs_raw = None
sap_ins_df = None

df_input = pd.read_excel(r"C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\Raw_Files_Folder\Input Paths.xlsx")
df_input = df_input.set_index('Variable Name')
df_input = df_input.fillna("")

folder_path = list(df_input.loc['folder_path'])[0]
supplier_folder_path = list(df_input.loc['supplier_folder_path'])[0]

supplier_names_file_path = folder_path + "\\" + list(df_input.loc['supplier_names_file'])[0] + ".xlsx"
combined_ins_path = supplier_folder_path + "\\" + list(df_input.loc['combined_ins'])[0] + ".xlsx"
bo_file_path = supplier_folder_path + "\\" + list(df_input.loc['bo_file'])[0] + ".xlsx"
reference_list_path = supplier_folder_path + "\\" + list(df_input.loc['reference_list'])[0] + ".xlsx"
sap_filter_list_path = supplier_folder_path + "\\" + list(df_input.loc['sap_filter_list'])[0] + ".xlsx"
sap_ins_file_path = folder_path + "\\" + list(df_input.loc['sap_ins_file'])[0] + ".xlsx"
branch_report_file_path = supplier_folder_path + "\\" + list(df_input.loc['branch_report_file'])[0] + ".xlsx"


#Data Sources Import
try:
    df = pd.read_excel(combined_ins_path)
except:
    print("Enter correct file path for Combined Ins File")
try:
    reference_list_df = pd.read_excel(reference_list_path)
except:
    print("Please enter correct path for Account Names Reference List")
try:
    df_outs_raw = pd.read_excel(bo_file_path, skiprows=1, usecols = 'B:Q')  #skipping first row and first column
except:
    print("Enter correct file path for BO Table File")
try:
    sap_ins_df = pd.read_excel(sap_ins_file_path,sheet_name=1)
except:
    print("Please enter correct path for Sapins file")
try:
    sap_filter_list_df = pd.read_excel(sap_filter_list_path)
except:
    print("Please enter correct path for Sapins filter list file")
try:
    supplier_names_df = pd.read_excel(supplier_names_file_path)
except:
    print("Please enter correct path for Supplier names file")

#Get Data month (previous month), and 13 months before data month
today = date.today()
data_month = today.month - 1
data_month_year = (date.today() - pd.offsets.DateOffset(months=1)).year
start_month = (date.today() - pd.offsets.DateOffset(months=13)).month
start_year = (date.today() - pd.offsets.DateOffset(months=13)).year

#For custom usage, comment it for automated updation
# data_month = 9
# data_month_year = 2023
# start_month = 9
# start_year = 2022

df_combined_ins = None
sap_ins_pivot = None
ins_pivot = None
ins_branch_pivot = None
outs_pivot = None
branch_pivot = None

if sap_ins_df is not None and sap_filter_list_df is not None:
    try:
        sap_ins_pivot = bo_analysis_functions.sap_ins_pivot_creation(sap_ins_df, sap_filter_list_df, data_month, data_month_year, start_month, start_year)
    except:
        print("Sapins Pivot creation Function did not execute properly")

if df is not None:
    try:
        ins_pivot, ins_branch_pivot, df_combined_ins = bo_analysis_functions.df_ins_pivot_creation(df, reference_list_df, data_month, data_month_year, start_month, start_year)
    except:
        print("Ins Pivot creation Function did not execute properly")

if df_outs_raw is not None:
    try:
        outs_pivot = bo_analysis_functions.df_outs_pivot_creation(data_month_year, data_month, df_outs_raw)
    except:
        print("Outs Pivot creation Function did not execute properly")

# Hardcoded Input (File Name)
supplier_name = list(df_input.loc['supplier_name'])[0]
supplier_names_df = supplier_names_df.set_index('File Name')
supplier_name = supplier_names_df[supplier_names_df.columns[0]][supplier_name.lower()]

output_path = supplier_folder_path + "\\" + list(df_input.loc['main_file'])[0] + ".xlsx"

# Calling main analysis function
bo_analysis_functions.bo_and_sap_analysis(ins_pivot, outs_pivot, sap_ins_pivot, supplier_name, output_path)

if df is not None and df_outs_raw is not None:
    # Unreported NDCs Analysis, output stored as separate tab in main file:
    bo_analysis_functions.unreported_ndc(ins_pivot, outs_pivot, output_path)
    
    # Unreported Branches Analysis, output stored as separate tab in main file:
    # Comment Variable has no use, just for sake of calling the function, it has been introduced here.
    comment = ''
    comment, branch_pivot = dq_branch_analysis.dq_non_trending_branch_analysis(branch_report_file_path,'','', output_path)
    bo_analysis_functions.unreported_branches(df_combined_ins, ins_branch_pivot, branch_pivot, output_path)

DQ_Analysis_Main.comment_generation()

months = {1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MAY",6:"JUN",7:"JUL",8:"AUG",9:"SEP",10:"OCT",11:"NOV",12:"DEC"}
output_destination_path = supplier_folder_path + "\\" + supplier_name + "_" + months[(date.today().month - 1)] + str(data_month_year)[-2:] + ".xlsx"
os.rename(output_path,output_destination_path)