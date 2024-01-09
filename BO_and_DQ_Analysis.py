#This code assumes that Ins and Outs data is to be analyzed for 13 months
#code will not work for files such as 'Qualitas Pharmacy' which has only 4 months in BO, can include in future scope for improvement

#INs NDC data type - int64
#OUTs NDC data type - str

import pandas as pd
import numpy as np
import os
from openpyxl import load_workbook
import xlsxwriter
import xlwings as xw

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
sap_filter_list_df = None
supplier_names_df = None

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
    sap_filter_list_df = pd.read_excel(sap_filter_list_path)
except:
    print("Please enter correct path for Sapins filter list file")
try:
    supplier_names_df = pd.read_excel(supplier_names_file_path)
except:
    print("Please enter correct path for Supplier names file")

#Get Data month (previous month), and 13 months before data month
today = date.today()
data_month = (date.today() - pd.offsets.DateOffset(months=1)).month
data_month_year = (date.today() - pd.offsets.DateOffset(months=1)).year
start_month = (date.today() - pd.offsets.DateOffset(months=13)).month
start_year = (date.today() - pd.offsets.DateOffset(months=13)).year

# For custom usage, comment it for automated updation
# data_month = 11
# data_month_year = 2023
# start_month = 11
# start_year = 2022

df_combined_ins = None
sap_ins_pivot = None
ins_pivot = None
ins_branch_pivot = None
outs_pivot = None
branch_pivot = None
sender_by_ndc_pivot = None

if sap_filter_list_df is not None:
    # Reading SAP Data only if SAP Filters file path is provided
    try:
        sap_ins_df = pd.read_excel(sap_ins_file_path,sheet_name=1)
    except:
        print("Please enter correct path for Sapins file")

    if sap_ins_df is not None:
        try:
            sap_ins_pivot = bo_analysis_functions.sap_ins_pivot_creation(sap_ins_df, sap_filter_list_df, data_month, data_month_year, start_month, start_year)
        except:
            print("Sapins Pivot creation Function did not execute properly")

if df is not None:
    try:
        # Here ins_branch_pivot is pivot created on Ins data with Index as id1_value.1, this is different from branch_pivot
        ins_pivot, ins_branch_pivot, sender_by_ndc_pivot, df_combined_ins = bo_analysis_functions.df_ins_pivot_creation(df, reference_list_df, data_month, data_month_year, start_month, start_year)
    except:
        print("Ins Pivot creation Function did not execute properly")

if df_outs_raw is not None:
    try:
        outs_pivot = bo_analysis_functions.df_outs_pivot_creation(data_month_year, data_month, df_outs_raw)
    except:
        print("Outs Pivot creation Function did not execute properly")


supplier_name_raw = list(df_input.loc['supplier_name'])[0]
supplier_names_df = supplier_names_df.set_index('File Name')
supplier_name = supplier_names_df[supplier_names_df.columns[0]][supplier_name_raw.lower()]

output_path = supplier_folder_path + "\\" + list(df_input.loc['main_file'])[0] + ".xlsx"


bo_analysis_df = None
ins_pivot_output = None
df_outs = None

# Calling main analysis function
if outs_pivot is not None:
    bo_analysis_df, ins_pivot_output, df_outs = bo_analysis_functions.bo_and_sap_analysis(ins_pivot, outs_pivot, sap_ins_pivot, supplier_name)

unreported_ndc_pivot_df = None
unreported_branches_pivot_df = None

if df is not None and df_outs_raw is not None:
    # Unreported NDCs Analysis, output stored as separate tab in main file:
    try:
        unreported_ndc_pivot_df = bo_analysis_functions.unreported_ndc(ins_pivot, outs_pivot, output_path)
    except:
        print("Unreported NDCs function did not run correctly")
    
    # Unreported Branches Analysis, output stored as separate tab in main file:
    # Comment Variable has no use, just for sake of calling the function, it has been introduced here.
    comment = ''
    try:
        comment, branch_pivot = dq_branch_analysis.dq_non_trending_branch_analysis(branch_report_file_path,'Inside BO Function','Inside BO Function', output_path)
    except:
        print("Branch analysis Function did not run properly")
    
    try:
        unreported_branches_pivot_df = bo_analysis_functions.unreported_branches(df_combined_ins, ins_branch_pivot, branch_pivot, output_path)
    except:
        print("Unreported Branches function did not run correctly")

#DQ Function
df_dq = None
branch_pivot = None

# try:
df_dq, branch_pivot = DQ_Analysis_Main.comment_generation()
# except:
#     print("DQ Function did not run correctly")

#Output_File:
list_of_dataframes = [sender_by_ndc_pivot, unreported_branches_pivot_df, unreported_ndc_pivot_df, branch_pivot, bo_analysis_df, ins_pivot, df_outs, df_dq]
names_of_dataframes = [supplier_name + " S_by_NDC", supplier_name + " U_Branches", supplier_name + " U_NDCs", supplier_name + " Branch Pivot", supplier_name + " BO Analysis", supplier_name + " Ins Pivot", supplier_name + " BO", supplier_name + " DQ"]

book = xw.Book(output_path)
for iter, df_op in enumerate(list_of_dataframes):
    # last_sheet_name = book.sheets[-1].name
    first_sheet_name = book.sheets[0].name
    if df_op is not None:
        try:
            #creating new sheet at end of File
            book.sheets.add(names_of_dataframes[iter], before=first_sheet_name)
        except:
            print(names_of_dataframes[iter] + " - new sheet not created/ already exists")
        
        try:
            ws = book.sheets[names_of_dataframes[iter]]
            ws["A1"].options(pd.DataFrame, header=1, index=True, expand='table').value = df_op
        except:
            print(names_of_dataframes[iter] + " DF not inserted")

        ws.autofit()

book.save()
book.close()

#Rename file to current Data month:
months = {1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MAY",6:"JUN",7:"JUL",8:"AUG",9:"SEP",10:"OCT",11:"NOV",12:"DEC"}
output_destination_path = supplier_folder_path + "\\" + supplier_name_raw + "_" + months[(date.today() - pd.offsets.DateOffset(months=1)).month] + str(data_month_year)[-2:] + ".xlsx"
os.rename(output_path,output_destination_path)