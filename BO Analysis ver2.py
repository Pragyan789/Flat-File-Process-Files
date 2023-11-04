#Takes two data sources: 
# Combined Ins File
# Combined Outs File
# and outputs results to another file
#
#SAP Analysis to be implemented
#Implement Name Filter through google sheets
#This code assumes that Ins and Outs data is to be analyzed for 13 months
#code will not work for files such as 'Qualitas Pharmacy' which has only 4 months in BO, can include in future scope for improvement

#INs NDC data type - int64
#OUTs NDC data type - str

#IMP: Need to include more exception handling

import pandas as pd
import numpy as np
from openpyxl import load_workbook

import warnings
warnings.filterwarnings('ignore')

from datetime import datetime
from datetime import date

#File that contains all functions
import bo_analysis_functions

df = None
reference_list_df = None
df_outs_raw = None
sap_ins_df = None


#Data Sources Import
try:
    df = pd.read_excel(r"C:\Users\pragyan.agrawal\Downloads\Humana Ins.xlsx")
except:
    print("Enter correct file path for Combined Ins File")
try:
    reference_list_df = pd.read_excel(r"C:\Users\pragyan.agrawal\Downloads\Humana Reference List.xlsx")
except:
    print("Please enter correct path for Account Names Reference List")
try:
    df_outs_raw = pd.read_excel(r"C:\Users\pragyan.agrawal\Downloads\Humana BO.xlsx", skiprows=1, usecols = 'B:Q')  #skipping first row and first column
except:
    print("Enter correct file path for BO Table File")
try:
    sap_ins_df = pd.read_excel(r"C:\Users\pragyan.agrawal\Downloads\SAP Data OCT'23.xlsx",sheet_name=1)
except:
    print("Please enter correct path for Sapins file")


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

sap_ins_pivot = None
try:
    sap_ins_pivot = bo_analysis_functions.sap_ins_pivot_creation(sap_ins_df, data_month, data_month_year, start_month, start_year)
except:
    print("Sapins Pivot creation Function did not execute properly")

if df is not None and df_outs_raw is not None:
    # try:
    ins_pivot = bo_analysis_functions.df_ins_pivot_creation(df, reference_list_df, data_month, data_month_year, start_month, start_year)
    # except:
    #     print("Ins Pivot creation Function did not execute properly")

    try:
        outs_pivot = bo_analysis_functions.df_outs_pivot_creation(data_month_year, data_month, df_outs_raw)
    except:
        print("Outs Pivot creation Function did not execute properly")

#Calling main analysis function
if df is not None and df_outs_raw is not None:
    bo_analysis_functions.bo_and_sap_analysis(ins_pivot, outs_pivot, sap_ins_pivot)

    bo_analysis_functions.unreported_ndc(ins_pivot, outs_pivot)