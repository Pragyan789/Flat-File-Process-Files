import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date

def qty_max_analysis(txn_count_file_path, dq_qty_max_file_path):

    txn_count_df = None
    orig_dq_qty_max_df = None

    try:
        txn_count_df = pd.read_excel(txn_count_file_path)
    except:
        print("Please enter correct path for Txn Count file")

    try:
        orig_dq_qty_max_df = pd.read_excel(dq_qty_max_file_path)
    except:
        print("DQ Qty MAX flags file not found")
    
    data_month_date = (date.today() - pd.offsets.DateOffset(months=1))       #date.today is used assuming that data month is previous month
    data_month = data_month_date.month
    data_month_year = data_month_date.year

    #Lists created to store NDC numbers according to whether they are valid flags or not
    pass_ndc_list = []
    fail_ndc_list = []

    next_max_month = None
    next_max_year = None

    comments_max = []

    if orig_dq_qty_max_df is not None and txn_count_df is not None:
        dq_qty_max_df = orig_dq_qty_max_df[1:]         #dropping first empty row

        #Following loop to check for each NDC number present in DQ dataframe:
        for i in dq_qty_max_df["NDC Number"].unique():
            #Extracting quantity reported for NDC for current month
            Quantity_Dispensed_ndc_max = int(dq_qty_max_df[dq_qty_max_df["NDC Number"]==i]["Quantity Dispensed"])
            
            #Creating new dataframe which contains data from TXN count table for particular NDC only
            df_ndc = txn_count_df[txn_count_df["NDC_NBR"] == i].sort_values(by=['FILE_DATE_OF_REPORT'],ascending=False)
            
            #minor index reset
            df_ndc = df_ndc.reset_index().drop("index", axis = "columns")
            
            #Creating new column which holds month and year, to be used for filtering data further
            df_ndc["Month Year"] = df_ndc["FILE_DATE_OF_REPORT"].apply(lambda val: str(val.month) + " " + str(val.year))
            df_ndc = df_ndc.loc[(df_ndc["Month Year"] != str(data_month) + " " + str(data_month_year))]
            
            #list that contains all 'MAX_QTY' for particular NDC, sorted in descending order
            qty_list = list(df_ndc["MAX_QTY"].sort_values(ascending=False))
            
            #calculating variance between current month reported qty and max qty in qty_list
            delta = (Quantity_Dispensed_ndc_max - max(qty_list))/Quantity_Dispensed_ndc_max
            
            #0.51 is the highest variance allowed, meaning that delta between current month reported qty and max qty in qty_list should not be greater than +51%
            if delta <= 0:
                pass_ndc_list.append(int(i))                 #this list is not being used
                
                comments_max.append(str(int(i)) +  ", similar observed in the past and no inconsistencies observed with top volumes")
            elif delta >0 and delta <= 0.51:
                pass_ndc_list.append(int(i))                 #this list is not being used
                
                df_1 = df_ndc.sort_values(by=["MAX_QTY"], ascending=False)                            #creating a dataframe sorted with largest MAX_QTY as top row
                next_max_month = df_1.iloc[0]["FILE_DATE_OF_REPORT"].month_name(locale='English')     #Extracting month and year of next maximum MAX_QTY
                next_max_year = df_1.iloc[0]["FILE_DATE_OF_REPORT"].year
                
                comments_max.append(str(int(i)) + " with qty dispensed " + str(Quantity_Dispensed_ndc_max) + " followed by " + str(max(qty_list)) + " in " + str(next_max_month) + " " + str(next_max_year))
            else:
                fail_ndc_list.append(i)                      #this list is not being used
                
                comments_max.append(str(int(i)) + " with qty dispensed " + str(Quantity_Dispensed_ndc_max) + " has high delta, needs to be verified")
    
    return comments_max


def qty_min_analysis(txn_count_file_path, dq_qty_min_file_path):
    
    txn_count_df = None
    orig_dq_qty_min_df = None

    try:
        txn_count_df = pd.read_excel(txn_count_file_path)
    except:
        print("Please enter correct path for Txn Count file")

    try:
        orig_dq_qty_min_df = pd.read_excel(dq_qty_min_file_path)
    except:
        print("DQ Qty MIN flags file not found")
    
    data_month_date = (date.today() - pd.offsets.DateOffset(months=1))       #date.today is used assuming that data month is previous month
    data_month = data_month_date.month
    data_month_year = data_month_date.year

    #Lists created to store NDC numbers according to whether they are valid flags or not
    pass_ndc_list = []
    fail_ndc_list = []

    next_min_month = None
    next_min_year = None

    comments_min = []

    if orig_dq_qty_min_df is not None and txn_count_df is not None:
        dq_qty_min_df = orig_dq_qty_min_df[1:]         #dropping first empty row

        #Following loop to check for each NDC number present in DQ dataframe:
        for i in dq_qty_min_df["NDC Number"].unique():
            #Extracting quantity reported for NDC for current month
            Quantity_Dispensed_ndc_min = int(dq_qty_min_df[dq_qty_min_df["NDC Number"]==i]["Quantity Dispensed"])
            
            #Creating new dataframe which contains data from TXN count table for particular NDC only
            df_ndc = txn_count_df[txn_count_df["NDC_NBR"] == i].sort_values(by=['FILE_DATE_OF_REPORT'],ascending=False)
            
            #minor index reset
            df_ndc = df_ndc.reset_index().drop("index", axis = "columns")
            
            #Creating new column which holds month and year, to be used for filtering data further
            df_ndc["Month Year"] = df_ndc["FILE_DATE_OF_REPORT"].apply(lambda val: str(val.month) + " " + str(val.year))
            df_ndc = df_ndc.loc[(df_ndc["Month Year"] != str(data_month) + " " + str(data_month_year))]
            
            #list that contains all 'MAX_QTY' for particular NDC, sorted in descending order
            qty_list = list(df_ndc["MIN_QTY"].sort_values())
            
            #calculating variance between current month reported qty and min qty in qty_list
            #delta = (abs(Quantity_Dispensed_ndc_min) - abs(min(qty_list)))/abs(Quantity_Dispensed_ndc_min)
            
            if min(qty_list) >= Quantity_Dispensed_ndc_min:
                pass_ndc_list.append(int(i))             #this list is not being used
                
                comments_min.append(str(int(i)) +  ", similar observed in the past and no inconsistencies observed with return volumes")
            
            # elif min(qty_list) <= Quantity_Dispensed_ndc_min:
            #     pass_ndc_list.append(int(i))
                
            #     df_1 = df_ndc.sort_values(by=["MIN_QTY"])                                             #creating a dataframe sorted with largest MIN_QTY as top row
            #     next_min_month = df_1.iloc[0]["FILE_DATE_OF_REPORT"].month_name(locale='English')     #Extracting month and year of next maximum MIN_QTY
            #     next_min_year = df_1.iloc[0]["FILE_DATE_OF_REPORT"].year
                
            #     comments_min.append(str(int(i)) + " with qty dispensed " + str(Quantity_Dispensed_ndc_min) + " followed by " + str(min(qty_list)) + " in " + str(next_min_month) + " " + str(next_min_year))
            else:
                fail_ndc_list.append(i)                   #this list is not being used
                
                comments_min.append(str(int(i)) + " with qty dispensed " + str(Quantity_Dispensed_ndc_min) + " has high delta, needs to be verified")

    return comments_min



