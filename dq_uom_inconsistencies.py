import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date

def dq_uom_inconsistencies_analysis(dq_uom_inconsistencies_file_path, raw_data_file_path, ndc_factoring_values_file_path):
    
    orig_dq_uom_inconsistencies_df = None
    raw_data_df = None
    ndc_factoring_values_df = None
    
    try:
        orig_dq_uom_inconsistencies_df = pd.read_excel(dq_uom_inconsistencies_file_path)
    except:
        print("Please enter correct path for UOM Inconsistencies DQ file")
    try:
        raw_data_df = pd.read_excel(raw_data_file_path,dtype = {'QTY_DISPENSED': float})
    except:
        print("Please enter correct path for Raw Data file")
    try:
        ndc_factoring_values_df = pd.read_excel(ndc_factoring_values_file_path)
    except:
        print("Please enter correct path for NDC Factoring values file")

    comments = []

    if orig_dq_uom_inconsistencies_df is not None and raw_data_df is not None and ndc_factoring_values_df is not None:
        #Dataframe containing factoring values for each ndc
        ndc_factoring_values_df = ndc_factoring_values_df.fillna(0)

        #DQ output containing UOM info
        dq_uom_inconsistencies_df = orig_dq_uom_inconsistencies_df[1:]

        raw_qty_list = []
        qty_dispensed_list = []
        ndc_numbers_list = []
        comments = ''
        #Appending Information of each NDC to different lists, and if it fails to append, 0 is appended
        for i in range(1,len(dq_uom_inconsistencies_df)+1):
            try:
                raw_qty_list.append(float(raw_data_df.loc[(raw_data_df["QTY_DISPENSED"] == round(dq_uom_inconsistencies_df["Quantity Dispensed"][i],3)) & (raw_data_df["NDC_NBR"] == dq_uom_inconsistencies_df["NDC Number"][i]) & (raw_data_df["ADDRESS"] == dq_uom_inconsistencies_df["Account Address 1"][i]) & (raw_data_df["MOST_RECENT_SHIP_DATE"] == dq_uom_inconsistencies_df["Most Recent Ship Date"][i])]["QTY_867_RAW"]))
            except:
                raw_qty_list.append(0)
            try:
                qty_dispensed_list.append(float(raw_data_df.loc[(raw_data_df["QTY_DISPENSED"] == round(dq_uom_inconsistencies_df["Quantity Dispensed"][i],3)) & (raw_data_df["NDC_NBR"] == dq_uom_inconsistencies_df["NDC Number"][i]) & (raw_data_df["ADDRESS"] == dq_uom_inconsistencies_df["Account Address 1"][i]) & (raw_data_df["MOST_RECENT_SHIP_DATE"] == dq_uom_inconsistencies_df["Most Recent Ship Date"][i])]["QTY_DISPENSED"]))
            except:
                qty_dispensed_list.append(0)
            try:
                ndc_numbers_list.append(float(raw_data_df.loc[(raw_data_df["QTY_DISPENSED"] == round(dq_uom_inconsistencies_df["Quantity Dispensed"][i],3)) & (raw_data_df["NDC_NBR"] == dq_uom_inconsistencies_df["NDC Number"][i]) & (raw_data_df["ADDRESS"] == dq_uom_inconsistencies_df["Account Address 1"][i]) & (raw_data_df["MOST_RECENT_SHIP_DATE"] == dq_uom_inconsistencies_df["Most Recent Ship Date"][i])]["NDC_NBR"]))
            except:
                ndc_numbers_list.append(0)
        
        for j, ndc in enumerate(ndc_numbers_list):
            if ndc != 0:
                #Two columns present in factoring values dataframe, extracting both of them if both present, otherwise extracting either:
                try:
                    factoring_value = float(ndc_factoring_values_df.loc[ndc_factoring_values_df["NDC_NBR"]==ndc]["Factoring Value"].values[0])
                except:
                    factoring_value = 0
                try:
                    factoring_value2 = float(ndc_factoring_values_df.loc[ndc_factoring_values_df["NDC_NBR"]==ndc]["Factoring Value2"].values[0])
                except:
                    factoring_value2 = 0
                
                if str(ndc).startswith('4'):
                    comments+="Factoring value unknown, Roche NDC, observed in past, " + str(int(ndc)) + ", qty: " + str(qty_dispensed_list[j]) + "; "
                
                #Case when raw qty is integer, which is incorrectly divided by factoring value
                elif raw_qty_list[j].is_integer():
                    if factoring_value != 0:
                        if round((raw_qty_list[j]/factoring_value),3) == round(qty_dispensed_list[j],3):
                            comments+=str(qty_dispensed_list[j]) + " to be manually changed to " + str(raw_qty_list[j]) + " for NDC " + str(int(ndc)) + "; "
                        else:
                            comments+="QTY Dispensed- " + str(qty_dispensed_list[j]) + ", Raw Data - " + str(raw_qty_list[j]) + " for NDC " + str(int(ndc)) + ", needs to be verified; "
                    elif factoring_value2 != 0:
                        if round((raw_qty_list[j]/factoring_value2),3) == round(qty_dispensed_list[j],3):
                            comments+=str(qty_dispensed_list[j]) + " to be manually changed to " + str(raw_qty_list[j]) + " for NDC " + str(int(ndc)) + "; "
                        else:
                            comments+="QTY Dispensed- " + str(qty_dispensed_list[j]) + ", Raw Data - " + str(raw_qty_list[j]) + " for NDC " + str(int(ndc)) + ", needs to be verified; "
                    if factoring_value == 0 and factoring_value2 == 0:
                        comments+="Factoring value unknown for NDC " + str(int(ndc)) + "; "
                
                #Case when raw qty is decimal, which needs to be divided by factoring value to become whole
                elif raw_qty_list[j].is_integer() == False:
                    if round((raw_qty_list[j]/factoring_value),3).is_integer() or round((raw_qty_list[j]/factoring_value2),3).is_integer():
                        if factoring_value2==0:
                            comments+= "Factoring of qty/" + str(factoring_value) + " to be applied to NDC " + str(int(ndc)) + "; "
                        else:
                            comments+= ("Factoring of qty/" + str(factoring_value2) + " to be applied to NDC " + str(int(ndc))) + "; "
    return comments