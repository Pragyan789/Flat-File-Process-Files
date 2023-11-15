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
        ndc_factoring_values_df = ndc_factoring_values_df.fillna(0)

        dq_uom_inconsistencies_df = orig_dq_uom_inconsistencies_df[1:]

        raw_qty_list = []
        qty_dispensed_list = []
        ndc_numbers_list = []

        for i in range(1,len(dq_uom_inconsistencies_df)+1):
            try:
                raw_qty_list.append(float(raw_data_df.loc[(raw_data_df["QTY_DISPENSED"] == round(dq_uom_inconsistencies_df["Quantity Dispensed"][i],3)) & (raw_data_df["NDC_NBR"] == dq_uom_inconsistencies_df["NDC Number"][i])]["QTY_867_RAW"]))
            except:
                raw_qty_list.append(0)
            try:
                qty_dispensed_list.append(float(raw_data_df.loc[(raw_data_df["QTY_DISPENSED"] == round(dq_uom_inconsistencies_df["Quantity Dispensed"][i],3)) & (raw_data_df["NDC_NBR"] == dq_uom_inconsistencies_df["NDC Number"][i])]["QTY_DISPENSED"]))
            except:
                qty_dispensed_list.append(0)
            try:
                ndc_numbers_list.append(float(raw_data_df.loc[(raw_data_df["QTY_DISPENSED"] == round(dq_uom_inconsistencies_df["Quantity Dispensed"][i],3)) & (raw_data_df["NDC_NBR"] == dq_uom_inconsistencies_df["NDC Number"][i])]["NDC_NBR"]))
            except:
                ndc_numbers_list.append(0)
        
        for j, ndc in enumerate(ndc_numbers_list):
            if ndc != 0:
                try:
                    factoring_value = float(ndc_factoring_values_df.loc[ndc_factoring_values_df["NDC_NBR"]==ndc]["Factoring Value"].values[0])
                except:
                    factoring_value = 0
                try:
                    factoring_value2 = float(ndc_factoring_values_df.loc[ndc_factoring_values_df["NDC_NBR"]==ndc]["Factoring Value2"].values[0])
                except:
                    factoring_value2 = 0
                
                if str(ndc).startswith('4'):
                    comments.append("Factoring value unknown, Roche NDC, observed in past, " + str(int(ndc)) + ", qty: " + str(qty_dispensed_list[j]))
                elif raw_qty_list[j].is_integer():
                    if round((raw_qty_list[j]/factoring_value),3) == round(qty_dispensed_list[j],3) or round((raw_qty_list[j]/factoring_value2),3) == round(qty_dispensed_list[j],3):
                        comments.append(str(qty_dispensed_list[j]) + " to be manually changed to " + str(raw_qty_list[j]) + " for NDC " + str(int(ndc)))
                elif raw_qty_list[j].is_integer() == False:
                    if round((raw_qty_list[j]/factoring_value),3).is_integer() or round((raw_qty_list[j]/factoring_value2),3).is_integer():
                        if factoring_value2==0:
                            comments.append("Factoring of qty/" + str(factoring_value) + " to be applied to NDC " + str(int(ndc)))
                        else:
                            comments.append("Factoring of qty/" + str(factoring_value2) + " to be applied to NDC " + str(int(ndc)))
    return comments