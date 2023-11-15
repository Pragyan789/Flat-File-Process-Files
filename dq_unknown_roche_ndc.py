import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date

def dq_unknown_roche_analysis(dq_config_file_path, dq_unknown_roche_ndc_file_path):
    
    orig_dq_unknown_roche_ndc_df = None
    dq_config_file_df = None

    try:
        orig_dq_unknown_roche_ndc_df = pd.read_excel(dq_unknown_roche_ndc_file_path)
    except:
        print("Please enter correct path for Unknown Roche NDC DQ file")

    try:
        dq_config_file_df = pd.read_excel(dq_config_file_path)
    except:
        print("Please enter correct path for DQ Config file")

    comment = ''
    
    if orig_dq_unknown_roche_ndc_df is not None and dq_config_file_df is not None:
        dq_unknown_roche_ndc_df = orig_dq_unknown_roche_ndc_df[1:]

        number_of_unique_NDC = dq_unknown_roche_ndc_df["NDC Number"].nunique()

        dq_ndc_set = set(dq_unknown_roche_ndc_df["NDC Number"].unique())

        config_ndc_set = set(dq_config_file_df["_NDC Num 11"].unique())

        if (dq_ndc_set & config_ndc_set) == set():                            #Comparing sets to find if any NDC is present in config file
            comment = str(number_of_unique_NDC) + " NDC(s) for " + ','.join(str(int(ndc)) for ndc in dq_ndc_set) + ", observed previously, pass as false flag and not in config file."
        else:
            comment = str(number_of_unique_NDC) + " NDC(s) for " + ','.join(str(int(ndc)) for ndc in dq_ndc_set) + ", observed previously, however " + str(dq_ndc_set & config_ndc_set) + " found in config file."

        return comment

