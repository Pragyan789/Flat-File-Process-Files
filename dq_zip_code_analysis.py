import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date

orig_dq_zip_code_df = None

def zip_code_analysis(dq_zip_code_file_path):
    
    comment = ""
    try:
        orig_dq_zip_code_df = pd.read_excel(dq_zip_code_file_path,dtype = {'Account Orig Zip': str})
    except:
        print("Please enter correct path for Zip Code DQ file")

    if orig_dq_zip_code_df is not None:
        dq_zip_code_df = orig_dq_zip_code_df[1:]                                                      #deleting first empty row

        dq_zip_code_df["Account Orig Zip"] = dq_zip_code_df["Account Orig Zip"].fillna('-')           #Filling blank records with '-' character

        zip_code_lengths = []
        for string in dq_zip_code_df["Account Orig Zip"]:
            zip_code_lengths.append(sum([1 for c in string if c.isdigit()]))                          #Calculating number of digits in each zip code
        zip_code_lengths = list(set(zip_code_lengths))                                                #Extracting unique zip lengths using property of sets

        str(zip_code_lengths).strip('[]')                                                             #Converting list to string to append into comment

        comment = "with " + str(zip_code_lengths).strip('[]') + " zip format"
        return comment