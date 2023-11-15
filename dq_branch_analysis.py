import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta

def dq_non_trending_branch_analysis(branch_report_file_path,current_month_branch_dq_file_path,previous_month_branch_dq_file_path):
    
    df = None
    orig_df_current_month_branch_dq = None
    orig_df_previous_month_branch_dq = None

    try:
        df = pd.read_excel(branch_report_file_path)
    except:
        print("Please enter correct path for Branch Report file")
    try:
        orig_df_current_month_branch_dq = pd.read_excel(current_month_branch_dq_file_path)
    except:
        print("Please enter correct path for Current Month Branch DQ file")
    try:
        orig_df_previous_month_branch_dq = pd.read_excel(previous_month_branch_dq_file_path)
    except:
        print("Please enter correct path for Previous Month Branch DQ file")

    if df is not None:
        def transform_columns(column):
            if column == 'Unnamed: 0':
                return 'SENDER NAME'
            elif column == 'Unnamed: 1':
                return 'BRANCH ID'
            elif column == 'Unnamed: 2':
                return 'NDC'
            elif column == 'Unnamed: 3':
                return 'PRODUCT INFO'
            else:
                return column

        # Apply the column transformation
        df.columns = df.columns.map(transform_columns)

        # Remove values after the decimal point using a loop
        new_columns = []
        for col in df.columns:
            if '.' in str(col):
                new_col = str(col).split('.')[0]
                new_columns.append(new_col)
            else:
                new_columns.append(str(col))

        df.columns = new_columns

        #Find Index of ' ' column that will act as separator
        index = list(df.columns).index(' ')

        l = df.iloc[0, index+1:]         #Month names (first row elements)
        m = df.columns[index+1:]         #Years from column Header

        concatenated_columns_list = []
        for i in range(0,len(m)):
            concatenated_columns_list.append(l[i]+m[i])   #simple string concatenation

        unaltered_columns_list = list(df.columns[:index+1])    #Sender_name, Branch ID ....

        columns_list = unaltered_columns_list + concatenated_columns_list    #creating new final column list
        df.columns = columns_list

        # Drop the first row
        df = df.iloc[1:]

        # Drop the 5th column (index 5)
        df = df.drop(df.columns[4], axis=1)

        #df.pivot(index ='A', columns ='B', values =['C', 'A'])
        table = pd.pivot_table(df, values =df.columns[4:], index = 'BRANCH ID',aggfunc = np.sum)

        # Determine the current month as "Sep2023" (September 2023)
        today = datetime.now()-pd.offsets.DateOffset(months=1)
        currentdata_month = today.strftime('%b%Y')

        # Define the sequence of months to extract (13 months)
        sequence = [currentdata_month]
        for _ in range(1, 13):
            today -= relativedelta(months=1)
            sequence.append(today.strftime('%b%Y'))

        # Select the 13 months' data and reorder the columns
        selected_data = table[sequence]

        if orig_df_current_month_branch_dq is not None and orig_df_previous_month_branch_dq is not None:
            # df_current_month_branch_dq
            df_current_month_branch_dq = orig_df_current_month_branch_dq[1:]
            df_previous_month_branch_dq = orig_df_previous_month_branch_dq[1:]
            non_trending_branches = list(set(df_current_month_branch_dq["_BRANCH_DEA_ID"]) ^ set(df_previous_month_branch_dq["_BRANCH_DEA_ID"]))

            # Function to check if a branch is only non-null in the current month and null in the remaining months
            def check_reported_first_time(row):
                #current_month = 'Sep2023'
                other_months = sequence[1:]
                
                if row[currentdata_month] != 0 and all(row[other] == 0 for other in other_months):
                    return 'Reported for the first time'
                elif row[currentdata_month] == 0 and all(row[other] != 0 for other in other_months):
                    return 'Missing for the first time'
                else:
                    return ''

            # Apply the function to each row
            selected_data.loc[:, 'Comment']  = selected_data.apply(check_reported_first_time, axis=1)

            #Comment formation
            comment = ''
            for branch in non_trending_branches:
                if selected_data.loc[branch]['Comment']=="":
                    if selected_data.loc[branch][currentdata_month] == 0:
                        if 0 < selected_data[selected_data.columns[1:]].loc[branch].astype(bool).sum(axis=0) < 12:
                            comment = "Missing sales again"
                    elif selected_data.loc[branch][currentdata_month] != 0:
                        if 0 < selected_data[selected_data.columns[1:]].loc[branch].astype(bool).sum(axis=0) < 12:
                            comment = "Reported Sales again"
                    # print(comment)
                    selected_data.loc[branch,'Comment'] = comment

        #Export data to file
        selected_data.to_csv(r"C:\Users\pragyan.agrawal\OneDrive - Incedo Technology Solutions Ltd\Desktop\output2.csv")