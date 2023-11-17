import pandas as pd
import numpy as np
from openpyxl import load_workbook

def df_ins_pivot_creation(df_ins, reference_list_df, data_month, data_month_year, start_month, start_year):
    print(1)

    if df_ins is not None:
        df_combined_ins = df_ins

        if reference_list_df is not None:
            reference_list_df = reference_list_df.fillna('')

            # Following if conditions will traverse reference list column wise, and join with main Ins data if the column exists, thus applying filters.
            # Requires a list to be maintained for each file.

            if len(reference_list_df[reference_list_df['Account Name']!='']['Account Name']) != 0:
                df_combined_ins.rename(columns = {'NAME':'Account Name'}, inplace = True)
                df_combined_ins = pd.merge(df_combined_ins, reference_list_df[reference_list_df['Account Name']!='']['Account Name'], on='Account Name', how='inner')
            if len(reference_list_df[reference_list_df['ID1_VALUE.1']!='']['ID1_VALUE.1']) != 0:
                df_combined_ins = pd.merge(df_combined_ins, reference_list_df[reference_list_df['ID1_VALUE.1']!='']['ID1_VALUE.1'], on='ID1_VALUE.1', how='inner')
            if len(reference_list_df[reference_list_df['CITY']!='']['CITY']) != 0:
                df_combined_ins = pd.merge(df_combined_ins, reference_list_df[reference_list_df['CITY']!='']['CITY'], on='CITY', how='inner')
            if len(reference_list_df[reference_list_df['STATE']!='']['STATE']) != 0:
                df_combined_ins = pd.merge(df_combined_ins, reference_list_df[reference_list_df['STATE']!='']['STATE'], on='STATE', how='inner')

        #Sender Name Filter
        df_combined_ins = df_combined_ins[df_combined_ins['SENDER_NAME'].isin(['Amerisource','BIOCARE','Cardinal','CARDSD','Cesar Castillo','CIBD','CuraScript','DMS','Dakota Drug','FFF','HARVARD','Henry Schien','ICS','McKesson','OTN','Mckesson PB','Metro Medical','Morris and Dickson','MUTUALDRUG','Prescription Supply','Rochester Drug Cooperative','SMITH','TAP','Value Drug'])]
        
        df_combined_ins["month"] = df_combined_ins.FILE_CONTENT_START_DATE.dt.month
        df_combined_ins["year"] = df_combined_ins.FILE_CONTENT_START_DATE.dt.year

        #converting date format into a standard format and storing it in a new column
        df_combined_ins['DATE'] = pd.to_datetime(pd.to_datetime(df_combined_ins['FILE_CONTENT_START_DATE']).dt.strftime('%Y-%m-%d'))

        start_check_date = str(start_year) + '-' + str(start_month) + '-01'
        
        if data_month in [1,3,5,7,8,10,12]:
            end_check_date = str(data_month_year) + '-' + str(data_month) + '-31'
        elif data_month in [2]:
            end_check_date = str(data_month_year) + '-' + str(data_month) + '-28'
        else:
            end_check_date = str(data_month_year) + '-' + str(data_month) + '-30'

        #Ins Data Pivot creation, need to substitute hardcoded values
        pivot = pd.pivot_table(df_combined_ins.loc[(df_combined_ins['DATE'] >= start_check_date) & (df_combined_ins['DATE'] <= end_check_date)], values='QTY_DISPENSED', index='NDC_NBR', columns=['year','month'], aggfunc=np.sum,fill_value=0)
        pivot = pivot.reset_index()
        pivot = pivot.set_index(pivot.columns[0])

        if 'ID1_VALUE.1' in df_combined_ins.columns:
            branch_pivot = pd.pivot_table(df_combined_ins.loc[(df_combined_ins['DATE'] >= start_check_date) & (df_combined_ins['DATE'] <= end_check_date)], values='QTY_DISPENSED', index='ID1_VALUE.1', columns=['year','month'], aggfunc=np.sum,fill_value=0)
        elif 'ID1_VALUE_1' in df_combined_ins.columns:
            branch_pivot = pd.pivot_table(df_combined_ins.loc[(df_combined_ins['DATE'] >= start_check_date) & (df_combined_ins['DATE'] <= end_check_date)], values='QTY_DISPENSED', index='ID1_VALUE_1', columns=['year','month'], aggfunc=np.sum,fill_value=0)
        
        return pivot, branch_pivot, df_combined_ins
    
    else:
        # Returning None if Combined Ins Data is not provided
        return None, None, None


def df_outs_pivot_creation(data_month_year, data_month, df_outs_raw):
    print(2)
    df_outs = df_outs_raw.copy()

    df_outs.columns = df_outs.loc[1]
    df_outs = df_outs.drop(df_outs.index[0:2])
    df_outs = df_outs.fillna(0)

    #Creating column in main Outs table to indicate which NDCs have trend break
    for column in df_outs.columns:                  #Compares with columns in BO Table
        if type(column) is not str:
            if column.year == data_month_year and column.month == data_month:
                df_outs["Trend_Break"] = df_outs.apply(lambda val: True if val[column] < val["MIN"] or val[column] > val["MAX"] or val[column] == 0 else 0, axis=1)
    
    df_outs = df_outs.set_index(df_outs.columns[0])

    return df_outs

def sap_ins_pivot_creation(sap_ins_df, sap_filter_list_df, data_month, data_month_year, start_month, start_year):
    print(3)
    if sap_ins_df is not None:
        sap_ins_df["Month"] = sap_ins_df.SHIP_DATE.dt.month
        sap_ins_df["Year"] = sap_ins_df.SHIP_DATE.dt.year
        #
        start_check_date = str(start_year) + '-' + str(start_month) + '-01'
        
        if data_month in [1,3,5,7,8,10,12]:
            end_check_date = str(data_month_year) + '-' + str(data_month) + '-31'
        else:
            end_check_date = str(data_month_year) + '-' + str(data_month) + '-30'

        if sap_filter_list_df is not None:
            sap_filter_list_df = sap_filter_list_df.fillna('')

            # Following if conditions will traverse reference list column wise, and join with main Ins data if the column exists, thus applying filters.
            # Requires a list to be maintained for each file.

            if len(sap_filter_list_df[sap_filter_list_df['SHIP_TO_PARTY']!='']['SHIP_TO_PARTY']) != 0:
                sap_ins_df = pd.merge(sap_ins_df, sap_filter_list_df[sap_filter_list_df['SHIP_TO_PARTY']!='']['SHIP_TO_PARTY'], on='SHIP_TO_PARTY', how='inner')
            if len(sap_filter_list_df[sap_filter_list_df['SOLD_TO_PARTY']!='']['SOLD_TO_PARTY']) != 0:
                sap_ins_df = pd.merge(sap_ins_df, sap_filter_list_df[sap_filter_list_df['SOLD_TO_PARTY']!='']['SOLD_TO_PARTY'], on='SOLD_TO_PARTY', how='inner')
            
        # Filter Names (ship_to and sold_to) to be replaced below
        sap_ins_pivot = pd.pivot_table((sap_ins_df.loc[(sap_ins_df['SHIP_DATE'] >= start_check_date) & (sap_ins_df['SHIP_DATE'] <= end_check_date)]), values='SALES_UNIT', index='NATIONAL_DRUG_CODE', columns=['Year','Month'], aggfunc=np.sum,fill_value=0)

        return sap_ins_pivot
    else:
        return None


def bo_and_sap_analysis(pivot, df_outs, sap_ins_pivot, supplier_name, output_path):
    print(4)
    
    #List of Sap only analysis suppliers
    sap_only_suppliers = ["NYBC", "CIBD", "SUPERIOR BIOLOGICS"]

    total_ins_df = None
    #Combined Ins data preparation:
    #'pivot' will remain 'None' if it is 'None'
    if pivot is not None:
        NDC_pivot = list(pivot.index)

        #Calculating Sum of Quantity Dispensed for each NDC in Ins Pivot
        l = []
        for i in NDC_pivot:
            ins_sum = 0
            for j in pivot.columns.levels[0][0:2]:     # 'pivot' is Multi-index dataframe, so data is extracted by referencing each column level; This method is adopted because data types vary in source data
                ins_sum += sum(pivot[j].loc[i])        # 'pivot.columns[i][0]': Year
            l.append(ins_sum)

        total_ins_df = pd.DataFrame(l, index = NDC_pivot, columns = ['Sum of QTY_DISPENSED'])
        # print(total_ins_df)

    #Sapins data preparation:
    #Calculating Sum of Sales_Unit for each NDC in SAP_Ins Pivot
    if sap_ins_pivot is not None:
        NDC_pivot_sapins = list(sap_ins_pivot.index)
        s = []
        for i in NDC_pivot_sapins:
            sap_ins_sum = 0
            for j in sap_ins_pivot.columns.levels[0][0:2]:
                sap_ins_sum += sum(sap_ins_pivot[j].loc[i])        # 'pivot.columns[i][0]': Year
            s.append(sap_ins_sum)
        
        total_sap_ins_df = pd.DataFrame(s, index = NDC_pivot_sapins, columns = ['Sum of SALES_UNIT'])
    
    #Outs data preparation:
    #Extracting Indices of Ins and Outs table into lists
    NDC_Outs = list(df_outs.index)

    #Below df to be used when needed
    MIN_MAX_df = df_outs[['MIN','MAX']]

    #Creating copy of Outs dataframe for different analysis
    df_outs_copy = df_outs.drop(["MIN","MAX","Trend_Break"], axis = 1)
    df_outs_copy = df_outs_copy.fillna(0)

    #Calculating Sum of Quantity Reported for each NDC in Outs
    o = []
    for i in NDC_Outs:
        outs_sum = 0
        for j, column in enumerate(df_outs_copy.loc[i]):
            outs_sum += df_outs_copy.loc[i][j]
        o.append(outs_sum)

    total_outs_df = pd.DataFrame(o, index = NDC_Outs, columns = ['Sum of QTY_DISPENSED'])
    total_outs_df.index.name = "NDC"

    #'df_outs_modified' : this df contains 'Trend_Break' and 'Number of recent Blank Months'
    df_outs_modified = df_outs.drop(["MIN","MAX"], axis = 1)
    df_outs_modified["Number_of_zeroes"] = None
    df_outs_modified["Number_of_zeroes"] = (df_outs_modified.iloc[:,-7:-2] == 0).sum(axis=1)    # Hardcoded index is used, might raise issues in future

    #'df_trend_break' : this df contains only those NDCs which are required to be analyzed further
    if supplier_name in sap_only_suppliers:
        df_trend_break = pd.merge(total_outs_df,df_outs_modified,on="NDC")
    else:
        df_trend_break = pd.merge(total_outs_df,df_outs_modified,on="NDC").query("Trend_Break == True and Number_of_zeroes <= 4")
    
    df_trend_break = pd.DataFrame(df_trend_break["Sum of QTY_DISPENSED"])   # inserting only 'Sum of QTY_DISPENSED' column

    #Following block of code is for calculating month wise variance
    df_outs_only = df_outs_modified.drop(["Trend_Break","Number_of_zeroes"],axis=1)
    df_variance = pd.DataFrame().reindex_like(df_outs_only).fillna(0)

    bo_analysis_df = pd.DataFrame()                    #creating new empty dataframe

    for ndc in df_trend_break.index:
        if sap_ins_pivot is None and pivot is not None:                      #For Suppliers with Sapins
            try:
                ins_list = list(pivot.loc[int(ndc)])
            except:
                ins_list = [0]*13                      #ins not present, appending 0's
        else:
            #Following will calculate month wise sum of Ins and Sapins (whichever is present), for each ndc with trend break
            try:
                if pivot is not None and int(ndc) in pivot.index and supplier_name not in sap_only_suppliers:
                    if int(ndc) in sap_ins_pivot.index:
                        ins_list = list(pivot.loc[int(ndc)] + sap_ins_pivot.loc[int(ndc)])
                    else:
                        ins_list = list(pivot.loc[int(ndc)])
                else:
                    ins_list = list(sap_ins_pivot.loc[int(ndc)])
            except:
                ins_list = [0]*13
        
        #outs sum month wise
        outs_list = list(df_outs_only.loc[ndc])
        
        result = [a - b for a, b in zip(outs_list, ins_list)]
        df_variance.loc[ndc] = result
        
        #Creating Outs and Ins Analysis table:
        multiindex_arrays = [
        np.array([ndc, ndc, ndc]),
        np.array(["OUTs", "Total Ins", "Variance"]),
        ]
        
        #Creating multiindex dataframe with Outs, Ins, Variance for each NDC:
        try:
            bo_analysis_df = pd.concat([bo_analysis_df,pd.DataFrame([outs_list,ins_list,result], index = multiindex_arrays)])
        except:
            print(ndc + " not inserted into BO Analysis Table")
    
    #Setting columns in newly created multiindex dataframe
    bo_analysis_df.columns = df_outs_copy.columns

    #Calculating variance percentage (Outs/Ins) for trend break NDCs
    Sum_outs = 0
    Sum_ins = 0
    NDCs = []
    Percentage = []
    bo_analysis_df["Percentage"] = ""
    bo_analysis_df["Comment"] = ""

    for i in df_trend_break.index:
        if sap_ins_pivot is None and pivot is not None:
            try:
                Sum_ins = int(total_ins_df.loc[int(i)])
            except:
                print(str(i) + " - ins not present")
        else:
            #Following will calculate total sum of Ins and Sapins (whichever is present), for each ndc with trend break
            try:
                if pivot is not None and int(i) in total_ins_df.index and supplier_name not in sap_only_suppliers:
                    if int(i) in total_sap_ins_df.index:
                        Sum_ins = int(total_ins_df.loc[int(i)])+int(total_sap_ins_df.loc[int(i)])
                    else:
                        Sum_ins = int(total_ins_df.loc[int(i)])
                else:
                    Sum_ins = int(total_sap_ins_df.loc[int(i)])
            except:
                print(str(i) + " - ins not present")

        Sum_outs = int(df_trend_break.loc[i])
        NDCs.append(i)
        try:
            Percentage.append(int(round((Sum_outs/Sum_ins)*100,0)))
        except:
            Percentage.append(0)                                    #Appending 0 if ins are not present
            print(str(i) + " - Ins = 0")

    for iter, ndc in enumerate(NDCs):
        #Adding Percentage column to bo_analysis_df
        bo_analysis_df["Percentage"].loc[ndc,"OUTs"] = Percentage[iter]
    
    # Final_Output = pd.DataFrame(Percentage,index=NDCs,columns = ["Percentage"])

    #dropping NDCs from 'df_variance' which are not required
    for idx in df_variance.index:
        if idx not in df_trend_break.index:
            df_variance = df_variance.drop(idx,axis = 'index')

    #Adding Comments for 'Pass' cases
    # Final_Output["Comment"] = None

    variance_list = []

    for ndc in df_trend_break.index:
        if bo_analysis_df["Percentage"].loc[ndc,"OUTs"] >= 95 and bo_analysis_df["Percentage"].loc[ndc,"OUTs"] <= 105:
            if all(var == 0 for var in list(df_outs_only.loc[ndc])[:-1]):
                bo_analysis_df["Comment"].loc[ndc,"OUTs"] = "New NDC " + str(ndc) + ", pass via Sellsins"
            else:
                bo_analysis_df["Comment"].loc[ndc,"OUTs"] = "Pass"

        elif bo_analysis_df["Percentage"].loc[ndc,"OUTs"] == 0:
            bo_analysis_df["Comment"].loc[ndc,"OUTs"] = "SellsIns seem incomplete"
        else:
            variance_list = list(df_variance.loc[ndc])
            if bo_analysis_df["Percentage"].loc[ndc,"OUTs"] <=50 or bo_analysis_df["Percentage"].loc[ndc,"OUTs"] >=150:
                bo_analysis_df["Comment"].loc[ndc,"OUTs"] += "Very high variance. "

            if variance_list[-1] == 0:
                bo_analysis_df["Comment"].loc[ndc,"OUTs"] += "Pass as inventory remained steady"
            elif variance_list[-1] > 0:
                bo_analysis_df["Comment"].loc[ndc,"OUTs"] += "Pass as inventory went down by " + str(variance_list[-1]) + " this month"
            elif any(var <= variance_list[-1] for var in variance_list[:-1]):
                bo_analysis_df["Comment"].loc[ndc,"OUTs"] += "Pass as similar/higher inventory observed in past"
            elif all(var == 0 for var in variance_list[:-1]):
                bo_analysis_df["Comment"].loc[ndc,"OUTs"] += "New NDC " + str(ndc)
            else:
                bo_analysis_df["Comment"].loc[ndc,"OUTs"] += "Case to be monitored"

    #Appending all required dataframes to csv file
    list_of_dataframes = [bo_analysis_df,pivot,df_outs]
    name_of_dataframes = ['BO Analysis','Combined Ins Pivot','BO Table']
    print("x")
    with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        for i,df in enumerate(list_of_dataframes):
            try:
                df.to_excel(writer, sheet_name=name_of_dataframes[i])
            except:
                print("A DF could not be printed to excel")
    print("z")

def unreported_ndc(ins_pivot, outs_pivot, output_path):
    unreported_ndc_pivot = ins_pivot.copy()

    for ndc in unreported_ndc_pivot.index:
        if str(ndc) in outs_pivot.index:
            unreported_ndc_pivot = unreported_ndc_pivot.drop(ndc, axis = 'index')
    
    ins_pivot["Comment"] = None

    for unr_ndc in unreported_ndc_pivot.index:
        if not str(unr_ndc).startswith('5'):
            ins_pivot.loc[unr_ndc,"Comment"] = "Roche NDC, pass"
        elif list(unreported_ndc_pivot.loc[unr_ndc])[-1] == 0:
            ins_pivot.loc[unr_ndc,"Comment"] = "Pass as no recent purchases"
        elif list(unreported_ndc_pivot.loc[unr_ndc])[-1] < 0:
            ins_pivot.loc[unr_ndc,"Comment"] = "Pass as return volumes observed"
        else:
            ins_pivot.loc[unr_ndc,"Comment"] = "Need to Email POC"
    
    with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        ins_pivot.to_excel(writer, sheet_name="Unreported NDCs")

    return ins_pivot

def unreported_branches(df_combined_ins,ins_branch_pivot,branch_pivot, output_path):
    
    # Extract the first column of result_selected_data
    result_selected_data_first_column = branch_pivot.index.tolist()
    # Extract the first column of ins_pivot_branch
    ins_pivot_branch_first_column = ins_branch_pivot.index.tolist()

    # Check for common values
    common_values = set(result_selected_data_first_column) & set(ins_pivot_branch_first_column)

    # Add a new column in ins_pivot_branch with the comment "trending" or "not trending"
    ins_branch_pivot['Trend_Status'] = 'Unreported'
    ins_branch_pivot.loc[ins_branch_pivot.index.isin(common_values), 'Trend_Status'] = ''

    # Total sum column
    ins_branch_pivot['Total'] = 0
    # Create a new column 'Comment' based on Total value
    ins_branch_pivot['Comment'] = ''
    # Create Address column
    ins_branch_pivot['Address'] = ''

    ins_branch_pivot['Total'] = ins_branch_pivot.iloc[:, :-1].sum(axis=1)
    ins_branch_pivot.loc[(ins_branch_pivot['Total'] < 100) & (ins_branch_pivot['Trend_Status'] == 'Unreported'), 'Comment'] = 'Pass as low volume'
    ins_branch_pivot.loc[(ins_branch_pivot['Total'] > 100) & (ins_branch_pivot['Trend_Status'] == 'Unreported'), 'Comment'] = 'Over 100'

    # unreported_branches = list(ins_branch_pivot[ins_branch_pivot['Comment'] == 'Over 100'].index)
    unreported_branches = list(ins_branch_pivot.index)

    for branch in unreported_branches:
        if 'ID1_VALUE.1' in df_combined_ins.columns:
            ins_branch_pivot.loc[branch,'Address'] = str(list(df_combined_ins[df_combined_ins['ID1_VALUE.1'] == branch]['ADDRESS1'].unique()))
        elif 'ID1_VALUE_1' in df_combined_ins.columns:
            ins_branch_pivot.loc[branch,'Address'] = str(list(df_combined_ins[df_combined_ins['ID1_VALUE_1'] == branch]['ADDRESS1'].unique()))
        else:
            print('ID1_VALUE_1 or ID1_VALUE.1 column does not exist in Ins data')
    
    with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        ins_branch_pivot.to_excel(writer, sheet_name="Unreported Branches")

    return ins_branch_pivot