#Takes two data sources: 
# Combined Ins File
# Combined Outs File
#, and outputs results to another file
#
#SAP Analysis to be implemented
#Implement Name Filter through google sheets
#This code assumes that Ins and Outs data is to be analyzed for 13 months
#code will not work for files such as 'Qualitas Pharmacy' which has only 4 months in BO, can include in future scope for improvement


#IMP: Need to include more exception handling

import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date

df = None
reference_list_df = None
df_outs = None
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
    df_outs = pd.read_excel(r"C:\Users\pragyan.agrawal\Downloads\Humana BO.xlsx", skiprows=1, usecols = 'B:Q')  #skipping first row and first column
except:
    print("Enter correct file path for BO Table File")
try:
    sap_ins_df = pd.read_excel(r"C:\Users\pragyan.agrawal\Downloads\SAP Data SEP'23.xlsx",sheet_name=1)
except:
    print("Please enter correct path for Sapins file")


if df is not None and df_outs is not None:
    if reference_list_df is not None:
        df.rename(columns = {'NAME':'Account Name'}, inplace = True)
        df_combined_ins = pd.merge(df, reference_list_df, on='Account Name', how='inner')
    else:
        df_combined_ins = df
    
    sap_ins_pivot = None
    if sap_ins_df is not None:
        sap_ins_df["Month"] = sap_ins_df.SHIP_DATE.dt.month
        sap_ins_df["Year"] = sap_ins_df.SHIP_DATE.dt.year
        #
        # Filter Names (ship_to and sold_to) to be replaced below
        sap_ins_pivot = pd.pivot_table((sap_ins_df.loc[(sap_ins_df['SHIP_DATE'] >= '2022-09-01') & (sap_ins_df['SHIP_DATE'] <'2023-09-30') & ((sap_ins_df['SHIP_TO_PARTY'] == ('CENTERWELL PHARMACY')) | (sap_ins_df['SHIP_TO_PARTY'] == ('CENTERWELL PHARMACY INC')))]), values='SALES_UNIT', index='NATIONAL_DRUG_CODE', columns=['Year','Month'], aggfunc=np.sum,fill_value=0)

    df_combined_ins["month"] = df_combined_ins.MOST_RECENT_SHIP_DATE.dt.month
    df_combined_ins["year"] = df_combined_ins.MOST_RECENT_SHIP_DATE.dt.year

    #Get Data month, and 13 months before data month
    today = date.today()
    data_month = today.month - 1
    data_month_year = (date.today() - pd.offsets.DateOffset(months=1)).year
    start_month = (date.today() - pd.offsets.DateOffset(months=13)).month
    start_year = (date.today() - pd.offsets.DateOffset(months=13)).year


    #Ins Data Pivot creation, need to substitute hardcoded values
    pivot = pd.pivot_table(df_combined_ins.loc[(df_combined_ins['MOST_RECENT_SHIP_DATE'] >= '2022-09-01') & (df_combined_ins['MOST_RECENT_SHIP_DATE'] <'2023-09-30')], values='QTY_DISPENSED', index='NDC_NBR', columns=['year','month'], aggfunc=np.sum,fill_value=0)
    pivot = pivot.reset_index()
    print(data_month_year,data_month)

    #Cleaning Outs data source
    df_outs.columns = df_outs.loc[1]
    df_outs = df_outs.drop(df_outs.index[0:2])
    df_outs = df_outs.fillna(0)

    #Creating column in main Outs table to indicate which NDCs have trend break
    for column in df_outs.columns:                  #Checks columns in BO Table
        if type(column) is not str:
            if column.year == data_month_year and column.month == data_month:
                df_outs["Trend_Break"] = df_outs.apply(lambda val: True if val[column] < val["MIN"] or val[column] > val["MAX"] or val[column] == 0 else 0, axis=1)

    #Setting index as 'NDC' in both ins and outs table
    pivot = pivot.set_index(pivot.columns[0])
    df_outs = df_outs.set_index(df_outs.columns[0])

    #Extracting Indices of Ins and Outs table into lists
    NDC_Outs = list(df_outs.index)
    NDC_pivot = list(pivot.index)

    #Calculating Sum of Quantity Dispensed for each NDC in Ins Pivot
    l = []
    for i in NDC_pivot:
        ins_sum = 0
        for j in pivot.columns.levels[0][0:2]:     # 'pivot' is Multi-index dataframe, so data is extracted by referencing each column level; This method is adopted because data types vary in source data
            ins_sum += sum(pivot[j].loc[i])        # 'pivot.columns[i][0]': Year
        l.append(ins_sum)

    total_ins_df = pd.DataFrame(l, index = NDC_pivot, columns = ['Sum of QTY_DISPENSED'])
    print(total_ins_df)

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
        print(total_sap_ins_df)

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
    df_trend_break = pd.merge(total_outs_df,df_outs_modified,on="NDC").query("Trend_Break == True and Number_of_zeroes <= 4")
    df_trend_break = pd.DataFrame(df_trend_break["Sum of QTY_DISPENSED"])   # inserting only 'Sum of QTY_DISPENSED' column
    print(df_trend_break)

    #Following block of code is for calculating month wise variance
    df_outs_only = df_outs_modified.drop(["Trend_Break","Number_of_zeroes"],axis=1)
    df_variance = pd.DataFrame().reindex_like(df_outs_only).fillna(0)
    for ndc in df_trend_break.index:
        if sap_ins_pivot is None:                      #For Suppliers with Sapins
            try:
                ins_list = list(pivot.loc[int(ndc)])
            except:
                ins_list = [0]*13                      #ins not present, appending 0's
        else:
            #Following will calculate month wise sum of Ins and Sapins (whichever is present), for each ndc with trend break
            try:
                if int(ndc) in pivot.index:
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

    #Calculating variance percentage (Outs/Ins) for trend break NDCs
    Sum_outs = 0
    Sum_ins = 0
    NDCs = []
    Percentage = []

    for i in df_trend_break.index:
        if sap_ins_pivot is None:
            try:
                Sum_ins = int(total_ins_df.loc[int(i)])
            except:
                print(str(i) + " - ins not present")
        else:
            #Following will calculate total sum of Ins and Sapins (whichever is present), for each ndc with trend break
            try:
                if int(i) in total_ins_df.index:
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
            print(str(i) + " - Ins = 0")

    Final_Output = pd.DataFrame(Percentage,index=NDCs,columns = ["Percentage"])

    #dropping NDCs from 'df_variance' which are not required
    for idx in df_variance.index:
        if idx not in df_trend_break.index:
            df_variance = df_variance.drop(idx,axis = 'index')

    #Adding Comments for 'Pass' cases
    Final_Output["Comment"] = None

    variance_list = []

    for ndc in df_trend_break.index:
        if Final_Output["Percentage"].loc[ndc] >= 95 and Final_Output["Percentage"].loc[ndc] <= 105:
            Final_Output["Comment"].loc[ndc] = "Pass"
        else:
            variance_list = list(df_variance.loc[ndc])
            if variance_list[-1] == 0:
                Final_Output["Comment"].loc[ndc] = "Pass as inventory remained steady"
            elif variance_list[-1] > 0:
                Final_Output["Comment"].loc[ndc] = "Pass as inventory went down by " + str(variance_list[-1]) + " this month"
            elif any(var <= variance_list[-1] for var in variance_list[:-1]):
                Final_Output["Comment"].loc[ndc] = "Pass as similar/higher inventory observed in past"
            else:
                Final_Output["Comment"].loc[ndc] = "Case to be monitored"

    print(Final_Output)


    #Appending all required dataframes to csv file
    list_of_dataframes = [total_ins_df,total_outs_df,df_variance,Final_Output]

    with open('all_dataframes.csv','a') as f:
        for df in list_of_dataframes:
            df.to_csv(f)
            f.write("\n")