import pandas as pd
import numpy as np

def df_ins_pivot_creation(df_ins, reference_list_df, data_month, data_month_year, start_month, start_year):
    
    df_combined_ins = df_ins

    if reference_list_df is not None:
        reference_list_df = reference_list_df.fillna('')

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
    df_combined_ins = df_combined_ins[df_combined_ins['SENDER_NAME'].isin(['Morris and Dickson','Cardinal','CARDSD','BIOCARE','Amerisource','McKesson','OTN'])]
    
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

    return pivot


def df_outs_pivot_creation(data_month_year, data_month, df_outs_raw):
    
    df_outs = df_outs_raw.copy()

    df_outs.columns = df_outs.loc[1]
    df_outs = df_outs.drop(df_outs.index[0:2])
    df_outs = df_outs.fillna(0)

    #Creating column in main Outs table to indicate which NDCs have trend break
    for column in df_outs.columns:                  #Checks columns in BO Table
        if type(column) is not str:
            if column.year == data_month_year and column.month == data_month:
                df_outs["Trend_Break"] = df_outs.apply(lambda val: True if val[column] < val["MIN"] or val[column] > val["MAX"] or val[column] == 0 else 0, axis=1)
    
    df_outs = df_outs.set_index(df_outs.columns[0])

    return df_outs

def sap_ins_pivot_creation(sap_ins_df, data_month, data_month_year, start_month, start_year):
    if sap_ins_df is not None:
        sap_ins_df["Month"] = sap_ins_df.SHIP_DATE.dt.month
        sap_ins_df["Year"] = sap_ins_df.SHIP_DATE.dt.year
        #
        start_check_date = str(start_year) + '-' + str(start_month) + '-01'
        
        if data_month in [1,3,5,7,8,10,12]:
            end_check_date = str(data_month_year) + '-' + str(data_month) + '-31'
        else:
            end_check_date = str(data_month_year) + '-' + str(data_month) + '-30'


        # Filter Names (ship_to and sold_to) to be replaced below
        sap_ins_pivot = pd.pivot_table((sap_ins_df.loc[(sap_ins_df['SHIP_DATE'] >= start_check_date) & (sap_ins_df['SHIP_DATE'] < end_check_date) & ((sap_ins_df['SHIP_TO_PARTY'] == ('WALGREEN SPECIALTY PHARMACY #15443')) | (sap_ins_df['SHIP_TO_PARTY'] == ('WALGREENS SPECIALTY PHARMACY')) | (sap_ins_df['SHIP_TO_PARTY'] == ('WALGREEN LOUISIANA CO., INC.')) | (sap_ins_df['SHIP_TO_PARTY'] == ('JOHNS HOPKINS USFHP AT WALGREENS')) | (sap_ins_df['SHIP_TO_PARTY'] == ('WALGREEN CO.')))]), values='SALES_UNIT', index='NATIONAL_DRUG_CODE', columns=['Year','Month'], aggfunc=np.sum,fill_value=0)

        return sap_ins_pivot


def bo_and_sap_analysis(pivot, df_outs, sap_ins_pivot):
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
    # print(total_ins_df)

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
        # print(total_sap_ins_df)

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
    # print(df_trend_break)

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
            Percentage.append(0)                                    #Appending 0 if ins are not present
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
            if all(var == 0 for var in list(df_outs_only.loc[ndc])[:-1]):
                Final_Output["Comment"].loc[ndc] = "New NDC " + str(ndc) + ", pass via Sellsins"
            else:
                Final_Output["Comment"].loc[ndc] = "Pass"

        elif Final_Output["Percentage"].loc[ndc] == 0:
            Final_Output["Comment"].loc[ndc] = "SellsIns seem incomplete"

        else:
            variance_list = list(df_variance.loc[ndc])
            if variance_list[-1] == 0:
                Final_Output["Comment"].loc[ndc] = "Pass as inventory remained steady"
            elif variance_list[-1] > 0:
                Final_Output["Comment"].loc[ndc] = "Pass as inventory went down by " + str(variance_list[-1]) + " this month"
            elif any(var <= variance_list[-1] for var in variance_list[:-1]):
                Final_Output["Comment"].loc[ndc] = "Pass as similar/higher inventory observed in past"
            elif all(var == 0 for var in variance_list[:-1]):
                Final_Output["Comment"].loc[ndc] = "New NDC " + str(ndc)
            else:
                Final_Output["Comment"].loc[ndc] = "Case to be monitored"

    #Appending all required dataframes to csv file
    list_of_dataframes = [total_ins_df,total_outs_df,df_variance,Final_Output,pivot,df_outs]

    with open('all_dataframes.csv','a') as f:
        for df in list_of_dataframes:
            df.to_csv(f)
            f.write("\n")

def unreported_ndc(ins_pivot, outs_pivot):
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
    
    with open('unreported_ndcs.csv','a') as f:
        ins_pivot.to_csv(f)

    return ins_pivot