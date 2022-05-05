from importlib.resources import path
import pandas as pd
import numpy as np
import datetime 
from datetime import date
import os
from flask import Flask, redirect, render_template, request, redirect, url_for, request, Response, make_response, abort
from werkzeug.utils import secure_filename
from werkzeug.datastructures import  FileStorage
from os import getcwd
import io
#import xlrd
import calendar
from werkzeug.exceptions import HTTPException

##############################################################################################################################################################################
def process_data(df_query, df, df_currency, df_le, df_cost_center):
    pass
    base_month = date.today().month - 1
    if date.today().day < 15:
        base_month = date.today().month - 2
    
    
    if base_month == 0:
        currentMonth = '12'
        currentYear = str(date.today().year - 1)
    else:
        currentMonth = str(base_month).zfill(2)
        currentYear = str(date.today().year)
     


    payment_col = calendar.month_abbr[date.today().month].upper() + ' ' + str(date.today().year)




  
    path_statics = ".\\Statics_file\\"


    df_usa = pd.read_excel(path_statics + 'USA CC APPROACH.xlsx')
    df_ssc = pd.read_excel(path_statics + 'SAP-LE.xlsx', sheet_name= 'Company Codes')
    df_ag = pd.read_excel(path_statics + 'AG cars.xlsx')




    
    # ## Give the correct format to the columns
    # give format to query
    relevant_col_query = ['VIN', 'Legal Entity', 'Status', 'AssetNumber', 'SAP Cost Center', 'System', 'Start Date', 'End Date', 'Currency', payment_col]
    cell_range = range(50)
    for  i in cell_range: 
        if str(df_query.iloc[i, 3]) != 'nan':
            header_n = i
            break
    if header_n > 0:
        new_header = df_query.iloc[header_n] 
        df_query = df_query[header_n + 1:] 
        df_query.columns = new_header
        df_query = df_query.reset_index(drop=True)
        df_query.columns.name = None
    df_query = df_query[relevant_col_query]
    drop_condition_query = df_query[(df_query['VIN'] == 'VIN') | (df_query['VIN'].isnull())].index
    df_query = df_query.drop(drop_condition_query)
    df_query = df_query.reset_index(drop=True)


    # Before format company code, identify null company code and date
    df['Null Company Code'] = np.where(df['Company Code'].isnull(), "X", "")

    df['End_year'] =  pd.to_numeric(df['Contract End Date'].str[-4:], downcast='integer')
    df['Start_year'] =  pd.to_numeric(df['Contract Start Date'].str[-4:], downcast='integer')

    cond1 = (df['End_year'] > 2050) | (df['End_year'] < 1980)
    cond2 = (df['Start_year'] > 2050) | (df['Start_year'] < 1980)

    df['Contract End Date'] = np.where(cond1, np.nan, df['Contract End Date'])
    df['Contract Start Date'] = np.where(cond2, np.nan, df['Contract Start Date'])

    del(df['End_year'])
    del(df['Start_year'])



    df[['Payments (monthly)']] = df[['Payments (monthly)']].astype(str)
    df['Payments (monthly)'] = df[['Payments (monthly)']].applymap(lambda x: str(x.replace(',','.')))
    df[['Payments (monthly)']] = df[['Payments (monthly)']].astype(float)
    df['Company Code'] = df['Company Code'].astype(str)
    df['Company Code'] = df['Company Code'].str.split('.').str[0]
    df['Company Code'] = df['Company Code'].str.zfill(4)
    df_usa['Ccode'] = df_usa['Ccode'].astype(str)
    df_usa['Ccode'] = df_usa['Ccode'].str.zfill(4)
    df['Contract Duration(Months)'].fillna(0, inplace=True)
    df['Contract Duration(Months)'] = df['Contract Duration(Months)'].astype(int)
    df['Contract Start Date'] = pd.to_datetime(df['Contract Start Date'],infer_datetime_format = True)                                         
    df['Contract End Date'] = pd.to_datetime(df['Contract End Date'],infer_datetime_format = True) 
    df_query['Start Date'] = pd.to_datetime(df_query['Start Date'],infer_datetime_format = True)
    df_query['End Date'] = pd.to_datetime(df_query['End Date'],infer_datetime_format = True)
    df_query.rename(columns={payment_col : 'Payments'}, inplace=True)
    #df_query.columns = [*df_query.columns[:-1], 'Payments']
    df_query['Legal Entity'] = df_query['Legal Entity'].astype(str)
    df_query['Legal Entity'] = df_query['Legal Entity'].str.split('.').str[0]
    df_query['Legal Entity'] = df_query['Legal Entity'].str.zfill(4)
    df_currency.columns = [*df_currency.columns[:-1], 'Rates']



    # create dictionary for currency rates --------------- calculate difference with exchange rates
    currency_dict = dict(zip(df_currency['ISO'], df_currency['Rates']))

    
    # ## Manipulation

    
    # #### Change all German LE to LE2000


    df['Country'][df['Company Code'] == '2000'] = 'DE'
    df['Cost Center'][df['Company Code'] == '2000'] = ''
    df['Division / Subgroup'][df['Company Code'] == '2000'] = ''

    
    # ### Look for missing values


    #df['CC missing'] = np.where(df['Cost Center'].isnull(), 'X', '')
    #df['Payments in EUR'] = df['Payments (monthly)'] / [currency_dict[x] if x != 'EUR' else 1 for x in df['Currency']]

    #df['Payments missing / below 50'] = np.where((df['Payments (monthly)'].isnull()) | (df['Payments (monthly)'] <= 50), 'X', '')

    df['Payments missing'] = np.where((df['Payments (monthly)'].isnull()) | (df['Payments (monthly)'] <= 0), 'X', '')


    df['Ended in the past'] = np.where((df['Contract End Date'].dt.month <= datetime.datetime.now().month) & (df['Contract End Date'].dt.year <= datetime.datetime.now().year), 'X', '')

    df['Start/End missing '] = np.where(df['Contract Start Date'].isnull() | df['Contract End Date'].isnull() , 'X', '')

    
    # ### Delete those obsolete Company Code from LE list dataframe


    df_le['CCode'] = df_le['CCode'].str[1:5]
    df_le_obsolete = df_le[df_le['CA'] == 'B']
    df_le_valid = df_le[df_le['CA'] == 'A']


    df = df[-df['Company Code'].isin(df_le_obsolete['CCode'])]

    
    #  Exclude AG cars


    df_ag['VIN'] = df_ag['Contract ID'].str[-17:]
    df = df[-df['VIN'].isin(df_ag['VIN'])]

    
    # ### Removing values


    df = df[(df['Car Policy Type'] != 'Employee Model') & (df['Car Policy Type'] != 'Pharmacy Car')]
    df = df[(df['Lessor'] != 'Allowance Car') & (df['Lessor'] != 'Wheels')]
    df = df[(df['Country'] != 'JP') & (df['Country'] != 'LV') & (df['Country'] != 'EE')]


    df['Contract Start Date'][(df['Contract Start Date'] < '2021-01-01') & (df['Country'] == 'CA')] = datetime.datetime(2021, 1, 1)
    df['Contract Start Date'][(df['Contract Start Date'] < '2020-09-01') & (df['Country'] == 'HU')] = datetime.datetime(2020, 9, 1)
    df['Contract Start Date'][(df['Contract Start Date'] < '2021-02-01') & (df['Country'] == 'AU')] = datetime.datetime(2021, 2, 1)




    df['Company Code'][df['Country'] == 'TU'].fillna('0085', inplace = True) 
    df['Country'] = np.where(df['Country'] == 'TU', 'MA', df['Country'])

    df = df[(df['Company Code'] != '1925') & (df['Company Code'] != '2476')]
    df = df[(df['Country'] != 'CZ') & (df['Country'] != 'SK')]


    
    # #### Change CC according table


    df['Cost Center'][df['Company Code'] == '0201'] = 'SZBSCS9007'
    df['Cost Center'][df['Company Code'] == '1611'] = 'VQ62600002'
    df['Cost Center'][df['Company Code'] == '1994'] = '6Z90910002' 

    
    # #### Check currency and amounts


    # replace currency for Chile CLP instead of CPL
    df['Currency'] = np.where(((df['Country'] == 'CL') & (df['Currency'] == 'CPL')), 'CLP', df['Currency'])


    df['Payments (monthly) in EUR'] = df['Payments (monthly)'] / [currency_dict[x] if ((x != 'EUR') & (x != 'na')) else 1 for x in df['Currency']]


    df['GB Car not in GBP'] = np.where((df['Country'] == 'GB') & (df['Currency'] != 'GBP'), 'X', '')
    df['Amount higher than 1K'] = np.where((df['Payments (monthly) in EUR'] > 1000) & (df['Currency'] != 'na'), 'X', '')
    df['Wrong VIN'] = np.where((df['VIN'].apply(lambda x: len(x) < 17)) & (-df['Country'].isin(['TW', 'PK', 'PE', 'PH', 'MA'])), 'X', '')
    del(df['Payments (monthly) in EUR'])


    # delete blank spaces for cc
    df['Cost Center'] = df['Cost Center'].replace(' ', '', regex=True)


    pd.set_option('display.max_columns', None)

    
    # #### Change USA companies


    ccode_list = df_usa['Ccode']
    cc_list = df_usa['Cost Center']

    y = 0
    for le in ccode_list:
        df['Cost Center'] = np.where(df['Company Code'] == le, cc_list[y], df['Cost Center'])
        y = y + 1

    df['Company Code'][df['Company Code'].isin(ccode_list)] = '1372'

    
    # ### Combine with Query


    df_query['LE_VIN'] = df_query['Legal Entity'] + '_' +df_query['VIN']  


    # combine with vin and LE
    df_query['Payments'].fillna(0, inplace=True)
    vin_list = df_query['VIN'].to_list()
    le_vin_list = df_query['LE_VIN'].to_list()


    df['Exact Car Exist in H2R'] = np.where(df['VIN'].isin(vin_list), "X", "")

    #df['Exact Car Exist in H2R'] = np.where((df['Company Code'] + '_' + df['VIN']).isin(le_vin_list), "X", "")


    # Here I create a new DF with those that not appear in the query
    new_cars = df[df['Exact Car Exist in H2R'] == ""]
    old_cars = df[df['Exact Car Exist in H2R'] == "X"]


    # If the duration is shorter than 12 months (include) we should exclude them --> New Cars
    cond_for_old_contracts = (((new_cars['Contract End Date'].dt.month > datetime.datetime.now().month) & (new_cars['Contract End Date'].dt.year == datetime.datetime.now().year)) | (new_cars['Contract End Date'].dt.year > datetime.datetime.now().year))
    new_cars = new_cars[cond_for_old_contracts]
    new_cars = new_cars[new_cars['Contract Duration(Months)'] > 12]


    # Take out those vehicules with null cc, company code o payments --> New Cars
    #new_cars = new_cars[-((new_cars['Company Code'].isnull()) | (new_cars['Cost Center'].isnull()) | (new_cars['Payments (monthly)'].isnull()))]

    
    # # NEW CARS MANIPULATION

    
    # ### Combine CC with new cars data frame


    cc_dict_DE = {'UI20792610' : 'CF', 'UI20792620' : 'Employee cars = out of scope', 'UIX0000005' : 'CS', 'UIX0000006' : 'PH', 'UIX0000007' : 'CH', 'UIX0000008' : 'PH', 'UIX0000009' : 'CS', 'UIX0000010' : 'PH', 'UIX0000013' : 'CS'}
    cc_list_DE = list(cc_dict_DE.keys())
    available_division = ['CS', 'PH', 'CH', 'CF', 'BS', 'CP']


    # give format to df cost center
    columns_to_drop = []
    for c in df_cost_center.columns:
        if (c[:4] != 'Cost') & (c[:3] != 'VIN'):
            columns_to_drop.append(c)
    for i in columns_to_drop:
        df_cost_center.drop(i, axis=1, inplace=True)
        
    if ((len(df_cost_center.iloc[0,0]) >= 14) & (len(df_cost_center.iloc[0,1]) == 10)):
        df_cost_center.rename(columns= {df_cost_center.columns[0] : 'VIN_No'}, inplace = True)
        df_cost_center.rename(columns= {df_cost_center.columns[1] : 'Cost_Center_Car'}, inplace = True)



    # Replace CC from DE that appear in our cc list, if they don't an X will be placed on "Wrong Cost Center"

    new_cars = new_cars.merge(df_cost_center, how='left', left_on='VIN', right_on='VIN_No')
    new_cars['Wrong_Cost_Center'] = np.where((new_cars['Country'] == 'DE') & -(new_cars['Cost_Center_Car'].isin(cc_list_DE)), "X", '')
    new_cars['Wrong_Cost_Center'] = np.where(new_cars['Cost Center'].isnull(), "X", new_cars['Wrong_Cost_Center'])
    new_cars['Cost Center'] = np.where((new_cars['Country'] == 'DE') & (new_cars['Wrong_Cost_Center'] == ''), new_cars['Cost_Center_Car'], new_cars['Cost Center'])
    #new_cars['Wrong_Cost_Center'] = np.where(new_cars['Country'] == 'DE', "", new_cars['Wrong_Cost_Center'])

    del(new_cars['VIN_No'])
    del(new_cars['Cost_Center_Car'])
    del(new_cars['Exact Car Exist in H2R'])


    # Assign division based on CC
    new_cars['new division'] = new_cars[new_cars['Wrong_Cost_Center'] == ""]['Cost Center'].map(cc_dict_DE)
    new_cars['Division / Subgroup'] = np.where(new_cars['new division'].isnull(), new_cars['Division / Subgroup'], new_cars['new division'])
    new_cars = new_cars[new_cars['Division / Subgroup'] != 'Employee cars = out of scope']
    new_cars['Division / Subgroup'] = np.where(new_cars['Division / Subgroup'] == 'CPL', 'CP', new_cars['Division / Subgroup'])  
    new_cars['Division / Subgroup'] = np.where(new_cars['Division / Subgroup'] == 'GFI', 'CF', new_cars['Division / Subgroup'])  
    new_cars['Division / Subgroup'] = np.where(new_cars['Division / Subgroup'] == 'BAB', 'CF', new_cars['Division / Subgroup'])  
    del(new_cars['new division'])
    #new_cars['Wrong Division'] = np.where(new_cars['Division / Subgroup'].isin(available_division), "", "X")


    # delete blank spaces for cc
    new_cars['Cost Center'] = new_cars['Cost Center'].replace(' ', '', regex=True)


    # define Wrong LE in Report


    new_cars['cc_2digits'] = pd.to_numeric(new_cars['Company Code'].str[:2].replace('0n', '00'))


    conditionBAYER = (new_cars["Monsanto_Bayer"] == 'Bayer') & (new_cars['Contract End Date'] > currentYear + '-' + currentMonth + '-01') & (new_cars['cc_2digits'] >=25) & (new_cars['Country'] != 'US')
    conditionMONSANTO = (new_cars["Monsanto_Bayer"] == 'Monsanto') & (new_cars['Contract End Date'] > currentYear + '-' + currentMonth + '-01') & ((new_cars['cc_2digits'] < 25)) & (new_cars['Country'] != 'US')


    new_cars['Wrong BAYER LE in Report'] =  np.where(conditionBAYER , "X", "") 
    new_cars['Wrong MONSANTO LE in Report'] =  np.where(conditionMONSANTO , "X", "") 

    del(new_cars['cc_2digits'])


    # Last changes in company code

    new_cars['Company Code'] =  np.where((new_cars['Country'] == 'DE') & (new_cars["Monsanto_Bayer"] != 'Monsanto'), '2000', new_cars['Company Code'])
    new_cars['Null Company Code'] = np.where(new_cars['Company Code'] == '2000', "", new_cars['Null Company Code'])




    new_cars['Non existing Company Code'] = np.where(new_cars['Company Code'].isin(df_le_valid['CCode']), "", np.where(new_cars['Null Company Code'] == "X", "", "X"))

    new_cars['Company Code'] =  np.where((new_cars['Country'] != 'US') & (new_cars['Company Code'].str[:2] == '26'), "D" + new_cars['Company Code'], new_cars['Company Code'])
    new_cars['Company Code'] =  np.where((new_cars['Country'] != 'US') & (new_cars['Company Code'].str[:2] == '27'), "D" + new_cars['Company Code'], new_cars['Company Code'])

    
    # 

    
    # # OLD CARS MANIPULATION


    # add cost center to LE 2000
    old_cars = old_cars.merge(df_cost_center, how='left', left_on='VIN', right_on='VIN_No')

    old_cars['Cost Center'] = np.where(((old_cars['Company Code'] == '2000') & (old_cars['Cost_Center_Car'].isin(cc_list_DE))), old_cars['Cost_Center_Car'], old_cars['Cost Center'])

    #old_cars['Cost Center'] = np.where((old_cars['Company Code'] == '2000'), old_cars['Cost_Center_Car'], old_cars['Cost Center'])


    del(old_cars['Cost_Center_Car'])
    del(old_cars['VIN_No'])


    old_cars['LE_VIN'] = old_cars['Company Code'] + '_' + old_cars['VIN'] 

    df_query_tomerge = df_query[['VIN', 'LE_VIN', 'Legal Entity', 'Status', 'SAP Cost Center', 'End Date', 'Payments', 'Currency', 'System']]
    df_query_tomerge.rename(columns = {'VIN' : 'VIN in H2R', 'Legal Entity' : 'LE from H2R', 'Status' : 'Status in H2R','Payments' : 'Base rent in H2R', 'End Date' : 'End date from H2R', 'Currency' : 'Currency in H2R', 'SAP Cost Center' : 'Cost Center in H2R', 'System' : 'System in H2R'}, inplace = True)


    old_cars = old_cars.merge(df_query_tomerge, how='left',left_on= 'LE_VIN', right_on= 'LE_VIN', copy=False)



    del(old_cars['Exact Car Exist in H2R'])




    # delete those car not active (leaving those that doesn't match by cocode)
    old_cars = old_cars[(((old_cars['Status in H2R'] == 'active') | old_cars['Status in H2R'].isnull()) & (old_cars['Company Code'] != '0nan'))]


    # refill those null values
    null_values_old_cars = old_cars[old_cars['Status in H2R'].isnull()]
    old_cars = old_cars[-(old_cars['Status in H2R'].isnull())]

    columns_to_drop_ = ['LE_VIN', 'LE from H2R', 'Status in H2R', 'Cost Center in H2R', 'End date from H2R', 'Base rent in H2R', 'Currency in H2R', 'System in H2R']
    for i in columns_to_drop_:
        null_values_old_cars.drop(i, axis=1, inplace=True)


    null_values_old_cars = null_values_old_cars.merge(df_query_tomerge[df_query_tomerge['Status in H2R'] == 'active'], how='left',left_on= 'VIN', right_on= 'VIN in H2R', copy=False)
    old_cars = old_cars.append(null_values_old_cars, ignore_index=True)

    old_cars['Currency in H2R'] = old_cars['Currency in H2R'].str[-3:]

    del(old_cars['VIN in H2R'])
    del(old_cars['VIN in H2R_x'])
    del(old_cars['VIN in H2R_y'])

    old_cars = old_cars[old_cars['Status in H2R'] == 'active']


    #check to know where is in query
    old_cars['VIN Bayer in Monsanto H2R report (P08)'] = np.where((old_cars['System in H2R'] == 'P08') & (old_cars['Monsanto_Bayer'] == 'Bayer'), "X", "")
    old_cars['VIN Monsanto in Bayer H2R report'] = np.where((old_cars['System in H2R'] != 'P08') & (old_cars['Monsanto_Bayer'] == 'Monsanto'), "X", "")



    for i in old_cars.columns[19:26]:
        del(old_cars[i])


    # delete blank spaces for cc
    old_cars['Cost Center'] = old_cars['Cost Center'].replace(' ', '', regex=True)



    old_cars['Payments (monthly)'].fillna(0, inplace=True)
    old_cars['FLIX Base rent'] = old_cars['Payments (monthly)']
    old_cars['FLIX End Date'] = old_cars['Contract End Date']
    old_cars['Currency FLIX'] = old_cars['Currency']
    old_cars['FLIX CC'] = old_cars['Cost Center']
    old_cars['FLIX LE'] = old_cars['Company Code']



    old_cars['Diff Base rent'] = old_cars['FLIX Base rent'] - old_cars['Base rent in H2R'] 
    old_cars['Diff Base rent to EUR'] = old_cars['Diff Base rent'] / [currency_dict[x] if x != 'EUR' else 1 for x in old_cars['Currency in H2R']]
    old_cars['Differences End Date in days'] = (old_cars['FLIX End Date'] - old_cars['End date from H2R']).dt.days
    old_cars['check currency'] = np.where(old_cars['Currency in H2R'] == old_cars['Currency FLIX'], 'ok',np.where(old_cars['Currency FLIX'] == 'na', 'N/A currency missing','wrong currency'))
    old_cars['Differences of CC'] = np.where(old_cars['Cost Center in H2R'] == old_cars['FLIX CC'], 'ok', np.where((old_cars['System in H2R'] == 'N8P') | (old_cars['System in H2R'] == 'P08'), 'N/A SAP disconnected','wrong cc'))
    old_cars['Differences of CC'] = np.where(old_cars['Company Code'] == '2000', 'ok', old_cars['Differences of CC'])
    old_cars['Diff LE'] = np.where(old_cars['LE from H2R'] == old_cars['FLIX LE'], 'ok', 'probably transfer')
    old_cars['Diff LE'] = np.where(old_cars['Country'] == 'DE', 'ok', old_cars['Diff LE'])


    # corrections columns
    #for rent
    old_cars['Correction base rent'] = np.where(abs(old_cars['Diff Base rent to EUR']) > 5 , 'yes', np.where(abs(old_cars['Diff Base rent']) == 0 , 'N/A', 'N/A difference below 5€'))
    old_cars['Correction base rent'] = np.where(old_cars['check currency'] == 'wrong currency', 'N/A', old_cars['Correction base rent'])
    old_cars['Correction base rent'] = np.where((old_cars['Base rent in H2R'] == 0) | (old_cars['FLIX Base rent'] == 0), 'N/A', old_cars['Correction base rent'])
    old_cars['Correction base rent'] = np.where(old_cars['FLIX Base rent'] < 0, 'N/A', old_cars['Correction base rent'])

    # for date
    h2r_condition = (((old_cars['End date from H2R'].dt.month <= datetime.datetime.now().month) & (old_cars['End date from H2R'].dt.year == datetime.datetime.now().year)) | (old_cars['End date from H2R'].dt.year < datetime.datetime.now().year))
    flix_condition = (((old_cars['FLIX End Date'].dt.month <= datetime.datetime.now().month) & (old_cars['FLIX End Date'].dt.year == datetime.datetime.now().year)) | (old_cars['FLIX End Date'].dt.year < datetime.datetime.now().year))
    old_cars['Correction end date'] = np.where(abs(old_cars['Differences End Date in days']) > 31 , 'yes', np.where(abs(old_cars['Differences End Date in days']) == 0 , 'N/A', 'N/A difference below 1 month'))
    old_cars['Correction end date'] = np.where((h2r_condition) & (flix_condition), 'N/A ended in the past', old_cars['Correction end date'])

    # consolidate
    old_cars['Correction should be done'] = old_cars['Correction end date']
    old_cars['Correction should be done'] = np.where(old_cars['Correction should be done'].str[:3] == 'yes', 'yes', np.where(old_cars['Correction end date'] == 'N/A ended in the past', 'N/A ended in the past', old_cars['Correction base rent'].str[:3]))


    old_cars['Correction should be done'] = np.where((old_cars['FLIX Base rent'] == 0) & (old_cars['Correction end date'] == 'yes'), 'check with FM, FLIX base rent is 0', old_cars['Correction should be done'])


    #last column for user
    old_cars['LE'] = old_cars['LE from H2R']
    old_cars['FO action'] = ''
    old_cars['User'] = ''
    old_cars['Correction done'] = ''
    old_cars['type of correction'] = ''
    old_cars['IBR change'] = ''





    # assign SSC
    old_cars['LE'] =  np.where((old_cars['Country'] != 'US') & (old_cars['LE'].str[:2] == '26'), "D" + old_cars['LE'], old_cars['LE'])
    old_cars['LE'] =  np.where((old_cars['Country'] != 'US') & (old_cars['LE'].str[:2] == '27'), "D" + old_cars['LE'], old_cars['LE'])
    old_cars['LE'] =  np.where(old_cars['LE'] == '2681', "D" + old_cars['LE'], old_cars['LE'])



    old_cars = old_cars.merge(df_ssc[['LE', 'SSC']], how='left', left_on= 'LE', right_on= 'LE', copy=False)


    # delete blank spaces for cc
    old_cars['FLIX CC'] = old_cars['FLIX CC'].replace(' ', '', regex=True)
    old_cars['Cost Center'] = old_cars['Cost Center'].replace(' ', '', regex=True)



    old_cars_final = old_cars[['Brand', 'Car Policy Type', 'Contract Start Date', 'Contract End Date', 'Contract Status', 'Contract Duration(Months)', 'Cost Center', 'Division / Subgroup', 'License Number', 'Model', 'Payments (monthly)', 'Currency', 'VIN', 'Country', 'Lessor', 'Company Code', 'Legal Entity', 'NewOldMarker', 'Monsanto_Bayer', 'System in H2R', 'Status in H2R', 'VIN Bayer in Monsanto H2R report (P08)', 'VIN Monsanto in Bayer H2R report', 'Base rent in H2R', 'FLIX Base rent', 'Diff Base rent', 'Diff Base rent to EUR', 'End date from H2R', 'FLIX End Date', 'Differences End Date in days', 'Currency FLIX', 'Currency in H2R', 'check currency', 'Cost Center in H2R', 'FLIX CC', 'Differences of CC', 'FLIX LE', 'LE from H2R', 'Diff LE', 'Correction base rent', 'Correction end date', 'Correction should be done', 'LE', 'SSC', 'FO action', 'User', 'Correction done', 'type of correction', 'IBR change']]

 


    return new_cars, old_cars_final, currentMonth, currentYear


##############################################################################################################################################################################

app=Flask(__name__, template_folder='templates')
@app.route('/')
def upload_file():
    return render_template('index.html')



@app.route("/return", methods=['POST'])
def return_():
    if request.method == "POST":
        return render_template('index.html')





@app.route("/outputfile", methods=['POST'])
def uploader():
    if request.method == "POST":
        df = request.files['fleet_report']
        df = pd.read_csv(df)
        df_query = pd.DataFrame()
        fo = request.files.getlist('query_folder')
        for  i in fo:
            data_ = pd.read_excel(i)
            df_query = df_query.append(data_)
        df_le = request.files['le_report']
        df_le = pd.read_excel(df_le, header = 10)
        df_currency = request.files['currency_report']
        df_currency = pd.read_excel(df_currency, header=0)
        df_cost_center = request.files['fie_report']
        df_cost_center = pd.read_excel(df_cost_center)
        
        

        new_cars, old_cars_final, currentMonth, currentYear = process_data(df_query=df_query, df=df, df_currency=df_currency, df_le=df_le, df_cost_center=df_cost_center)
        myoutput = currentMonth + "." + currentYear + " Cars Report ToAsk.xlsx"
        out = io.BytesIO()
        writer = pd.ExcelWriter(out, engine='xlsxwriter', datetime_format='yyyy-mm-dd')
        

        
        new_cars.to_excel(writer, index=False, header=True, sheet_name='New Cars')
        old_cars_final.to_excel(writer, index=False, header=True, sheet_name='Corrections')


        # Get the dimensions of the dataframe
        (max_row_old, max_col_old) = old_cars_final.shape
        (max_row_new, max_col_new) = new_cars.shape

        # Make the columns wider for clarity.
        writer.sheets['New Cars'].set_column(0,  max_col_new - 1, 20)
        writer.sheets['Corrections'].set_column(0,  max_col_old - 1, 20)


        # Set the autofilter.
        writer.sheets['Corrections'].autofilter(0, 0, max_row_old, max_col_old - 1)
        writer.sheets['New Cars'].autofilter(0, 0, max_row_new, max_col_new - 1)


        # Number format
        format1 = writer.book.add_format({'num_format': '0.00'})
        writer.sheets['Corrections'].set_column('AA:AA', None, format1)


        writer.save()
        writer.close()

   
        r = make_response(out.getvalue())

        print(f'New file created. {myoutput} has been generated.')

        r.headers["Content-Disposition"] = "attachment; filename=Cars_Report_ToAsk.xlsx"
        r.headers["Content-type"] = "application/x-xls"

        return  r



"""
@app.errorhandler(404)
def page_not_found(err):
    return render_template('page_not_found.html'), 404



@app.errorhandler(Exception)
def handle_exception(e):
    # pass through HTTP errors
    if isinstance(e, HTTPException):
        return e
    # now you're handling non-HTTP exceptions only
    return render_template("script_failed.html", e=e), 500
"""


if __name__ == '__main__':
    app.run(debug=True, port=5000)

