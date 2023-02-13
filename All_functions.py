
import tejapi 
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "YOURKEY"
tejapi.ApiConfig.ignoretz = True
import pandas as pd
import numpy as np
import datetime
from parameters import *

def get_fin_acc_code():
    acc_info = tejapi.get('TWN/AINVFACC_INFO_C',
                        paginate = True,
                        chinese_column_name=False)
    return acc_info['acct_code'].to_list()

def get_history_data(ticker, columns = None, start = '2005-01-01', end = datetime.datetime.now(), transfer_to_chinese = True):
    all_tables = triggers(ticker, columns, start, end, transfer_to_chinese)
    # 搜尋觸發到那些 table
    trigger_tables = [i for i in all_tables.keys() if i in ['fin_data', 'stk_price', 'share_dist','monthly_rev','overbought']]
    # 根據 OD 進行排序
    trigger_tables.sort(key = lambda x: map_table.loc[map_table['TABLE_NAME']==x, 'OD'].item())
    check = map_table.merge(merge_keys)
    funct_map = funct_dict()
    # tables 兩兩合併
    for i in range(len(trigger_tables)-1):
        # keys
        left_keys = check.loc[check['TABLE_NAME']==trigger_tables[i], 'KEYS'].tolist()
        right_keys = check.loc[check['TABLE_NAME']==trigger_tables[i+1], 'KEYS'].tolist()
        # od
        od1 = map_table.loc[map_table['TABLE_NAME']==trigger_tables[i], 'OD'].item()
        od2 = map_table.loc[map_table['TABLE_NAME']==trigger_tables[i+1], 'OD'].item()
        if i == 0:
            data = funct_map[(od1, od2)](all_tables, trigger_tables[i], trigger_tables[i+1])
            # print(len(data))
            # print(funct_map[(od1, od2)])
        else:
            left_keys = check.loc[check['TABLE_NAME']==trigger_tables[0], 'KEYS'].tolist()
            right_keys = check.loc[check['TABLE_NAME']==trigger_tables[i+1], 'KEYS'].tolist()
            data = data.merge(all_tables[trigger_tables[i+1]], left_on = left_keys, right_on = right_keys, how = 'left', suffixes = ('', '_surfeit')).ffill()
            # 刪除多於欄位
            data = data[[i for i in data.columns if not i.__contains__('_surfeit')]]
            # print(len(data))

        # print(left_keys, right_keys)
    return data
    


def triggers(ticker, columns = None, start = '2005-01-01', end = datetime.datetime.now(), transfer_to_chinese = True):
    # 取得每張 table 的欄位名稱(internal_code)
    trading_columns_group = tejapi.table_info('TWN/APRCD1')['filters']
    all_fin_acc_code = get_fin_acc_code()
    fin_columns_group = ['coid', 'mdate', 'fin_od'] + get_fin_acc_code()
    alternative_group = tejapi.table_info('TWN/ASHR1')['filters']
    share_dist_group = tejapi.table_info('TWN/ADCSHR')['filters']
    monthly_revenue_group = tejapi.table_info('TWN/ASALE')['filters']
    market_columns = [c for c in columns if c in trading_columns_group]
    fin_columns = [c for c in columns if c in fin_columns_group]
    # fin_columns = TransferInternalCode([c for c in columns if c in fin_columns_group], 'fin')
    alternative_columns = [c for c in columns if c in alternative_group]
    monthly_columns = [c for c in columns if c in monthly_revenue_group]
    main_holder_columns = [c for c in columns if c in share_dist_group]
    # transfer fin_ann_date to daily basis
    # 營業日
    days = pd.date_range(start=start, end=end, freq='D')
    days = pd.DataFrame({'all_dates':days})
    # get fin data
    if len(fin_columns)>0 and sorted(fin_columns) != ['coid','mdate']:
        fin_data = pd.DataFrame()
        if 'fin_od' not in fin_columns:
            fin_columns+=['fin_od']
        for i in ticker:
        # financial data
            data = tejapi.get('TWN/AINVFINQ',
                            coid = i,
                            paginate = True,
                            chinese_column_name=transfer_to_chinese,
                            mdate = {'gte':start,'lte':end},
                            opts= {'pivot':True, 'columns':fin_columns})
            fin_data = pd.concat([fin_data, data])
        # announce date
        announce_date = get_announce_date(ticker=ticker)
        # announce date with financial data
        # fin_data = fin_data.merge(announce_date, left_on = ['coid', 'mdate', 'fin_od'], right_on=['coid', 'mdate', 'fin_od'], how='left')
        fin_data = fin_data.merge(announce_date, on = ['coid', 'mdate', 'fin_od'] , how='left')
        # a_dd:發布日
        fin_data = days.merge(fin_data, left_on='all_dates', right_on = 'a_dd', how = 'left').ffill()
        
    # get stock price data
    if len(market_columns)>0:
        stk_price = pd.DataFrame()
        for i in ticker:
            # stock price
            data = tejapi.get('TWN/APRCD1',
                            coid = i,
                            paginate = True,
                            chinese_column_name=transfer_to_chinese,
                            mdate = {'gte':start,'lte':end},
                            opts = {'columns':market_columns})
            stk_price = pd.concat([stk_price, data])
    
    # get overbought data
    if len(alternative_columns)>0:
    # if len(alternative_columns)>0 and sorted(alternative_columns) != ['coid','mdate']:
        overbought = pd.DataFrame()
        for i in ticker:
            # stock price
            # 修改Table
            data = tejapi.get('TWN/ASHR1',
                            coid = i,
                            paginate = True,
                            chinese_column_name=transfer_to_chinese,
                            mdate = {'gte':start,'lte':end},
                            opts = {'columns':alternative_columns})
            overbought = pd.concat([overbought, data])
    # get monthly revenue data
    if len(monthly_columns)>0 and sorted(monthly_columns) != ['coid','mdate']:
        monthly_rev = pd.DataFrame()
        for i in ticker:
            # stock price
            # 修改Table
            data = tejapi.get('TWN/ASALE',
                            coid = i,
                            paginate = True,
                            chinese_column_name=transfer_to_chinese,
                            mdate = {'gte':start,'lte':end},
                            opts = {'columns':monthly_columns})
            monthly_rev = pd.concat([monthly_rev, data])
        # transfer to daily basis
        monthly_rev = days.merge(monthly_rev, left_on=['all_dates'], right_on = ['annd_s'], how='left').ffill()
    # get weekly share holders' distrbution
    if len(main_holder_columns)>0 and sorted(main_holder_columns) != ['coid','mdate']:
        share_dist = pd.DataFrame()
        for i in ticker:
            # stock price
            # 修改Table
            data = tejapi.get('TWN/ADCSHR',
                            coid = i,
                            paginate = True,
                            chinese_column_name=transfer_to_chinese,
                            mdate = {'gte':start,'lte':end},
                            opts = {'columns':main_holder_columns})
            share_dist = pd.concat([share_dist, data])
        # transfer to daily basis
        share_dist =  days.merge(share_dist, left_on=['all_dates'], right_on = ['mdate'], how='left').ffill()
    del data, i

    return locals()


def get_announce_date(ticker, transfer_to_chinese = False):
    # option_dict = {}
    data = tejapi.get('TWN/AINVFINQA',
                    coid = ticker,
                    paginate = True,
                    chinese_column_name=transfer_to_chinese)
    
    # 對資料進行排序，相同['公司碼','年月','發布日','合併']的情況下，選次數大者
    # 
    x = data.sort_values(['coid','mdate','a_dd','merg','fin_od']).groupby(['coid','mdate','a_dd','merg']).tail(1)
    unique_date = x.drop_duplicates()
    # 針對相同['公司碼','發布日']的資料，若遇到相同發布日的情況，選財報年月最大者
    unique_date = unique_date.sort_values(['coid','mdate','a_dd']).groupby(['coid','a_dd']).tail(1).reset_index(drop=True)
    unique_date = unique_date.sort_values(['coid','mdate','a_dd','merg','fin_od'])
    return unique_date

def TransferInternalCode(series:list, groups):
    # test = ['asdsd', 'sadasda', 'adas21','中文','a中','中a']
    # list(map(lambda x :x.isalpha(), test))
    # Output: [True, True, False, True, True, True]
    # -----------------------------------------------------------
    # 需先轉換成UTF8的編碼，再進行是否為英文的判斷
    # test = ['asdsd', 'sadasda', 'adas21','中文','a中','中a']
    # list(map(lambda x :x.encode('UTF-8').isalpha(), test))
    # Output: [True, True, False, False, False, False]
    if groups == 'fin':
        acc_info = tejapi.get('TWN/AINVFACC_INFO_C',
                    paginate = True,
                    chinese_column_name=False)
        internal_code = [i for i in series if i in acc_info]
        require_transfer = [i for i in series if i not in acc_info]
    else:
        pass
    # 
    Eng_series = [i for i in require_transfer if str(i).encode('UTF-8').isalpha()]
    Chn_series = [i for i in require_transfer if not str(i).encode('UTF-8').isalpha()]
    Eng_df = pd.DataFrame({'eng_code':Eng_series})
    Chn_df = pd.DataFrame({'chn_code':Chn_series})
    Eng_df = Eng_df.merge(acc_info[['acct_code','cdesc']], how= 'inner', left_on = 'eng_code', right_on = 'cdesc')
    Chn_df = Chn_df.merge(acc_info[['acct_code','cdesc']], how= 'inner', left_on = 'chn_code', right_on = 'cdesc')
    result = Eng_df['acct_code'].tolist() + Chn_df['acct_code'].tolist() + internal_code
            
    return result



def merge_1_1(var_dict, table1:str, table2:str):
    if len(var_dict[table1]) >= len(var_dict[table2]):
        t1 = var_dict[table1]
        t2 = var_dict[table2]
    else:
        t1 = var_dict[table2]
        t2 = var_dict[table1]
    data = t1.merge(t2, on = ['coid', 'mdate'], how = 'left', suffixes = ('', '_surfeit')).ffill()
    # 刪除多餘的欄位
    data = data[[i for i in data.columns if not i.__contains__('_surfeit')]]
    return data

def merge_2_2(var_dict, table1:str, table2:str):
    if len(var_dict[table1]) >= len(var_dict[table2]):
        t1 = var_dict[table1]
        t2 = var_dict[table2]
    else:
        t1 = var_dict[table2]
        t2 = var_dict[table1]
    data = t1.merge(t2, on = ['coid', 'all_dates'], how = 'left', suffixes = ('', '_surfeit')).ffill()
    # 刪除多餘的欄位
    data = data[[i for i in data.columns if not i.__contains__('_surfeit')]]
    return data

def merge_1_2(var_dict, table1:str, table2:str):
    # 
    if map_table.loc[map_table['TABLE_NAME']==table1, 'OD'].item() == 1:
        t1 = var_dict[table1]
        t2 = var_dict[table2]
    else:
        t1 = var_dict[table2]
        t2 = var_dict[table1]
    data = t1.merge(t2, left_on = ['coid', 'mdate'], right_on = ['coid', 'all_dates'], how = 'left' ,suffixes = ('', '_surfeit')).ffill()
    # 刪除多餘的欄位
    data = data[[i for i in data.columns if not i.__contains__('_surfeit')]]
    return data

def funct_dict():
    funct_map = {
    (1,1):merge_1_1,
    (2,2):merge_2_2,
    (1,2):merge_1_2,
    (2,1):merge_1_2
    }
    return funct_map
