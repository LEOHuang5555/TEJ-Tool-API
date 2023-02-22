
import tejapi 
tejapi.ApiConfig.api_base="YOURBASE"
tejapi.ApiConfig.api_key = "YOURKEY"
tejapi.ApiConfig.ignoretz = True
import pandas as pd
import numpy as np
import datetime
import parameters as para



def get_history_data(ticker, columns = None, start = '2013-01-01', end = datetime.datetime.now(), transfer_to_chinese = True):
    # Active triggers function and return the involving tables.
    all_tables = triggers(ticker, columns, start, end, transfer_to_chinese)
    # 搜尋觸發到那些 table
    trigger_tables = [i for i in all_tables.keys() if i in ['fin_data', 'stk_price', 'share_dist','monthly_rev','overbought']]
    # 根據 OD 進行排序
    trigger_tables.sort(key = lambda x: para.map_table.loc[para.map_table['TABLE_NAME']==x, 'OD'].item())
    # consecutive_merge
    exec(f'''data = consecutive_merge(all_tables,  trigger_tables)''') 
    return locals()['data']
    

def triggers(ticker, columns = None, start = '2013-01-01', end = datetime.datetime.now(), transfer_to_chinese = True):
    market_columns = [c for c in columns if c in para.trading_columns_group]
    fin_company_columns = [c for c in columns if c in para.fin_company_group]
    # fin_columns = TransferInternalCode([c for c in columns if c in fin_columns_group], 'fin')
    alternative_columns = [c for c in columns if c in para.alternative_group]
    monthly_columns = [c for c in columns if c in para.monthly_revenue_group]
    main_holder_columns = [c for c in columns if c in para.share_dist_group]
    # transfer fin_ann_date to daily basis
    # 營業日
    days = pd.date_range(start=start, end=end, freq='D')
    days = pd.DataFrame({'all_dates':days})
    # get fin data
    if len(para.fin_company_group)>0 and sorted(para.fin_company_group) != ['coid','mdate']:
        fin_data = pd.DataFrame()
        fin_company_columns+=['annd', 'no', 'sem', 'fin_type','key3']
        fin_company_columns = list(set(fin_company_columns))
        for i in ticker:
        # financial data
            data = tejapi.get('TWN/AFESTM1',
                            coid = i,
                            paginate = True,
                            chinese_column_name=transfer_to_chinese,
                            mdate = {'gte':start,'lte':end},
                            opts= {'columns':fin_company_columns})
            fin_data = pd.concat([fin_data, data])
        
        # table 轉置
        fin_data = fin_pivot(fin_data, remain_keys=['coid','mdate','no','sem','fin_type','annd'])
        # financial data ipsale to dayily basis 
        # annd:發布日
        fin_data = days.merge(fin_data, left_on='all_dates', right_on = 'annd', how = 'left').ffill()
        
    # get stock price data
    if len(market_columns)>0:
        stk_price = pd.DataFrame()
        for i in ticker:
            # stock price
            data = tejapi.get('TWN/APIPRCD',
                            coid = i,
                            paginate = True,
                            chinese_column_name=transfer_to_chinese,
                            mdate = {'gte':start,'lte':end},
                            opts = {'columns':market_columns})
            stk_price = pd.concat([stk_price, data])
    
    # get overbought data
    if len(alternative_columns)>0:
        overbought = pd.DataFrame()
        for i in ticker:
            # stock price
            # 修改Table
            data = tejapi.get('TWN/APISHRACT',
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
            data = tejapi.get('TWN/APISALE',
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
            data = tejapi.get('TWN/APISHRACTW',
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


def consecutive_merge(local_var, loop_array):
    table_keys = para.map_table.merge(para.merge_keys)

    # tables 兩兩合併
    for i in range(len(loop_array)-1):
        left_keys = table_keys.loc[table_keys['TABLE_NAME']==loop_array[i], 'KEYS'].tolist()
        right_keys = table_keys.loc[table_keys['TABLE_NAME']==loop_array[i+1], 'KEYS'].tolist()
        if i == 0:
            exec(f'''
data = local_var[loop_array[{i}]].merge(local_var[loop_array[{i+1}]], left_on = {left_keys}, right_on = {right_keys}, how = 'left', suffixes = ('','_surfeit')).ffill()
data = data.loc[:,~data.columns.str.contains('_surfeit')]''')
            exec(f'print(data.columns)')
        else:
        
            exec(f'''
data = data.merge(local_var[loop_array[{i+1}]], left_on = {left_keys}, right_on = {right_keys}, how = 'left', suffixes = ('','_surfeit')).ffill()
data = data.loc[:,~data.columns.str.contains('_surfeit')]''')
            exec(f'print(data.columns)')       
    return locals()['data']

def fin_pivot(df, remain_keys):
    def pivot(df, remain_keys, pattern):
        data = df.loc[df['key3']==pattern, :]
        # Create a mapping table of column names and their corresponding new names.
        new_keys = {i:i+'_'+str(pattern) for i in data.columns.difference(remain_keys)}
        # Replace old names with the new ones.
        data = data.rename(columns = new_keys)
        data = data.loc[:,~data.columns.str.contains('key3')]
        return data
    # for loop execute pviot function
    uni = df['key3'].dropna().unique()
    for i in range(len(uni)):
        if i ==0:
            # 
            data = pivot(df, remain_keys, uni[i])
        else:
            temp = pivot(df, remain_keys, uni[i])
            data = data.merge(temp, on = remain_keys)
    return data



