import pandas as pd
import datetime
import tejapi 
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "3jUCETU2KiPwGJeyETYOQd1TCoDoxX"
tejapi.ApiConfig.ignoretz = True

default_start = '2013-01-01'
default_end = datetime.datetime.now().date().strftime('%Y-%m-%d') 

def get_fin_data(table, ticker, columns = None, start = default_start, end = default_end, transfer_to_chinese = False):
    # transfer fin_ann_date to daily basis
    # 營業日
    days = pd.date_range(start=start, end=end, freq='D')
    days = pd.DataFrame({'all_dates':days})
    # get fin data
    data_sets = pd.DataFrame()
    # 自動補上 coid, mdate
    columns += ['coid', 'mdate','annd', 'no', 'sem', 'fin_type','key3']
    columns = list(set(columns))
    for i in ticker:
    # financial data
        data = tejapi.get(table,
                        coid = i,
                        paginate = True,
                        chinese_column_name=transfer_to_chinese,
                        mdate = {'gte':start,'lte':end},
                        opts= {'columns':columns})
        # financial data ipsale to dayily basis 
        data = fin_pivot(data, remain_keys=['coid','mdate','no','sem','fin_type','annd'])
        data = days.merge(data, left_on='all_dates', right_on = 'annd', how = 'left').ffill()
        data_sets = pd.concat([data_sets, data])
    data_sets = data_sets.drop(columns='mdate')
    # data_sets = data_sets.rename(columns={'annd':'mdate'})
    # print(data_sets)
    return data_sets

def get_trading_data(table, ticker, columns = None, start = default_start, end = default_end, transfer_to_chinese = False):
    # 自動補上 coid, mdate
    for i in ['coid', 'mdate']:
        if i not in columns:
            columns.append(i)
    # get stock price data
    data_sets = pd.DataFrame()
    for i in ticker:
        # stock price
        data = tejapi.get(table,
                        coid = i,
                        paginate = True,
                        chinese_column_name=transfer_to_chinese,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':columns})
        data_sets = pd.concat([data_sets, data])
    # print(data_sets)
    return data_sets

def get_alternative_data(table, ticker, columns = None, start = default_start, end = default_end, transfer_to_chinese = False):
    # 自動補上 coid, mdate, 發布日
    if table == 'TWN/APISALE':
        # 月營收
        columns += ['coid','annd_s']
        columns = list(set(columns))
    else:
        # 集保資料 
        columns += ['coid','edate1']
        columns = list(set(columns))
    # 營業日
    days = pd.date_range(start=start, end=end, freq='D')
    days = pd.DataFrame({'all_dates':days})
    data_sets = pd.DataFrame()
    for i in ticker:
        # alternative data
        data = tejapi.get(table,
                        coid = i,
                        paginate = True,
                        chinese_column_name=transfer_to_chinese,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':columns})
        # transfer to daily basis
        if table == 'TWN/APISALE':
            data = days.merge(data, left_on=['all_dates'], right_on = ['annd_s'], how='left').ffill()
            # drop mdate (ex. 2013一月營收，mdate = 2013-01-01)
            # data = data.drop(columns=['mdate'])
            # # 將 annd_s(發布日) 改成 mdate
            # data = data.rename(columns={'annd_s':'mdate'})
        else:
            data = days.merge(data, left_on=['all_dates'], right_on = ['edate1'], how='left').ffill()
            # drop mdate (ex. 2013一月營收，mdate = 2013-01-01)
            # data = data.drop(columns=['mdate'])
            # # 將 annd_s(發布日) 改成 mdate
            # data = data.rename(columns={'edate1':'mdate'})
        data_sets = pd.concat([data_sets, data])
    # print(data_sets)
    return data_sets

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