import pandas as pd
import datetime
import tejapi 
import numpy as np
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "3jUCETU2KiPwGJeyETYOQd1TCoDoxX"
tejapi.ApiConfig.ignoretz = True
import dask.dataframe as dd
import dask 

default_start = '2013-01-01'
default_end = datetime.datetime.now().date().strftime('%Y-%m-%d')

def get_fin_data(table, ticker, columns = [], start = default_start, end = default_end, fin_type = ['A','Q','TTM'], transfer_to_chinese=False):
    # transfer fin_ann_date to daily basis
    days = pd.date_range(start=start, end=end, freq='D')
    days = pd.DataFrame({'all_dates':days})
    # get fin data
    data_sets = pd.DataFrame()
    # 自動補上 coid, mdate
    columns += ['coid', 'mdate','annd', 'no', 'sem','key3']
    columns = list(set(columns))
    for i in ticker:
        # financial data
        data = tejapi.get(table,
                        coid = i,
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':columns})
        # 若沒抓到資料換下一檔股票
        try:
            # select certain fin_type
            data = get_certain_fin_type(data, fin_type)
            # financial data ipsale to dayily basis 
            data = fin_pivot(data, remain_keys=['coid','mdate','no','sem','annd'])
            data = days.merge(data, left_on='all_dates', right_on = 'annd', how = 'left').ffill()
            data_sets = pd.concat([data_sets, data])
            data_sets = data_sets.drop_duplicates()
        except:
            continue
    return data_sets


def get_most_recent_date(data, sort_keys, subset, keep_mothod):
    # sort data order by mdate(accural date) and annd_s(announce date)
    data = data.sort_values(sort_keys)
    # when multiple rows have the same annd_s(announce date), keep the last row, which has the greatest mdate.
    data = data.drop_duplicates(subset = subset, keep = keep_mothod)
    return data


def get_trading_data(table, ticker, columns = [], start = default_start, end = default_end):
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
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':columns})
        data_sets = pd.concat([data_sets, data])
    return data_sets


def get_alternative_data(table, ticker, columns = [], start = default_start, end = default_end):
    # 自動補上 coid, mdate, 發布日
    if table == 'TWN/APISALE':
        # 月營收
        columns += ['coid', 'mdate','annd_s']
        columns = list(set(columns))
    else:
        # 集保資料 
        columns += ['coid','mdate','edate1']
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
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':columns})
        # transfer to daily basis
        if table == 'TWN/APISALE':
            data = get_most_recent_date(data, ['mdate', 'annd_s'], 'annd_s', keep_mothod='last')
            # drop mdate
            data = data.drop(columns=['mdate'])
            data = days.merge(data, left_on=['all_dates'], right_on = ['annd_s'], how='left').ffill()
        else:
            data = get_most_recent_date(data, ['mdate', 'edate1'], 'edate1', keep_mothod='last')
            # drop mdate
            data = data.drop(columns=['mdate'])
            data = days.merge(data, left_on=['all_dates'], right_on = ['edate1'], how='left').ffill()
        # fill coid with ticker 
        data['coid'] = np.where(data['coid'].isna(), i, data['coid'])
        data_sets = pd.concat([data_sets, data])
    return data_sets

def get_fin_auditor(table, tickers, columns = [], start = default_start, end = default_end, fin_type = ['A','Q','TTM'], transfer_to_chinese = False):
    # transfer fin_ann_date to daily basis
    # 營業日
    days = pd.date_range(start=start, end=end, freq='D')
    days = pd.DataFrame({'all_dates':days})
    # get fin data
    data_sets = pd.DataFrame()
    # 自動補上 coid, mdate
    columns += ['coid', 'mdate','key3','no','sem','annd']
    columns = list(set(columns))
    for ticker in tickers:
        # financial data
        data = tejapi.get(table,
                        coid = ticker,
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts= {'pivot':True})
        # modify the name of the columns from upper case to lower case.
        lower_columns = {i:i.lower() for i in data.columns}
        data = data.rename(columns=lower_columns)
        # select certain fin_type
        data = get_certain_fin_type(data, fin_type)
        # get most recent announce date of the company
        fin_date = get_announce_date(ticker=ticker)
        fin_date = get_certain_fin_type(fin_date, fin_type)
        data = fin_date.merge(data, how = 'left', on = ['coid', 'mdate', 'key3'])
        # select columns
        data = data.loc[:,columns]
        # parallel fin_type to columns 
        data = fin_pivot(data, remain_keys=['coid','mdate','no','sem','annd'])
        # financial data to dayily basis
        data = days.merge(data, left_on='all_dates', right_on = 'annd', how = 'left').ffill()
        # fill coid with ticker 
        data['coid'] = np.where(data['coid'].isna(), ticker, data['coid'])
        data_sets = pd.concat([data_sets, data])
    data_sets = data_sets.drop(columns='mdate')
    # print(data_sets.tail(10))
    return data_sets


def get_announce_date(ticker):
    data = tejapi.get('TWN/AINVFINQA',
                    coid = ticker,
                    paginate = True,
                    chinese_column_name=False)
    # 對資料進行排序，相同['公司碼','年月','發布日','合併']的情況下，選次數大者
    # 針對相同['公司碼','發布日']的資料，若遇到相同發布日的情況，選財報年月最大者
    unique_date = get_most_recent_date(data, sort_keys=['coid','mdate','annd'], subset=['coid','annd','key3'], keep_mothod='last')
    return unique_date

def get_certain_fin_type(data, fin_type):
    if type(fin_type) is str:
        data = data.query(f'key3 == "{fin_type}"')
    else:
        data = data.query(f'key3.isin({fin_type})')
    return data

def fin_pivot(df, remain_keys):
    # for loop execute pviot function
    uni = df['key3'].dropna().unique()
    data = pivot(df, remain_keys, uni[0])
    for i in range(1, len(uni)):
        temp = pivot(df, remain_keys, uni[i])
        data = data.merge(temp, on = remain_keys)
    return data

def pivot(df, remain_keys, pattern):
    try:
        data = df.loc[df['key3']==pattern, :]
        # Create a mapping table of column names and their corresponding new names.
        new_keys = {i:i+'_'+str(pattern) for i in data.columns.difference(remain_keys)}
        # Replace old names with the new ones.
        data = data.rename(columns = new_keys)
        data = data.loc[:,~data.columns.str.contains('key3')]
    except:
        raise ValueError('請使用 get_announce_date 檢查該檔股票的財務數據發布日是否為空值。')
    return data