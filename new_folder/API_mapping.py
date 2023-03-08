import pandas as pd
import datetime
import tejapi 
tejapi.ApiConfig.api_base="BASE"
tejapi.ApiConfig.api_key = "YOURKEY"
tejapi.ApiConfig.ignoretz = True
import parameters as para

default_start = '2013-01-01'
default_end = datetime.datetime.now().date().strftime('%Y-%m-%d') 

def get_fin_data(table, ticker, columns = None, start = default_start, end = default_end, transfer_to_chinese = False, fin_type = ['A','Q','TTM']):
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
        # 若沒抓到資料換下一檔股票
        if len(data) == 0:
            table_chn_name = para.fin_invest_tables.loc[para.fin_invest_tables['API_TABLE']==table, 'CHN_NAMES'].item()
            print(table_chn_name, table)
            print(str(i)+table_chn_name+str(table)+'沒資料')
            continue
        else:
            # select certain fin_type
            data = get_certain_fin_type(data, fin_type)
            # financial data ipsale to dayily basis 
            data = fin_pivot(data, remain_keys=['coid','mdate','no','sem','fin_type','annd'])
            data = days.merge(data, left_on='all_dates', right_on = 'annd', how = 'left').ffill()
            data_sets = pd.concat([data_sets, data])
            data_sets = data_sets.drop_duplicates()
    data_sets = data_sets.drop(columns='mdate')
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
    data_sets = data_sets.drop(columns='mdate')
    return data_sets

def get_fin_auditor(table, ticker, columns = None, start = default_start, end = default_end, transfer_to_chinese = False):
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
                        opts= {'columns':columns, 'pivot':True})
        # get most recent announce date of the company
        fin_date = get_announce_date(ticker=i)
        temp = fin_date.merge(data, how = 'left', on = ['coid', 'mdate', 'fin_od'])
        # transfer columns to
        # data = fin_pivot(data, remain_keys=['coid','mdate','no','sem','fin_type','annd'])
        # financial data ipsale to dayily basis
        data = days.merge(temp, left_on='all_dates', right_on = 'a_dd', how = 'left').ffill()
        data_sets = pd.concat([data_sets, data])
    data_sets = data_sets.drop(columns='mdate')
    return data_sets

def get_announce_date(ticker, transfer_to_chinese = False):
    # option_dict = {}
    data = tejapi.get('TWN/AINVFINQA',
                    coid = ticker,
                    paginate = True,
                    chinese_column_name=transfer_to_chinese)
    # 對資料進行排序，相同['公司碼','年月','發布日','合併']的情況下，選次數大者
    x = data.sort_values(['coid','mdate','a_dd','merg','fin_od']).groupby(['coid','mdate','a_dd','merg']).tail(1)
    unique_date = x.drop_duplicates()
    # 針對相同['公司碼','發布日']的資料，若遇到相同發布日的情況，選財報年月最大者
    unique_date = unique_date.sort_values(['coid','mdate','a_dd']).groupby(['coid','a_dd']).tail(1).reset_index(drop=True)
    unique_date = unique_date.sort_values(['coid','mdate','a_dd','merg','fin_od'])
    return unique_date

def get_certain_fin_type(data, fin_type):
    if type(fin_type) is str:
        data = data[data['key3']==fin_type]
    else:
        data = data[data['key3'].isin(fin_type)]
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
    data = df.loc[df['key3']==pattern, :]
    # Create a mapping table of column names and their corresponding new names.
    new_keys = {i:i+'_'+str(pattern) for i in data.columns.difference(remain_keys)}
    # Replace old names with the new ones.
    data = data.rename(columns = new_keys)
    data = data.loc[:,~data.columns.str.contains('key3')]
    return data
