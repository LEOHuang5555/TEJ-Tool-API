
import tejapi 
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "3jUCETU2KiPwGJeyETYOQd1TCoDoxX"
tejapi.ApiConfig.ignoretz = True
import pandas as pd
import parameters as para



def get_history_data(ticker, columns = None, start = para.default_start, end = para.default_end, transfer_to_chinese = False):
    all_tables = triggers(ticker, columns, start, end, transfer_to_chinese)
    # 搜尋觸發到那些 table
    trigger_tables = [i for i in all_tables.keys() if i in para.fin_invest_tables['TABLE_NAMES'].unique().tolist()]
    # 根據 OD 進行排序
    trigger_tables.sort(key = lambda x: para.map_table.loc[para.map_table['TABLE_NAMES']==x, 'OD'].item())    
    # tables 兩兩合併
    data = consecutive_merge(all_tables,  trigger_tables)
    data = data.drop(columns=[i for i in data.columns if i in para.drop_keys])

    return locals()['data']
    

def search_table(columns:list):
    index = para.table_columns['COLUMNS'].isin(columns)
    tables = para.table_columns.loc[index, :]
    return tables


def triggers(ticker, columns = None, start = para.default_start, end = para.default_end, transfer_to_chinese = False):
    trading_calendar = get_trading_calendar(ticker, start, end)
    columns = [i for i in columns if i !='coid' or i!='mdate']
    trigger_tables = search_table(columns)
    for table in trigger_tables['TABLE_NAMES'].unique():
        selected_columns = trigger_tables.loc[trigger_tables['TABLE_NAMES']==table, 'COLUMNS'].tolist()
        api_code = para.table_API.loc[para.table_API['TABLE_NAMES']==table, 'API_CODE'].item()
        api_table = para.fin_invest_tables.loc[para.fin_invest_tables['TABLE_NAMES']==table,'API_TABLE'].item()
        print(table)
        exec(f'{table} = get_api_data(api_code, api_table, ticker, selected_columns, start, end, transfer_to_chinese)')
    return locals()

def get_api_data(api_code, table, ticker, columns = None, start = para.default_start, end = para.default_end, transfer_to_chinese = False):
    data = para.funct_map[api_code](table, ticker, columns, start, end, transfer_to_chinese) 
    return data

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


def consecutive_merge(local_var, loop_array):
    table_keys = para.map_table.merge(para.merge_keys)

    # tables 兩兩合併
    data = local_var['trading_calendar']
    for i in range(len(loop_array)):
        # left_keys = table_keys.loc[table_keys['TABLE_NAMES']==loop_array[i], 'KEYS'].tolist()
        right_keys = table_keys.loc[table_keys['TABLE_NAMES']==loop_array[i], 'KEYS'].tolist()
        data = data.merge(local_var[loop_array[i]], left_on = ['coid', 'mdate'], right_on = right_keys, how = 'left', suffixes = ('','_surfeit'))
        data = data.loc[:,~data.columns.str.contains('_surfeit')]
        
    return data

def get_trading_calendar(ticker, start, end):
    trading_calendar = pd.DataFrame()
    for i in ticker:
        # stock price
        data = tejapi.get('TWN/APIPRCD',
                        coid = i,
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':['coid','mdate']})
        trading_calendar = pd.concat([trading_calendar, data])
    return trading_calendar








