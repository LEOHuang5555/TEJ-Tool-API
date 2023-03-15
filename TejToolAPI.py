import tejapi 
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "3jUCETU2KiPwGJeyETYOQd1TCoDoxX"
tejapi.ApiConfig.ignoretz = True
import pandas as pd
import parameters as para



def get_history_data(ticker, columns = None, start = para.default_start, end = para.default_end, transfer_to_chinese = False, fin_type = ['A','Q','TTM']):
    all_tables = triggers(ticker, columns, start, end, transfer_to_chinese, fin_type)
    # 搜尋觸發到那些 table
    trigger_tables = [i for i in all_tables.keys() if i in para.fin_invest_tables['TABLE_NAMES'].unique().tolist()]
    # 根據 OD 進行排序
    trigger_tables.sort(key = lambda x: para.map_table.loc[para.map_table['TABLE_NAMES']==x, 'OD'].item())    
    # tables 兩兩合併
    history_data = consecutive_merge(all_tables,  trigger_tables)
    history_data = history_data.drop(columns=[i for i in history_data.columns if i in para.drop_keys])

    return history_data
    

def search_table(columns:list):
    index = para.table_columns['COLUMNS'].isin(columns)
    tables = para.table_columns.loc[index, :]
    return tables

def search_columns(columns:list):
    index = para.internal_code_mapping_table['columns'].isin(columns)
    tables = para.internal_code_mapping_table.loc[index, :]
    tables = tables.merge(para.fin_invest_tables, left_on = 'table_names', right_on='TABLE_NAMES')
    return tables

def triggers(ticker, columns = None, start = para.default_start, end = para.default_end, transfer_to_chinese = False, fin_type = ['A','Q','TTM']):
    columns = get_internal_code(columns)
    trading_calendar = get_trading_calendar(ticker, start, end)
    columns = [i for i in columns if i !='coid' or i!='mdate']
    trigger_tables = search_table(columns)
    for table_name in trigger_tables['TABLE_NAMES'].unique():
        selected_columns = trigger_tables.loc[trigger_tables['TABLE_NAMES']==table_name, 'COLUMNS'].tolist()
        api_code = para.table_API.loc[para.table_API['TABLE_NAMES']==table_name, 'API_CODE'].item()
        api_table = para.fin_invest_tables.loc[para.fin_invest_tables['TABLE_NAMES']==table_name,'API_TABLE'].item()
        if api_code == 'A0002':
            exec(f'{table_name} = para.funct_map[api_code](api_table, ticker, selected_columns, start, end, transfer_to_chinese, fin_type)')
        else:
            exec(f'{table_name} = para.funct_map[api_code](api_table, ticker, selected_columns, start, end, transfer_to_chinese)')
    return locals()

def get_api_data(api_code, table, ticker, columns = None, start = para.default_start, end = para.default_end, transfer_to_chinese = False):
    data = para.funct_map[api_code](table, ticker, columns, start, end, transfer_to_chinese) 
    return data


def get_internal_code(fields:list):
    columns = []
    for c in ['eng_column_names', 'chn_column_names', 'columns']:
        temp = para.internal_code_mapping_table.loc[para.internal_code_mapping_table[c].isin(fields), 'columns'].tolist()
        columns += temp
    columns = list(set(columns))
    return columns


def consecutive_merge(local_var, loop_array):
    table_keys = para.map_table.merge(para.merge_keys)
    # tables 兩兩合併
    data = local_var['trading_calendar']
    for i in range(len(loop_array)):
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








