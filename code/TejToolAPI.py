import tejapi 
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "3jUCETU2KiPwGJeyETYOQd1TCoDoxX"
tejapi.ApiConfig.ignoretz = True
import dask.dataframe as dd
import pandas as pd
import gc
import parameters as para
import dask
import dask.dataframe as dd



def get_history_data(ticker:list, columns:list = [], fin_type:list = ['A','Q','TTM'], include_self_acc:str = 'Y', **kwargs):
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    fin_type = kwargs.get('fin_type', ['A', 'Q', 'TTM'])
    transfer_to_chinese = kwargs.get('transfer_to_chinese', False)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)
    all_tables = triggers(ticker = ticker, columns= columns, start= start, end= end,fin_type= fin_type, include_self_acc= include_self_acc, npartitions = npartitions)
    if include_self_acc == 'Y':
        try:
            data_concat = pd.concat([all_tables['fin_self_acc'], all_tables['fin_auditor']])
            all_tables['fin_self_acc'] = data_concat.drop_duplicates(subset=['all_dates', 'coid'], keep='last')
            del all_tables['fin_auditor']
        except:
            pass
    # 搜尋觸發到那些 table
    trigger_tables = [i for i in all_tables.keys() if i in para.fin_invest_tables['TABLE_NAMES'].unique().tolist()]
    # 根據 OD 進行排序
    trigger_tables.sort(key = lambda x: para.map_table.loc[para.map_table['TABLE_NAMES']==x, 'OD'].item())
    # 將 pandas dataframe 轉為 dask dataframe
    # all_tables = to_daskDataFrame(all_tables, trigger_tables, npartitions=npartitions)
    # tables 兩兩合併
    history_data = consecutive_merge(all_tables,  trigger_tables)
    history_data = history_data.drop(columns=[i for i in history_data.columns if i in para.drop_keys])
    if transfer_to_chinese:
        # transfer to Chinese version
        lang_map = transfer_language_columns(history_data.columns, isEnglish=False)
    else:
        # transfer to English version
        lang_map = transfer_language_columns(history_data.columns)
    history_data = history_data.rename(columns= lang_map)
    return history_data

def to_daskDataFrame(locals, indexs, npartitions=para.npartitions_local):
    for i in indexs:
        locals[i] = dd.from_pandas(locals[i], npartitions=npartitions)
    return locals

def transfer_language_columns(columns, isEnglish = True):
    def get_col_name(col, isEnglish):
        transfer_lang = 'ENG_COLUMN_NAMES' if isEnglish else 'CHN_COLUMN_NAMES'
        # print(search_columns([col])[transfer_lang])
        try:
            col_name = search_columns([col])[transfer_lang].dropna().drop_duplicates(keep='last').item()
        except:
            col_name = search_columns([col])[transfer_lang].dropna().tail(1).item()
        return col_name if col_name else col
    
    mapping = {}
    for col in columns:
        # 將 _A, _Q, _TTM 等字串移除
        check_fin_type = [col.__contains__('_A'), col.__contains__('_Q'), col.__contains__('_TTM')]
        if any(check_fin_type):
            col_stripped = col.split('_')[:-1]
            fin_type = '_' + col.split('_')[-1]
            # 若欄位名稱本身含有 '_'，則要將底線組合回去
            if type(col_stripped) is list:
                col_stripped = '_'.join(col_stripped)
        else:
            col_stripped = col
            fin_type = ''
        # 尋找對應中文欄位名稱
        col_name = get_col_name(col_stripped, isEnglish)
        if col_name not in mapping.keys():
            # 將對應關係加入 mapping
            mapping[col] = f"{col_name}{fin_type}"

    return mapping

def search_table(columns:list):
    columns = list(map(lambda x:x.lower(), columns))
    index = para.table_columns['COLUMNS'].isin(columns)
    tables = para.table_columns.loc[index, :]
    return tables

def search_columns(columns:list):
    index = para.transfer_language_table['COLUMNS'].isin(columns)
    tables = para.transfer_language_table.loc[index, :]
    # tables = tables.merge(para.fin_invest_tables, left_on = 'table_names', right_on='TABLE_NAMES')
    return tables

def triggers(ticker:list, columns:list = [], fin_type:list = ['A','Q','TTM'],  include_self_acc:str = 'Y', **kwargs):
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)
    columns = get_internal_code(columns)    
    trading_calendar = get_trading_calendar(ticker, start = start, end = end, npartitions = npartitions)
    columns = [i for i in columns if i !='coid' or i!='mdate']
    trigger_tables = search_table(columns)
    # if include_self_acc equals to 'N', then delete the fin_self_acc in the trigger_tables list
    if include_self_acc =='N':
        trigger_tables = trigger_tables.loc[trigger_tables['TABLE_NAMES']!='fin_self_acc',:]
    for table_name in trigger_tables['TABLE_NAMES'].unique():
        selected_columns = trigger_tables.loc[trigger_tables['TABLE_NAMES']==table_name, 'COLUMNS'].tolist()
        api_code = para.table_API.loc[para.table_API['TABLE_NAMES']==table_name, 'API_CODE'].item()
        api_table = para.fin_invest_tables.loc[para.fin_invest_tables['TABLE_NAMES']==table_name,'API_TABLE'].item()
        if api_code == 'A0002' or api_code == 'A0004':
            exec(f'{table_name} = para.funct_map[api_code](api_table, ticker, selected_columns, start = start,  end = end, fin_type = fin_type, npartitions = npartitions)')
        else:
            exec(f'{table_name} = para.funct_map[api_code](api_table, ticker, selected_columns, start = start,  end = end, npartitions = npartitions)')
    return locals()

def get_internal_code(fields:list):
    columns = []
    for c in ['ENG_COLUMN_NAMES', 'CHN_COLUMN_NAMES', 'COLUMNS']:
        temp = para.transfer_language_table.loc[para.transfer_language_table[c].isin(fields), 'COLUMNS'].tolist()
        columns += temp
    columns = list(set(columns))
    return columns

def consecutive_merge(local_var, loop_array):
    table_keys = para.map_table.merge(para.merge_keys)
    # tables 兩兩合併
    data = local_var['trading_calendar']
    for i in range(len(loop_array)):
        right_keys = table_keys.loc[table_keys['TABLE_NAMES']==loop_array[i], 'KEYS'].tolist()
        # pandas merge
        # data = data.merge(local_var[loop_array[i]], left_on = ['coid', 'mdate'], right_on = right_keys, how = 'left', suffixes = ('','_surfeit'))
        
        # dask merge
        data = dd.merge(data, local_var[loop_array[i]], left_on = ['coid', 'mdate'], right_on = right_keys, how = 'left', suffixes = ('','_surfeit'))
        # 將已合併完的DF刪除，並釋放記憶體
        del local_var[loop_array[i]]
        gc.collect()
        data = data.loc[:,~data.columns.str.contains('_surfeit')]
    # if data.npartitions > 1:
    #     return data.compute()
    # else:
    #     return data
    return data.compute()

def get_trading_calendar(tickers, **kwargs):
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)
    trading_calendar = pd.DataFrame()
    def _get_data(ticker):
        # trading calendar
        data = tejapi.get('TWN/APIPRCD',
                        coid = ticker,
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':['coid','mdate']})
        return data
    trading_calendar = dd.from_delayed([dask.delayed(_get_data)(ticker) for ticker in tickers])
    trading_calendar = trading_calendar.repartition(npartitions=npartitions)

    # for i in tickers:
    #     # stock price
    #     data = tejapi.get('TWN/APIPRCD',
    #                     coid = i,
    #                     paginate = True,
    #                     chinese_column_name=False,
    #                     mdate = {'gte':start,'lte':end},
    #                     opts = {'columns':['coid','mdate']})
    #     trading_calendar = pd.concat([trading_calendar, data])

    return trading_calendar








