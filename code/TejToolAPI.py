import tejapi 
tejapi.ApiConfig.ignoretz = True
tejapi.ApiConfig.page_limit=10000
import dask.dataframe as dd
import pandas as pd
import gc
from . import parameters as para
import dask
import dask.dataframe as dd



def get_history_data(tickers:list, columns:list = [], fin_type:list = ['A','Q','TTM'], include_self_acc:str = 'N', **kwargs):
    # Setting default value of the corresponding parameters
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    transfer_to_chinese = kwargs.get('transfer_to_chinese', False)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)
    all_tables = triggers(ticker = tickers, columns= columns, start= start, end= end, fin_type= fin_type, include_self_acc= include_self_acc, npartitions = npartitions)

    def process_fin_data(all_tables, variable, tickers, start, end):
        # transfer to daily basis
        days = para.dask_mpf.generate_multicalendars(tickers, start = start, end = end)
        all_tables[variable] = dd.merge(days, all_tables[variable], left_on=['coid','all_dates'], right_on=['coid', 'annd'], how='left')
        
        # Drop mdate column
        all_tables[variable] = all_tables[variable].drop(columns = 'mdate')

        # Delete the redundant dataframe to release memory space
        del days
        gc.collect()

        # Filling NaN with the precending value
        all_tables[variable] = all_tables[variable].groupby('coid', group_keys = False).apply(para.dask_mpf.fillna_multicolumns, meta = all_tables[variable])
        
        return all_tables[variable]
    
       
    # 針對自結損益和財簽資料作處理
    try:
        # Concate fin_self_acc with fin_auditor
        data_concat = dd.concat([all_tables['fin_self_acc'], all_tables['fin_auditor']]).reset_index(drop=True)
        all_tables['fin_auditor'] = data_concat.drop_duplicates(subset=['coid','mdate','annd'], keep='last')

        # Process two fin dataframe
        all_tables['fin_auditor'] = process_fin_data(all_tables=all_tables, variable='fin_auditor', tickers=tickers, start=start, end= end)
        
        del all_tables['fin_self_acc']
        # return all_tables

    except:
        if 'fin_auditor' in all_tables.keys():
            all_tables['fin_auditor'] = process_fin_data(all_tables=all_tables, variable='fin_auditor', tickers=tickers, start=start, end= end)

        elif 'fin_self_acc' in all_tables.keys():
            all_tables['fin_self_acc'] = process_fin_data(all_tables=all_tables, variable='fin_self_acc', tickers=tickers, start=start, end= end)
        
        else:
            pass

    # 搜尋觸發到那些 table
    trigger_tables = [i for i in all_tables.keys() if i in para.fin_invest_tables['TABLE_NAMES'].unique().tolist()]

    # 根據 OD 進行排序
    trigger_tables.sort(key = lambda x: para.map_table.loc[para.map_table['TABLE_NAMES']==x, 'OD'].item())

    # tables 兩兩合併
    history_data = consecutive_merge(all_tables,  trigger_tables)
    history_data = history_data.drop(columns=[i for i in history_data.columns if i in para.drop_keys])
    history_data = history_data.drop_duplicates(subset=['coid', 'mdate'], keep='last').repartition(npartitions=npartitions)

    if transfer_to_chinese is False:
        # transfer to Chinese version
        lang_map = transfer_language_columns(history_data.columns, isEnglish=True)
        history_data = history_data.rename(columns= lang_map)

    elif transfer_to_chinese is True:
        # transfer to English version
        lang_map = transfer_language_columns(history_data.columns, isEnglish=False)
        history_data = history_data.rename(columns= lang_map)

    else:
        pass

    return history_data.compute(meta = history_data)


def to_daskDataFrame(locals, indexs, npartitions=para.npartitions_local):
    for i in indexs:
        locals[i] = dd.from_pandas(locals[i], npartitions=npartitions)
    return locals

def transfer_language_columns(columns, isEnglish = True):
    def get_col_name(col, isEnglish):
        transfer_lang = 'ENG_COLUMN_NAMES' if isEnglish else 'CHN_COLUMN_NAMES'

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
    return tables

def triggers(ticker:list, columns:list = [], fin_type:list = ['A','Q','TTM'],  include_self_acc:str = 'N', **kwargs):
    # Setting default value of the corresponding parameters
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)
    
    # Tranfer columns from any type (chinese, english) to internal code  
    columns = get_internal_code(columns)    

    # Kick out `coid` and `mdate` from
    columns = [i for i in columns if i !='coid' or i!='mdate']

    # Qualify the table triggered by the given `columns`
    trigger_tables = search_table(columns)

    # Get trading calendar of all given tickers
    # if 'stk_price' not in trigger_tables['TABLE_NAMES'].unique():
    trading_calendar = get_trading_calendar(ticker, start = start, end = end, npartitions = npartitions)

    # If include_self_acc equals to 'N', then delete the fin_self_acc in the trigger_tables list
    if include_self_acc =='N':
        trigger_tables = trigger_tables.loc[trigger_tables['TABLE_NAMES']!='fin_self_acc',:]

    for table_name in trigger_tables['TABLE_NAMES'].unique():
        # print(table_name)
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
    #
    table_keys = para.map_table.merge(para.merge_keys)

    # tables 兩兩合併
    data = local_var['trading_calendar']

    for i in range(len(loop_array)):
        right_keys = table_keys.loc[table_keys['TABLE_NAMES']==loop_array[i], 'KEYS'].tolist()
        # dask merge
        # print(loop_array[i])
        data = dd.merge(data, local_var[loop_array[i]], left_on = ['coid', 'mdate'], right_on = right_keys, how = 'left', suffixes = ('','_surfeit'))
        
        # Clear the right table to release memory
        del local_var[loop_array[i]]
        gc.collect()
        
        # Drop surfeit columns
        data = data.loc[:,~data.columns.str.contains('_surfeit')]

    # Ensure the type of mdate is appropriate
    data['mdate'] = dd.to_datetime(data['mdate'])

    return data

def get_trading_calendar(tickers, **kwargs):
    # Setting default value of the corresponding parameters
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)

    def get_data(tickers):
        # trading calendar
        data = tejapi.get('TWN/APIPRCD',
                        coid = tickers,
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':['coid','mdate'], 'sort':{'coid.asc', 'mdate.asc'}})
        if len(data)>1:
            return data
        else:
            return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ns]')})
    
    # Define the meta of the dataframe
    meta = pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ns]')})

    # Calculate the number of tickers in each partition. 
    ticker_partitions = para.dask_mpf.get_partition_group(tickers = tickers, npartitions= npartitions)

    # Submit jobs to the parallel cores
    trading_calendar = dd.from_delayed([dask.delayed(get_data)(tickers[(i-1)*npartitions:i*npartitions]) for i in range(1, ticker_partitions)], meta = meta)

    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if trading_calendar.npartitions < 12:
        trading_calendar = trading_calendar.repartition(npartitions=npartitions)

    return trading_calendar






