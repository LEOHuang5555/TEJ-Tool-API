import pandas as pd
import numpy as np
import tejapi 
import datetime
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "3jUCETU2KiPwGJeyETYOQd1TCoDoxX"
tejapi.ApiConfig.ignoretz = True
import API_mapping as mpf

class Parameters():
    def __init__(self) -> None:
        self.default_start = '2013-01-01'
        self.default_end = datetime.datetime.now().date().strftime('%Y-%m-%d')
        self.drop_keys = [ 'no','sem','fin_type','annd', 'annd_s','edate1','all_dates']
        # 取得每張 table 的欄位名稱(internal_code)
        # get table_names, API_table, CHN_NAMES
        self.fin_invest_tables = pd.read_excel('columns_group.xlsx', sheet_name='fin_invest_tables')
        # get table_names, columns
        self.table_columns = pd.read_excel('columns_group.xlsx', sheet_name='group')
        # get table_names, API_code
        self.table_API = pd.read_excel('columns_group.xlsx', sheet_name='API')
        # 取得欄位中英文名稱和內部編碼對照表
        self.internal_code_mapping_table = pd.read_csv('table_columns.csv', encoding='big5')
        # 取得 table_names, od, keys
        # map_table: table_name, od
        self.map_table = pd.read_excel('columns_group.xlsx',sheet_name='table_od')
        # merge_keys: od, merge_keys
        self.merge_keys = pd.read_excel('columns_group.xlsx',sheet_name='merge_keys')
        # mapping functions
        self.funct_map = {
            'A0001':mpf.get_trading_data,
            'A0002':mpf.get_fin_data,
            'A0003':mpf.get_alternative_data,
            'A0004':mpf.get_fin_auditor
        }
        
class ToolAPI(Parameters):
    def __init__(self, tickers:list = ['2330'], fields:list = [], start = '', end = '', transfer_chinese = False, fin_type = ['A','Q','TTM'], include_self_acc = 'Y') -> None:
    # def __init__(self):
        super().__init__()
        # super().__init__(tickers = tickers, fields = fields, start = start, end = end, transfer_chinese= transfer_chinese, fin_type= fin_type)
        self.tickers = tickers
        self.fields = fields
        self.start = self.default_start if not start else start
        self.end = self.default_end if not end else end
        self.transfer_to_chinese = transfer_chinese
        self.fin_type = fin_type
        self.include_self_acc = include_self_acc

    def get_history_data(self):
        self.all_tables = self.triggers()
        if self.include_self_acc == 'Y':
            try:
                data_concat = pd.concat([self.all_tables['fin_self_acc'], self.all_tables['fin_auditor']])
                self.all_tables['fin_self_acc'] = data_concat.drop_duplicates(subset=['all_dates', 'coid'], keep='last')
                del self.all_tables['fin_auditor']
            except:
                pass
        # 搜尋觸發到那些 table
        trigger_tables = [i for i in self.all_tables.keys() if i in self.fin_invest_tables['TABLE_NAMES'].unique().tolist()]
        # 根據 OD 進行排序
        trigger_tables.sort(key = lambda x: self.map_table.loc[self.map_table['TABLE_NAMES']==x, 'OD'].item())    
        # tables 兩兩合併
        history_data = self.consecutive_merge(self.all_tables,  trigger_tables)
        history_data = history_data.drop(columns=[i for i in history_data.columns if i in self.drop_keys])
        if self.transfer_to_chinese:
            # history_data.columns
            chn_map = self.transfer_chinese_columns(history_data.columns)
            history_data = history_data.rename(columns= chn_map)
        return history_data

    def transfer_chinese_columns(self, columns):
        def get_chn_name(col):
            chn_name = self.search_columns([col])['chn_column_names'].drop_duplicates(keep='last').item()
            return chn_name if chn_name else col
        
        mapping = {}
        for col in columns:
            # 將 _A, _Q, _TTM 等字串移除
            check_fin_type = [col.__contains__('_A'), col.__contains__('_Q'), col.__contains__('_TTM')]
            if any(check_fin_type):
                col_stripped = col.split('_')[0]
                fin_type = col.split('_')[1]
            else:
                col_stripped = col
                fin_type = ''
            # 尋找對應中文欄位名稱
            chn_name = get_chn_name(col_stripped)
            # 將對應關係加入 mapping
            mapping[col] = f"{chn_name}_{fin_type}" if '_' in col else chn_name

        return mapping

    def search_table(self):
        index = self.table_columns['COLUMNS'].isin(self.fields)
        tables = self.table_columns.loc[index, :]
        return tables

    def search_columns(self, columns:list):
        index = self.internal_code_mapping_table['columns'].isin(columns)
        tables = self.internal_code_mapping_table.loc[index, :]
        tables = tables.merge(self.fin_invest_tables, left_on = 'table_names', right_on='TABLE_NAMES')
        return tables

    def triggers(self):
        self.fields = self.get_internal_code()
        trading_calendar = self.get_trading_calendar()
        columns = [i for i in self.fields if i !='coid' or i!='mdate']
        self.trigger_tables = self.search_table()
        # if include_self_acc equals to False, then delete the fin_self_acc in the trigger_tables list
        if not self.include_self_acc:
            self.trigger_tables = self.trigger_tables.loc[self.trigger_tables['TABLE_NAMES']!='fin_self_acc',:]
        for table_name in self.trigger_tables['TABLE_NAMES'].unique():
            selected_columns = self.trigger_tables.loc[self.trigger_tables['TABLE_NAMES']==table_name, 'COLUMNS'].tolist()
            api_code = self.table_API.loc[self.table_API['TABLE_NAMES']==table_name, 'API_CODE'].item()
            api_table = self.fin_invest_tables.loc[self.fin_invest_tables['TABLE_NAMES']==table_name,'API_TABLE'].item()
            if api_code == 'A0002' or api_code == 'A0004':
                exec(f'{table_name} = self.funct_map[api_code](api_table, self.tickers, selected_columns, self.start, self.end, self.fin_type, self.transfer_to_chinese)')
            else:
                exec(f'{table_name} = self.funct_map[api_code](api_table, self.tickers, selected_columns, self.start, self.end)')
        return locals()

    def get_internal_code(self):
        columns = []
        for col in ['eng_column_names', 'chn_column_names', 'columns']:
            temp = self.internal_code_mapping_table.loc[self.internal_code_mapping_table[col].isin(self.fields), 'columns'].tolist()
            columns += temp
        columns = list(set(columns))
        return columns

    def consecutive_merge(self, local_var, loop_array):
        table_keys = self.map_table.merge(self.merge_keys)
        # tables 兩兩合併
        data = local_var['trading_calendar']
        for i in range(len(loop_array)):
            right_keys = table_keys.loc[table_keys['TABLE_NAMES']==loop_array[i], 'KEYS'].tolist()
            data = data.merge(local_var[loop_array[i]], left_on = ['coid', 'mdate'], right_on = right_keys, how = 'left', suffixes = ('','_surfeit'))
            data = data.loc[:,~data.columns.str.contains('_surfeit')]
            
        return data

    def get_trading_calendar(self):
        trading_calendar = pd.DataFrame()
        for i in self.tickers:
            # stock price
            data = tejapi.get('TWN/APIPRCD',
                            coid = i,
                            paginate = True,
                            chinese_column_name=False,
                            mdate = {'gte':self.start,'lte':self.end},
                            opts = {'columns':['coid','mdate']})
            trading_calendar = pd.concat([trading_calendar, data])
        return trading_calendar 
    

