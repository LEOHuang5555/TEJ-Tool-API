import pandas as pd
import tejapi 
import datetime
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "3jUCETU2KiPwGJeyETYOQd1TCoDoxX"
tejapi.ApiConfig.ignoretz = True
import API_mapping as mpf
# from All_functions import get_fin_acc_code

default_start = '2013-01-01'
default_end = datetime.datetime.now().date().strftime('%Y-%m-%d')
drop_keys = [ 'no','sem','fin_type','annd', 'annd_s']

# 取得每張 table 的欄位名稱(internal_code)
# get table_names, API_table
fin_invest_tables = pd.read_excel('columns_group.xlsx', sheet_name='fin_invest_tables')
# get table_names, columns
table_columns = pd.read_excel('columns_group.xlsx', sheet_name='group')
# get table_names, API_code
table_API = pd.read_excel('columns_group.xlsx', sheet_name='API')

# 取得 table_names, od, keys
# map_table: table_name, od
map_table = pd.read_excel('columns_group.xlsx',sheet_name='table_od')
# merge_keys: od, merge_keys
merge_keys = pd.read_excel('columns_group.xlsx',sheet_name='merge_keys')

# 映射函數
funct_map = {
    'A0001':mpf.get_trading_data,
    'A0002':mpf.get_fin_data,
    'A0003':mpf.get_alternative_data
}



