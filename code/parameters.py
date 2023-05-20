import pandas as pd
import datetime
from TejToolAPI import Map_Dask_API as dask_mpf
import multiprocessing
import os

# current directory
module_dir = os.path.dirname(os.path.abspath(__file__))
xlsx_path = os.path.join(module_dir,'tables','columns_group.xlsx')
# print(module_dir, xlsx_path)
# get number of cpu
npartitions_local = multiprocessing.cpu_count()
# set default start and end date
default_start = '2013-01-01'
default_end = datetime.datetime.now().date().strftime('%Y-%m-%d')
# drop useless keys
drop_keys = [ 'no','sem','fin_type','annd', 'annd_s','edate1','edate2','all_dates', 'fin_ind', 'curr']
# 取得每張 table 的欄位名稱(internal_code)
# get table_names, API_table, CHN_NAMES
fin_invest_tables = pd.read_excel(xlsx_path, sheet_name='fin_invest_tables')
# get table_names, columns
table_columns = pd.read_excel(xlsx_path, sheet_name='table_columns')
# get table_names, API_code
table_API = pd.read_excel(xlsx_path, sheet_name='API')
# get chinese name and english name of the columns
transfer_language_table = pd.read_excel(xlsx_path, sheet_name='transfer_language')
# 取得 table_names, od, keys
# map_table: table_name, odd
map_table = pd.read_excel(xlsx_path,sheet_name='table_od')
# merge_keys: od, merge_keys
merge_keys = pd.read_excel(xlsx_path,sheet_name='merge_keys')


# 映射函數 (dask_version)
funct_map = {
    'A0001':dask_mpf.get_trading_data,
    'A0002':dask_mpf.get_fin_data,
    'A0003':dask_mpf.get_alternative_data,
    'A0004':dask_mpf.get_fin_auditor
}



