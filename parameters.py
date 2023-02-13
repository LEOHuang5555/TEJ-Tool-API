import pandas as pd
# map_table: table_name, od
map_table = pd.read_excel('columns_group.xlsx',sheet_name='table_od')
# merge_keys: od, merge_keys
merge_keys = pd.read_excel('columns_group.xlsx',sheet_name='merge_keys')
