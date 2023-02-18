def merge_1_1(var_dict, table1:str, table2:str):
    if len(var_dict[table1]) >= len(var_dict[table2]):
        t1 = var_dict[table1]
        t2 = var_dict[table2]
    else:
        t1 = var_dict[table2]
        t2 = var_dict[table1]
    data = t1.merge(t2, on = ['coid', 'mdate'], how = 'left', suffixes = ('', '_surfeit')).ffill()
    # 刪除多餘的欄位
    data = data[[i for i in data.columns if not i.__contains__('_surfeit')]]
    return data

def merge_2_2(var_dict, table1:str, table2:str):
    if len(var_dict[table1]) >= len(var_dict[table2]):
        t1 = var_dict[table1]
        t2 = var_dict[table2]
    else:
        t1 = var_dict[table2]
        t2 = var_dict[table1]
    data = t1.merge(t2, on = ['coid', 'all_dates'], how = 'left', suffixes = ('', '_surfeit')).ffill()
    # 刪除多餘的欄位
    data = data[[i for i in data.columns if not i.__contains__('_surfeit')]]
    return data

def merge_1_2(var_dict, table1:str, table2:str):
    # 
    if para.map_table.loc[para.map_table['TABLE_NAME']==table1, 'OD'].item() == 1:
        t1 = var_dict[table1]
        t2 = var_dict[table2]
    else:
        t1 = var_dict[table2]
        t2 = var_dict[table1]
    data = t1.merge(t2, left_on = ['coid', 'mdate'], right_on = ['coid', 'all_dates'], how = 'left' ,suffixes = ('', '_surfeit')).ffill()
    # 刪除多餘的欄位
    data = data[[i for i in data.columns if not i.__contains__('_surfeit')]]
    return data
