import pandas as pd
import tejapi 
tejapi.ApiConfig.api_base="http://10.10.10.66"
tejapi.ApiConfig.api_key = "3jUCETU2KiPwGJeyETYOQd1TCoDoxX"
tejapi.ApiConfig.ignoretz = True
import map_functions as mpf
# from All_functions import get_fin_acc_code

def get_fin_acc_code():
    acc_info = tejapi.get('TWN/AINVFACC_INFO_C',
                        paginate = True,
                        chinese_column_name=False)
    return acc_info['acct_code'].to_list()

# 取得每張 table 的欄位名稱(internal_code)
# 股價資料
trading_columns_group = tejapi.table_info('TWN/APIPRCD')['filters']
# all_fin_acc_code = get_fin_acc_code()
# fin_columns_group = ['coid', 'mdate', 'fin_od'] + get_fin_acc_code()
# 公司自結數
fin_company_group = list(set(['coid', 'mdate', 'annd']+tejapi.table_info('TWN/AFESTM1')['filters']))
# 董事會決議數
fin_board_group = list(set(['coid', 'mdate', 'annd']+tejapi.table_info('TWN/AFESTMD')['filters']))
# 籌碼資料
alternative_group = tejapi.table_info('TWN/APISHRACT')['filters']
# 集保資料
share_dist_group = tejapi.table_info('TWN/APISHRACTW')['filters']
# 月營收
monthly_revenue_group = tejapi.table_info('TWN/APISALE')['filters']

# 取得 table_names, od, keys
# map_table: table_name, od
map_table = pd.read_excel('columns_group.xlsx',sheet_name='table_od')
# merge_keys: od, merge_keys
merge_keys = pd.read_excel('columns_group.xlsx',sheet_name='merge_keys')

# 映射函數
funct_map = {
    (1,1):mpf.merge_1_1,
    (2,2):mpf.merge_2_2,
    (1,2):mpf.merge_1_2,
    (2,1):mpf.merge_1_2
    }
