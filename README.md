# TEJ-Tool-API
## 優化現有TEJ API的整合資料功能:<br>
## TEJ-API
撈取資料方式(指定TABLE):<br>
```python
import tejapi
tejapi.ApiConfig.api_key = "YOURAPIKEY"
tejapi.ApiConfig.ignoretz = True
data = tejapi.get('TWN/APRCD',coid='Y9999', mdate={'gt':'2018-01-01','lt':'2018-02-01'}, paginate=True)
```
更多詳細資訊可參考官方網站: https://api.tej.com.tw/<br> ## Tool-API<br>
撈取資料方式:<br>
- get_history_data
```python
import tejapi
tejapi.ApiConfig.api_key = "YOURAPIKEY"
tejapi.ApiConfig.ignoretz = True
from TejToolAPI import *
list_of_Stocks = ['2330','2303','2454', '2882', '2881']
# 撈取歷史資料
data = get_history_data(ticker=list_of_Stocks,columns= ['稅前淨利成長率', '單月營收成長率%'], transfer_to_chinese=False) ```
只需要給定股票代碼(ticker)、資料欄位(columns, ex: close_adj, debt, roe...)，即可自動合併資料，不需要訪問多個資料表和自行合併。<br>
- get_internal_code <br>
內部欄位編碼與中英文簡稱轉換功能已完成，閱讀official documnet的時間可進一步縮短:
```python
get_internal_code(['稅前淨利成長率', 'Gross_Profit_Loss_from_Operations'])
```
['r404', 'R404', 'gm', 'GM']<br>
- search_columns <br>
若想反向取得內部編碼之中英文欄位則可利用 search_columns 這個function <br>
```python
search_columns(['r404'])
```
||columns|    chn_column_names|eng_column_names|table_names|TABLE_NAMES|API_TABLE    CHN_NAMES|
|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|
|0|    r404|    稅前淨利成長率|     Pre_Tax_Income_Growth_Rate|    fin_self_acc|    fin_self_acc|    TWN/AFESTM1    財務-自結數|
|1|    r404|    稅前淨利成長率|    Pre_Tax_Income_Growth_Rate|    fin_board_select|    fin_board_select|    TWN/AFESTMD    財務-董事決議數| 
### On-going
不同頻率間的數據呼叫(目前僅完成日頻率，未來預計完成周、月和年)<br>
### More detailed
https://www.notion.so/TQ-Analytic-7e6b0604671d450d83d717b76bd78709
