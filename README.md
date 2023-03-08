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
更多詳細資訊可參考官方網站: https://api.tej.com.tw/<br>

## Tool-API<br>
撈取資料方式:<br>
```python
import tejapi
tejapi.ApiConfig.api_key = "YOURAPIKEY"
tejapi.ApiConfig.ignoretz = True
from TejToolAPI import *
list_of_Stocks = ['2330','2303','2454', '2882', '2881']
# 取得財務報表之欄位
fin_codes = get_fin_acc_code()
# 隨機抽取5筆進行測試
rand_series = np.random.randint(0,len(fin_codes),5)
test_columns = [fin_codes[i] for i in rand_series]
# 撈取歷史資料
data = get_history_data(ticker=list_of_Stocks,columns= test_columns, transfer_to_chinese=False)

```
只需要給定股票代碼(ticker)、資料欄位(columns, ex: close_adj, debt, roe...)，即可自動合併資料，不需要訪問多個資料表和自行合併。

### On-going
內部欄位編碼與中英文簡稱轉換功能<br>
不同頻率間的數據呼叫(目前僅完成日頻率，未來預計完成周、月和年)<br>
