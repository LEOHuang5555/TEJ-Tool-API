# TEJ-Tool-API

## Optimized the current usage of TEJ-API by a integrated package, TEJ-Tool-API:<br>
In the past, we had to access multi-tables and merge all tables into a dataframe by ourselves, but now we can get the same result in a faster and more friendly way!<br>
![image](https://user-images.githubusercontent.com/73147512/232701162-ce7d2c14-f5c1-42e3-9fe7-48aa912cfc1a.png)
## TEJ-API
Sample for retrieving stock data from TEJ-API(TABLE: TWN/APRCD):<br>
```python
import tejapi
tejapi.ApiConfig.api_key = "YOURAPIKEY"
tejapi.ApiConfig.ignoretz = True
data = tejapi.get('TWN/APRCD',coid='Y9999', mdate={'gt':'2018-01-01','lt':'2018-02-01'}, paginate=True)
```
For more information, please check the official website (https://api.tej.com.tw/)<br> 
## Tool-API<br>
Sample for retrieving stock data from TEJ-Tool-API: <br>
- get_history_data
```python
import os
os.environ['TEJAPI_KEY'] = "your key" 
import TejToolAPI
list_of_Stocks = ['2330','2303','2454', '2882', '2881']
# 撈取歷史資料
data = get_history_data(ticker=list_of_Stocks, columns= ['稅前淨利成長率', '單月營收成長率%'], transfer_to_chinese=False) ```
By setting two parameters: ticker and columns TEJ-Tool-API will simultaneously get the data and merge them in one place. <br>
```
- get_internal_code <br>
TEJ-Tool-API also support for language transformation. <br>
Users can use both Chinese and English name of columns to access the data. <br>
There is no needs to read our official document anymore!
```python
get_internal_code(['稅前淨利成長率', 'Gross_Profit_Loss_from_Operations'])
```
['r404', 'R404', 'gm', 'GM']<br>

- search_columns <br>
Users can transfer our internal code of columns to the corresponding Chinese and English name of columns by this function.<br>
```python
search_columns(['r404'])
```
||columns|    chn_column_names|eng_column_names|table_names|TABLE_NAMES|API_TABLE    CHN_NAMES|
|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|
|0|    r404|    稅前淨利成長率|     Pre_Tax_Income_Growth_Rate|    fin_self_acc|    fin_self_acc|    TWN/AFESTM1    財務-自結數|
|1|    r404|    稅前淨利成長率|    Pre_Tax_Income_Growth_Rate|    fin_board_select|    fin_board_select|    TWN/AFESTMD    財務-董事決議數| 


### Future Plan
Employed CUDA to enhance the performance of streaming TEJ's database.<br>

