# TEJ-Tool-API: Streamlined Data Retrieval

![image](https://user-images.githubusercontent.com/73147512/232701162-ce7d2c14-f5c1-42e3-9fe7-48aa912cfc1a.png)
Welcome to TEJ-Tool-API, a powerful package designed to optimize your experience with TEJ-API data retrieval. In the past, retrieving data involved accessing multiple tables and merging them into a dataframe manually. Now, you can achieve the same results faster and more efficiently with TEJ-Tool-API.<br>
## TEJ-API Integration
Here's a sample code for retrieving stock data from TEJ-API (TABLE: TWN/APRCD):<br>
```python
import tejapi
tejapi.ApiConfig.api_key = "YOURAPIKEY"
tejapi.ApiConfig.ignoretz = True
data = tejapi.get('TWN/APRCD',coid='Y9999', mdate={'gt':'2018-01-01','lt':'2018-02-01'}, paginate=True)
```
For more information, please visit the [official TEJ-API website](<https://api.tej.com.tw/>)<br>
## TEJ-Tool-API Features
With TEJ-Tool-API, you can easily retrieve stock data and streamline the process. Here are some key features:<br>
- get_history_data
Retrieve historical data for a list of stocks with specific columns.<br>
```python
import os
os.environ['TEJAPI_KEY'] = "your key" 
import TejToolAPI
list_of_Stocks = ['2330','2303','2454', '2882', '2881']
# 撈取歷史資料
data = get_history_data(ticker=list_of_Stocks, columns= ['稅前淨利成長率', '單月營收成長率%'], transfer_to_chinese=False) ```
```
- get_internal_code <br>
Easily access data using both Chinese and English column names.<br>
```python
get_internal_code(['稅前淨利成長率', 'Gross_Profit_Loss_from_Operations'])
```
outputs:<br>
['r404', 'R404', 'gm', 'GM']<br>

- search_columns <br>
Convert internal column codes to their corresponding Chinese and English names.<br>
```python
search_columns(['r404'])
```
outputs:<br>
||columns|    chn_column_names|eng_column_names|table_names|TABLE_NAMES|API_TABLE    CHN_NAMES|
|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|
|0|    r404|    稅前淨利成長率|     Pre_Tax_Income_Growth_Rate|    fin_self_acc|    fin_self_acc|    TWN/AFESTM1    財務-自結數|
|1|    r404|    稅前淨利成長率|    Pre_Tax_Income_Growth_Rate|    fin_board_select|    fin_board_select|    TWN/AFESTMD    財務-董事決議數| 


### Future Plans
We're continuously improving TEJ-Tool-API. Our next step is to employ CUDA to enhance the performance of streaming TEJ's database, providing even more efficiency and power to our users. Stay tuned for updates!<br>

