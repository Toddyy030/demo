import requests
import pandas as pd
import time
import math
import pymysql
from tqdm import tqdm
from sqlalchemy import create_engine
#final version
headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}
#用filter筛选条件
def get_payload(page):
  return {"page":page,"fundScale":"scope03","primaryInvestType":"smzqtzjjglr",'fundType': "smzqzzfx"
         }
All_Data = []
All_Data2 = []
url = "https://gs.amac.org.cn/amac-infodisc/api/pof/manager/query?&page={}&size=20"
#读取10-20亿first page的参数，得到total element和total page
first_request = requests.post(url = url.format(0),headers = headers,json = get_payload(0))
total_elements = first_request.json().get('totalElements')
total_pages = math.ceil(total_elements/20)
#用for loop爬取每一页的数据
for page in tqdm(range(0,total_pages)):
  request = requests.post(url=url.format(page),headers = headers,json = get_payload(page))
  if request.status_code == 200:
    data = request.json().get('content')
    All_Data.extend(data)
df = pd.DataFrame(All_Data)
#读取20-50亿first page的参数，得到total element和total page
def get_payload2(page):
  return {"page":page,"fundScale":"scope04","primaryInvestType":"smzqtzjjglr",'fundType': "smzqzzfx"
         }
url2 = 'https://gs.amac.org.cn/amac-infodisc/api/pof/manager/query?&page={}&size=20'
first_request2 = requests.post(url = url2.format(0),headers = headers,json = get_payload2(0))
total_elements2 = first_request.json().get('totalElements')
total_pages2 = math.ceil(total_elements/20)
for page in tqdm(range(0,total_pages)):
  request2 = requests.post(url=url2.format(page),headers = headers,json = get_payload2(page))
  if request.status_code == 200:
    data2 = request2.json().get('content')
    All_Data2.extend(data2)
df2 = pd.DataFrame(All_Data2)
engine = create_engine("mysql+pymysql://demo:%40123456%2B@120.48.57.24:3306/Euclid?charset=utf8mb4")
df.to_sql(name = "私募基金证券投资人_10-50亿",con = engine,if_exists = 'replace',index =False)
df2.to_sql(name = "私募基金证券投资人_10-50亿",con = engine,if_exists = 'append',index =False)
#read data from MySQL
#connect to the data base
df4 = pd.read_sql("SELECT*FROM bench_cons_rtn_from_wind WHERE code = '000001.SZ'",engine)
print(df4)






