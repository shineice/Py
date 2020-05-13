
import time
#import math
import pyodbc
import pandas as pd
from datetime import date, datetime
import numpy as np


conn=pyodbc.connect('dsn=NetSuite;UID=RHung@Top-Line.com;PWD=Netsuite888')
sqld=datetime.today().strftime("%Y/%m/%d")
ALLdata="""
select
i.name as 'Itemnumber',
i.weight as 'Cubic_Feet',
inventory.inv as "inventory",
price.cost as 'cost',
inventory.Loc as "location",
sts.status as "Status",
i.item_id as "ID",
imem.name as "mem"
from 
    items i
    left join (select ilm.item_id ,  ilm.available_count as inv , loc.NAME as "Loc"
               from item_location_map ilm
               join locations loc on loc.location_id=ilm.LOCATION_ID
               where 
               available_count>0 and
               Loc.NAME not like '%In Transit%') as inventory on inventory.item_id=i.item_id
    left join (select LIST_ID, LIST_ITEM_NAME as "status"
               from item_status
               ) as sts on sts.LIST_ID=i.STATUS_ID
    left join (select item_id,ITEM_UNIT_PRICE as cost from item_prices ip
    where ip.ITEM_PRICE_ID = '102' and ip.isinactive = 'No')as price on price.item_id=i.item_id
    left join item_group ig on ig.parent_id=i.item_id
    left join items imem on imem.item_id=ig.member_id
where
i.isinactive = 'No' and
i.STATUS_ID is not null 

order by i.name

"""




items=pd.read_sql(ALLdata,conn)
conn.close()

items.to_csv("items_all.csv",index=False)


#read Net Suite data
conn=pyodbc.connect('dsn=NetSuite;UID=RHung@Top-Line.com;PWD=Netsuite888')
sqld=datetime.today().strftime("%Y/%m/%d")
ALLdata="""
select
i.name as 'Itemnumber',
i.weight as 'Cubic_Feet',
inventory.inv as "inventory",
price.cost as 'cost',
inventory.Loc as "location",
sts.status as "Status"

from 
    items i
    left join (select ilm.item_id ,  ilm.available_count as inv , loc.NAME as "Loc"
               from item_location_map ilm
               join locations loc on loc.location_id=ilm.LOCATION_ID
               where 
               available_count>0 and
               Loc.NAME not like '%In Transit%') as inventory on inventory.item_id=i.item_id
    left join (select LIST_ID, LIST_ITEM_NAME as "status"
               from item_status
               ) as sts on sts.LIST_ID=i.STATUS_ID
    left join (select item_id,ITEM_UNIT_PRICE as cost from item_prices ip
    where ip.ITEM_PRICE_ID = '102' and ip.isinactive = 'No')as price on price.item_id=i.item_id
where
i.isinactive = 'No' and
i.STATUS_ID is not null and
inventory.inv is not null and
i.STATUS_ID not in ('5','8','9','10','11','12')
order by i.name
"""



last_sale="""
select 
              items.name as name,
              max(transactions.trandate) as \'Sales\',
	           locations.name as Destination,
              sts.status as "Status"
from
              transaction_lines
               left join transactions on transactions.transaction_id=transaction_lines.transaction_id
               left join items on items.item_id=transaction_lines.item_id
               left join locations on locations.location_id=transaction_lines.location_id
               left join (select LIST_ID, LIST_ITEM_NAME as "status"
               from item_status ) as sts on sts.LIST_ID=items.STATUS_ID
where
              transaction_lines.item_count < 0 and
              transaction_lines.ACCOUNT_ID=123 and
              transactions.entity_id is not null and
              transactions.transaction_type = \'Item Fulfillment\' and
              transactions.status = \'Shipped\' and
              transactions.trandate < \'"""+sqld+"""\' and
              items.isinactive = 'No' and
              items.STATUS_ID is not null and
              items.STATUS_ID not in ('5','8','9','10','11','12')
              
Group by
              items.name,locations.name, sts.status
order by items.name
"""




items=pd.read_sql(ALLdata,conn)
lastsale=pd.read_sql(last_sale,conn)

conn.close()

#items.to_csv("items_all.csv",index=False)
#lastsale.to_csv("lastsale.csv",index=False)
#quaO.to_csv("quaO.csv",index=False)

lastsale.sort_values(by=['name', 'Sales'],ascending=[True,False])

#lastsale.to_csv("lastsale.csv",index=False)

items=items[items['Status'].isin(['Tier 1 Seller','Current','Close Out'])]
items=items.fillna(0)

#整理資料表
#items_pivot=items.pivot_table(values='inventory',index=['Itemnumber'],columns=['location'],aggfunc=sum,margins=True)

items_status=items[['Itemnumber','Status']]
items_status=items_status.groupby('Itemnumber', as_index=False).min()


#inventory volume
items_Inv=items.pivot_table(values='inventory',index=['Itemnumber'],columns=['location'])
items_Inv=pd.merge(items_Inv,items_status,how='left',on='Itemnumber')
items_Inv=items_Inv.where(items_Inv!=0,"")
items_Inv=items_Inv.fillna(0)
items_Inv['Invnetory']=items_Inv['CA-F']+items_Inv['IL-S']
items_Inv=items_Inv[['Itemnumber','Status','CA-F',  'IL-S','Invnetory']]
#items_Inv.to_csv("D:\\inv0410.csv",index=False)


#Cubic_Feet
items_QBF=items.pivot_table(values='Cubic_Feet',index=['Itemnumber'],columns=['location'])
items_QBF=pd.merge(items_QBF,items_status,how='left',left_on='Itemnumber',right_on='Itemnumber')
items_QBF=items_QBF.where(items_QBF!=0,"")
items_QBF=items_QBF.fillna(0)
items_QBF['CubicFeet']=items_QBF['CA-F']+items_QBF['IL-S']
items_QBF=items_QBF[['Itemnumber', 'CA-F', 'IL-S','CubicFeet']]
#items_QBF.to_csv("D:\\cubic0410.csv",index=False)


#Cost
items_Cost=items.pivot_table(values='cost',index=['Itemnumber'])
items_Cost=pd.merge(items_Cost,items_status,how='left',left_on='Itemnumber',right_on='Itemnumber')
items_Cost['Value']=items_Cost['CA-F']+items_Cost['IL-S']
items_Cost=items_Cost.where(items_Cost!=0,"")
items_Cost=items_Cost.fillna(0)
#items_Cost["Itemname"]="'"+items_Cost["Itemnumber"]
items_Cost=items_Cost[['Itemnumber', 'cost']]
#items_Cost.to_csv("D:\\cost0410.csv",index=False)

#last sales date
#lastsale_status=lastsale[['name','Status']]
lastsale_pivot=lastsale.pivot(values='Sales',index='name',columns='Destination')
lastsale_pivot['Itemname']=lastsale_pivot.index
lastsale_pivot.reset_index()
#lastsale_pivot=pd.merge(lastsale_pivot,lastsale_status,how='left',left_on='name',right_on='name')
lastsale_pivot=lastsale_pivot.groupby('Itemname', as_index=False).min()
#lastsale_pivot['Itemname']="'"+lastsale_pivot['Itemname']
lastsale_pivot['lastfulfill']=lastsale_pivot[['CA-F', 'IL-S', 'CG-ER' ,'CG-CAN']].max(axis=1)
lastsale_pivot.dropna(axis=0,thresh=3, inplace=True)
lastsale_pivot=lastsale_pivot[['Itemname','lastfulfill']]
#lastsale_pivot.to_csv("D:\\slowmover_pivot.csv",index=False)


test=pd.merge(items_Inv,items_QBF,how='outer',on='Itemnumber')
test=pd.merge(test,items_Cost,how='outer',on='Itemnumber')
test=pd.merge(test,lastsale_pivot,how='outer',left_on='Itemnumber',right_on='Itemname')
#iia.rename(columns=new_dict).columns
#test.rename(columns=new_dict, inplace=True)
#test['CA_Cub']=test['CA_Cub']*test['CA_Inv']
#test['IL_Cub']=test['IL_Cub']*test['IL_Inv']
test['CubicFeet']=test['CA_Cub']+test['IL_Cub']
test['Value']=test['Inventory']*['Cost'] #sum price=volume* unit price
test=test[['Itemname','Status','CA-F_x', 'IL-S_x','Invnetory','CA-F_y', 'IL-S_y', 'CubicFeet','cost','Itemname','lastfulfill','diff']]
test.columns=(['Itemnumber','Status','CA_Inv', 'IL_Inv','Inventory',
               'CA_Cub', 'IL_Cub', 'CubicFeet','Cost','Itemname','lastfulfill','diff'])
test['lastfulfill'] = pd.to_datetime(test['lastfulfill'],format="%Y-%m-%d")
test['today']=datetime.datetime.strftime(datetime.date.today(), '%Y-%m-%d')
test['diff']=pd.to_datetime(test['today'])-pd.to_datetime(test['lastfulfill'])
test['diff']=test['diff'].map(lambda x:x.days)

#test.to_csv("D:\\test3.csv",index=False)


test.dropna(axis=0,thresh=3, inplace=True)


from google.cloud import bigquery
import pyarrow as pa
import pyarrow.parquet as pq
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/User/Downloads/stitch-270702-2cc8c96f8731.json"
client = bigquery.Client()

import datetime
Todate = datetime.datetime.strftime(datetime.date.today(), '%Y%m%d')
dataset_id = f"{client.project}.05121_DATA_SET"
table_id = f"{dataset_id}.05121_TABLE"

tschema = [
bigquery.SchemaField("Itemnumber", "STRING", mode="NULLABLE"),
bigquery.SchemaField("Status", "STRING", mode="NULLABLE"),
bigquery.SchemaField("CA_Inv", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("IL_Inv", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("Inventory","FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("CA_Cub", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("IL_Cub", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("CubicFeet", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("Cost", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("Itemname", "STRING", mode="NULLABLE"),
bigquery.SchemaField("lastfulfill", "DATE", mode="NULLABLE"),
bigquery.SchemaField("diff", "FLOAT64", mode="NULLABLE")
]

dataset = bigquery.Dataset(dataset_id)
#dataset.location = "asia-east1"
dataset = client.create_dataset(dataset_id)
table = bigquery.Table(table_id, schema=tschema)
table = client.create_table(table)  # API request
print(f"Created dataset {client.project}.{dataset.dataset_id}")
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

dataset_id = "05121_DATA_SET"
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

df = pd.DataFrame(test)
df['Cost'] = df.Cost.convert_objects(convert_numeric=True)

job = client.load_table_from_dataframe(df, table_id, location="US")
job.result()
assert job.state == "DONE"
df.to_csv("df.csv")
