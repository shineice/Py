#!/usr/bin/env python
# coding: utf-8

# In[5]:


import time
#import math
import pyodbc
import pandas as pd
from datetime import date, datetime
import numpy as np


# In[6]:


conn=pyodbc.connect('dsn=NetSuite;UID=RHung@Top-Line.com;PWD=Netsuite888')
sqld=datetime.today().strftime("%Y/%m/%d")
ALLdata="""
select
i.name as 'Itemnumber',
i.weight as 'Cubic_Feet',
inventory.inv as "inventory",
inventory.Loc as "location",
sts.status as "Status",
trl.amount as 'Amount'
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
    left join TRANSACTION_LINES trl on trl.item_ID=i.item_ID
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
quaSQL="""
select 
ib.name as 'name',ig.Quantity, ia.name as 'parent_name'
from 
    items ib
    join (item_group ig join items ia on ia.item_id=ig.parent_id) on ib.item_id=ig.member_id
where
    ia.isinactive = 'No'
"""


# In[7]:



items=pd.read_sql(ALLdata,conn)
lastsale=pd.read_sql(last_sale,conn)
quaO=pd.read_sql(quaSQL,conn)
conn.close()

#items.to_csv("items_all.csv",index=False)
#lastsale.to_csv("lastsale.csv",index=False)
#quaO.to_csv("quaO.csv",index=False)

lastsale.sort_values(by=['name', 'Sales'],ascending=[True,False])


qua=quaO[['name','Quantity']]
qua=qua.groupby('name', as_index=False).min()


# In[8]:


#items_pivot=items.pivot_table(values='inventory',index=['Itemnumber'],columns=['location'],aggfunc=sum,margins=True)

items_status=items[['Itemnumber','Status']]
items_status=items_status.groupby('Itemnumber', as_index=False).min()
items_status=pd.merge(items_status,qua,how='left',left_on='Itemnumber',right_on='name')

#inventory volume
items_pivot=items.pivot_table(values='inventory',index=['Itemnumber'],columns=['location'])
items_pivot=pd.merge(items_pivot,items_status,how='left',left_on='Itemnumber',right_on='Itemnumber')
items_pivot=items_pivot.fillna(0)

items_pivot['CA+IL']=items_pivot['CA-F']+items_pivot['IL-S']
items_pivot=items_pivot.where(items_pivot!=0,"")
items_pivot["Itemname"]="'"+items_pivot["Itemnumber"]
items_pivot=items_pivot[['Itemname','Itemnumber','Status','Quantity', 'CA-F',  'IL-S','CA+IL']]


# In[9]:


#Cubic_Feet
items_QBF=items.pivot_table(values='Cubic_Feet',index=['Itemnumber'],columns=['location'])
items_QBF=pd.merge(items_QBF,items_status,how='left',left_on='Itemnumber',right_on='Itemnumber')
items_QBF=items_QBF.fillna(0)
items_QBF['CA+IL_CubicFeet']=items_QBF['CA-F']+items_QBF['IL-S']
items_QBF=items_QBF.where(items_QBF!=0,"")
items_QBF["Itemname"]="'"+items_QBF["Itemnumber"]
items_QBF=items_QBF[['Itemname','Itemnumber', 'CA-F', 'IL-S','CA+IL_CubicFeet']]
items_QBF.head(3)


# In[10]:


#Amount
items_Am=items.pivot_table(values='Amount',index=['Itemnumber'],columns=['location'])
items_Am=pd.merge(items_Am,items_status,how='left',left_on='Itemnumber',right_on='Itemnumber')
items_Am=items_Am.fillna(0)

items_Am['CA+IL_Amount']=items_Am['CA-F']+items_Am['IL-S']
items_Am=items_Am.where(items_Am!=0,"")
items_Am["Itemname"]="'"+items_Am["Itemnumber"]
items_Am=items_Am[['Itemname','Itemnumber', 'CA-F', 'IL-S','CA+IL_Amount']]
items_Am.head(3)


# In[11]:



#last sales date
lastsale_status=lastsale[['name','Status']]
lastsale_pivot=lastsale.pivot(values='Sales',index='name',columns='Destination')

lastsale_pivot['Itemname']=lastsale_pivot.index
lastsale_pivot.reset_index()
lastsale_pivot=pd.merge(lastsale_pivot,lastsale_status,how='left',left_on='name',right_on='name')
lastsale_pivot=lastsale_pivot[['Itemname','name','Status','CA-F', 'IL-S']]
lastsale_pivot=lastsale_pivot.groupby('Itemname', as_index=False).min()
lastsale_pivot['Itemname']="'"+lastsale_pivot['Itemname']
lastsale_pivot['lastfulfill']=lastsale_pivot[['CA-F', 'IL-S']].max(axis=1)
lastsale_pivot.to_csv("D:\\slowmover_pivot.csv",index=False)



# In[12]:


test=pd.merge(items_pivot,lastsale_pivot,how='outer',on='Itemname')
test=pd.merge(test,items_QBF,how='outer',on='Itemname')
test=pd.merge(test,items_Am,how='outer',on='Itemname')
#new_dict = {key:key.replace('_x','_inventory').replace('_y','_lastfulfil') for i, key in enumerate(test.columns) }

#iia.rename(columns=new_dict).columns
#test.rename(columns=new_dict, inplace=True)
#


# In[13]:


test.columns=(['Itemname','Itemnumber_x','Status','Quantity','CA_Inv', 'IL_Inv','Inventory', 'name','Status_y','CA_last',
               'IL_last', 'lastfulfill', 'Itemnumber_y', 'CA_Cub', 'IL_Cub', 'CubicFeet', 'Itemnumber','CA_Am', 'IL_Am',
               'Amount'])
test=test[['Itemnumber','Status','CA_Inv', 'IL_Inv','Inventory', 'lastfulfill',
           'CA_Cub', 'IL_Cub', 'CubicFeet','CA_Am', 'IL_Am', 'Amount']]


# In[ ]:


#test.to_csv("D:\\last_fulfill&inventory.csv",index=False)


# In[14]:


#test=pd.read_csv("D:\\last_fulfill&inventory.csv")
test['lastfulfill'] = pd.to_datetime(test['lastfulfill'],format="%Y-%m-%d")
#test.info()


# In[ ]:


test.dropna(axis=0,how="any", inplace=True)
#test.head(5)


# In[15]:


from google.cloud import bigquery
import pyarrow as pa
import pyarrow.parquet as pq
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/User/Downloads/stitch-270702-2cc8c96f8731.json"
client = bigquery.Client()
print(client)


# In[26]:


import datetime
Todate = datetime.datetime.strftime(datetime.date.today(), '%Y%m%d')
dataset_id = f"{client.project}.04067_DATA_SET"
table_id = f"{client.project}."+Todate


# In[18]:


tschema = [
#bigquery.SchemaField("Itemname", "STRING", mode="NULLABLE"),
bigquery.SchemaField("Itemnumber", "STRING", mode="NULLABLE"),
bigquery.SchemaField("Status", "STRING", mode="NULLABLE"),
bigquery.SchemaField("CA_Inv", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("IL_Inv", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("Inventory","FLOAT64", mode="NULLABLE"),
#bigquery.SchemaField("CA_last", "DATETIME", mode="REQUIRED"),
#bigquery.SchemaField("IL_last", "DATETIME", mode="REQUIRED"),
bigquery.SchemaField("lastfulfill", "DATE", mode="NULLABLE"),
bigquery.SchemaField("CA_Cub", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("IL_Cub", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("CubicFeet", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("CA_Am", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("IL_Am", "FLOAT64", mode="NULLABLE"),
bigquery.SchemaField("Amount", "FLOAT64", mode="NULLABLE"),
]


# In[30]:


print(table_id)


# In[29]:


#dataset = bigquery.Dataset(dataset_id)
#dataset.location = "asia-east1"
#dataset = client.create_dataset(dataset_id)
table = bigquery.Table(table_id, schema=tschema)
table = client.create_table(table)  # API request
#print(f"Created dataset {client.project}.{dataset.dataset_id}")
#print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# In[28]:


dataset_id = "04067_DATA_SET"
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)


# In[24]:


df = pd.DataFrame(test)
job = client.load_table_from_dataframe(df, table_id, location="US")


# In[22]:


job.result()
assert job.state == "DONE"


# In[ ]:




