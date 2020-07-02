fac=pd.read_excel(r"04.登記工廠名錄_94099.xlsx") 
fac['工廠登記編號']=fac['工廠登記編號'].str.extract(r'(^T\d{7})',expand=True)
fac=fac[fac['工廠登記編號'].isnull() == False]
tax=pd.read_excel(r"裁處併列管併稅籍_移除不計與稅籍無資料_34460.xlsx") 
data2=pd.DataFrame.merge(tax, fac, how='left',  left_on='UNINO', right_on='統一編號')
data2=data2[data2['工廠名稱'].isnull() == False]
data2.to_csv('有稅籍是臨登.csv',encoding='utf_8_sig')