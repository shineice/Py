from datetime import date, timedelta                   
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")



import os
os.chdir('.//auto_report')
filename=yesterday+"_Discount.csv"



import sendmail
#today=date.today()
#tdate=time.strftime("%Y%m%d")
sendmail.sendreport( 'report_mailinfo.csv',2,filename)
