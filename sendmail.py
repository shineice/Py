# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 16:17:05 2018

@author: User
"""
def sendreport(mailinfo, infoSerial, AttachFile):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email import encoders
    import pandas
    import time
    import os

#    pwd = os.path.abspath('.//auto_report')
    info = pandas.read_csv(mailinfo)
    MyMail = pandas.read_csv( "UIDandPW.csv").ID[1]
    MyMailPW = pandas.read_csv( "UIDandPW.csv").PW[1]
    mailto = info.To[infoSerial].split("/")
    Cc = info.cc[infoSerial].split("/")

    emailMsg = MIMEMultipart('alternative')
    emailMsg['Subject'] = info.Subject[infoSerial]
    emailMsg['From'] = MyMail
    emailMsg['To'] = ", ".join(mailto)
    emailMsg['Cc'] = ", ".join(Cc)
    
    att = MIMEBase('application', "octet-stream")
    att.set_payload(open(AttachFile, "rb").read())
    encoders.encode_base64(att)
    att.add_header('Content-Disposition', 'attachment', filename=AttachFile)
    emailMsg.attach(att)

    smtpObj = smtplib.SMTP('smtp.office365.com',587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login(MyMail,MyMailPW)
    smtpObj.sendmail(MyMail,mailto+Cc,emailMsg.as_string())
    time.sleep(10)
    smtpObj.quit()
