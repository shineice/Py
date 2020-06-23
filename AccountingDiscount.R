rm(list=ls())
library(RODBC)
library(dplyr)
library(stringr)
library(readxl)
library(lubridate)
library(mailR)
m <- as.numeric(format(Sys.Date(), "%m"))
m=m-1
ddd=1
##get week data
channel <- odbcConnect("NetSuite", uid="yao.guan@top-line.com", pwd="NetYG@Davis")

discount<-sqlQuery(channel,paste0(
  "select
  ITEMS.Name as Name,
  ENTITY.Name as Entity,
  ENTITY.ENTITY_ID,
  TRANSACTION_LINES.ITEM_COUNT as 'qty',
  TRANSACTION_LINES.AMOUNT as 'amount'
  from
  TRANSACTION_LINES
  join (TRANSACTIONS join ENTITY on TRANSACTIONS.ENTITY_ID=ENTITY.ENTITY_ID)
  on TRANSACTION_LINES.TRANSACTION_ID=TRANSACTIONS.TRANSACTION_ID
  join LOCATIONS on TRANSACTION_LINES.LOCATION_ID=LOCATIONS.LOCATION_ID
  join ITEMS on TRANSACTION_LINES.ITEM_ID=ITEMS.ITEM_ID
  where
  TRANSACTION_LINES.AMOUNT is not null and
  TRANSACTIONS.TRANSACTION_TYPE='Invoice' and
  TRANSACTIONS.STATUS in ('Open','Paid In Full') and
  ENTITY.ENTITY_ID in (
  '9352',
  '11125',
  '14877',
  '21336',
  '14525',
  '24618',
  '11124',
  '22701',
  '10228',
  '12124',
  '20567',
  '24250',
  '20587',
  '10290',
  '8790',
  '6328',
  '3958',
  '8610',
  '18652',
  '12868',
  '10057',
  '5376',
'20010',
'22361',
'15932',
'7996'   ) and
  TRANSACTIONS.TRANDATE BETWEEN TO_DATE( '" , Sys.Date()-ddd ," 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  AND TO_DATE('" , Sys.Date()-ddd+1," 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  " ))
 
wfp<-sqlQuery(channel,
              "select  i.Name, p.ITEM_UNIT_PRICE
              from Items i, ITEM_PRICES p
              where p.NAME = 'Dot Com (Wayfair.com)' and p.ITEM_ID = Item_ID
              ")
wfcanp <- sqlQuery(channel,
                   "select  i.Name, p.ITEM_UNIT_PRICE
                   from Items i, ITEM_PRICES p
                   where p.NAME = 'Dot Com (Wayfair Canada)' and p.ITEM_ID = Item_ID
                   ")
hdp <- sqlQuery(channel,
                "select  i.Name, p.ITEM_UNIT_PRICE
                from Items i, ITEM_PRICES p
                where p.NAME = 'Dot Com (HomeDepot.com)' and p.ITEM_ID = Item_ID
                ")
hayp <- sqlQuery(channel,
                 "select  i.Name, p.ITEM_UNIT_PRICE
                 from Items i, ITEM_PRICES p
                 where p.NAME = 'Dot Com (NetShop)' and p.ITEM_ID = Item_ID
                 ")
zup <- sqlQuery(channel,
                "select  i.Name, p.ITEM_UNIT_PRICE
                from Items i, ITEM_PRICES p
                where p.NAME = 'Dot Com (Zulily.com)' and p.ITEM_ID = Item_ID
                ")
balp <- sqlQuery(channel,
                 "select  i.Name, p.ITEM_UNIT_PRICE
                 from Items i, ITEM_PRICES p
                 where p.NAME = 'Dot Com (Bellacor.COM)' and p.ITEM_ID = Item_ID
                 ")
houzp <- sqlQuery(channel,
                  "select i.Name, p.ITEM_UNIT_PRICE
                  from Items i, ITEM_PRICES p
                  where p.NAME = 'Dot Com (Houzz.com)' and p.ITEM_ID = Item_ID
                  ")
belp <- sqlQuery(channel,
                 "select i.Name, p.ITEM_UNIT_PRICE
                 from Items i, ITEM_PRICES p
                 where p.NAME = 'Dot Com (Bellacor.COM)' and p.ITEM_ID = Item_ID
                 ")

pierpt <- sqlQuery(channel,
                   "select i.Name, p.ITEM_UNIT_PRICE
                   from Items i, ITEM_PRICES p
                   where p.NAME = 'Trading (Pier 1)' and p.ITEM_ID = Item_ID
                   ")
pierpd <- sqlQuery(channel,
                   "select i.Name, p.ITEM_UNIT_PRICE
                   from Items i, ITEM_PRICES p
                   where p.NAME = 'Wh ( Pier 1)' and p.ITEM_ID = Item_ID
                   ")
pierpI <- sqlQuery(channel,
                   "select i.Name, p.ITEM_UNIT_PRICE
                   from Items i, ITEM_PRICES p
                   where p.NAME = 'Dot Com (Pier 1)' and p.ITEM_ID = Item_ID
                   ")
amap <- sqlQuery(channel,
                 "select i.Name, p.ITEM_UNIT_PRICE
                   from Items i, ITEM_PRICES p
                   where p.NAME = 'Dot Com (Amazon.com)' and p.ITEM_ID = Item_ID
                   ")

colnames(wfp) <- c("Name","wfp")
colnames(wfcanp) <- c("Name","wfcanp")
colnames(hdp) <- c("Name","hdp")
colnames(hayp) <- c("Name","hayp")
colnames(zup) <- c("Name","zup")
colnames(balp) <- c("Name","balp")
colnames(houzp) <- c("Name","houzp")
colnames(belp) <- c("Name","belp")
colnames(pierpt) <- c("Name","pierpt")
colnames(pierpd) <- c("Name","pierpd")
colnames(pierpI) <- c("Name","pierpI")
colnames(amap) <- c("Name","amap")




basic <- sqlQuery(channel,"
                  select NAME,
                    SALESDESCRIPTION,
                    ITEM_STATUS.LIST_ITEM_NAME as Status,
                    CTNSPC,
                    N_2019_CAT, 
                    CREATED,
                    WAYFAIR_COM_PARTNER_SKU,
                    WF_FIRST_ON_SITE_DATE
                  ,OVERSTOCK_COM_SKU,FIRST_ON_SITE_DATE as OVERSTOCK_FIREST_ON_SITE_DATE
                  from ITEMS
                  Join ITEM_STATUS on  STATUS_ID = ITEM_STATUS.LIST_ID
                  where ISINACTIVE='No'
                  ")
basic <- select(basic,Name=NAME,Status)
odbcCloseAll()
mergedisc <- list(discount,wfp,wfcanp,hdp,hayp,zup,balp,houzp,belp,pierpt,pierpd,pierpI,amap,basic) %>%
  Reduce(function(dtf1,dtf2) left_join(dtf1,dtf2,by="Name"), .)

table(mergedisc$Status)
c <- mergedisc$Status%in%c("Close Out","Discontinued" ,"Non-Replenishable (via DI)")

wf <- mergedisc$ENTITY_ID%in%c(9352,11125,14877,14525,24618,11124,22701,10228,12124,20567,24250,20587,10290,8790)
wc <- mergedisc$ENTITY_ID==21336
hd <- mergedisc$ENTITY_ID==6328
ha <- mergedisc$ENTITY_ID==3958
zu <- mergedisc$ENTITY_ID==8610
ba <- mergedisc$ENTITY_ID==18652
ho <- mergedisc$ENTITY_ID==12868
be <- mergedisc$ENTITY_ID==5376
pt <- mergedisc$ENTITY_ID==20010
pd <- mergedisc$ENTITY_ID==22361
pi <- mergedisc$ENTITY_ID==15932
am <- mergedisc$ENTITY_ID==7996



cwf <- c & wf
cwc <- c & wc
chd <- c & hd
cha <- c & ha
czu <- c & zu
cba <- c & ba
cho <- c & ho
cbe <- c & be
cpt	<- c & pt
cpd	<- c & pd
cpi	<- c & pi
cam	<- c & am






ewf <- !c & wf
ewc <- !c & wc
ehd <- !c & hd
eha <- !c & ha
ezu <- !c & zu
eba <- !c & ba
eho <- !c & ho
ebe <- !c & be
ept	<- !c &	pt
epd	<- !c &	pd
epi	<- !c &	pi
eam	<- !c &	am



mergedisc$EP <- 0
mergedisc$CD <- 0
mergedisc$qty <- abs(mergedisc$qty)
mergedisc$amount <- (-mergedisc$amount)

mergedisc$EP[ewf] <- mergedisc$wfp[ewf]*mergedisc$qty[ewf]- mergedisc$amount[ewf]
mergedisc$EP[ewc] <- mergedisc$wfcanp[ewc]*mergedisc$qty[ewc]- mergedisc$amount[ewc]
mergedisc$EP[ehd] <- mergedisc$hdp[ehd]*mergedisc$qty[ehd]- mergedisc$amount[ehd]
mergedisc$EP[eha] <- mergedisc$hayp[eha]*mergedisc$qty[eha]- mergedisc$amount[eha]
mergedisc$EP[ezu] <- mergedisc$zup[ezu]*mergedisc$qty[ezu]- mergedisc$amount[ezu]
mergedisc$EP[eba] <- mergedisc$balp[eba]*mergedisc$qty[eba]- mergedisc$amount[eba]
mergedisc$EP[eho] <- mergedisc$houzp[eho]*mergedisc$qty[eho]- mergedisc$amount[eho]
mergedisc$EP[ebe] <- mergedisc$belp[ebe]*mergedisc$qty[ebe]- mergedisc$amount[ebe]
mergedisc$EP[ept] <- mergedisc$pierpt[ept]*mergedisc$qty[ept]- mergedisc$amount[ept]
mergedisc$EP[epd] <- mergedisc$pierpd[epd]*mergedisc$qty[epd]- mergedisc$amount[epd]
mergedisc$EP[epi] <- mergedisc$pierpI[epi]*mergedisc$qty[epi]- mergedisc$amount[epi]
mergedisc$EP[eam] <- mergedisc$amap[eam]*mergedisc$qty[eam]- mergedisc$amount[eam]



mergedisc$CD[cwf] <- mergedisc$wfp[cwf]*mergedisc$qty[cwf]- mergedisc$amount[cwf]
mergedisc$CD[cwc] <- mergedisc$wfcanp[cwc]*mergedisc$qty[cwc]- mergedisc$amount[cwc]
mergedisc$CD[chd] <- mergedisc$hdp[chd]*mergedisc$qty[chd]- mergedisc$amount[chd]
mergedisc$CD[cha] <- mergedisc$hayp[cha]*mergedisc$qty[cha]- mergedisc$amount[cha]
mergedisc$CD[czu] <- mergedisc$zup[czu]*mergedisc$qty[czu]- mergedisc$amount[czu]
mergedisc$CD[cba] <- mergedisc$balp[cba]*mergedisc$qty[cba]- mergedisc$amount[cba]
mergedisc$CD[cho] <- mergedisc$houzp[cho]*mergedisc$qty[cho]- mergedisc$amount[cho]
mergedisc$CD[cbe] <- mergedisc$belp[cbe]*mergedisc$qty[cbe]- mergedisc$amount[cbe]
mergedisc$EP[cpt] <- mergedisc$pierpt[cpt]*mergedisc$qty[cpt]- mergedisc$amount[cpt]
mergedisc$EP[cpd] <- mergedisc$pierpd[cpd]*mergedisc$qty[cpd]- mergedisc$amount[cpd]
mergedisc$EP[cpi] <- mergedisc$pierpI[cpi]*mergedisc$qty[cpi]- mergedisc$amount[cpi]
mergedisc$EP[cam] <- mergedisc$amap[cam]*mergedisc$qty[cam]- mergedisc$amount[cam]


aggdisc <- aggregate(cbind(EP,CD)~Entity,mergedisc,sum)
colnames(aggdisc) <- c("Account","Event Promotion", "Close-Out Discount")



#send mail
list <- "Monthly dicount by account"
m1=Sys.Date()-ddd
path <- paste0("/home/topline/auto_report/",m1,"_Discount.csv")
write.csv(aggdisc,path,row.names = FALSE)
