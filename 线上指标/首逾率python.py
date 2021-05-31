# -*- coding: utf-8 -*-
"""
Created on Mon May 10 11:25:47 2021

@author: xujing003
"""

import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import math
from pandasql import sqldf
from openpyxl import load_workbook



#昨日
day = datetime.date.today()  - relativedelta(days=1)
day1 = (datetime.date.today()  - relativedelta(days=1)).strftime("%Y-%m-%d")
#上个月日期
day2 = datetime.date.today()  - relativedelta(days=1)- relativedelta(months = 1)

#上个月最后一天
last1 = datetime.date(datetime.date.today().year,datetime.date.today().month,1)-datetime.timedelta(1)
#上个月最后一天
last2 = (datetime.date(datetime.date.today().year,datetime.date.today().month,1)-datetime.timedelta(1) - relativedelta(months = 1)).strftime("%Y-%m-%d")

#连接mysql数据库
engine = create_engine('mysql+pymysql://apollo:apollo13@88.4.38.223:3306/temp')



#原始数据
originalDataParams = {'day1':day1,'day2':day2,"last1":last1,"last2":last2}

originalDataParams = {'day1':'2021-05-07','day2':'2021-03-23','last1':'2021-04-30','last2':'2021-03-31'}

#原始数据取出来
#
SQL1 = """select id,
case when product_name like '%%浦发备用金%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱'
else "其他" end as product_name
,first_repay_date
,stat_time,shoudpay_date
,loan_amt,overdue_days,loan_balance,balance_m1,balance_m2,shoudpay_term
,payout_date
from asset_analysis_theme
where shoudpay_term >= 2 
and stat_time <= %(day1)s
and stat_time > %(day2)s
and first_repay_date >= %(day2)s
and shoudpay_date is not null
and product_name not like "%%备用金%%"
and first_repay_date < stat_time
"""

DF1 = pd.read_sql_query(SQL1, engine,params = originalDataParams)

SQL2 = """
select id,case when product_name like '%%浦发备用金%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱'
else "其他" end as product_name
,stat_time
,date_add(first_repay_date, interval 1 MONTH)  as first_repay_date
,shoudpay_date
,loan_amt,overdue_days,loan_balance,balance_m1,balance_m2,shoudpay_term
,payout_date
from asset_analysis_theme
where shoudpay_term >= 3 
and stat_time <= %(day1)s
and stat_time > %(day2)s
and first_repay_date >= %(day2)s
and shoudpay_date is not null
and product_name like "%%备用金%%"
and first_repay_date < stat_time
"""

DF2 = pd.read_sql_query(SQL2, engine,params = originalDataParams)

originalDF = pd.concat([DF1,DF2], axis=0) # 纵向合并
#首次还款日期应该在统计日期之前
#originalDF = originalDF[originalDF["first_repay_date"]<originalDF["stat_time"]]
originalDF = originalDF[(originalDF["first_repay_date"]<originalDF["stat_time"]) & (originalDF["first_repay_date"]>=day2)]

#计算首次还款日和统计日期的差值
originalDF["days"] = originalDF.apply(lambda x: (x["stat_time"]-x["first_repay_date"]).days,axis=1)


#首逾率 
#计算不同天数产品对应的逾期金额

totalPDFsql = """select days,product_name ,first_repay_date 
,ifnull(sum(case when balance_m1>0 then loan_balance 
            when balance_m2>0 then loan_balance else 0 end),0) as m1
,ifnull(sum(loan_amt),0) as amt
,ifnull(sum(case when balance_m1>0 then loan_balance
            when balance_m2>0 then loan_balance else 0 end),0) /ifnull(sum(loan_amt),0) as ratio
from originalDF
group by product_name,days,first_repay_date
"""
totalPDF= sqldf(totalPDFsql)

countDFsql = """select
product_name,first_repay_date,count(id) as "放款件数"
,sum(loan_amt) as "放款金额"
from (select 
distinct
product_name,first_repay_date,id,loan_amt
from originalDF
group by product_name,first_repay_date,id,loan_amt)
group by product_name,first_repay_date
"""
countDF = sqldf(countDFsql)



product_name = ['浦发备用金','平安备用金','平安普惠','大道及时贷','库支票','微博借钱','其他']

#循环保存
writer = pd.ExcelWriter('D:\\风控建模工作\\线上评估指标\\首逾率\\test.xlsx')

    
for i in product_name:
    x = totalPDF[totalPDF["product_name"]==i]
    tempDF1 = pd.pivot_table(x,columns = 'days',index = "first_repay_date",values=(["ratio"])).sort_index(ascending=False)
    xx = countDF[countDF["product_name"]==i]
    tempDf = pd.merge(tempDF1,xx,on=['first_repay_date'],how='left')
    tempDf.to_excel(excel_writer=writer,sheet_name=i)


totalPDFsql = """select days ,first_repay_date 
,ifnull(sum(case when balance_m1>0 then loan_balance
            when balance_m2>0 then loan_balance else 0 end),0) as m1
,ifnull(sum(loan_amt),0) as amt
,ifnull(sum(case when balance_m1>0 then loan_balance 
            when balance_m2>0 then loan_balance else 0 end),0) /ifnull(sum(loan_amt),0) as ratio
from originalDF
group by product_name,days,first_repay_date
"""
totalPDF= sqldf(totalPDFsql)

tempDF1 = pd.pivot_table(x,columns = 'days',index = "first_repay_date",values=(["ratio"])).sort_index(ascending=False)

countDFsql = """select
first_repay_date,count(id) as "放款件数"
,sum(loan_amt) as "放款金额"
from (select 
distinct
first_repay_date,id,loan_amt
from originalDF
group by first_repay_date,id,loan_amt)
group by first_repay_date
"""
countDF = sqldf(countDFsql)

tempDf = pd.merge(tempDF1,countDF,on=['first_repay_date'],how='left')



writer.save()  
writer.close()
    #dataDF = 
    
