# -*- coding: utf-8 -*-
"""
Created on Wed May 12 16:34:39 2021

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

originalDataParams = {'day1':'2021-05-07','day2':'2021-04-08','last1':'2021-04-30','last2':'2021-03-31'}

#原始数据C取出来
CSQL = """
select shoudpay_date,product_name,sum(loan_balance) as c_amt from
(select shoudpay_date,loan_balance
,case when product_name like '%%浦发%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱' 
else '其他'
end as product_name
from asset_analysis_theme
where stat_time <= '2021-05-06'
and stat_time > '2021-04-06'
and shoudpay_date = stat_time
and balance_c>0
and shoudpay_term >2
and product_name like "%%备用金%%"
union all
select shoudpay_date,loan_balance
,case when product_name like '%%浦发%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱' 
else '其他'
end as product_name
from asset_analysis_theme
where stat_time <= '2021-05-06'
and stat_time > '2021-03-18'
and shoudpay_date = stat_time
and balance_c>0
and shoudpay_term >1
and product_name not like "%%备用金%%") xsw
group by shoudpay_date,product_name
"""

CDF = pd.read_sql_query(CSQL, engine)


M1SQL = """
select overdue_days,last_shoudpay_date,product_name
,sum(loan_balance) as m1_amt from
(select overdue_days - 1 as overdue_days,
date_sub(stat_time,interval overdue_days-1 day) as last_shoudpay_date
,loan_balance
,case when product_name like '%%浦发%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱' 
else '其他'
end as product_name
from asset_analysis_theme
where stat_time <= '2021-05-06'
and stat_time > '2021-04-06'
and balance_m1>0
and shoudpay_term >2
and product_name like "%%备用金%%"
union all
select overdue_days,
date_sub(stat_time,interval overdue_days day) as last_shoudpay_date
,loan_balance
,case when product_name like '%%浦发%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱' 
else '其他'
end as product_name
from asset_analysis_theme
where stat_time <= '2021-05-06'
and stat_time > '2021-03-18'
and (balance_m1>0 or balance_m2 >0)
and shoudpay_term >1
and product_name not like "%%备用金%%") sxz
where overdue_days>0
and overdue_days <31
group by overdue_days,last_shoudpay_date,product_name
"""

M1DF = pd.read_sql_query(M1SQL, engine)




sql1 = """select ta1.shoudpay_date 
,ta1.m1_amt,ta1.product_name,overdue_days
,ta1.ratio as "逾期比例"
,1-ta1.m1_amt/ta2.m1_amt as "催回比例"
from
(select CDF.shoudpay_date ,CDF.product_name,overdue_days
,m1_amt 
,m1_amt/c_amt as ratio
from M1DF LEFT JOIN CDF
on CDF.shoudpay_date =M1DF.last_shoudpay_date
and CDF.product_name =M1DF.product_name) ta1
left join
(select last_shoudpay_date ,product_name
,m1_amt 
from M1DF where overdue_days = 1) ta2
on ta1.shoudpay_date = ta2.last_shoudpay_date
and ta1.product_name = ta2.product_name
"""
totalDF = sqldf(sql1)

sql2 = """select  shoudpay_date ,
CDF.product_name,
c_amt as "贷款余额"
from CDF 
order by shoudpay_date DESC
"""

CtotalDF = sqldf(sql2)

product_name = ['浦发备用金','平安备用金','平安普惠','大道及时贷','库支票','微博借钱','其他']

#循环保存
writer = pd.ExcelWriter('D:\\风控建模工作\\线上评估指标\\流入率\\test.xlsx')



for i in product_name:
    x = totalDF[totalDF["product_name"]==i]
    xx1 = CtotalDF[CtotalDF["product_name"]==i]
    DF2= pd.DataFrame(pd.pivot_table(x,columns = 'overdue_days',index = "shoudpay_date",values=(["逾期比例","催回比例"]),fill_value=0).sort_index(ascending=False))
    tempDf = pd.merge(xx1,DF2,on=['shoudpay_date'],how='right')
    tempDf.rename(columns={'shoudpay_date':'流入日'}, inplace = True)
    tempDf.to_excel(excel_writer=writer,sheet_name=i)

sql1 = """select ta1.shoudpay_date 
,ta1.m1_amt,ta1.product_name,overdue_days
,ta1.ratio as "逾期比例"
,1-ta1.m1_amt/ta2.m1_amt as "催回比例"
from
(select CDF.shoudpay_date ,CDF.product_name,overdue_days
,sum(m1_amt) as m1_amt
,sum(m1_amt)/sum(c_amt) as ratio
from CDF LEFT JOIN M1DF
on CDF.shoudpay_date =M1DF.last_shoudpay_date
and CDF.product_name =M1DF.product_name
group by CDF.shoudpay_date ,CDF.product_name,overdue_days) ta1
left join
(select last_shoudpay_date 
,sum(m1_amt)  as m1_amt
from M1DF where overdue_days = 1
group by last_shoudpay_date) ta2
on ta1.shoudpay_date = ta2.last_shoudpay_date
"""
totalDF = sqldf(sql1)

sql2 = """select shoudpay_date,
sum(c_amt) as "贷款余额"
from CDF 
group by shoudpay_date
order by shoudpay_date desc
"""

CtotalDF = sqldf(sql2)

DF2= pd.DataFrame(pd.pivot_table(totalDF,columns = 'overdue_days',index = "shoudpay_date",values=(["逾期比例","催回比例"]),fill_value=0).sort_index(ascending=False))
tempDf = pd.merge(CtotalDF,DF2,on=['shoudpay_date'],how='right')
tempDf.rename(columns={'shoudpay_date':'流入日'}, inplace = True)
tempDf.to_excel(excel_writer=writer,sheet_name="汇总")


writer.save()  
writer.close()

