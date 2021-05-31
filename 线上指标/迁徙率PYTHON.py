# -*- coding: utf-8 -*-
"""
Created on Sat May  8 09:16:55 2021

@author: xujing003
"""

import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import math
from pandasql import sqldf




#昨日
day1 = (datetime.date.today()  - relativedelta(days=1)).strftime("%Y-%m-%d")
#上个月最后一天
last1 = (datetime.date(datetime.date.today().year,datetime.date.today().month,1)-datetime.timedelta(1)).strftime("%Y-%m-%d")
#上个月最后一天
last2 = (datetime.date(datetime.date.today().year,datetime.date.today().month,1)-datetime.timedelta(1) - relativedelta(months = 1)).strftime("%Y-%m-%d")

#连接mysql数据库
engine = create_engine('mysql+pymysql://apollo:apollo13@88.4.38.223:3306/temp')



#原始数据
#originalDataParams = {'day1':day1,'day2':day2,"last1":last1,"last2":last2}

originalDataParams = {'day1':'2021-05-07','last1':'2021-04-30','last2':'2021-03-31'}

originalSQL1 = """select product_name
,datediff(stat_time,%(last1)s) as dt
,sum(case when balance_c>0  then loan_balance else 0 end) as c_balance_df1
,sum(case when balance_m1>0 then loan_balance else 0 end) as m1_balance_df1
,sum(case when balance_m2>0 then loan_balance else 0 end) as m2_balance_df1
,sum(case when balance_m3>0 then loan_balance else 0 end) as m3_balance_df1
,sum(case when balance_c>0  then 1 else 0 end) as c_cnt_df1
,sum(case when balance_m1>0 then 1 else 0 end) as m1_cnt_df1
,sum(case when balance_m2>0 then 1 else 0 end) as m2_cnt_df1
,sum(case when balance_m3>0 then 1 else 0 end) as m3_cnt_df1
from 
(select stat_time
,case when product_name like '%%浦发备用金%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱'
else "其他" end as product_name
,balance_c,balance_m1,balance_m2,balance_m3,loan_balance
from asset_analysis_theme
where stat_time <= %(day1)s
and stat_time > %(last1)s
) df1
group by product_name,datediff(stat_time,%(last1)s)
"""
DF1 = pd.read_sql_query(originalSQL1, engine,params = originalDataParams)


originalSQL2 = """select product_name
,datediff(stat_time,%(last2)s) as dt
,sum(case when balance_c>0  then loan_balance else 0 end) as c_balance_df2
,sum(case when balance_m1>0 then loan_balance else 0 end) as m1_balance_df2
,sum(case when balance_m2>0 then loan_balance else 0 end) as m2_balance_df2
,sum(case when balance_m3>0 then loan_balance else 0 end) as m3_balance_df2
,sum(case when balance_c>0  then 1 else 0 end) as c_cnt_df2
,sum(case when balance_m1>0 then 1 else 0 end) as m1_cnt_df2
,sum(case when balance_m2>0 then 1 else 0 end) as m2_cnt_df2
,sum(case when balance_m3>0 then 1 else 0 end) as m3_cnt_df2
from 
(select stat_time
,case when product_name like '%%浦发备用金%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱'
else "其他" end as product_name
,balance_c,balance_m1,balance_m2,balance_m3,loan_balance
from asset_analysis_theme
where stat_time <= %(last1)s
and stat_time > %(last2)s) df1
group by product_name,datediff(stat_time,%(last2)s)
"""
DF2 = pd.read_sql_query(originalSQL2, engine,params = originalDataParams)


originalSQLlast1 = """select product_name
,sum(case when balance_c>0  then loan_balance else 0 end) as c_balance_dflast1
,sum(case when balance_m1>0 then loan_balance else 0 end) as m1_balance_dflast1
,sum(case when balance_m2>0 then loan_balance else 0 end) as m2_balance_dflast1
,sum(case when balance_m3>0 then loan_balance else 0 end) as m3_balance_dflast1
,sum(case when balance_c>0  then 1 else 0 end) as c_cnt_dflast1
,sum(case when balance_m1>0 then 1 else 0 end) as m1_cnt_dflast1
,sum(case when balance_m2>0 then 1 else 0 end) as m2_cnt_dflast1
,sum(case when balance_m3>0 then 1 else 0 end) as m3_cnt_dflast1
from 
(select stat_time
,case when product_name like '%%浦发备用金%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱'
else "其他" end as product_name
,balance_c,balance_m1,balance_m2,balance_m3,loan_balance
from asset_analysis_theme
where stat_time = %(last1)s
) df1
group by product_name
"""
DFlast1 = pd.read_sql_query(originalSQLlast1, engine,params = originalDataParams)


originalSQLlast2 = """select product_name
,sum(case when balance_c>0  then loan_balance else 0 end) as c_balance_dflast2
,sum(case when balance_m1>0 then loan_balance else 0 end) as m1_balance_dflast2
,sum(case when balance_m2>0 then loan_balance else 0 end) as m2_balance_dflast2
,sum(case when balance_m3>0 then loan_balance else 0 end) as m3_balance_dflast2
,sum(case when balance_c>0  then 1 else 0 end) as c_cnt_dflast2
,sum(case when balance_m1>0 then 1 else 0 end) as m1_cnt_dflast2
,sum(case when balance_m2>0 then 1 else 0 end) as m2_cnt_dflast2
,sum(case when balance_m3>0 then 1 else 0 end) as m3_cnt_dflast2
from 
(select stat_time
,case when product_name like '%%浦发备用金%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱'
else "其他" end as product_name
,balance_c,balance_m1,balance_m2,balance_m3,loan_balance
from asset_analysis_theme
where stat_time = %(last2)s
) df1
group by product_name
"""
DFlast2 = pd.read_sql_query(originalSQLlast2, engine,params = originalDataParams)



#迁徙率 MigrationRate
#计算次数和金额


#计算迁徙率
MigrationRateSQL = """
select t1.product_name,t1.dt
,t1."上月 c-m2金额"
,t1."上月m1-m2金额"
,t1."上月m2-m3金额"
,t1."上月 c-m2次数"
,t1."上月m1-m2次数"
,t1."上月m2-m3次数"
,t2."本月 c-m2金额"
,t2."本月m1-m2金额"
,t2."本月m2-m3金额"
,t2."本月 c-m2次数"
,t2."本月m1-m2次数"
,t2."本月m2-m3次数"
from 
(select DF2.product_name,DF2.dt
,ifnull(cast(m1_balance_df2 as float)/cast(c_balance_dflast2 as float), 0) as "上月 c-m2金额"
,ifnull(cast(m2_balance_df2 as float)/cast(m1_balance_dflast2 as float),0) as "上月m1-m2金额"
,ifnull(cast(m3_balance_df2 as float)/cast(m2_balance_dflast2 as float),0) as "上月m2-m3金额"
,ifnull(cast(m1_cnt_df2 as float)/cast(c_cnt_dflast2 as float), 0) as "上月 c-m2次数"
,ifnull(cast(m2_cnt_df2 as float)/cast(m1_cnt_dflast2 as float),0) as "上月m1-m2次数"
,ifnull(cast(m3_cnt_df2 as float)/cast(m2_cnt_dflast2 as float),0) as "上月m2-m3次数"
from DFlast2 left join DF2
on DFlast2.product_name = DF2.product_name ) t1
left join
(select DFlast1.product_name,DF1.dt
,ifnull(cast(m1_balance_df1 as float)/cast(c_balance_dflast1 as float), 0) as "本月 c-m2金额"
,ifnull(cast(m2_balance_df1 as float)/cast(m1_balance_dflast1 as float),0) as "本月m1-m2金额"
,ifnull(cast(m3_balance_df1 as float)/cast(m2_balance_dflast1 as float),0) as "本月m2-m3金额"
,ifnull(cast(m1_cnt_df1 as float)/cast(c_cnt_dflast1 as float),0)  as "本月 c-m2次数"
,ifnull(cast(m2_cnt_df1 as float)/cast(m1_cnt_dflast1 as float),0) as "本月m1-m2次数"
,ifnull(cast(m3_cnt_df1 as float)/cast(m2_cnt_dflast1 as float),0) as "本月m2-m3次数"
from DFlast1 left join DF1
on DFlast1.product_name = DF1.product_name
) t2
on t1.product_name = t2.product_name
and t1.dt = t2.dt

"""

MigrationRateDF = sqldf(MigrationRateSQL).fillna(0)

#MigrationRateDF.to_csv("MigrationRateDF.csv", index_label="index_label",encoding='utf_8_sig')
writer = pd.ExcelWriter('D:\\风控建模工作\\线上评估指标\\迁徙率\\test.xlsx')

product_name = ['浦发备用金','平安备用金','平安普惠','大道及时贷','库支票','微博借钱','其他']


for i in product_name:
    x = MigrationRateDF[MigrationRateDF["product_name"]==i]
    x.to_excel(excel_writer=writer,sheet_name=i)


sql2 = """
select t1.dt
,ifnull(m1_balance_df2/c_balance_dflast2 ,0) as "上月 c-m2金额"
,ifnull(m2_balance_df2/m1_balance_dflast2,0) as "上月m1-m2金额"
,ifnull(m3_balance_df2/m2_balance_dflast2,0) as "上月m2-m3金额"
,ifnull(m1_cnt_df2/c_cnt_dflast2 ,0) as "上月 c-m2次数"
,ifnull(m2_cnt_df2/m1_cnt_dflast2,0) as "上月m1-m2次数"
,ifnull(m3_cnt_df2/m2_cnt_dflast2,0) as "上月m2-m3次数"
,ifnull(m1_balance_df1/c_balance_dflast1 ,0) as "本月 c-m2金额"
,ifnull(m2_balance_df1/m1_balance_dflast1,0) as "本月m1-m2金额"
,ifnull(m3_balance_df1/m2_balance_dflast1,0) as "本月m2-m3金额"
,ifnull(m1_cnt_df1/c_cnt_dflast1 ,0) as "本月 c-m2次数"
,ifnull(m2_cnt_df1/m1_cnt_dflast1,0) as "本月m1-m2次数"
,ifnull(m3_cnt_df1/m2_cnt_dflast1,0) as "本月m2-m3次数"
from 
(select DF2.dt
,cast(sum(m1_balance_df2)    as float) as m1_balance_df2
,cast(sum(c_balance_dflast2) as float) as c_balance_dflast2
,cast(sum(m2_balance_df2)    as float) as m2_balance_df2
,cast(sum(m1_balance_dflast2)as float) as m1_balance_dflast2
,cast(sum(m3_balance_df2)    as float) as m3_balance_df2
,cast(sum(m2_balance_dflast2)as float) as m2_balance_dflast2
,cast(sum(m1_cnt_df2)        as float) as m1_cnt_df2
,cast(sum( c_cnt_dflast2)    as float) as c_cnt_dflast2
,cast(sum(m2_cnt_df2)        as float) as m2_cnt_df2
,cast(sum(m1_cnt_dflast2)    as float) as m1_cnt_dflast2
,cast(sum(m3_cnt_df2)        as float) as m3_cnt_df2
,cast(sum(m2_cnt_dflast2)    as float) as m2_cnt_dflast2
from DFlast2 left join DF2 
group by DF2.dt) t1
left join
(select DF1.dt
,cast(sum(m1_balance_df1)    as float) as m1_balance_df1
,cast(sum(c_balance_dflast1) as float) as c_balance_dflast1
,cast(sum(m2_balance_df1)    as float) as m2_balance_df1
,cast(sum(m1_balance_dflast1)as float) as m1_balance_dflast1
,cast(sum(m3_balance_df1)    as float) as m3_balance_df1
,cast(sum(m2_balance_dflast1)as float) as m2_balance_dflast1
,cast(sum(m1_cnt_df1)        as float) as m1_cnt_df1
,cast(sum( c_cnt_dflast1)    as float) as c_cnt_dflast1
,cast(sum(m2_cnt_df1)        as float) as m2_cnt_df1
,cast(sum(m1_cnt_dflast1)    as float) as m1_cnt_dflast1
,cast(sum(m3_cnt_df1)        as float) as m3_cnt_df1
,cast(sum(m2_cnt_dflast1)    as float) as m2_cnt_dflast1
from DFlast1 left join DF1
group by DF1.dt
) t2
on t1.dt =t2.dt
"""
totalDF= sqldf(sql2)
totalDF.to_excel(excel_writer=writer,sheet_name="汇总")

#DF2.to_csv("xx.csv", index_label="index_label",encoding='utf_8_sig')
#DF2.to_excel("D:\\风控建模工作\\线上评估指标\\首逾率\\week_05.xlsx")

writer.save()  
writer.close()
    #dataDF = 
