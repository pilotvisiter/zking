# -*- coding: utf-8 -*-
"""
Created on Fri May 21 10:43:06 2021
@author: xujing003
"""

import pandas as pd
from sqlalchemy import create_engine
import datetime
from dateutil.relativedelta import relativedelta
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

# 初始化数据库连接
# 按实际情况依次填写MySQL的用户名、密码、IP地址、端口、数据库名
engine = create_engine('mysql+pymysql://apollo:apollo13@88.4.38.223:3306/temp')
# 如果觉得上方代码不够优雅也可以按下面的格式填写
# engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '12345678', 'localhost', '3306', 'testdb'))

# MySQL导入DataFrame
# 填写自己所需的SQL语句，可以是复杂的查询语句
sql_query = """select shoudpay_date,loan_balance
,case when product_name like '%%浦发%%' then '浦发备用金'
when product_name like '%%平安备用金%%' then '平安备用金'
when product_name like '%%平安普惠%%' then '平安普惠'
when product_name like '%%大道及时贷%%' then '大道及时贷'
when product_name like '%%库支票%%' then '库支票'
when product_name like '%%微聚%%' then '微博借钱' 
else '其他'
end as product_name
from asset_analysis_theme
where stat_time <= %(day2)s

"""
# 使用pandas的read_sql_query函数执行SQL语句，并存入DataFrame
df_read = pd.read_sql_query(sql_query, engine,params = originalDataParams)
