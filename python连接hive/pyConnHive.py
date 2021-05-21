# -*- coding: utf-8 -*-
"""
Created on Fri May 21 10:39:08 2021
@author: xujing003
"""
import pandas as pd
from impala.dbapi import connect
##通过impala连接hive数据库
conn = connect(host='***', port=10000, 
               auth_mechanism='PLAIN',
               user='***', password='***',
               database='**')
cur=conn.cursor()
cur.execute("""select substring(apply_time,0,10) as apply_date1
,count(distinct business_doc_no) as CREDIT_apply_num
,count(distinct user_id)as people_num
,sum(case when param_code='OUT_CREDITLIMIT' then param_value else 0 end) as limit_sums
,count(distinct case when out_finaldecision = 'A' then user_id else null end) as CREDIT_A_num
,count(distinct case when out_finaldecision = 'D' then user_id else null end) as CREDIT_D_num
 from 
decision_theme_outparam 
where biz_type = 'CREDIT'
and stat_time='2021-05-19'
GROUP by substring(apply_time,0,10) 
            """)

df = pd.DataFrame(cur.fetchall())