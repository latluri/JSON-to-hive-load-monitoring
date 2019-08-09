import os
import sys
import re
#import pandas as pd
import commands
import ast
import itertools
import pyspark.sql.functions
from pyspark.sql.functions import col
from pyspark.sql.functions import current_date
from datetime import datetime,timedelta
from collections import Counter
import re
#import numpy as np


from pyspark import SparkContext, SparkConf, HiveContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext
sqlContext = HiveContext(sparkContext=sc)
sqlCtx = HiveContext(sparkContext=sc)


def freq(lst):
    d = {}
    for i in lst:
        if d.get(i):
            d[i] += 1
        else:
            d[i] = 1
    return d


def get_nested_keys(a):
    key_list=[]
    for i in a.keys():
        b=a[i]
        if "{" in str(b):
                key_list=key_list+[i]
    return(key_list)


ignore_list_1=['entitylogicalname', 'SinkCreatedOn', 'SinkModifiedOn', 'messageid', 'sourcesystem', 'Id', 'entitydata']
ignore_list=[col.lower() for col in ignore_list_1]

sqlContext.sql("use dsc60263_fsm_tz_db")

for m in sqlContext.tables("dsc60263_fsm_tz_db").select('tableName').where("tableName not like '%dup%'").where("tableName not like '%bkp%'").where("tableName not like '%temptz%'").collect():
            k=m.asDict().values()[0].encode('utf-8')
            v1=sqlContext.table(k).filter("to_date(lastupdatedatetime)  = date_sub(CAST(current_timestamp() as DATE), 1)")
	    y1=v1.select("message__id").distinct()
            if y1.count()>0:
		v2=list(v1.select("message__id").toPandas()["message__id"])
            	for msg_id in v2:
			print(str(msg_id).strip()+"\\n")


sys.exit(0)
