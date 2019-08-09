import os
import sys
import re
import pandas as pd
import commands
import ast
import itertools
import pyspark.sql.functions
from pyspark.sql.functions import col
from datetime import datetime
from collections import Counter
import re
import numpy as np


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


ignore_list=['entitylogicalname', 'SinkCreatedOn', 'SinkModifiedOn', 'messageid', 'sourcesystem', 'Id']

g1=0
g2=0
sqlContext.sql("use dsc60230_gcct_tz_db")

for m in sqlContext.tables("dsc60230_gcct_tz_db").select('tableName').where("tableName not like '%bad%'").where("tableName not like '%dup%'").where("tableName not like '%bkp%'").where("tableName not like '%temptz%'").collect():
            k=m.asDict().values()[0].encode('utf-8')
            v1=sqlCtx.table(k).where("to_date(lastupdatedatetime)  = date_sub(current_date, 1)")
            g1_1=v1.count()
            g1=g1+g1_1  
            v3={}
            v2=v1.rdd.map(lambda x: str(x).replace("Row(","").replace("u\'","").replace("\'","").replace("\\\\","").replace("\"","")).map(lambda z: re.findall(r',([a-zA-Z]+):{', z)).filter(lambda x: len(x) > 0).collect()
            v3=set([item for sublist in v2 for item in sublist])
            if len(v3) >0:
                print "\\nThe table "+str(k)+" seem to have nested data " +str(v3)+"\\n\\n"
            y1=v1.groupby("shakey").count().filter("count >1")
            if y1.count()>0:
                y2=y1.select("shakey").toPandas().to_string().replace("\n",";")
                print("\\nTable: "+str(k)+" has "+str(y1.count())+" duplicate(s)"+str(y2)+"\\n\\n")
            col_list=set(v1.columns)
            v4=v1.select("entitystring").rdd.map(lambda x: str(x).replace("Row(","").replace("u\'","").replace("\'","").replace("\\\\","").replace("\"","").replace("entitystring={entitydata:{","").replace("},entityid:",",entityid:").replace("})","").replace(r'{', '(').replace("}",")")).map(lambda r: re.sub(r'\(.+\)', '',r)).map(lambda z: re.findall(r',([a-zA-Z]+):', z)).map(lambda x: set(x).difference(col_list)).collect()
            v5=set([item for sublist in v4 for item in sublist]).difference(set(ignore_list))
            if len(v5) >0:
                print("\\nTable: "+str(k)+" seem to get new Field(s) "+str(v5)+"\\n\\n")





g3=sqlCtx.table('bad_record').filter("to_date(lastupdatedatetime)  = date_sub(current_date, 1) and exception like '%Datatype discrepancy%'").groupby("datatype").count().sort("datatype")

if g3.count() > 0:
     print "The following table(s) seem to have messages with data discrepancy"
     print str(g3.toPandas().to_string()).replace("\n","\\n")

g4=sqlCtx.table('bad_record').filter("to_date(lastupdatedatetime)  = date_sub(current_date, 1) and exception like '%not part of approved entities%'").groupby("datatype").count().sort("datatype")

if g4.count() > 0:
     print "The following table(s) seem to have non-approved entities. New Entities are flowing into bad records"
     print str(g4.toPandas().to_string()).replace("\n","\\n")


print g1
g2=sqlCtx.table("bad_record").where("to_date(lastupdatedatetime)  = date_sub(current_date, 1)").count()
print g2


sys.exit(0)
