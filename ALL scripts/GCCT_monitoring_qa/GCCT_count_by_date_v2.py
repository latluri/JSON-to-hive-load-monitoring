import os
import sys
import re
#import pandas as pd
import commands
import ast
import itertools
import pyspark.sql.functions
from pyspark.sql.functions import col, current_date
from datetime import datetime
from collections import Counter
import re
#import numpy as np


from pyspark import SparkContext, SparkConf, HiveContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext
sqlContext = HiveContext(sparkContext=sc)
sqlCtx = HiveContext(sparkContext=sc)

#current_date=datetime.datetime.now().strftime ("%Y-%m-%d")

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
g1=0
g2=0
sqlContext.sql("use dsc60230_gcct_tz_db")

for m in sqlContext.tables("dsc60230_gcct_tz_db").select('tableName').where("tableName not like '%bad%'").where("tableName not like '%dup%'").where("tableName not like '%bkp%'").where("tableName not like '%temptz%'").collect():
            k=m.asDict().values()[0].encode('utf-8')
            v1=sqlContext.table(k).filter("to_date(lastupdatedatetime)  = date_sub(current_date, 1)")
            g1_1=v1.count()
            g1=g1+g1_1  
            v3={}
            v2=v1.select("entitystring").rdd.map(lambda x:x.asDict().values()[0].encode('utf-8')).map(lambda u:str(u).replace('null','None').replace('false','False').replace('true','True')).map(lambda y:ast.literal_eval(ast.literal_eval(y)['entitydata'])).map(lambda u: get_nested_keys(u)).filter(lambda x: len(x) > 0).collect()
            v3=set([item.lower() for sublist in v2 for item in sublist])
            if len(v3) >0:
                print "\\nThe table "+str(k)+" seem to have nested data " +str(v3).replace("set([","").replace("])","")+"\\n\\n"
            y1=v1.groupby("shakey").count().filter("count >1")
            if y1.count()>0:
                y2=y1.select("shakey").toPandas().to_string(index=False).replace("\n",";")
                print("\\nTable: "+str(k)+" has "+str(y1.count())+" duplicate(s)"+str(y2)+"\\n\\n")
            col_list=set([col.lower() for col in v1.columns])
            v4=v1.select("entitystring").rdd.map(lambda x:x.asDict().values()[0].encode('utf-8')).map(lambda u:str(u).replace('null','None').replace('false','False').replace('true','True')).map(lambda y:ast.literal_eval(ast.literal_eval(y)['entitydata']).keys()+ast.literal_eval(y).keys()).map(lambda w:[x.lower() for x in w]).map(lambda x: set(x).difference(col_list)).collect()
            v5=set([item.lower() for sublist in v4 for item in sublist]).difference(set(ignore_list))
            if len(v5) >0:
                print("\\nTable: "+str(k)+" seem to get new Field(s) "+str(v5).replace("set([","").replace("])","")+"\\n\\n")





g3=sqlContext.table('bad_record').filter("to_date(lastupdatedatetime)  = date_sub(current_date, 1) and exception like '%Datatype discrepancy%'").groupby("datatype").count().sort("datatype")

if g3.count() > 0:
     print "The following table(s) seem to have messages with data discrepancy\\nTable name \t Count of records\\n"
     print str(g3.toPandas().to_string(index=False)).replace("\n","\\n")

g4=sqlContext.table('bad_record').filter("to_date(lastupdatedatetime)  = date_sub(current_date, 1) and exception like '%not part of approved entities%'").groupby("datatype").count().sort("datatype")

if g4.count() > 0:
     print "The following table(s) seem to have non-approved entities. New Entities are flowing into bad records\\nTable name \t Count of records\\n"
     print str(g4.toPandas().to_string(index=False)).replace("\n","\\n")


print g1
g2=sqlContext.table("bad_record").filter("to_date(lastupdatedatetime)  = date_sub(current_date, 1)").count()
print g2


sys.exit(0)
