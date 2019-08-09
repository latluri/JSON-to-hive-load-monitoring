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
g1=0
g2=0

sqlContext.sql("use dsc60263_fsm_tz_db")

tables_except_bad_rec=['account', 'accountleads', 'attributemetadata', 'businessunit', 'competitor', 'competitoraddress', 'contact', 'contactleads', 'fcs_account_contact', 'fcs_casetracking', 'fcs_fcssalesprocess', 'fcs_incident_fcs_vehicle', 'fcs_vehicle', 'fcs_vintracking', 'fcs_yearlygoals', 'globaloptionsetmetadata', 'incident', 'invoice', 'invoicedetail', 'lead', 'leadcompetitors', 'opportunity', 'optionsetmetadata', 'phonetocaseprocess', 'pricelevel', 'product', 'productpricelevel', 'statemetadata', 'statusmetadata', 'sysdiagrams', 'systemuser', 'systemuserbusinessunitentitymap', 'targetmetadata', 'transactioncurrency', 'uom', 'uomschedule']

nested=''
duplicates=''
data_discrepancy=''
new_fields=''
new_entities=''
#for m in sqlContext.tables("dsc60263_fsm_tz_db").select('tableName').where("tableName not like '%bad%'").where("tableName not like '%dup%'").where("tableName not like '%bkp%'").where("tableName not like '%temptz%'").collect():
            #k=m.asDict().values()[0].encode('utf-8')
for k in tables_except_bad_rec:
            v1=sqlContext.table(k).filter("to_date(lastupdatedatetime)  = date_sub(CAST(current_timestamp() as DATE), 1)")
            g1_1=v1.count()
            g1=g1+g1_1
            v3={}
            v2=v1.select("entitystring").rdd.map(lambda x:x.asDict().values()[0].encode('utf-8')).map(lambda u:str(u).replace('null','None').replace('false','False').replace('true','True')).map(lambda y:ast.literal_eval(ast.literal_eval(y)['entitydata'])).map(lambda u: get_nested_keys(u)).filter(lambda x: len(x) > 0).collect()
            v3=set([item.lower() for sublist in v2 for item in sublist])
            if len(v3) >0:
                print "\\nThe table "+str(k)+" seem to have nested data " +str(v3).replace("set([","").replace("])","")+"\\n\\n"
		nested='Nested data is observed'
            if sqlContext.table(k).select("shakey").distinct().count()!=sqlContext.table(k).count():
		y1=sqlContext.table(k).groupby("shakey").count().filter("count >1")
                #y2=y1.select("shakey").toPandas().to_string(index=False).replace("\n",";")
                y2=str(y1.select("shakey").distinct().collect()).replace("Row(shakey=u","").replace("),",";").replace("[","").replace("]","").replace(")","")
                print("\\nTable: "+str(k)+" has "+str(y1.count())+" duplicate(s). The following are the shakeys: "+str(y2)+"\\n\\n")
		#print("\\nTable: "+str(k)+" has "+str(y1.count())+" duplicate(s)"+str(y2)+"\\n\\n")
		duplicates='Duplicate(s) are observed'
            col_list=set([col.lower() for col in v1.columns])
            v4=v1.select("entitystring").rdd.map(lambda x:x.asDict().values()[0].encode('utf-8')).map(lambda u:str(u).replace('null','None').replace('false','False').replace('true','True')).map(lambda y:ast.literal_eval(ast.literal_eval(y)['entitydata']).keys()+ast.literal_eval(y).keys()).map(lambda w:[x.lower() for x in w]).map(lambda x: set(x).difference(col_list)).collect()
            v5=set([item.lower() for sublist in v4 for item in sublist]).difference(set(ignore_list))
            if len(v5) >0:
                print("\\nTable: "+str(k)+" seem to get new Field(s) "+str(v5).replace("set([","").replace("])","")+"\\n\\n")
		new_fields='New field(s) are observed'




g3=sqlContext.table('bad_record').filter("to_date(lastupdatedatetime)  = date_sub(CAST(current_timestamp() as DATE), 1) and exception like '%Datatype discrepancy%'").groupby("datatype").count().sort("datatype")

if g3.count() > 0:
     print("The following table(s) seem to have messages with data discrepancy\\n\\nTable_name \t Count_of_records\\n\\n")
     g3_1=str(g3.collect()).replace("Row(datatype=u","").replace("),","\\n\\n").replace("[","").replace("]","").replace(")","").replace("count=","")
     print(g3_1)
     data_discrepancy='Data discrepancy observed'

g4=sqlContext.table('bad_record').filter("to_date(lastupdatedatetime)  = date_sub(CAST(current_timestamp() as DATE), 1) and exception like '%not part of approved entities%'").groupby("datatype").count().sort("datatype")

if g4.count() > 0:
     print("The following table(s) seem to have non-approved entities. New Entities are flowing into bad records\\nTable_name \t Count_of_records\\n")
     g4_1=str(g4.collect()).replace("Row(datatype=u","").replace("),","\\n\\n").replace("[","").replace("]","").replace(")","").replace("count=","")
     print(g4_1)
     new_entities='New table(s) that are not approved are observed'



print("|")
print(data_discrepancy+"\\n\\n"+new_entities+"\\n\\n"+duplicates+"\\n\\n"+nested+"\\n\\n"+new_fields)
g5=sqlContext.table("bad_record").filter("to_date(lastupdatedatetime)  = date_sub(CAST(current_timestamp() as DATE), 1)").filter("reason like '%Duplicate%' ").count()
g6=sqlContext.table("bad_record").filter("to_date(lastupdatedatetime)  = date_sub(CAST(current_timestamp() as DATE), 1)").filter("exception = 'Empty Entity Data'").count()
g2=sqlContext.table("bad_record").filter("to_date(lastupdatedatetime)  = date_sub(CAST(current_timestamp() as DATE), 1)").count()

if g2==g5+g6:
	if g5>0:
		print("\\n\\n"+str(g5)+" Duplicate message(s)")
	if g6>0:
		print("\\n\\n"+str(g6)+" Empty message(s)\\n\\n")
else:
	print("\\n\\nBad records contains data other than empty message(s) "+ str(g6)+" and duplicate(s) "+str(g5)+" . Further investigation needed on "+str(g2-g5-g6)+" messages\\n\\n")
print g1
print g2


sys.exit(0)
