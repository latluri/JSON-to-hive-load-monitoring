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
sqlContext.sql("use dsc10742_gcctmsd_lz_db")

tables_gcct_except_bad_rec=['account','activityparty','annotation','appointment','businessunit','calendar','calendarrule','contact','cxlvhlp_chatactivity','cxlvhlp_chatqueuestatistic','cxlvhlp_surveyitem','email','fax','gcct_accountresponsibleagent','gcct_additionalsymptomcodes','gcct_addportalmessage','gcct_arbitrationclaimprocessing','gcct_buybackevaluationmilestones','gcct_caseassignment','gcct_caseclassification','gcct_casedispositiontype','gcct_coachback','gcct_country','gcct_customersatisfactiontools','gcct_delegationofauthority','gcct_demandltrtpsmclaimsprocessing','gcct_doaprogramcode','gcct_documentcustomerrecontact','gcct_engine','gcct_executive','gcct_executiveliaison','gcct_fieldinvolvementassistancerequest','gcct_fmcccontract','gcct_fulfillmentvendor','gcct_genericinformation','gcct_genericinformationtopic','gcct_generictopic','gcct_goodfaithreview','gcct_knownpartsdelay','gcct_lemonlawcriteria','gcct_loanercost','gcct_loyaltyassistance','gcct_loyaltycostdetails','gcct_loyaltyprogramcode','gcct_material','gcct_materialrequest','gcct_materialrequestdetail','gcct_offer','gcct_offerdetail','gcct_onlinegaragevehicles','gcct_partsorderstatus','gcct_partsordersystems','gcct_partssmeassistancerequest','gcct_pcarequest','gcct_priordealerdecision','gcct_qaincident','gcct_qamonitoring','gcct_queuepriority','gcct_ravprocessingmilestones','gcct_reactdata','gcct_reacttransmission','gcct_reasoncode','gcct_recall','gcct_region','gcct_rentaldetail','gcct_slaconfiguration','gcct_sms','gcct_socialmediaconversation','gcct_socialmediamessage','gcct_socialmediaprofile','gcct_specialloanercode','gcct_state','gcct_surveyconfiguration','gcct_symptomclassification','gcct_tasktype','gcct_technicalassistancerequest','gcct_testdriverequest','gcct_timezone','gcct_userdelegationofauthority','gcct_vehicle','gcct_vehiclebrand','gcct_vehicleclass','gcct_vehicleloyaltyallowance','gcct_vehicleoffroad','gcct_vehicleowner','gcct_warrantyhistory','gcct_warrantyloanerpartsdelay','gcct_warrantyloanerrequest','gcct_warrantyloanerrequesthistory','globaloptionsetmetadata','incident','incidentresolution','letter','msdyn_answer','msdyn_question','msdyn_questionresponse','msdyn_survey','msdyn_surveyinvite','msdyn_surveyresponse','optionsetmetadata','phonecall','queue','queueitem','sla','slaitem','slakpiinstance','socialactivity','socialprofile','statemetadata','statusmetadata','systemuser','task','team','teammembership','territory','gcct_marketingprogram']

nested=''
duplicates=''
data_discrepancy=''
new_fields=''
new_entities=''
#for m in sqlContext.tables("dsc10742_gcctmsd_lz_db").select('tableName').where("tableName not like '%bad%'").where("tableName not like '%dup%'").where("tableName not like '%bkp%'").where("tableName not like '%temptz%'").collect():
            #k=m.asDict().values()[0].encode('utf-8')
for k in tables_gcct_except_bad_rec:
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
