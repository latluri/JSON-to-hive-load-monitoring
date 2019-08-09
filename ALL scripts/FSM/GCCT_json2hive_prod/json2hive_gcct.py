import os
import sys
import re
import pandas as pd
import commands
import uuid
import json
import ast
import itertools
import pyspark.sql.functions
from pyspark.sql.functions import col
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import row_number,lit,monotonically_increasing_id
from pyspark.sql.functions import col, size, length
from collections import Counter
from datetime import datetime,timedelta
from pyspark.sql.functions import sha2, concat_ws
import re
from pyspark.sql.types import StructType, StructField, StringType
import numpy as np
#from pyspark.sql import SparkSession
spark_home = os.environ.get('SPARK_HOME', None)
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')
if not os.path.isdir(spark_home):
    raise ValueError('SPARK_HOME environment variable is not a directory')
if not os.path.isdir(os.path.join(spark_home, 'python')):
    raise ValueError('SPARK_HOME directory does not contain python')
sys.path.insert(0, os.path.join(spark_home, 'python'))
pylib_list = (item for item in os.listdir(os.path.join(spark_home, 'python/lib/'))
              if re.match(r'py4j-\d+(\.\d+)+-src\.zip\Z', item)
              )
try:
    py4j_file = max(pylib_list)
    py4j = os.path.join(spark_home, os.path.join('python/lib', py4j_file))
except ValueError:
    raise ValueError(
        'Could not find py4j'
    )
sys.path.insert(0, py4j)

spark_release_file = spark_home + "/RELEASE"
if os.path.exists(spark_release_file) and "Spark" in  open(spark_release_file).read():
    pyspark_submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", " --master yarn-client \
                                         --executor-memory 4g --executor-cores 5 --driver-memory 16g"
                                        )
    if not "pyspark-shell" in pyspark_submit_args: pyspark_submit_args += " pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

with open(os.path.join(spark_home, 'python/pyspark/shell.py')) as f:
    code = compile(f.read(), os.path.join(spark_home, 'python/pyspark/shell.py'), 'exec')
    exec(code)


today=datetime.today().strftime('%Y-%m-%d')
today_full=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
yesterday = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
day_b4_yesterday = (datetime.today() - timedelta(2)).strftime('%Y-%m-%d')

db="json2hive"
db_location="hdfs://hdp2cluster/project/dz/collab/"
Json_location="/project/dsc/prod/archive/10742_gcctmsd"


messages_2_days=commands.getoutput("hadoop fs -ls  "+Json_location+" |grep -e "+today+" -e "+yesterday+" | awk -F' +' -v g=\"{\" -v h=\"}\" -v q=\"'\" -v s=\":\" '{print g q$8q s q$6q h}'").splitlines()[1:]
msg_2_days_dict={}
for i in messages_2_days:
    msg_2_days_dict.update(ast.literal_eval(i))

list_ids_in_db_row=sqlContext.table(db+".audit").where("to_date(date_)  = date_sub(CAST(current_timestamp() as DATE), 2) ").select("id").collect()
list_ids_in_db=[i424["id"] for i424 in list_ids_in_db_row]

list_2_load_keys=set(msg_2_days_dict.keys()).difference(set(list_ids_in_db))
list_2_load_str="/* ".join(list_2_load_keys)+"/*"

ids_2_load=[[i425,msg_2_days_dict[i425]] for i425 in list_2_load_keys]

#ids_2_load=[i400.split(" ") for i400 in list_ids_2_load]
R = Row('id', 'date_')
ids_2_load_df=spark.createDataFrame([R(i421[0],i421[1]) for i421 in ids_2_load])

l228=commands.getoutput("hadoop fs -cat "+list_2_load_str+"| perl -p -e 's/{\"entitydata\":/\n{\"entitydata\":/g'| grep -v \"^$\" |hadoop fs -put -f - JSON_INPUT")
l229=commands.getoutput("hadoop fs -cat "+list_2_load_str+"| perl -p -e 's/{\"entitydata\":/\n{\"entitydata\":/g'| grep -v \"^$\" |wc -l")


today_full=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
df=sqlContext.read.json("JSON_INPUT")
from pyspark.sql.functions import to_json,struct

df=df.withColumn("Process__id", lit(str(uuid.uuid1())))
df=df.withColumn("gdia_load_date", lit(str(today_full)))
df=df.withColumnRenamed('messageid','message__id')

bad_record=spark.createDataFrame(Row(**x) for x in [{"entitystring":"","process__id":"","gdia_load_date":"","reason":""}])
#spark.createDataFrame(Row(**x) for x in mylist)

df_columns=df.columns
if "_corrupt_record" in df_columns:
    bad_record_new=df.filter("_corrupt_record is not null")
    bad_record_new=bad_record_new.withColumnRenamed("_corrupt_record","entitystring")
    bad_record_new=bad_record_new.withColumn("reason", lit("JSON parsing error in entitystring"))
    bad_record_new=bad_record_new.select("entitystring","process__id","gdia_load_date","reason")
    bad_record=bad_record.union(bad_record_new)
    df=df.filter("_corrupt_record is null")
    df=df.drop("_corrupt_record")
    df_columns.remove("_corrupt_record")
    df=df.withColumn("entitystring", to_json(struct(df_columns)))
else:
    print("JSON audit columns clean")
    df=df.withColumn("entitystring", to_json(struct(df_columns)))

tables_1=df.select("entitylogicalname").distinct().collect()
tables_2_load=[row.entitylogicalname for row in tables_1 if row.entitylogicalname is not None]




def rep(x):
    if x != '':
        return Row(**x)
    else:
        return()

    

from pyspark.sql.window import Window
w = Window().orderBy(lit('A'))

def create_table(db_location,db,i344):
    return("""create external table """+i344+"""
stored as avro
location '"""+db_location+db+"""/"""+i344+"""'
tblproperties('avro.schema.url'='"""+db_location+db+"""/schema/"""+i344+""".avsc')
""")


#https://stackoverflow.com/questions/5508509/how-do-i-check-if-a-string-is-valid-json-in-python
def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError, e:
        return({"_corrupt_record_data":"True"})
    return(json_object)
	

def create_and_update_new_table(final,db_location,db,i344):
        print("Found new table: "+i344)
        final.write.option("path",db_location+db+"/"+i344).format("com.databricks.spark.avro").saveAsTable(db+"."+i344)
        sqlContext.sql("drop table if exists "+db+"."+i344)
        avro_file=commands.getoutput("hdfs dfs -ls  "+db_location+db+"/"+i344+"/|awk -F' ' '{print $8}'").splitlines()[3]
        commands.getoutput("hadoop jar  avro-tools-1.8.2.jar getschema "+avro_file+" | hadoop fs -put -f - "+db_location+db+"/schema/"+i344+".avsc")    
        stmnt=create_table(db_location,db,i344)
        #print(stmnt)
        sqlContext.sql(stmnt)
		
		

sqlContext.sql("use "+db)
tables_in_db_1=sqlContext.tables(db).select("tableName").collect()
tables_in_db=[j12["tableName"] for j12 in tables_in_db_1 ]


for i344 in tables_2_load:
#i344="systemuserbusinessunitentitymap"
#if i344=="systemuserbusinessunitentitymap":
    print(i344)
    df2=df.filter('entitylogicalname =='+'\"'+i344+'\"')
    df2_1=df2.select("entitydata").rdd.map(lambda p1:is_json(p1["entitydata"])).map(lambda g2: dict((k.lower(), unicode(v)) if type(v) != "unicode" else ((k.lower(), v)) for k, v in g2.iteritems()))
    l2=df2_1.collect()
    #l2=df2.select("entitydata").rdd.map(lambda r: json.loads(r["entitydata"])).map(lambda g2: dict((k.lower(), unicode(v)) if type(v) != "unicode" else ((k.lower(), v)) for k, v in g2.iteritems())).collect()
    df3_2=spark.createDataFrame(l2)
    #df3=df2.select("entitydata").rdd.map(lambda s:re.sub(r'(:)(null)',r'\1"NULL"',s["entitydata"])).map(lambda s2:re.sub(r'(:)(true)',r'\1"True"',s2)).map(lambda s3:re.sub(r'(:)(false)',r'\1"False"',s3)).map(lambda k: re.sub(r'(:)([0-9a-zA-Z.:]+)(,)', r'\1"\2"\3', k)).map(lambda k: re.sub(r'(:)([0-9a-zA-Z,:]+)(})', r'\1"\2"\3', k)).map(lambda d21: re.sub('[a-zA-Z"]+:', lambda m: m.group(0).lower(), d21)).map(lambda p: ast.literal_eval(p))
    #.map(lambda g1: str(g1).lower())
    #df3_2=df3.map(lambda v:Row(**v)).toDF()
    #df3_2=spark.createDataFrame(df3.collect())
    df2=df2.withColumn("columnindex",  row_number().over(w))
    df3_2=df3_2.withColumn("columnindex", row_number().over(w))
    final=df2.join(df3_2, df2.columnindex == df3_2.columnindex, 'inner').drop(df3_2.columnindex)
    final=final.drop('columnindex')
    sha_columns=df3_2.columns
    sha_columns.append("eventtype")
    sha_columns.remove("columnindex")
    final=final.withColumn("sha_key", sha2(concat_ws("||", *sha_columns), 256))
    if "_corrupt_record_data" in final.columns:
        print("_corrupt_JSON_data found")
        bad_record_new=final.filter("_corrupt_record_data is not null").select("entitystring","process__id","gdia_load_date")
        bad_record_new=bad_record_new.withColumn("reason", lit("JSON parsing error in entity data"))
        bad_record=bad_record.union(bad_record_new)
        final=final.filter("_corrupt_record_data is null")
        final=final.drop("_corrupt_record_data")
    else:
        print(" json data is clean")
    final=final.drop('entitydata')
    final=final.drop('entitystring')
    if i344.lower() in tables_in_db:
        print("Table already existing: "+i344)
        col_in_new_data=[j28.lower() for j28 in final.columns]
        col_in_table=sqlContext.table(i344).columns
        list_2_add=set(col_in_new_data).difference(set(col_in_table))
        list_2_add_maintain_schema=set(col_in_table).difference(set(col_in_new_data))
        if len(list_2_add_maintain_schema)>0:
            print("JSON contains less columns than expected such as: "+str(list_2_add_maintain_schema).replace("set([","").replace("])",""))
            for i386 in list_2_add_maintain_schema:
                final=final.withColumn(i386,lit(""))
        if len(list_2_add)>0:
            print("______________________New columns added "+str(list_2_add).replace("set([","").replace("])",""))
            a1=sc.textFile(db_location+db+"/schema/"+i344+".avsc")
            string_schema="\n".join(j23 for j23 in a1.collect())
            schema_end_part="\n  } ]\n}"
            schema_attachment="".join("\n  }, {\n    \"name\" : \""+j16+"\",\n    \"type\" : [ \"string\", \"null\" ],\n    \"default\" :\"null\"" for j16 in list_2_add)
            updated_schema=re.sub(r"(\n  } ]\n})$","",string_schema)+schema_attachment+schema_end_part
            ##commands.getoutput("echo \""+updated_schema+"\"|hadoop fs -put -f - hdfs://hdp2cluster/apps/hive/warehouse/latluri1.db/schema/account.avsc")
            commands.getoutput("hadoop fs -rm -R -f schema_temp_avsc")
            sc.parallelize([updated_schema]).coalesce(1,True).saveAsTextFile("schema_temp_avsc")
            commands.getoutput("hadoop distcp --overwrite  schema_temp_avsc/part-00000 "+db_location+db+"/schema/"+i344+".avsc")
            final.write.mode("append").format("com.databricks.spark.avro").insertInto(db+"."+i344)
        else:
            print("______________________No new columns found, adding new rows to the table")
            final.write.mode("append").format("com.databricks.spark.avro").insertInto(db+"."+i344)
    else:
        create_and_update_new_table(final,db_location,db,i344)


bad_record=bad_record.cache()
if "bad_record" in tables_in_db:
    bad_record.write.mode("append").format("com.databricks.spark.avro").insertInto(db+".bad_record")
else:
    create_and_update_new_table(bad_record,db_location,db,"bad_record")
	

ids_2_load_df.write.mode("append").insertInto(db+".audit")

m=0
for i in sqlContext.tables(db).select("tableName").where("tableName != 'audit'").collect():
    j=i["tableName"]
    k=sqlContext.table(j).where("to_date(gdia_load_date)==CAST(current_timestamp() as DATE)").count()
    print(j,k)
    m=m+k


print("Total:"+str(m))
