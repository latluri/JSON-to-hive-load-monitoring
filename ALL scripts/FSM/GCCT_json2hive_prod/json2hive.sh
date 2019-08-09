#!/bin/bash

# run this script as shown below
# sh gcct_monitoring_v1.sh

#This script is written by Laxmi Narayana Atluri, CSID latluri1


#set PYSPARK_DRIVER_PYTHON

kinit  $USER@HPC.FORD.COM -k -t latluri1_keytab


#kinit $USER@HPC.FORD.COM -k -t /s/$USER/.$USER.krb5.keytab


unset PYSPARK_DRIVER_PYTHON


export SPARK_MAJOR_VERSION=2 
export SPARK_HOME=/usr/hdp/current/spark2-client 


#output_tz=`spark-submit  --principal latluri1 --keytab /user/latluri1/.latluri1.krb5.keytab --driver-memory 2g   --jars spark-avro_2.11-4.0.0.jar json2hive.py `


output_tz=`spark-submit --driver-memory 2g   --jars spark-avro_2.11-4.0.0.jar json2hive.py `


#output_tz=`spark-submit --version`

echo -e " $output_tz" | mailx -s 'j2h testing'   latluri1@ford.com
