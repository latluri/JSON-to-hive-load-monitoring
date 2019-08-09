#!/bin/sh

# run this script as shown below
# sh gcct_monitoring_v1.sh

#This script is written by Laxmi Narayana Atluri, CSID latluri1


#set PYSPARK_DRIVER_PYTHON
kinit $USER@HPC.FORD.COM -k -t latluri1_keytab


output=`spark-submit --principal latluri1 --keytab /user/latluri1/.latluri1.krb5.keytab  --driver-memory 2g --jars spark-avro_2.11-4.0.0.jar json2hive_gcct.py`


echo -e "${output}" | mailx -s GCCT\ Prod\ ITMS\ 24204\ data\ load\ Report   latluri1@ford.com

