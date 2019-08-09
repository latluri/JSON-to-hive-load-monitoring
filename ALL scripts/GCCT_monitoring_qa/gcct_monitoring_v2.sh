#!/bin/sh

# run this script as shown below
# sh gcct_monitoring_v1.sh

#This script is written by Laxmi Narayana Atluri, CSID latluri1


kinit $USER@HPC.FORD.COM -k -t latluri1_keytab
#kinit $USER@HPC.FORD.COM -k -t /s/$USER/.$USER.krb5.keytab

yesterday=`date --date yesterday "+%Y-%m-%d"`
#yesterday="2019-02-07"
all_files=`hadoop fs -ls -t /project/dsc/qa/archive/60230_gcct/ | grep ${yesterday}| awk -F' +' '{print $8}'|sed 's/$/\/*/g'`

#echo $all_files
#echo ${yesterday}
# $all_files | awk -F'/' '{print $7}'


lz_count=`hadoop fs -cat  $all_files| grep -o "messageid\":\"................................." |awk -F'"' '{print $3}'|wc -l`


unset PYSPARK_DRIVER_PYTHON
#set PYSPARK_PYTHON='/s/anaconda/users/latluri1/miniconda2/bin/python'

#set PYSPARK_DRIVER_PYTHON='/s/anaconda/users/latluri1/miniconda2/bin/python'

output_tz=`spark-submit GCCT_count_by_date_v2.py`

tz_count_tables=`echo $output_tz |awk -F' ' '{print $(NF-1)}'`
tz_count_bad_r=`echo $output_tz |awk -F' ' '{print $(NF)}'`
output_g3=`echo $output_tz | rev |cut -d " " -f 3- | rev`

tz_total=$((tz_count_tables+tz_count_bad_r))

#echo $tz_total;

if [[ "$lz_count" -eq "$tz_total" ]]; 
then
	echo -e "No missing data,${lz_count} received, ${tz_count_tables} good records and ${tz_count_bad_r} bad records\n\n\nAdditional info\n\n ${output_g3}" | mailx -s GCCT\ data\ load\ verification\ for\ ${yesterday_1}   latluri1@ford.com
else
        echo -e "Missing data alert, Please verify the GCCT records, ${lz_count} received, ${tz_count_tables} good records and ${tz_count_bad_r} bad records\n\n\nAdditional info\n\n ${output_g3}" | mailx -s GCCT\ data\ load\ verification\ for\ ${yesterday_1}   latluri1@ford.com
fi
