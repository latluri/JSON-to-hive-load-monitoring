#!/bin/bash
#!/bin/sh

# run this script as shown below
# sh gcct_monitoring_v1.sh

#This script is written by Laxmi Narayana Atluri, CSID latluri1


#set PYSPARK_DRIVER_PYTHON
kinit $USER@HPC.FORD.COM -k -t latluri1_keytab
#kinit $USER@HPC.FORD.COM -k -t /s/$USER/.$USER.krb5.keytab

#getting yesterday's date
yesterday_1=`date --date yesterday "+%Y-%m-%d"`
#example date
#yesterday="2019-02-07"

#yesterday's date in quotes
yesterday="\"$yesterday_1\""
#echo $yesterday


all_files=`hadoop fs -ls  /project/dsc/prod/archive/10742_gcctmsd/ | grep ${yesterday_1}| awk -F' +' '{print $8}'|sed 's/$/\/*/g'`
#PID=`hadoop fs -ls  /project/dsc/prod/archive/10742_gcctmsd/ | grep ${yesterday_1}| awk -F' +' '{print $8}'|sed 's/$/\/*/g'| awk -F'/' '{print $7}'`
PID=$(echo "$all_files" | while IFS= read -r line ; do echo $line|awk -F' ' '{print $1}' | awk -F'/' '{print $7}'; done)
#echo $PID
#echo $PID2
#echo $all_files
# $all_files | awk -F'/' '{print $7}'


lz_count=`hadoop fs -cat  $all_files| grep -o "messageid\":\"................................." |awk -F'"' '{print $3}'|wc -l`
#echo $lz_count

unset PYSPARK_DRIVER_PYTHON


output_tz=`spark-submit --driver-memory 16g --executor-memory 4g GCCT_count_by_date_v3.py `

tz_count_tables=`echo $output_tz |awk -F' ' '{print $(NF-1)}'`
tz_count_bad_r=`echo $output_tz |awk -F' ' '{print $(NF)}'`

output_info_all=`echo $output_tz | rev |cut -d " " -f 3- | rev`
output_info=`echo $output_info_all |awk -F'|' '{print $(NF)}'`
output_g3=`echo $output_info_all | rev |cut -d "|" -f 2- | rev`
tz_total=$((tz_count_tables+tz_count_bad_r))

#echo $output_g3 

if [[ "$lz_count" -eq "$tz_total" ]];
then
        echo -e "The following process id(s) are observed on ${yesterday_1} \n${PID}\n\nNo missing data,${lz_count} received, ${tz_count_tables} good records and ${tz_count_bad_r} bad records\n\n${output_info}\n\nAdditional info\n\n ${output_g3}" | mailx -s GCCT\ Prod\ ITMS\ 24204\ data\ load\ verification\ for\ ${yesterday_1}   latluri1@ford.com
else
        echo -e "The following process id(s) are observed on ${yesterday_1} \n${PID}\n\nMissing data alert, Please verify the GCCT records, ${lz_count} received, ${tz_count_tables} good records and ${tz_count_bad_r} bad records\n\n${output_info}\n\nAdditional info\n\n ${output_g3}" | mailx -s GCCT\ Prod\ ITMS\ 24204\ data\ load\ verification\ for\ ${yesterday_1}   latluri1@ford.com
fi

