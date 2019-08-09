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


all_files=`hadoop fs -ls  /project/dsc/qa/archive/60230_gcct/ | grep ${yesterday_1}| awk -F' +' '{print $8}'|sed 's/$/\/*/g'`

#echo $all_files
# $all_files | awk -F'/' '{print $7}'

hadoop fs -cat  $all_files| grep -o "messageid\":\"................................." |awk -F'"' '{print $3}'|sort> ${yesterday_1}_lz_msg_ids.txt

unset PYSPARK_DRIVER_PYTHON

tz_output=`spark-submit get_msg_id_tz.py` 

echo -e $tz_output|grep -v "^$"|grep -v "None"|sort>  ${yesterday_1}_tz_msg_ids.txt
#sort temp_1 > ${yesterday_1}_tz_msg_ids.txt

#rm -f temp_1

sed -i 's/^ //g' ${yesterday_1}_tz_msg_ids.txt


comm -23 ${yesterday_1}_lz_msg_ids.txt ${yesterday_1}_tz_msg_ids.txt |sort|uniq>${yesterday_1}_missing_msgs.txt

echo -e "\n\nThe following Process ids are observed for $yesterday \n"
echo $all_files
#echo -e "\n\nThe following messge ids are missing in the database"

#at ${yesterday_1}_missing_msgs.txt

#unknown_msgs=`comm -23 ${yesterday_1}_tz_msg_ids.txt ${yesterday_1}_lz_msg_ids.txt|wc -l`

#if [[ "$unknown_msgs" -gt 0 ]];
#then
#        echo -e "\n\nThe following messge ids are unknown based on input lz msgs, please investigate"
#	comm -23 ${yesterday_1}_tz_msg_ids.txt ${yesterday_1}_lz_msg_ids.txt|sort|uniq
#fi


messages=`cat ${yesterday_1}_missing_msgs.txt`

beeline --silent=true  --showHeader=false --outputformat=tsv2 -e "use dsc60230_gcct_tz_db; select data from bad_record where message__id is null and to_date(lastupdatedatetime)  = date_sub(CAST(current_timestamp() as DATE), 1)" >  ${yesterday_1}_bad_rec_null_msg_id.txt

for msg in $messages;
	do
		grep -o $msg  ${yesterday_1}_bad_rec_null_msg_id.txt >> ${yesterday_1}_bad_rec_containing_missing_msg_1.txt
	done


sort ${yesterday_1}_bad_rec_containing_missing_msg_1.txt > ${yesterday_1}_bad_rec_containing_missing_msg.txt

comm -23 ${yesterday_1}_missing_msgs.txt ${yesterday_1}_bad_rec_containing_missing_msg.txt

