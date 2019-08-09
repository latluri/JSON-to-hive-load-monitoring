#!/bin/sh

# run this script as shown below
# nohup sh <this_script>.sh <db> <file> > log &

#This script is written by Laxmi Narayana Atluri, CSID latluri1

#This script takes one input
# target db name
# file to be named

#pre requisites
#Text file containing comarch tables with the file name comrach_tables.txt
 echo -e "#!/bin/bash\n" >  ${2}
 echo -e "kinit -k -t \"./latluri1.keytab\" \"latluri1@HPC.FORD.COM\"\n\n" >>   ${2}
 echo "beeline -e \"set mapreduce.job.queuename=dz-data-ops; " >>   ${2}
 tables=`cat comarch_tables.txt`
            for tab in $tables;
                do
                short=$(echo "${tab}" | awk -F'_' '{print $1}' |awk '{print substr ($0, 2,6 )}')
                #echo "drop table ${1}.${tab};" >>  ${2}
                #echo "create table if not exists ${1}.${tab} AS  select * from cvdp.${tab};" >>  ${2}
                #echo "create table if not exists ${1}.${tab}  AS select * from cvdp.${tab} where to_date(${short}_HDR_TS_UTC_S) >= '2018-10-05';" >> ${2}
                #echo "insert overwrite table ${1}.${tab}  select * from cvdp.${tab};" >> ${2}
                echo "insert overwrite table  ${1}.${tab}  select * from cvdp.${tab} where to_date(${short}_HDR_TS_UTC_S) >= '2018-10-05';" >> ${2}
                done
            echo "\"" >>  ${2}


