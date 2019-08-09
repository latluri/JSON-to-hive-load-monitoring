#!/bin/sh

# run this script as shown below
# nohup sh load_comarch_fillnfill.sh <db> <file> > log &

#This script is written by Laxmi Narayana Atluri, CSID latluri1

#This script takes one input
# target db name
# file to be named

#pre requisites
#Text file containing comarch tables with the file name comrach_tables.txt

tables=`cat comarch_tables.txt`

echo -e "#!/bin/bash\n\n" >  ${2}

echo -e "kinit -k -t \"./latluri1.keytab\" \"latluri1@HPC.FORD.COM\"\n\n" >>  ${2}
echo "date=\$(date +'%Y-%m-%d')" >> ${2}
echo "beeline --showHeader=false --outputformat=tsv2 -e \"set mapreduce.job.queuename=dz-data-ops; " >>  ${2}
for tab in $tables;
                do
                 short=$(echo "${tab}" | awk -F'_' '{print $1}' |awk '{print substr ($0, 2,6 )}')
                 echo "select '${tab}',count(*) from ${1}.${tab} where to_date(${short}_HDR_TS_UTC_S) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()));" >> ${2}
                 #echo "select '${tab}',count(*) from ${1}.${tab} ;" >> ${2}
                done
echo "\">> ${2}\${date}.txt" >> ${2} 

echo "echo -e \"Hello\n\n\nTable Counts for the comarch files  is completed.\n\nPlease find the attached\" |  mailx -s Table\ Counts\ Comarch  -a  ${2}\${date}.txt  latluri1@ford.com" >> ${2}

