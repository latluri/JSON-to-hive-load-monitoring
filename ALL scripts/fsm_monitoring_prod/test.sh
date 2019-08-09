kinit $USER@HPC.FORD.COM -k -t latluri1_keytab
#kinit $USER@HPC.FORD.COM -k -t /s/$USER/.$USER.krb5.keytab

#getting yesterday's date
yesterday_1=`date --date yesterday "+%Y-%m-%d"`
#example date
#yesterday="2019-02-07"

#yesterday's date in quotes
yesterday="\"$yesterday_1\""
#echo $yesterday


all_files=`hadoop fs -ls  /project/dsc/prod/archive/60263_fsm/ | grep ${yesterday_1}| awk -F' +' '{print $8}'|sed 's/$/\/*/g'`
PID=`hadoop fs -ls  /project/dsc/prod/archive/60263_fsm/ | grep ${yesterday_1}| awk -F' +' '{print $8}'|sed 's/$/\/*/g' | awk -F'/' '{print $7}'`

echo $PID
