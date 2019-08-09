#!/bin/sh


beeline-ranger --showHeader=false --outputformat=csv2 -e "use ${1}; show tables" >> all_list_tables_${1}.txt

grep __ct all_list_tables_${1}.txt| >> list_tables_${1}.txt



echo "beeline-ranger --showHeader=false --force=true --outputformat=csv2 -e \" "> ${1}_exec.sh
tables=`cat list_tables_${1}.txt|head -n10`
for tab in $tables;
        do
                        #echo "select '${tab}',count(*) as record_count from ${1}.${tab}; " >> ${1}_${m}_exec.sh
                        #echo "select '${tab}',count(*) as Count, date(dsc_load_s) as Load_Date  from ${1}.${tab} where date(dsc_load_s) > '2018-04-30' group by date(dsc_load_s);" >> ${1}_${m}_exec.sh
                        #echo "select '${tab}',count(*) as Count, date(dsc_load_s) as Load_Date  from ${1}.${tab} group by date(dsc_load_s);" >> ${1}_${m}_exec.sh
                        
			echo "select '${tab}' as Table_name,max(date(lastchange)) as last_change_Max, min(date(lastchange)) as last_change_Min, max(date(header__timestamp)) as header_time_Max, min(date(header__timestamp)) as header_time_Min from ${1}.${tab} ;" >> ${1}_exec.sh
            
        done

echo "\"" >> ${1}_exec.sh
echo ">> ${1}_Counts.txt" >> ${1}_exec.sh    
perl -p -e 's/\n//g'  ${1}_exec.sh > ${1}_execute.sh


