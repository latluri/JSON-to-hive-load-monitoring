#!/bin/sh

kinit $USER@HPC.FORD.COM -k -t /s/$USER/.$USER.krb5.keytab

rm -f  ${1}_table_description.csv

beeline  --showHeader=false --outputformat=tsv2 -e "use $1; show tables"  > ${1}_tables_list.csv

tables=`cat ${1}_tables_list.csv`
#tables="ncvdg01_fpas_uniq_cust_pii"

code=""
for tab in $tables;
     do
	code="${code} select \"Q${tab},\";desc ${tab};" 
     done
#echo $code

echo "aQtable,column,data_type,comments">${1}_table_description.csv
beeline  --showHeader=false --outputformat=csv2 --force -e " use ${1};${code}" >>${1}_table_description.csv

sed -i "s/^/,/g" ${1}_table_description.csv
sed -i "s/^,.Q//g" ${1}_table_description.csv

awk -F ',' '$1 != ""{p1=$1} NF==3{p2=$2} p1 && $1 == ""{$1=p1} p2 && NF==2{$0=$1 OFS p2 OFS $2} 1' OFS=',' ${1}_table_description.csv > ${1}_table_description_csv.csv

echo "Table descriptions of ${1}" | mailx -a  ${1}_table_description_csv.csv  -s Table\ Descriptions\ ${1} ${USER}@ford.com

#rm -f ${1}_tables_list.csv  ${1}_table_description.csv  ${1}_table_description_csv.csv

