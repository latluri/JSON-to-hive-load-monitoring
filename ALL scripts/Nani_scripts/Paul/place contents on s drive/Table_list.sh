#!/bin/sh

kinit $USER@HPC.FORD.COM -k -t /s/$USER/.$USER.krb5.keytab

beeline  --showHeader=false --outputformat=csv2 -e "use $1; show tables"  > ${1}_tables_list.csv

echo "List of tables ${1}" | mailx -a ${1}_tables_list.csv -s Table\ List\ ${1} ${USER}@ford.com

rm -f ${1}_tables_list.csv
