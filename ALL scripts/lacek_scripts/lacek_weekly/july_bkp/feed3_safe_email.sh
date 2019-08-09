#!/bin/bash

kinit $USER@HPC.FORD.COM -k -t latluri1_keytab


#count b4
b4=`beeline --showHeader=false --outputformat=csv2 -e "select count(*) from lacek_fpr.pipe_del_email_safe_cks_current"`

sqoop import \
-Dhadoop.security.credential.provider.path=jceks://hdfs/user/latluri1/pass.jceks \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--hive-import \
--driver com.teradata.jdbc.TeraDriver \
--connect jdbc:teradata://tera07.dearborn.ford.com/Database=CKSPRD_PII_VIEW \
--username=latluri1 --password-alias userid_alias \
--query "select safe_email_address from cksprd_pii_view.email_safe_cks_current  where cks_a_update_date >= (CURRENT_DATE- INTERVAL '9' DAY) AND \$CONDITIONS" \
--hive-table lacek_fpr.pipe_del_email_safe_cks_current \
--hive-overwrite \
--as-textfile \
--fields-terminated-by '|' \
--split-by safe_email_address -m 10 \
--delete-target-dir \
--target-dir  hdfs://hdp2cluster/project/dz/collab/LACEK/lacek_fpr/temp311 \

#count b4
after=`beeline --showHeader=false --outputformat=csv2 -e "select count(*) from lacek_fpr.pipe_del_email_safe_cks_current"`



echo -e "Counts b4 loading: ${b4} \nCounts after loading ${after}"

echo -e "Counts b4 loading: ${b4} \nCounts after loading ${after}"|mailx -s "Lacek_loading_report_feed3_safe_email" latluri1@ford.com


