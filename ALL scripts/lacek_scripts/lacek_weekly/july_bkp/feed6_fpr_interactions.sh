#!/bin/bash

kinit $USER@HPC.FORD.COM -k -t latluri1_keytab

#count b4
b4=`beeline -e "select count(*) from lacek_fpr.pipe_del_gdf0r10_fordpass_clickstr_app"`


beeline -e "set mapreduce.job.queuename=dz-data-ops;
drop table lacek_fpr.pipe_del_gdf0r10_fordpass_clickstr_app;
create table if not exists lacek_fpr.pipe_del_gdf0r10_fordpass_clickstr_app as select  df0r10_pevar31_x,df0r10_pevar46_guid_d_2,df0r10_evar54_ui_x,df0r10_date_time_s  from 10290_fp_app_cs_us_lz_db.gdf0r10_fordpass_clickstr_app where to_date(df0r10_load_y) >= '2018-10-05' limit 0;
ALTER TABLE lacek_fpr.pipe_del_gdf0r10_fordpass_clickstr_app SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_gdf0r10_fordpass_clickstr_app SET SERDEPROPERTIES ('field.delim' = '|');"

beeline -e "
INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_gdf0r10_fordpass_clickstr_app' row format delimited FIELDS TERMINATED BY '|' select  df0r10_pevar31_x,df0r10_pevar46_guid_d_2,df0r10_evar54_ui_x,df0r10_date_time_s  from 10290_fp_app_cs_us_lz_db.gdf0r10_fordpass_clickstr_app where to_date(df0r10_load_y) >= date_sub(CAST(current_timestamp() as DATE), 9);"



#count after
after=`beeline -e "select count(*) from lacek_fpr.pipe_del_gdf0r10_fordpass_clickstr_app"`


echo "Counts b4 loading: $b4 \nCounts after loading $after"|mailx -s "Lacek loading_report_feed6_FPR_interactions_lacek" latluri1@ford.com


