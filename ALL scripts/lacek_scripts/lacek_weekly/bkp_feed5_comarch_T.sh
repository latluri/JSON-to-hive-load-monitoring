#!/bin/bash

kinit $USER@HPC.FORD.COM -k -t latluri1_keytab

date=$(date +'%Y-%m-%d')

beeline --showHeader=false --outputformat=csv2 -e "set mapreduce.job.queuename=dz-data-ops;
select 'PIPE_DEL_NCVDG10_FPAS_MTHLY_PT_BAL_TREND_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG10_FPAS_MTHLY_PT_BAL_TREND_PII ;
select 'PIPE_DEL_NCVDG09_FPAS_CURR_PT_BAL_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG09_FPAS_CURR_PT_BAL_PII ;
select 'PIPE_DEL_NCVDG08_FPAS_RUNNG_ENGMT_FLAG_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG08_FPAS_RUNNG_ENGMT_FLAG_PII ;
select 'PIPE_DEL_NCVDG05_FPAS_TRN_WITH_VALS_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG05_FPAS_TRN_WITH_VALS_PII ;
select 'PIPE_DEL_NCVDG04_FPAS_TRN_WITH_PTS_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG04_FPAS_TRN_WITH_PTS_PII ;
select 'PIPE_DEL_NCVDG03_FPAS_CUST_DLR_RELSHP_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG03_FPAS_CUST_DLR_RELSHP_PII ;
select 'PIPE_DEL_NCVDG02_FPAS_UNIQ_TRN_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG02_FPAS_UNIQ_TRN_PII ;
select 'PIPE_DEL_NCVDG01_FPAS_UNIQ_CUST_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG01_FPAS_UNIQ_CUST_PII ;
"> b4${date}.txt

status=$?
if [ $status != 0 ];
then
    echo "Failed at row counts before loading lacek_comarch_weekly"|mailx -s "Lacek Load_failure_lacek_comarch_transformed" latluri1@ford.com
    exit 1
fi

beeline -e "set mapreduce.job.queuename=dz-data-ops;
drop table if exists lacek_fpr.pipe_del_ncvdg10_fpas_mthly_pt_bal_trend_pii;
create table if not exists lacek_fpr.pipe_del_ncvdg10_fpas_mthly_pt_bal_trend_pii  AS select * from cvdp.ncvdg10_fpas_mthly_pt_bal_trend_pii limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdg10_fpas_mthly_pt_bal_trend_pii SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdg10_fpas_mthly_pt_bal_trend_pii SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdg09_fpas_curr_pt_bal_pii;
create table if not exists lacek_fpr.pipe_del_ncvdg09_fpas_curr_pt_bal_pii  AS select * from cvdp.ncvdg09_fpas_curr_pt_bal_pii limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdg09_fpas_curr_pt_bal_pii SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdg09_fpas_curr_pt_bal_pii SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdg08_fpas_runng_engmt_flag_pii;
create table if not exists lacek_fpr.pipe_del_ncvdg08_fpas_runng_engmt_flag_pii  AS select * from cvdp.ncvdg08_fpas_runng_engmt_flag_pii limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdg08_fpas_runng_engmt_flag_pii SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdg08_fpas_runng_engmt_flag_pii SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdg05_fpas_trn_with_vals_pii;
create table if not exists lacek_fpr.pipe_del_ncvdg05_fpas_trn_with_vals_pii  AS select * from cvdp.ncvdg05_fpas_trn_with_vals_pii limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdg05_fpas_trn_with_vals_pii SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdg05_fpas_trn_with_vals_pii SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdg04_fpas_trn_with_pts_pii;
create table if not exists lacek_fpr.pipe_del_ncvdg04_fpas_trn_with_pts_pii  AS select * from cvdp.ncvdg04_fpas_trn_with_pts_pii limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdg04_fpas_trn_with_pts_pii SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdg04_fpas_trn_with_pts_pii SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdg03_fpas_cust_dlr_relshp_pii;
create table if not exists lacek_fpr.pipe_del_ncvdg03_fpas_cust_dlr_relshp_pii  AS select * from cvdp.ncvdg03_fpas_cust_dlr_relshp_pii limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdg03_fpas_cust_dlr_relshp_pii SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdg03_fpas_cust_dlr_relshp_pii SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdg02_fpas_uniq_trn_pii;
create table if not exists lacek_fpr.pipe_del_ncvdg02_fpas_uniq_trn_pii  AS select * from cvdp.ncvdg02_fpas_uniq_trn_pii limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdg02_fpas_uniq_trn_pii SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdg02_fpas_uniq_trn_pii SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdg01_fpas_uniq_cust_pii;
create table if not exists lacek_fpr.pipe_del_ncvdg01_fpas_uniq_cust_pii  AS select * from cvdp.ncvdg01_fpas_uniq_cust_pii limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdg01_fpas_uniq_cust_pii SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdg01_fpas_uniq_cust_pii SET SERDEPROPERTIES ('field.delim' = '|');"

status=$?
if [ $status != 0 ];
then
    echo "Failed at dropping the old tables"|mailx -s "Lacek Load_failure_lacek_comarch_transformed" latluri1@ford.com
    exit 1
fi


beeline -e "set mapreduce.job.queuename=dz-data-ops;
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdg10_fpas_mthly_pt_bal_trend_pii' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdg10_fpas_mthly_pt_bal_trend_pii where to_date(timeoflasttransofmonthfortestpurposes) >= date_sub(CAST(current_timestamp() as DATE), 9)
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdg09_fpas_curr_pt_bal_pii' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdg09_fpas_curr_pt_bal_pii where cvdpda_partition_home_cntry_c = 'USA'  AND cvdpda_addr_cntry_c_2 = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdg08_fpas_runng_engmt_flag_pii' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdg08_fpas_runng_engmt_flag_pii 
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdg05_fpas_trn_with_vals_pii' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdg05_fpas_trn_with_vals_pii
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdg04_fpas_trn_with_pts_pii' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdg04_fpas_trn_with_pts_pii where to_date(cvdpdc_trn_process_s_2) >= date_sub(CAST(current_timestamp() as DATE), 9)
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdg03_fpas_cust_dlr_relshp_pii' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdg03_fpas_cust_dlr_relshp_pii where to_date(cvdpda_custx_last_actvy_s_2) >= date_sub(CAST(current_timestamp() as DATE), 9)
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdg02_fpas_uniq_trn_pii' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdg02_fpas_uniq_trn_pii where to_date(cvdpdc_hdr_ts_utc_s) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdc_partition_home_cntry_c = 'USA'  AND cvdpdc_trn_cntry_c_2 = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdg01_fpas_uniq_cust_pii' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdg01_fpas_uniq_cust_pii where to_date(cvdpda_hdr_ts_utc_s) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpda_partition_home_cntry_c = 'USA'  AND cvdpda_addr_cntry_c_2 = 'USA'
"

status=$?
if [ $status != 0 ];
then
    echo "Failed at inserting the data into lacek_comarch_weekly"|mailx -s "Lacek Load_failure_lacek_comarch_transformed" latluri1@ford.com
    exit 1
fi

beeline --showHeader=false --outputformat=csv2 -e "set mapreduce.job.queuename=dz-data-ops;
select 'PIPE_DEL_NCVDG10_FPAS_MTHLY_PT_BAL_TREND_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG10_FPAS_MTHLY_PT_BAL_TREND_PII ;
select 'PIPE_DEL_NCVDG09_FPAS_CURR_PT_BAL_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG09_FPAS_CURR_PT_BAL_PII ;
select 'PIPE_DEL_NCVDG08_FPAS_RUNNG_ENGMT_FLAG_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG08_FPAS_RUNNG_ENGMT_FLAG_PII ;
select 'PIPE_DEL_NCVDG05_FPAS_TRN_WITH_VALS_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG05_FPAS_TRN_WITH_VALS_PII ;
select 'PIPE_DEL_NCVDG04_FPAS_TRN_WITH_PTS_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG04_FPAS_TRN_WITH_PTS_PII ;
select 'PIPE_DEL_NCVDG03_FPAS_CUST_DLR_RELSHP_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG03_FPAS_CUST_DLR_RELSHP_PII ;
select 'PIPE_DEL_NCVDG02_FPAS_UNIQ_TRN_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG02_FPAS_UNIQ_TRN_PII ;
select 'PIPE_DEL_NCVDG01_FPAS_UNIQ_CUST_PII',count(*) from lacek_fpr.PIPE_DEL_NCVDG01_FPAS_UNIQ_CUST_PII ;
"> after${date}.txt

status=$?
if [ $status != 0 ];
then
    echo "Failed at row counts after loading lacek_comarch_weekly"|mailx -s "Lacek Load_failure_lacek_comarch_transformed" latluri1@ford.com
    exit 1
fi

join -1 1 -2 1 -t ',' b4${date}.txt after${date}.txt |perl -p -e 's/^PIPE/\nPIPE/g' |  mailx -s "Lacek Table Counts Comarch Transformed"  -a after${date}.txt -a b4${date}.txt latluri1@ford.com


