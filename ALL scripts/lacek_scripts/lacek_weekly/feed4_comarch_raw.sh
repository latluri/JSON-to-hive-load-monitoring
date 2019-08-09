#!/bin/bash

kinit $USER@HPC.FORD.COM -k -t latluri1_keytab

date=$(date +'%Y-%m-%d')

beeline --showHeader=false --outputformat=csv2 -e "set mapreduce.job.queuename=dz-data-ops;
select 'pipe_del_ncvdpda_fordpass_appr_acct_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpda_fordpass_appr_acct_pii_hte;
select 'pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte;
select 'pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte',count(*) from lacek_fpr.pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte;
select 'pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte;
select 'pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte;
select 'pipe_del_ncvdtdb_fordpass_appr_dict_item_hte',count(*) from lacek_fpr.pipe_del_ncvdtdb_fordpass_appr_dict_item_hte;
select 'pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte',count(*) from lacek_fpr.pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte;
select 'pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte;
select 'pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte;
select 'pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte;
select 'pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte;
select 'pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte;
select 'pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte;
select 'pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte;
select 'pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte;
select 'pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte;
select 'pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte;
select 'pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte;
select 'pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte;
select 'pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte;
select 'pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte;
select 'pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte;
select 'pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte;
select 'pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte;
select 'pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte;
select 'pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte;
select 'pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte;
">b4${date}.txt

status=$?
if [ $status != 0 ];
then
    echo "Failed at row counts before loading lacek_comarch_raw_clm "|mailx -s "Lacek_Load_failure_lacek_comarch_raw_clm" latluri1@ford.com
    exit 1
fi

beeline -e "set mapreduce.job.queuename=dz-data-ops;
drop table if exists lacek_fpr.pipe_del_ncvdpda_fordpass_appr_acct_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpda_fordpass_appr_acct_pii_hte  AS select * from cvdp.ncvdpda_fordpass_appr_acct_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpda_fordpass_appr_acct_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpda_fordpass_appr_acct_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte  AS select * from cvdp.ncvdphl_fordpass_appr_win_mv_actvy_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte;
create table if not exists lacek_fpr.pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte  AS select * from cvdp.ncvdtdp_fordpass_appr_cstm_attr_def_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte  AS select * from cvdp.ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte  AS select * from cvdp.ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdtdb_fordpass_appr_dict_item_hte;
create table if not exists lacek_fpr.pipe_del_ncvdtdb_fordpass_appr_dict_item_hte  AS select * from cvdp.ncvdtdb_fordpass_appr_dict_item_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdtdb_fordpass_appr_dict_item_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdtdb_fordpass_appr_dict_item_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte;
create table if not exists lacek_fpr.pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte  AS select * from cvdp.ncvdcdr_fordpass_appr_loc_sec_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte  AS select * from cvdp.ncvdphn_fordpass_appr_win_mv_ltry_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte  AS select * from cvdp.ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte  AS select * from cvdp.ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte  AS select * from cvdp.ncvdpq7_fordpass_appr_prtnr_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte  AS select * from cvdp.ncvdpds_fordpass_appr_pts_bal_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte  AS select * from cvdp.ncvdpq6_fordpass_appr_pt_type_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte  AS select * from cvdp.ncvdpdh_fordpass_appr_reward_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte  AS select * from cvdp.ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte  AS select * from cvdp.ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte  AS select * from cvdp.ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte  AS select * from cvdp.ncvdphr_fordpass_appr_win_mv_srvy_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte  AS select * from cvdp.ncvdpdc_fordpass_appr_trn_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte  AS select * from cvdp.ncvdpdl_fordpass_appr_trn_attr_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte  AS select * from cvdp.ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte  AS select * from cvdp.ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte  AS select * from cvdp.ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte  AS select * from cvdp.ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte  AS select * from cvdp.ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte  AS select * from cvdp.ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');
drop table if exists lacek_fpr.pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte;
create table if not exists lacek_fpr.pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte  AS select * from cvdp.ncvdpdf_fordpass_appr_vin_pii_hte limit 0;
ALTER TABLE lacek_fpr.pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE lacek_fpr.pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte SET SERDEPROPERTIES ('field.delim' = '|');"

status=$?
if [ $status != 0 ];
then
    echo "Failed at dropping the existing tables lacek_comarch_raw_clm "|mailx -s "Lacek_Load_failure_lacek_comarch_raw_clm" latluri1@ford.com
    exit 1
fi


beeline --showHeader=false --outputformat=csv2 -e "set mapreduce.job.queuename=dz-data-ops;
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpda_fordpass_appr_acct_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpda_fordpass_appr_acct_pii_hte where to_date(cvdpda_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpda_partition_home_cntry_c = 'USA'  AND cvdpda_addr_cntry_c_2 = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdphl_fordpass_appr_win_mv_actvy_pii_hte where to_date(cvdphl_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdphl_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdtdp_fordpass_appr_cstm_attr_def_hte where to_date(cvdtdp_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdtdp_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte where to_date(cvdpdm_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdm_partition_home_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte where to_date(cvdphm_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdphm_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdtdb_fordpass_appr_dict_item_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdtdb_fordpass_appr_dict_item_hte where to_date(cvdtdb_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdtdb_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdcdr_fordpass_appr_loc_sec_hte where to_date(cvdcdr_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdcdr_partition_addr_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdphn_fordpass_appr_win_mv_ltry_pii_hte where to_date(cvdphn_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdphn_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte where to_date(cvdphp_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdphp_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte where to_date(cvdphq_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdphq_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpq7_fordpass_appr_prtnr_pii_hte where to_date(cvdpq7_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpq7_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpds_fordpass_appr_pts_bal_pii_hte where to_date(cvdpds_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpds_partition_home_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpq6_fordpass_appr_pt_type_pii_hte where to_date(cvdpq6_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpq6_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdh_fordpass_appr_reward_pii_hte where to_date(cvdpdh_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdh_partition_pts_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte where to_date(cvdphu_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdphu_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte where to_date(cvdpht_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpht_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte where to_date(cvdphs_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdphs_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdphr_fordpass_appr_win_mv_srvy_pii_hte where to_date(cvdphr_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdphr_partition_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdc_fordpass_appr_trn_pii_hte where to_date(cvdpdc_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdc_partition_home_cntry_c = 'USA'  AND cvdpdc_trn_cntry_c_2 = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdl_fordpass_appr_trn_attr_pii_hte where to_date(cvdpdl_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdl_partition_home_cntry_c = 'USA'  AND cvdpdl_trn_cntry_c_2 = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte where to_date(cvdpdv_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdv_partition_home_cntry_c = 'USA'  AND cvdpdv_trn_cntry_c_2 = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte where to_date(cvdpdg_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdg_trn_cntry_c_2 = 'USA'  AND cvdpdg_partition_home_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte where to_date(cvdpdw_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdw_trn_cntry_c_2 = 'USA'  AND cvdpdw_partition_home_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte where to_date(cvdpdy_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdy_partition_home_cntry_c = 'USA'  AND cvdpdy_trn_cntry_c_2 = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte where to_date(cvdpdu_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdu_trn_cntry_c_2 = 'USA'  AND cvdpdu_partition_home_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte where to_date(cvdpdx_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdx_trn_cntry_c_2 = 'USA'  AND cvdpdx_partition_home_cntry_c = 'USA'
; INSERT OVERWRITE DIRECTORY '/project/dz/collab/LACEK/lacek_fpr/pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte' row format delimited FIELDS TERMINATED BY '|' SELECT * from cvdp.ncvdpdf_fordpass_appr_vin_pii_hte where to_date(cvdpdf_HDR_TS_UTC_S) >= date_sub(CAST(current_timestamp() as DATE), 9) AND cvdpdf_partition_home_cntry_c = 'USA'"


status=$?
if [ $status != 0 ];
then
    echo "Failed at loading data into lacek_comarch_raw_clm "|mailx -s "Lacek_Load_failure_lacek_comarch_raw_clm" latluri1@ford.com
    exit 1
fi

beeline --showHeader=false --outputformat=csv2 -e "set mapreduce.job.queuename=dz-data-ops;
select 'pipe_del_ncvdpda_fordpass_appr_acct_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpda_fordpass_appr_acct_pii_hte;
select 'pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte;
select 'pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte',count(*) from lacek_fpr.pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte;
select 'pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte;
select 'pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte;
select 'pipe_del_ncvdtdb_fordpass_appr_dict_item_hte',count(*) from lacek_fpr.pipe_del_ncvdtdb_fordpass_appr_dict_item_hte;
select 'pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte',count(*) from lacek_fpr.pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte;
select 'pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte;
select 'pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte;
select 'pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte;
select 'pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte;
select 'pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte;
select 'pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte;
select 'pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte;
select 'pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte;
select 'pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte;
select 'pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte;
select 'pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte;
select 'pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte;
select 'pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte;
select 'pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte;
select 'pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte;
select 'pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte;
select 'pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte;
select 'pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte;
select 'pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte;
select 'pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte',count(*) from lacek_fpr.pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte;
">after${date}.txt

status=$?
if [ $status != 0 ];
then
    echo "Failed at row counts after loading lacek_comarch_raw_clm "|mailx -s "Lacek_Load_failure_lacek_comarch_raw_clm" latluri1@ford.com
    exit 1
fi

join -1 1 -2 1 -t ',' b4${date}.txt after${date}.txt| perl -p -e 's/^pipe/\npipe/g' |  mailx -s "Lacek Table Counts Comarch Raw CLM"  -a after${date}.txt -a b4${date}.txt  latluri1@ford.com

