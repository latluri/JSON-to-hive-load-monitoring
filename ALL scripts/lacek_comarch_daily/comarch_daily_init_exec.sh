#!/bin/bash

kinit -k -t "./latluri1.keytab" "latluri1@HPC.FORD.COM"


beeline -e "set mapreduce.job.queuename=dz-data-ops; 
drop table lacek_fpr.NCVDPDA_FORDPASS_APPR_ACCT_PII_HTE;
create table if not exists lacek_fpr.NCVDPDA_FORDPASS_APPR_ACCT_PII_HTE  AS select * from cvdp.NCVDPDA_FORDPASS_APPR_ACCT_PII_HTE where to_date(CVDPDA_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHL_FORDPASS_APPR_WIN_MV_ACTVY_PII_HTE;
create table if not exists lacek_fpr.NCVDPHL_FORDPASS_APPR_WIN_MV_ACTVY_PII_HTE  AS select * from cvdp.NCVDPHL_FORDPASS_APPR_WIN_MV_ACTVY_PII_HTE where to_date(CVDPHL_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDTDP_FORDPASS_APPR_CSTM_ATTR_DEF_HTE;
create table if not exists lacek_fpr.NCVDTDP_FORDPASS_APPR_CSTM_ATTR_DEF_HTE  AS select * from cvdp.NCVDTDP_FORDPASS_APPR_CSTM_ATTR_DEF_HTE where to_date(CVDTDP_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDM_FORDPASS_APPR_CSTM_ATTR_DYN_PII_HTE;
create table if not exists lacek_fpr.NCVDPDM_FORDPASS_APPR_CSTM_ATTR_DYN_PII_HTE  AS select * from cvdp.NCVDPDM_FORDPASS_APPR_CSTM_ATTR_DYN_PII_HTE where to_date(CVDPDM_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHM_FORDPASS_APPR_WIN_MV_CUST_ACTVY_ATTR_VAL_PII_HTE;
create table if not exists lacek_fpr.NCVDPHM_FORDPASS_APPR_WIN_MV_CUST_ACTVY_ATTR_VAL_PII_HTE  AS select * from cvdp.NCVDPHM_FORDPASS_APPR_WIN_MV_CUST_ACTVY_ATTR_VAL_PII_HTE where to_date(CVDPHM_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDTDB_FORDPASS_APPR_DICT_ITEM_HTE;
create table if not exists lacek_fpr.NCVDTDB_FORDPASS_APPR_DICT_ITEM_HTE  AS select * from cvdp.NCVDTDB_FORDPASS_APPR_DICT_ITEM_HTE where to_date(CVDTDB_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDCDR_FORDPASS_APPR_LOC_SEC_HTE;
create table if not exists lacek_fpr.NCVDCDR_FORDPASS_APPR_LOC_SEC_HTE  AS select * from cvdp.NCVDCDR_FORDPASS_APPR_LOC_SEC_HTE where to_date(CVDCDR_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHN_FORDPASS_APPR_WIN_MV_LTRY_PII_HTE;
create table if not exists lacek_fpr.NCVDPHN_FORDPASS_APPR_WIN_MV_LTRY_PII_HTE  AS select * from cvdp.NCVDPHN_FORDPASS_APPR_WIN_MV_LTRY_PII_HTE where to_date(CVDPHN_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHP_FORDPASS_APPR_WIN_MV_LTRY_PRTCPT_PII_HTE;
create table if not exists lacek_fpr.NCVDPHP_FORDPASS_APPR_WIN_MV_LTRY_PRTCPT_PII_HTE  AS select * from cvdp.NCVDPHP_FORDPASS_APPR_WIN_MV_LTRY_PRTCPT_PII_HTE where to_date(CVDPHP_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHQ_FORDPASS_APPR_WIN_MV_LTRY_WINR_PII_HTE;
create table if not exists lacek_fpr.NCVDPHQ_FORDPASS_APPR_WIN_MV_LTRY_WINR_PII_HTE  AS select * from cvdp.NCVDPHQ_FORDPASS_APPR_WIN_MV_LTRY_WINR_PII_HTE where to_date(CVDPHQ_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPQ7_FORDPASS_APPR_PRTNR_PII_HTE;
create table if not exists lacek_fpr.NCVDPQ7_FORDPASS_APPR_PRTNR_PII_HTE  AS select * from cvdp.NCVDPQ7_FORDPASS_APPR_PRTNR_PII_HTE where to_date(CVDPQ7_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDS_FORDPASS_APPR_PTS_BAL_PII_HTE;
create table if not exists lacek_fpr.NCVDPDS_FORDPASS_APPR_PTS_BAL_PII_HTE  AS select * from cvdp.NCVDPDS_FORDPASS_APPR_PTS_BAL_PII_HTE where to_date(CVDPDS_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPQ6_FORDPASS_APPR_PT_TYPE_PII_HTE;
create table if not exists lacek_fpr.NCVDPQ6_FORDPASS_APPR_PT_TYPE_PII_HTE  AS select * from cvdp.NCVDPQ6_FORDPASS_APPR_PT_TYPE_PII_HTE where to_date(CVDPQ6_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDH_FORDPASS_APPR_REWARD_PII_HTE;
create table if not exists lacek_fpr.NCVDPDH_FORDPASS_APPR_REWARD_PII_HTE  AS select * from cvdp.NCVDPDH_FORDPASS_APPR_REWARD_PII_HTE where to_date(CVDPDH_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHU_FORDPASS_APPR_WIN_MV_SRVY_ANSW_PII_HTE;
create table if not exists lacek_fpr.NCVDPHU_FORDPASS_APPR_WIN_MV_SRVY_ANSW_PII_HTE  AS select * from cvdp.NCVDPHU_FORDPASS_APPR_WIN_MV_SRVY_ANSW_PII_HTE where to_date(CVDPHU_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHT_FORDPASS_APPR_WIN_MV_SRVY_ANSW_HDR_PII_HTE;
create table if not exists lacek_fpr.NCVDPHT_FORDPASS_APPR_WIN_MV_SRVY_ANSW_HDR_PII_HTE  AS select * from cvdp.NCVDPHT_FORDPASS_APPR_WIN_MV_SRVY_ANSW_HDR_PII_HTE where to_date(CVDPHT_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHS_FORDPASS_APPR_WIN_MV_SRVY_QSN_PII_HTE;
create table if not exists lacek_fpr.NCVDPHS_FORDPASS_APPR_WIN_MV_SRVY_QSN_PII_HTE  AS select * from cvdp.NCVDPHS_FORDPASS_APPR_WIN_MV_SRVY_QSN_PII_HTE where to_date(CVDPHS_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPHR_FORDPASS_APPR_WIN_MV_SRVY_PII_HTE;
create table if not exists lacek_fpr.NCVDPHR_FORDPASS_APPR_WIN_MV_SRVY_PII_HTE  AS select * from cvdp.NCVDPHR_FORDPASS_APPR_WIN_MV_SRVY_PII_HTE where to_date(CVDPHR_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDC_FORDPASS_APPR_TRN_PII_HTE;
create table if not exists lacek_fpr.NCVDPDC_FORDPASS_APPR_TRN_PII_HTE  AS select * from cvdp.NCVDPDC_FORDPASS_APPR_TRN_PII_HTE where to_date(CVDPDC_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDL_FORDPASS_APPR_TRN_ATTR_PII_HTE;
create table if not exists lacek_fpr.NCVDPDL_FORDPASS_APPR_TRN_ATTR_PII_HTE  AS select * from cvdp.NCVDPDL_FORDPASS_APPR_TRN_ATTR_PII_HTE where to_date(CVDPDL_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDV_FORDPASS_APPR_TRN_BSKT_ITEM_EARN_PRD_PII_HTE;
create table if not exists lacek_fpr.NCVDPDV_FORDPASS_APPR_TRN_BSKT_ITEM_EARN_PRD_PII_HTE  AS select * from cvdp.NCVDPDV_FORDPASS_APPR_TRN_BSKT_ITEM_EARN_PRD_PII_HTE where to_date(CVDPDV_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDG_FORDPASS_APPR_TRN_BSKT_ITEM_RDMPTN_PII_HTE;
create table if not exists lacek_fpr.NCVDPDG_FORDPASS_APPR_TRN_BSKT_ITEM_RDMPTN_PII_HTE  AS select * from cvdp.NCVDPDG_FORDPASS_APPR_TRN_BSKT_ITEM_RDMPTN_PII_HTE where to_date(CVDPDG_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDW_FORDPASS_APPR_TRN_CNVRT_PTS_PII_HTE;
create table if not exists lacek_fpr.NCVDPDW_FORDPASS_APPR_TRN_CNVRT_PTS_PII_HTE  AS select * from cvdp.NCVDPDW_FORDPASS_APPR_TRN_CNVRT_PTS_PII_HTE where to_date(CVDPDW_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDY_FORDPASS_APPR_TRN_PTS_AGG_PII_HTE;
create table if not exists lacek_fpr.NCVDPDY_FORDPASS_APPR_TRN_PTS_AGG_PII_HTE  AS select * from cvdp.NCVDPDY_FORDPASS_APPR_TRN_PTS_AGG_PII_HTE where to_date(CVDPDY_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDU_FORDPASS_APPR_TRN_PTS_BAL_PII_HTE;
create table if not exists lacek_fpr.NCVDPDU_FORDPASS_APPR_TRN_PTS_BAL_PII_HTE  AS select * from cvdp.NCVDPDU_FORDPASS_APPR_TRN_PTS_BAL_PII_HTE where to_date(CVDPDU_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDX_FORDPASS_APPR_TRN_PROMO_PTS_PII_HTE;
create table if not exists lacek_fpr.NCVDPDX_FORDPASS_APPR_TRN_PROMO_PTS_PII_HTE  AS select * from cvdp.NCVDPDX_FORDPASS_APPR_TRN_PROMO_PTS_PII_HTE where to_date(CVDPDX_HDR_TS_UTC_S) >= '2018-10-05';
drop table lacek_fpr.NCVDPDF_FORDPASS_APPR_VIN_PII_HTE;
create table if not exists lacek_fpr.NCVDPDF_FORDPASS_APPR_VIN_PII_HTE  AS select * from cvdp.NCVDPDF_FORDPASS_APPR_VIN_PII_HTE where to_date(CVDPDF_HDR_TS_UTC_S) >= '2018-10-05';
"
