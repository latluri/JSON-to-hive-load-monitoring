#!/bin/bash


kinit -k -t "./latluri1.keytab" "latluri1@HPC.FORD.COM"


date=$(date +'%Y-%m-%d')
beeline --showHeader=false --outputformat=tsv2 -e "set mapreduce.job.queuename=dz-data-ops; 
select 'NCVDPDA_FORDPASS_APPR_ACCT_PII_HTE',count(*) from cvdp.NCVDPDA_FORDPASS_APPR_ACCT_PII_HTE where to_date(CVDPDA_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHL_FORDPASS_APPR_WIN_MV_ACTVY_PII_HTE',count(*) from cvdp.NCVDPHL_FORDPASS_APPR_WIN_MV_ACTVY_PII_HTE where to_date(CVDPHL_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDTDP_FORDPASS_APPR_CSTM_ATTR_DEF_HTE',count(*) from cvdp.NCVDTDP_FORDPASS_APPR_CSTM_ATTR_DEF_HTE where to_date(CVDTDP_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDM_FORDPASS_APPR_CSTM_ATTR_DYN_PII_HTE',count(*) from cvdp.NCVDPDM_FORDPASS_APPR_CSTM_ATTR_DYN_PII_HTE where to_date(CVDPDM_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHM_FORDPASS_APPR_WIN_MV_CUST_ACTVY_ATTR_VAL_PII_HTE',count(*) from cvdp.NCVDPHM_FORDPASS_APPR_WIN_MV_CUST_ACTVY_ATTR_VAL_PII_HTE where to_date(CVDPHM_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDTDB_FORDPASS_APPR_DICT_ITEM_HTE',count(*) from cvdp.NCVDTDB_FORDPASS_APPR_DICT_ITEM_HTE where to_date(CVDTDB_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDCDR_FORDPASS_APPR_LOC_SEC_HTE',count(*) from cvdp.NCVDCDR_FORDPASS_APPR_LOC_SEC_HTE where to_date(CVDCDR_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHN_FORDPASS_APPR_WIN_MV_LTRY_PII_HTE',count(*) from cvdp.NCVDPHN_FORDPASS_APPR_WIN_MV_LTRY_PII_HTE where to_date(CVDPHN_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHP_FORDPASS_APPR_WIN_MV_LTRY_PRTCPT_PII_HTE',count(*) from cvdp.NCVDPHP_FORDPASS_APPR_WIN_MV_LTRY_PRTCPT_PII_HTE where to_date(CVDPHP_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHQ_FORDPASS_APPR_WIN_MV_LTRY_WINR_PII_HTE',count(*) from cvdp.NCVDPHQ_FORDPASS_APPR_WIN_MV_LTRY_WINR_PII_HTE where to_date(CVDPHQ_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPQ7_FORDPASS_APPR_PRTNR_PII_HTE',count(*) from cvdp.NCVDPQ7_FORDPASS_APPR_PRTNR_PII_HTE where to_date(CVDPQ7_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDS_FORDPASS_APPR_PTS_BAL_PII_HTE',count(*) from cvdp.NCVDPDS_FORDPASS_APPR_PTS_BAL_PII_HTE where to_date(CVDPDS_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPQ6_FORDPASS_APPR_PT_TYPE_PII_HTE',count(*) from cvdp.NCVDPQ6_FORDPASS_APPR_PT_TYPE_PII_HTE where to_date(CVDPQ6_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDH_FORDPASS_APPR_REWARD_PII_HTE',count(*) from cvdp.NCVDPDH_FORDPASS_APPR_REWARD_PII_HTE where to_date(CVDPDH_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHU_FORDPASS_APPR_WIN_MV_SRVY_ANSW_PII_HTE',count(*) from cvdp.NCVDPHU_FORDPASS_APPR_WIN_MV_SRVY_ANSW_PII_HTE where to_date(CVDPHU_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHT_FORDPASS_APPR_WIN_MV_SRVY_ANSW_HDR_PII_HTE',count(*) from cvdp.NCVDPHT_FORDPASS_APPR_WIN_MV_SRVY_ANSW_HDR_PII_HTE where to_date(CVDPHT_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHS_FORDPASS_APPR_WIN_MV_SRVY_QSN_PII_HTE',count(*) from cvdp.NCVDPHS_FORDPASS_APPR_WIN_MV_SRVY_QSN_PII_HTE where to_date(CVDPHS_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPHR_FORDPASS_APPR_WIN_MV_SRVY_PII_HTE',count(*) from cvdp.NCVDPHR_FORDPASS_APPR_WIN_MV_SRVY_PII_HTE where to_date(CVDPHR_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDC_FORDPASS_APPR_TRN_PII_HTE',count(*) from cvdp.NCVDPDC_FORDPASS_APPR_TRN_PII_HTE where to_date(CVDPDC_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDL_FORDPASS_APPR_TRN_ATTR_PII_HTE',count(*) from cvdp.NCVDPDL_FORDPASS_APPR_TRN_ATTR_PII_HTE where to_date(CVDPDL_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDV_FORDPASS_APPR_TRN_BSKT_ITEM_EARN_PRD_PII_HTE',count(*) from cvdp.NCVDPDV_FORDPASS_APPR_TRN_BSKT_ITEM_EARN_PRD_PII_HTE where to_date(CVDPDV_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDG_FORDPASS_APPR_TRN_BSKT_ITEM_RDMPTN_PII_HTE',count(*) from cvdp.NCVDPDG_FORDPASS_APPR_TRN_BSKT_ITEM_RDMPTN_PII_HTE where to_date(CVDPDG_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDW_FORDPASS_APPR_TRN_CNVRT_PTS_PII_HTE',count(*) from cvdp.NCVDPDW_FORDPASS_APPR_TRN_CNVRT_PTS_PII_HTE where to_date(CVDPDW_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDY_FORDPASS_APPR_TRN_PTS_AGG_PII_HTE',count(*) from cvdp.NCVDPDY_FORDPASS_APPR_TRN_PTS_AGG_PII_HTE where to_date(CVDPDY_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDU_FORDPASS_APPR_TRN_PTS_BAL_PII_HTE',count(*) from cvdp.NCVDPDU_FORDPASS_APPR_TRN_PTS_BAL_PII_HTE where to_date(CVDPDU_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDX_FORDPASS_APPR_TRN_PROMO_PTS_PII_HTE',count(*) from cvdp.NCVDPDX_FORDPASS_APPR_TRN_PROMO_PTS_PII_HTE where to_date(CVDPDX_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
select 'NCVDPDF_FORDPASS_APPR_VIN_PII_HTE',count(*) from cvdp.NCVDPDF_FORDPASS_APPR_VIN_PII_HTE where to_date(CVDPDF_HDR_TS_UTC_S) = date_sub(CAST(current_timestamp() as DATE), ${1}));
">> comarch_counts_daily_exec.sh${date}.txt
echo -e "Hello\n\n\nTable Counts for the comarch files  is completed.\n\nPlease find the attached" |  mailx -s Table\ Counts\ Comarch  -a  comarch_counts_daily_exec.sh${date}.txt  latluri1@ford.com
