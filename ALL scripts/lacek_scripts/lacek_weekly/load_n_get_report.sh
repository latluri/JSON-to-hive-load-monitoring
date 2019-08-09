#!/bin/sh

kinit $USER@HPC.FORD.COM -k -t latluri1_keytab

for script in feed1_esp.sh feed1_veh_sales.sh feed3_safe_email.sh feed4_comarch_raw.sh feed5_comarch_T.sh feed6_fpr_interactions.sh;
do
	sh ${script}
	status=$?
	if [ $status != 0 ];
		then
		    echo "Failed at execute ${script} in load_n_get_report.sh "|mailx -s "Lacek_load_n_get_report.sh failure at ${script}" latluri1@ford.com
    		    exit 1
	fi
done


beeline  --showHeader=false --outputformat=tsv2 -e "use lacek_fpr; show tables"  > tables_list.csv

tables=`cat tables_list.csv`


code=""
#for tab in pipe_del_email_safe_cks_current pipe_del_esp_table pipe_del_gdf0r10_fordpass_clickstr_app pipe_del_ncvdcdr_fordpass_appr_loc_sec_hte pipe_del_ncvdg01_fpas_uniq_cust_pii pipe_del_ncvdg02_fpas_uniq_trn_pii pipe_del_ncvdg03_fpas_cust_dlr_relshp_pii pipe_del_ncvdg04_fpas_trn_with_pts_pii pipe_del_ncvdg05_fpas_trn_with_vals_pii pipe_del_ncvdg08_fpas_runng_engmt_flag_pii pipe_del_ncvdg09_fpas_curr_pt_bal_pii pipe_del_ncvdg10_fpas_mthly_pt_bal_trend_pii pipe_del_ncvdpda_fordpass_appr_acct_pii_hte pipe_del_ncvdpdc_fordpass_appr_trn_pii_hte pipe_del_ncvdpdf_fordpass_appr_vin_pii_hte pipe_del_ncvdpdg_fordpass_appr_trn_bskt_item_rdmptn_pii_hte pipe_del_ncvdpdh_fordpass_appr_reward_pii_hte pipe_del_ncvdpdl_fordpass_appr_trn_attr_pii_hte pipe_del_ncvdpdm_fordpass_appr_cstm_attr_dyn_pii_hte pipe_del_ncvdpds_fordpass_appr_pts_bal_pii_hte pipe_del_ncvdpdu_fordpass_appr_trn_pts_bal_pii_hte pipe_del_ncvdpdv_fordpass_appr_trn_bskt_item_earn_prd_pii_hte pipe_del_ncvdpdw_fordpass_appr_trn_cnvrt_pts_pii_hte pipe_del_ncvdpdx_fordpass_appr_trn_promo_pts_pii_hte pipe_del_ncvdpdy_fordpass_appr_trn_pts_agg_pii_hte pipe_del_ncvdphl_fordpass_appr_win_mv_actvy_pii_hte pipe_del_ncvdphm_fordpass_appr_win_mv_cust_actvy_attr_val_pii_hte pipe_del_ncvdphn_fordpass_appr_win_mv_ltry_pii_hte pipe_del_ncvdphp_fordpass_appr_win_mv_ltry_prtcpt_pii_hte pipe_del_ncvdphq_fordpass_appr_win_mv_ltry_winr_pii_hte pipe_del_ncvdphr_fordpass_appr_win_mv_srvy_pii_hte pipe_del_ncvdphs_fordpass_appr_win_mv_srvy_qsn_pii_hte pipe_del_ncvdpht_fordpass_appr_win_mv_srvy_answ_hdr_pii_hte pipe_del_ncvdphu_fordpass_appr_win_mv_srvy_answ_pii_hte pipe_del_ncvdpq6_fordpass_appr_pt_type_pii_hte pipe_del_ncvdpq7_fordpass_appr_prtnr_pii_hte pipe_del_ncvdtdb_fordpass_appr_dict_item_hte pipe_del_ncvdtdp_fordpass_appr_cstm_attr_def_hte pipe_del_veh_sales ;

for tab in ${tables};
     do
	#code="${code} select \"Q${tab},\"; select count(*) from ${tab};" 
	 code="${code} select \"${tab}\",count(*) from ${tab};" 
     done
#echo $code

#echo "aQtable,column,data_type,comments">${1}_table_description.csv
beeline  --showHeader=false --outputformat=csv2 --force -e " use lacek_fpr;${code}" >table_row_counts.csv

for tab in $tables;
     do
        val=`hdfs dfs -cat  /project/dz/collab/LACEK/lacek_fpr/${tab}/* |wc -l`
        echo ${tab},${val}>>table_row_counts2.csv
     done


echo "table,hive_count,hdfs_count">summary_row_count.csv
join -1 1 -2 1 -t ',' table_row_counts.csv table_row_counts2.csv>>summary_row_count.csv

row_check=`awk -F',' '$2!=$3 {print $2,"!=",$3}' summary_row_count.csv |wc -l`
if [ $row_check != 0 ];
                then
                echo "Lacek tables are updated, however, hive row count and hdfs row count do not match. Please investigate"| mailx -s "Lacek tables loaded, further investigation needed" -a summary_row_count.csv latluri1@ford.com
fi



for tab in $tables;
     do
         beeline  --showHeader=false --outputformat=csv2 --force -e " use lacek_fpr; desc ${tab}" >table_schema_${tab}.csv
	 sed -i s/^/"${tab},"/g table_schema_${tab}.csv
     done


cat table_schema_* >schema_file.csv


hdfs dfs -put -f table_row_counts.csv /project/dz/collab/LACEK/lacek_fpr/transfer_summary/
hdfs dfs -put -f schema_file.csv /project/dz/collab/LACEK/lacek_fpr/transfer_summary/

date_=`date "+%Y-%m-%d"`
list=`hadoop fs -ls /project/dz/collab/LACEK/lacek_fpr/|awk -F' ' '{print $8}'|grep -v 'backup'`

hadoop distcp ${list}  /project/dz/collab/LACEK/lacek_fpr/backup/backup_${date_}

