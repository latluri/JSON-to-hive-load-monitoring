#!/bin/bash

kinit $USER@HPC.FORD.COM -k -t latluri1_keytab

#count b4
b4=`beeline --showHeader=false --outputformat=csv2 -e "select count(*) from lacek_fpr.pipe_del_esp_table"`


sqoop import \
-Dhadoop.security.credential.provider.path=jceks://hdfs/user/latluri1/pass.jceks \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--hive-import \
--driver com.teradata.jdbc.TeraDriver \
--connect jdbc:teradata://tera07.dearborn.ford.com/Database=CKSPRD_PII_VIEW \
--username=latluri1 --password-alias userid_alias \
--query "select current_date as File_Extract_Date, CASE WHEN (ConEsp.cks_a_update_date >= Plan.cks_a_update_date) and (Plan.cks_a_update_date >= VSales.cks_a_update_date) THEN ConEsp.cks_a_update_date WHEN (ConEsp.cks_a_update_date >= VSales.cks_a_update_date) and (VSales.cks_a_update_date >= Plan.cks_a_update_date) THEN ConEsp.cks_a_update_date WHEN (Plan.cks_a_update_date >= ConEsp.cks_a_update_date) and (ConEsp.cks_a_update_date >= VSales.cks_a_update_date) THEN Plan.cks_a_update_date WHEN (Plan.cks_a_update_date >= VSales.cks_a_update_date) and (VSales.cks_a_update_date >= ConEsp.cks_a_update_date) THEN Plan.cks_a_update_date WHEN (VSales.cks_a_update_date >= ConEsp.cks_a_update_date) and (ConEsp.cks_a_update_date >= Plan.cks_a_update_date) THEN VSales.cks_a_update_date WHEN (VSales.cks_a_update_date >= Plan.cks_a_update_date) and (Plan.cks_a_update_date >= ConEsp.cks_a_update_date) THEN VSales.cks_a_update_date   END as Source_Last_Update_Date, ConEsp.consumer_id, ConEsp.country_code, ConEsp.VIN_ID, LEFT (ConEsp.VIN_ID, 10) as VIN_10_Char,  ConEsp.vehicle_key, ConEsp.vehicle_ownership_cycle_num, ConEsp.contract_number, ConEsp.esp_effective_date, ConEsp.esp_end_date, Plan.esp_expire_date, Plan.ESP_CONTRACT_CANCEL_DATE, VSales.ACQUISITION_DATE,  Plan.ESP_PLAN_TYPE, Plan.ESP_GROUP_PLAN_CODE, Plk.ESP_PLAN_TYPE_DESC,  Plan.ESP_EXPIRE_MILEAGE, /* Plan.esp_coverage_months, */ Plan.esp_coverage_mileage, Plan.ESP_CONTRACT_MILEAGE_COVERED   from Cksprd_view.ESP_CONSUMER_DATA ConEsp  inner join Cksprd_view.VEHICLE_OWNERSHIP_CYCLE_FACT VSales  on ConEsp.VIN_ID = VSales.VIN_ID  and ConEsp.vehicle_ownership_cycle_num = VSales.vehicle_ownership_cycle_num  and ConEsp.consumer_id = VSales.consumer_id  and ConEsp.country_code = VSales.country_code  and VSales.ACQUISITION_DATE >= (CURRENT_DATE- INTERVAL '9' DAY)   inner join Cksprd_view.ESP_PLAN Plan  on ConEsp.VIN_ID = Plan.VIN_ID  and ConEsp.contract_number = Plan.contract_number   inner join Cksprd_view.ESP_PLAN_TYPE_LOOKUP Plk on Plan.ESP_PLAN_TYPE = Plk.ESP_PLAN_TYPE  where (ConEsp.consumer_id, ConEsp.country_code) IN  (select consumer_id, country_code    from CKSPRD_VIEW.OWNER_WEB_ACCT_LATEST_ACTIVITY)  AND \$CONDITIONS" \
--hive-table lacek_fpr.pipe_del_esp_table \
--hive-overwrite \
--as-textfile \
--fields-terminated-by '|' \
--split-by File_Extract_Date -m 10 \
--target-dir  hdfs://hdp2cluster/project/dz/collab/LACEK/lacek_fpr/temp3 \
--delete-target-dir 


#count after
after=`beeline --showHeader=false --outputformat=csv2 -e "select count(*) from lacek_fpr.pipe_del_esp_table"`

echo -e "Counts b4 loading: ${b4} \nCounts after loading ${after}"

echo -e "Counts b4 loading: $b4 \nCounts after loading $after"|mailx -s "Lacek_loading_report_feed1_esp_lacek" latluri1@ford.com


