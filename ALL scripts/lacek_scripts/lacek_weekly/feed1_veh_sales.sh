#!/bin/bash

kinit $USER@HPC.FORD.COM -k -t latluri1_keytab


#count b4
b4=`beeline --showHeader=false --outputformat=csv2 -e "select count(*) from lacek_fpr.pipe_del_veh_sales"`

######################### steps to create the table manually
#sqoop import \
#-Dhadoop.security.credential.provider.path=jceks://hdfs/user/latluri1/pass.jceks \
#-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
#--hive-import \
#--driver com.teradata.jdbc.TeraDriver \
#--connect jdbc:teradata://tera07.dearborn.ford.com/Database=CKSPRD_PII_VIEW \
#--username=latluri1 --password-alias userid_alias \
#--query "select current_date as File_Extract_Date, CON.consumer_id, CON.country_code, VSales.ACQUISITION_DATE, WA.B2C_ACCOUNT_USER_ID as GUID, VSales.VIN_ID, RANK() OVER (PARTITION BY CON.consumer_id order by VSales.VIN_ID) as VIN_Count, LEFT (VSales.VIN_ID, 10) as VIN_10_Char, VSales.VEHICLE_OWNERSHIP_CYCLE_NUM, VSales.NEW_OR_USED_IND, CON.consumer_type_code, CTYPE.consumer_type, CON.postal_cd_first_six_positions, CON.postal_cd_last_four_positions , CON.consumer_id || CON.country_code as cmkey, VDim.VEHICLE_MAKE, VDim.VEHICLE_MODEL, VDim.VEHICLE_MODEL_YEAR, VSales.SELLING_DEALER_KEY, VSales.secondary_selling_dealer_key, DlrDim.dealer_primary_PA_Acct_code from Cksprd_PII_view.CONSUMER_CURRENT Con inner join Cksprd_view.CONSUMER_TYPE_LOOKUP CType on Con.CONSUMER_TYPE_CODE = CType.CONSUMER_TYPE_CODE inner join Cksprd_view.VEHICLE_OWNERSHIP_CYCLE_FACT VSales on Con.CONSUMER_ID = VSales.CONSUMER_ID and Con.COUNTRY_CODE = VSales.COUNTRY_CODE inner join cksprd_view.VEHICLE_DIMENSION VDim on VSales.Vehicle_key = VDim.Vehicle_Key  left join CKSPRD_VIEW.OWNER_WEB_ACCT_LATEST_ACTIVITY  Owa on Owa.CONSUMER_ID = Con.CONSUMER_ID and Owa.COUNTRY_CODE = Con.COUNTRY_CODE left join CKSPRD_PII_View.web_account_table WA on WA.web_account_number = Owa.web_account_number left join cksprd_view.DEALER_DIMENSION DlrDim on VSales.selling_dealer_key = DlrDim.dealer_key where VSales.ACQUISITION_DATE >= (CURRENT_DATE- INTERVAL '9' DAY) and DlrDim.dealer_country_code = 'USA' and (Con.consumer_id, Con.country_code) in (select consumer_id, country_code from CKSPRD_VIEW.OWNER_WEB_ACCT_LATEST_ACTIVITY) AND \$CONDITIONS" \
#--hive-table lacek_fpr.pipe_del_veh_sales \
#--hive-overwrite \
#--as-textfile \
#--fields-terminated-by '|' \
#--split-by File_Extract_Date -m 10 \
#--delete-target-dir \
#--target-dir  hdfs://hdp2cluster/project/dz/collab/LACEK/lacek_fpr/temp2 \
###################################
# ALTER TABLE lacek_fpr.pipe_del_veh_sales SET TBLPROPERTIES('EXTERNAL'='TRUE');

#oozie workflow to automate updating data into the tables
sqoop import \
-Dhadoop.security.credential.provider.path=jceks://hdfs/user/latluri1/pass.jceks \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
-libjars ojdbc6.jar \
--connection-manager org.apache.sqoop.teradata.TeradataConnManager \
--connect jdbc:teradata://tera07.dearborn.ford.com/Database=CKSPRD_PII_VIEW \
--username=latluri1 --password-alias userid_alias \
--query "select current_date as File_Extract_Date, CON.consumer_id, CON.country_code, VSales.ACQUISITION_DATE, WA.B2C_ACCOUNT_USER_ID as GUID, VSales.VIN_ID, RANK() OVER (PARTITION BY CON.consumer_id order by VSales.VIN_ID) as VIN_Count, LEFT (VSales.VIN_ID, 10) as VIN_10_Char, VSales.VEHICLE_OWNERSHIP_CYCLE_NUM, VSales.NEW_OR_USED_IND, CON.consumer_type_code, CTYPE.consumer_type, CON.postal_cd_first_six_positions, CON.postal_cd_last_four_positions , CON.consumer_id || CON.country_code as cmkey, VDim.VEHICLE_MAKE, VDim.VEHICLE_MODEL, VDim.VEHICLE_MODEL_YEAR, VSales.SELLING_DEALER_KEY, VSales.secondary_selling_dealer_key, DlrDim.dealer_primary_PA_Acct_code from Cksprd_PII_view.CONSUMER_CURRENT Con inner join Cksprd_view.CONSUMER_TYPE_LOOKUP CType on Con.CONSUMER_TYPE_CODE = CType.CONSUMER_TYPE_CODE inner join Cksprd_view.VEHICLE_OWNERSHIP_CYCLE_FACT VSales on Con.CONSUMER_ID = VSales.CONSUMER_ID and Con.COUNTRY_CODE = VSales.COUNTRY_CODE inner join cksprd_view.VEHICLE_DIMENSION VDim on VSales.Vehicle_key = VDim.Vehicle_Key  left join CKSPRD_VIEW.OWNER_WEB_ACCT_LATEST_ACTIVITY  Owa on Owa.CONSUMER_ID = Con.CONSUMER_ID and Owa.COUNTRY_CODE = Con.COUNTRY_CODE left join CKSPRD_PII_View.web_account_table WA on WA.web_account_number = Owa.web_account_number left join cksprd_view.DEALER_DIMENSION DlrDim on VSales.selling_dealer_key = DlrDim.dealer_key where VSales.ACQUISITION_DATE >= (CURRENT_DATE- INTERVAL '9' DAY) and DlrDim.dealer_country_code = 'USA' and (Con.consumer_id, Con.country_code) in (select consumer_id, country_code from CKSPRD_VIEW.OWNER_WEB_ACCT_LATEST_ACTIVITY) AND \$CONDITIONS" \
--delete-target-dir \
--as-textfile \
--fields-terminated-by '|' \
--split-by File_Extract_Date -m 1 \
--target-dir  hdfs://hdp2cluster/project/dz/collab/LACEK/lacek_fpr/pipe_del_veh_sales \



#count after
after=`beeline --showHeader=false --outputformat=csv2 -e "select count(*) from lacek_fpr.pipe_del_veh_sales"`

echo -e "Counts b4 loading: ${b4} \nCounts after loading ${after}"

echo -e "Counts b4 loading: ${b4} \nCounts after loading ${after}"|mailx -s "Lacek_loading_report_feed1_veh_sales_lacek" latluri1@ford.com


