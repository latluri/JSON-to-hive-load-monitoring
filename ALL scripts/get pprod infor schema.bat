pushd S:\GCCT_DES_monitoring\PPROD\DES_infor_schema

for /F "tokens=2 delims==." %%I in ('%SystemRoot%\System32\wbem\wmic.exe OS GET LocalDateTime /VALUE') do set "LocalDate=%%I"
set "LocalDate=%LocalDate:~0,8%"
echo %LocalDate%


del /F /Q PPROD_info_schema_%LocalDate%.csv
del /F /Q test0414.csv
Sqlcmd -Q "set nocount on; USE FordGCCTIntegrationDESPPROD;select * from Information_schema.COLUMNs" -S fordgcctintegrationstaging.database.windows.net  -d FordGCCTIntegrationDESPPROD -U latluri1 -P GCCT#4321 -o test0413.csv -s , -N -W -m1
findstr /v /C:"--"  test0413.csv >test0414.csv

rename test0414.csv PPROD_info_schema_%LocalDate%.csv
xcopy /Y /F  S:\GCCT_DES_monitoring\PPROD\DES_infor_schema\PPROD_info_schema_%LocalDate%.csv Y:\CurrentProjects\GCCT\"Data_source_DES"\PPROD\DES_infor_schema
del /F /Q test0413.csv 
REM del /F /Q test0413.csv

