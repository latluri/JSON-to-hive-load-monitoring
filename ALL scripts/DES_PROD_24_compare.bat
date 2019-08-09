


@echo off
cls
setlocal enabledelayedexpansion

set "_STRING=%USERNAME%"
REM ***** Modify as necessary for the string source. *****

set "_UCASE=ABCDEFGHIJKLMNOPQRSTUVWXYZ"
set "_LCASE=abcdefghijklmnopqrstuvwxyz"

for /l %%a in (0,1,25) do (
   call set "_FROM=%%_UCASE:~%%a,1%%
   call set "_TO=%%_LCASE:~%%a,1%%
   call set "_STRING=%%_STRING:!_FROM!=!_TO!%%
)

REM set _STRING





plink.exe -ssh %_STRING%@hpchdp2e.hpc.ford.com -pw Apple4 sh /s/%_STRING%/GCCT_DES_monitoring/DES_PROD_24_Compare.sh 
endlocal

for /F "tokens=2 delims==." %%I in ('%SystemRoot%\System32\wbem\wmic.exe OS GET LocalDateTime /VALUE') do set "LocalDate=%%I"
set "LocalDate=%LocalDate:~0,8%"
echo %LocalDate%

xcopy /Y /F  S:\GCCT_DES_monitoring\PROD\DES_schema changes\DESPROD_schema_changesin24hrs%LocalDate%.xlsx Y:\CurrentProjects\GCCT\"Data_source_DES"\PROD\DES_schema changes
REM pause>nul