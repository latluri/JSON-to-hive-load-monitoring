


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





plink.exe -ssh %_STRING%@hpchdd2e.hpc.ford.com -pw Apple4 sh /s/%_STRING%/GCCT_DES_monitoring/DES_Customized_24_Compare.sh 
endlocal

REM pause>nul