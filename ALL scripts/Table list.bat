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



cd C:\Program Files\PuTTY\

SET /P _db_name= Please enter the database:

plink.exe -ssh %_STRING%@hpchdp2e.hpc.ford.com sh /s/%_STRING%/Table_list.sh %_db_name%
endlocal
REM pause>nul