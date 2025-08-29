@echo off
set JAVA_CMD=.\jre\bin\java.exe
REM OPTION="rebuild" will delete and rebuild the song/folder table, keeping favorites and tags.
set OPTION=
%JAVA_CMD% -Xms4g -jar songdata-updater.jar %OPTION%
pause
