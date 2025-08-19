@echo off
set JAVA_CMD=.\jre\bin\java.exe
%JAVA_CMD% -Xms4g -jar songdata-updater.jar
pause
