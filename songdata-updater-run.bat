@echo off
set JAVA_CMD=.\jre\bin\java.exe
REM OPTION="rebuild"とすれば「楽曲全更新」と同じ挙動になる(お気に入りとタグを保持した状態でsong/folderテーブルを削除して再構築)
set OPTION=
%JAVA_CMD% -Xms4g -jar songdata-updater.jar %OPTION%
pause
