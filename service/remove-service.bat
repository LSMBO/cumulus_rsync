@echo off

rem put the path of the current script in a variable and print it
set script_path=%~dp0

rem call nssm to remove the service
"%script_path%\nssm-2.24\win64\nssm.exe" remove "Cumulus RSync Agent" confirm

pause