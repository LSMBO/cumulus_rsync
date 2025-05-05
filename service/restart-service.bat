@echo off

rem put the path of the current script in a variable and print it
set script_path=%~dp0

rem call nssm to stop the service
"%script_path%\nssm-2.24\win64\nssm.exe" stop "Cumulus RSync Agent"

rem call nssm to start the service
"%script_path%\nssm-2.24\win64\nssm.exe" start "Cumulus RSync Agent"

pause