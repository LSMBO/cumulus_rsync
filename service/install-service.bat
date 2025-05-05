@echo off

rem put the path of the current script in a variable and print it
set script_path=%~dp0

rem call nssm to install the service
"%script_path%\nssm-2.24\win64\nssm.exe" install "Cumulus RSync Agent" "%script_path%\..\Cumulus RSync Agent.exe"

pause