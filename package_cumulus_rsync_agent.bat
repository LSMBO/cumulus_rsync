@echo off

:: delete any previous distribution folder and recreate it
if exist "dist" rmdir /S /Q "dist"
mkdir "dist"

:: activate virtual environment
call .venv\Scripts\activate.bat

:: call pyinstaller
pyinstaller.exe cumulus_rsync_agent.spec

:: copy files
copy README.md "dist\Cumulus RSync Agent\README.md"
copy LICENSE.txt "dist\Cumulus RSync Agent\LICENSE.txt"
xcopy /K cumulus.pem "dist\Cumulus RSync Agent\"
copy cumulus_rsync.conf "dist\Cumulus RSync Agent\cumulus_rsync.conf"
:: copy service
xcopy /E service "dist\Cumulus RSync Agent\service\"
:: copy cwrsync
xcopy /E cwrsync_6.3.0_x64_free "dist\Cumulus RSync Agent\cwrsync_6.3.0_x64_free\"

:: remove permissions from the pem file
icacls "dist\Cumulus RSync Agent\cumulus.pem" /c /t /inheritance:r
Icacls "dist\Cumulus RSync Agent\cumulus.pem" /grant:r "%userName%":"(R)"
icacls "dist\Cumulus RSync Agent\cumulus.pem" /c /t /remove:g "Authenticated Users" Everyone Users

:: extract the version from the config file
for /f "tokens=2 delims==" %%a in ('findstr "version" cumulus_rsync.conf') do set version=%%a

:: compress the folder
py7zr c "dist\Cumulus RSync Agent%version%" "dist\Cumulus RSync Agent"

pause
