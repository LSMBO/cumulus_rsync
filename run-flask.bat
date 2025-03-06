rem call cumulus_rsync\.venv\Scripts\activate.bat
set CUMULUS_DEBUG=true
rem flask --app cumulus_rsync run --host=0.0.0.0 --port 8800
python cumulus_rsync\cumulus_rsync_main.py
