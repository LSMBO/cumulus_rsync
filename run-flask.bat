call .venv\Scripts\activate.bat
flask --app cumulus-rsync-daemon run --host=0.0.0.0 --port 8800
