# Copyright or © or Copr. Alexandre BUREL for LSMBO / IPHC UMR7178 / CNRS (2025)
# 
# [a.burel@unistra.fr]
# 
# This software is the Rsync agent for Cumulus, a client-server to operate jobs on a Cloud.
# 
# This software is governed by the CeCILL license under French law and
# abiding by the rules of distribution of free software.  You can  use, 
# modify and/ or redistribute the software under the terms of the CeCILL
# license as circulated by CEA, CNRS and INRIA at the following URL
# "http://www.cecill.info". 
# 
# As a counterpart to the access to the source code and  rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty  and the software's author,  the holder of the
# economic rights,  and the successive licensors  have only  limited
# liability. 
# 
# In this respect, the user's attention is drawn to the risks associated
# with loading,  using,  modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean  that it is complicated to manipulate,  and  that  also
# therefore means  that it is reserved for developers  and  experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systems and/or 
# data to be ensured and,  more generally, to use and operate it in the 
# same conditions as regards security. 
# 
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL license and that you accept its terms.

import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import requests
import time

logger = logging.getLogger(__name__)

# default config
LOCAL_HOST = "0.0.0.0" # hostname or IP address on which to listen
LOCAL_PORT = 8800 # port on which to listen
STORAGE_HOST = "localhost" # the host where the cumulus server is
STORAGE_PATH = "/storage" # the remote path where data will be sent
STORAGE_PORT = 8800 # do not use a port already used on the controller (in this case by flask)
STORAGE_USER = "me" # the remote user name
STORAGE_KEY = os.path.abspath("cumulus.pem") # the public key to connect to the server, it has to be an absolute path
REFRESH_RATE = 15 # the number of seconds before the daemon wakes up again and checks if there is something in the queue
FINAL_FILE = ".cumulus.rsync" # a blank file to transfer at the end of each job, to tell the controller that all the files have been transferred
PROGRESS_FILE = ".cumulus.progress"
QUEUE_FILE = "cumulus_rsync_queue.db"
VERSION = ""
RSYNC_BIN_PATH = "" # the path to the rsync binary
# prepare the logs
LOGS_DIR = "logs"
if not os.path.isdir(LOGS_DIR): os.mkdir(LOGS_DIR)

### GENERIC FUNCTIONS ###

def reset_configuration():
    global LOCAL_HOST, LOCAL_PORT, STORAGE_HOST, STORAGE_PATH, STORAGE_PORT, STORAGE_USER, STORAGE_KEY, REFRESH_RATE, FINAL_FILE, PROGRESS_FILE, QUEUE_FILE, RSYNC_BIN_PATH, VERSION
    LOCAL_HOST = "0.0.0.0" # hostname or IP address on which to listen
    LOCAL_PORT = 8800 # port on which to listen
    STORAGE_HOST = "localhost" # the host where the cumulus server is
    STORAGE_PATH = "/storage" # the remote path where data will be sent
    STORAGE_PORT = 8800 # do not use a port already used on the controller (in this case by flask)
    STORAGE_USER = "me" # the remote user name
    STORAGE_KEY = os.path.abspath("cumulus.pem") # the public key to connect to the server, it has to be an absolute path
    REFRESH_RATE = 15 # the number of seconds before the daemon wakes up again and checks if there is something in the queue
    FINAL_FILE = ".cumulus.rsync" # a blank file to transfer at the end of each job, to tell the controller that all the files have been transferred
    PROGRESS_FILE = ".cumulus.progress"
    QUEUE_FILE = "cumulus_rsync_queue.db"
    VERSION = ""
    RSYNC_BIN_PATH = "" # the path to the rsync binary

def initialize(config_file):
    global LOCAL_HOST, LOCAL_PORT, STORAGE_HOST, STORAGE_PATH, STORAGE_PORT, STORAGE_USER, STORAGE_KEY, REFRESH_RATE, FINAL_FILE, PROGRESS_FILE, QUEUE_FILE, RSYNC_BIN_PATH, VERSION
    # check that the config file exists
    if not os.path.isfile(config_file): raise FileNotFoundError(f"Configuration file '{config_file}' not found")
    # configure the logs
    log_format = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    log_date = "%Y/%m/%d %H:%M:%S"
    if os.getenv("CUMULUS_DEBUG"): logging.basicConfig(level = logging.DEBUG, format = log_format, datefmt = log_date)
    else:
        logging.basicConfig(
            handlers = [RotatingFileHandler(filename = f"{LOGS_DIR}/cumulus-rsync.log", maxBytes = 10000000, backupCount = 10)],
            level = logging.INFO,
            format = log_format,
            datefmt = log_date
        )
    # read the config file
    f = open(config_file, "r")
    for line in f.read().splitlines():
        # skip if the line does not look like "key = value"
        if not re.match(r"^\s*[^=]+\s*=\s*.+\s*$", line): continue
        # split the line and remove the spaces
        [key, value] = list(map(lambda item: item.strip(), line.split("=")))
        # store the values
        if key == "local.path": LOCAL_HOST = value
        elif key == "local.port": LOCAL_PORT = value
        elif key == "storage.path": STORAGE_PATH = value
        elif key == "storage.host": STORAGE_HOST = value
        elif key == "storage.port": STORAGE_PORT = value
        elif key == "storage.user": STORAGE_USER = value
        elif key == "storage.public_key": STORAGE_KEY = os.path.abspath(value)
        elif key == "refresh.rate": REFRESH_RATE = int(value)
        elif key == "final.file": FINAL_FILE = value
        elif key == "progress.file": PROGRESS_FILE = os.path.abspath(value)
        elif key == "queue.file": QUEUE_FILE = os.path.abspath(value)
        elif key == "rsync.bin.path": RSYNC_BIN_PATH = os.path.abspath(value)
        elif key == "version": VERSION = value
    f.close()
    # test that files are actually found
    if not os.path.isfile(STORAGE_KEY): raise FileNotFoundError(f"Public key '{STORAGE_KEY}' not found")
    if not os.path.isdir(RSYNC_BIN_PATH): raise FileNotFoundError(f"RSync binary '{RSYNC_BIN_PATH}' not found")
    if not os.path.isfile(FINAL_FILE): raise FileNotFoundError(f"Public key '{FINAL_FILE}' not found")
    # add RSync to path
    os.environ["PATH"] = RSYNC_BIN_PATH + os.pathsep + os.environ["PATH"]

def get_size(file):
	if os.path.isfile(file):
		return os.path.getsize(file)
	else:
		total_size = 0
		for dirpath, _, filenames in os.walk(file):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				# skip if it is symbolic link
				if not os.path.islink(fp): total_size += os.path.getsize(fp)
		return total_size

def get_storage_info():
    return f"{STORAGE_USER}@{STORAGE_HOST}:{STORAGE_PATH}"

def wait(seconds = REFRESH_RATE):
    time.sleep(seconds)

### FUNCTIONS FOR REMOTE SERVER ###

def get_rsync_command(file, job_dir):
    # Rsync options:
    # -r: recurse into directories
    # --ignore-existing: skip updating files that exist on receiver
    # --exclude: do not send wal files, they shouldn't even be here
    # --progress: monitor progression of the transfer on stdout
    # -e: specify the remote shell to use
    #   -l: login
    #   -i: the path to the public key
    #   -o 'StrictHostKeyChecking no': do not ask if the key has to be trusted
    # --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r: make sure that directories have permission 755 and files 644
    # TODO send the file even if it exists on receiver but with a different size
    options = f"-r --ignore-existing --exclude='*-wal' --progress -e 'ssh -l {STORAGE_USER} -i \"{STORAGE_KEY}\" -o \"StrictHostKeyChecking no\"' --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r"
    # determine the remote folder (either main storage, or job folder)
    remote_path = f"{STORAGE_HOST}:{STORAGE_PATH}/jobs/{job_dir}" if job_dir != "" else f"{STORAGE_HOST}:{STORAGE_PATH}/data"
    # log the action
    if os.path.isdir(file): logger.debug(f"Sending directory '{os.path.basename(file)}' to '{remote_path}'")
    else: logger.debug(f"Sending file '{os.path.basename(file)}' to '{remote_path}'")
    # cwrsync requires drives to be prepended (Windows only)
    if os.name == 'nt': file = re.sub(r"^([a-zA-Z]):", r"/cygdrive/\1", file.replace("\\", "/"))
    # return the command
    return f"rsync {options} \"{file}\" \"{remote_path}\" > {PROGRESS_FILE}"

STORAGE_FREE_LIMIT = 10737418240 # below this amount of free space, there will be no upload (10GB by default)
STORAGE_FREE_LIMIT_HR = STORAGE_FREE_LIMIT // 2**30
STORAGE_FREE_LIMIT_SLEEP = 900 # wait 15 minutes between each check
STORAGE_USAGE_LAST_CALL = 0 # timestamp in seconds of the last call to diskusage
STORAGE_USAGE_WAITING_TIME = 60 # 1 minute between two calls to diskusage, should be small enough to avoid that STORAGE_FREE_LIMIT is reached

def get_server_free_space():
    response = requests.get(f"http://{STORAGE_HOST}:{STORAGE_PORT}/diskusage")
    return response.json()[2]

def is_enough_free_space_on_server(fake_free_space_fot_test = None):
    global STORAGE_USAGE_LAST_CALL
    # if the last check was less than a minute ago, say it's ok (the time of last check is only recorded when it's successful)
    current_timestamp = time.time()
    if STORAGE_USAGE_LAST_CALL != 0 and current_timestamp - STORAGE_USAGE_LAST_CALL < STORAGE_USAGE_WAITING_TIME:
        return True
    else:
         # call the server
        free_space = get_server_free_space() if fake_free_space_fot_test is None else fake_free_space_fot_test
        # if the server has enough space, store the current time and return True
        if free_space > STORAGE_FREE_LIMIT:
            STORAGE_USAGE_LAST_CALL = current_timestamp
            return True
        else: return False

# def check_server_disk_usage(test_free_space = None):
#     global STORAGE_USAGE_LAST_CALL
#     # do not check the server disk usage if it has been checked less than a minute ago
#     # it can happen if files are already on the server
#     current_timestamp = time.time()
#     if STORAGE_USAGE_LAST_CALL == 0 or current_timestamp - STORAGE_USAGE_LAST_CALL > STORAGE_USAGE_WAITING_TIME:
#         STORAGE_USAGE_LAST_CALL = current_timestamp
#         # call the server
#         free_space = get_server_free_space() if test_free_space is None else test_free_space
#         # if the server is almost full, pause the uploads
#         if free_space < STORAGE_FREE_LIMIT:
#             # TODO this has not been tested
#             while free_space < STORAGE_FREE_LIMIT:
#                 logger.warning(f"Storage free space is below {STORAGE_FREE_LIMIT // 2**30}GB, uploads are paused for now...")
#                 # wait 15 minutes before checking again
#                 wait(STORAGE_FREE_LIMIT_SLEEP)
#                 free_space = get_server_free_space() if test_free_space is None else test_free_space
#                 # at this point, the server can accept uploads
#                 logger.info("Storage free space is above the limit, uploads can resume now")


### PROGRESS FILE MANAGEMENT ###

def delete_progress_file():
    if os.path.exists(PROGRESS_FILE): os.remove(PROGRESS_FILE)

def read_progress_file():
	current_file = ""
	current_size = 0
	total_size = 0
    # open the file
	if os.path.exists(PROGRESS_FILE): 
		with open(PROGRESS_FILE) as file:
            # read line by line
			for line in file:
				line = line.rstrip()
				if line != "":
					if line.startswith(" "):
                        # on lines indicating the progress, store the size that is given
						current_size = int(line.split()[0].replace(".", ""))
					else:
                        # on lines indicating which file is being transferred (can be several when transferring a folder)
                        # add the last size that was recorded (so we do not add up the size at 25% and 50% for the same file)
						total_size += current_size # this size should correspond to the size of the previous file
						current_size = 0
						current_file = line # store the name of the file currently transferred
        # return the basename of the file (or name of the folder) and the size corresponding to the complete amount of what has been transferred
		return [current_file.split("/")[0], total_size + current_size]
	else:
		return ["", 0]

# def get_progress_for_job(job_id, owner):
#     # read the progress file
#     [current_file, current_amount] = read_progress_file()
#     # prepare a dict for the results
#     progress_dict = {}
#     # look for the files in the queue
#     for file in SEND_QUEUE:
#         id, username, filepath, _, _, size = file
#         if job_id == int(id) and owner == username:
#             filename = os.path.basename(filepath)
#             if size > 0 and os.path.basename(filename) == current_file:
#                 progress_dict[filename] = int(current_amount * 100 / size)
#                 logger.info(f"Job {job_id}: File '{filename}' is being uploaded, current progress is {progress_dict[filename]}%")
#             else:
#                 progress_dict[filename] = 0
#     return progress_dict

def get_progress_for_job(job_id, files):
    # read the progress file
    [current_file, current_amount] = read_progress_file()
    # prepare a dict for the results
    progress_dict = {}
    # look for the files in the list
    for file in files:
        filepath, size = file
        filename = os.path.basename(filepath)
        if size > 0 and os.path.basename(filename) == current_file:
            progress_dict[filename] = int(current_amount * 100 / size)
            logger.info(f"Job {job_id}: File '{filename}' is being uploaded, current progress is {progress_dict[filename]}%")
        else:
            progress_dict[filename] = 0
    return progress_dict
      

### QUEUE MANAGEMENT ###

# # each time the user wants to send files, the files are put in a queue and a job id is returned; the queue and the id are not stored and will be reseted when the daemon is stopped
# SEND_QUEUE = list()
# # we use another queue to store the ids of the jobs canceled, so we do not have to worry about synchronizing the main queue between threads
# CANCEL_QUEUE = list()
# # global variables to store the progress of the file currently uploaded
# CURRENT_JOB = ""
# CURRENT_FILE = ""

# def is_send_queue_empty():
#     return len(SEND_QUEUE) == 0

# def get_first_job_in_queue():
#     job_id, _, file, _, job_dir, _ = SEND_QUEUE[0]
#     # make sure that a folder does not end with a slash
#     if os.path.isdir(file) and (file.endswith("/") or file.endswith("\\")): file = file[0:-1]
#     # return the job id, the file to send and the job directory
#     return job_id, file, job_dir

# def is_job_cancelled(job_id):
#     return job_id in CANCEL_QUEUE

# def remove_first_job_in_queue():
#     SEND_QUEUE.pop(0)

# def clean_cancel_queue(job_id):
#     for id in CANCEL_QUEUE:
#         if id < job_id: CANCEL_QUEUE.remove(id)

def extract_from_settings(settings):
    job_id = settings["job_id"]
    job_dir = settings["job_dir"]
    owner = settings["owner"]
    shared_files = json.loads(settings["files"]) # raw files
    local_files = json.loads(settings["local_files"]) # fasta files
    return job_id, job_dir, owner, shared_files, local_files

# def add_to_queue(job_id, job_dir, owner, shared_files, local_files):
#     nb = len(shared_files) + len(local_files)
#     for file in local_files:
#         if os.path.isfile(file) or os.path.isdir(file):
#             logger.debug(f"Add '{file}' to the queue, it will be sent to {job_dir}")
#             SEND_QUEUE.append([job_id, owner, file, nb, job_dir, get_size(file)])
#     for file in shared_files:
#         if os.path.isfile(file) or os.path.isdir(file):
#             logger.debug(f"Add '{file}' to the queue, it will be shared for all jobs")
#             SEND_QUEUE.append([job_id, owner, file, nb, "", get_size(file)])
#     # send a blank file to the job folder to warn the controller that all the transfers are done for this job
#     SEND_QUEUE.append([job_id, owner, FINAL_FILE, nb, job_dir, get_size(FINAL_FILE)])
#     logger.info(f"Job {job_id}: {nb} files have been added to the queue")
#     return nb

# def list_shared_files_in_queue():
#     # this function is called to check which files are stored on the server
#     files = []
#     for job_id, _, file, _, job_dir, _ in SEND_QUEUE:
#         # do not list the fasta files or the files that have been cancelled
#         if job_dir == "" and not job_id in CANCEL_QUEUE:
#             files.append(os.path.basename(file))
#     # return the list of files, without duplicates
#     return list(dict.fromkeys(files))

# def get_number_of_cancelled_file_transfers(job_id):
#     return len(list(filter(lambda job: list(job)[0] == job_id, SEND_QUEUE)))

# def cancel_job(job_id):
#     CANCEL_QUEUE.append(job_id)
#     return get_number_of_cancelled_file_transfers(job_id)
