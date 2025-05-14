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
CONFIG = {}
# storage free space limit
STORAGE_FREE_LIMIT = 10737418240 # below this amount of free space, there will be no upload (10GB by default)
STORAGE_FREE_LIMIT_HR = STORAGE_FREE_LIMIT // 2**30
STORAGE_FREE_LIMIT_SLEEP = 900 # wait 15 minutes between each check
STORAGE_USAGE_LAST_CALL = 0 # timestamp in seconds of the last call to diskusage
STORAGE_USAGE_WAITING_TIME = 60 # 1 minute between two calls to diskusage, should be small enough to avoid that STORAGE_FREE_LIMIT is reached
# prepare the logs
LOGS_DIR = "logs"
if not os.path.isdir(LOGS_DIR): os.mkdir(LOGS_DIR)

### CONFIG FUNCTIONS ###

def get_config_value(key):
    if key in CONFIG: return CONFIG[key]
    else: 
        logger.error(f"Key '{key}' not found in the configuration file")
        return None
def get_local_host(): return get_config_value("local.host")
def get_local_port(): return get_config_value("local.port")
def get_storage_host(): return get_config_value("storage.host")
def get_storage_path(): return get_config_value("storage.path")
def get_storage_port(): return get_config_value("storage.port")
def get_storage_user(): return get_config_value("storage.user")
def get_storage_key(): return get_config_value("storage.public_key")
def get_refresh_rate(): return get_config_value("refresh.rate")
def get_final_file(): return get_config_value("final.file")
def get_progress_file(): return get_config_value("progress.file")
def get_queue_file(): return get_config_value("queue.file")
def get_rsync_path(): return get_config_value("rsync.bin.path")
def get_ssh_path(): return get_config_value("ssh.bin.path")
def get_version(): return get_config_value("version")
def is_survey_activated(): return get_config_value("survey.enabled")
def get_survey_depth(): return get_config_value("survey.depth")
def get_survey_time(): return get_config_value("survey.time")
def get_surveyed_directories(): return get_config_value("surveyed_directories")

def reset_configuration():
    global CONFIG
    CONFIG = {}
    CONFIG["local.host"] = "0.0.0.0" # hostname or IP address on which to listen
    CONFIG["local.port"] = 8800 # port on which to listen
    CONFIG["storage.host"] = "localhost" # the host where the cumulus server is
    CONFIG["storage.path"] = "/storage" # the remote path where data will be sent
    CONFIG["storage.port"] = 8800 # do not use a port already used on the controller (in this case by flask)
    CONFIG["storage.user"] = "me" # the remote user name
    CONFIG["storage.public_key"] = os.path.abspath("cumulus.pem") # the public key to connect to the server, it has to be an absolute path
    CONFIG["refresh.rate"] = 15 # the number of seconds before the daemon wakes up again and checks if there is something in the queue
    CONFIG["final.file"] = ".cumulus.rsync" # a blank file to transfer at the end of each job, to tell the controller that all the files have been transferred
    CONFIG["progress.file"] = ".cumulus.progress"
    CONFIG["queue.file"] = "cumulus_rsync_queue.db"
    CONFIG["version"] = ""
    CONFIG["rsync.bin.path"] = "" # the path to the rsync binary
    CONFIG["ssh.bin.path"] = "" # the path to the ssh binary
    CONFIG["survey.enabled"] = False
    CONFIG["survey.depth"] = 1
    CONFIG["survey.time"] = "23:00"
    CONFIG["surveyed_directories"] = {}

def read_config_file(config_file):
    global CONFIG
    # check that the config file exists
    if not os.path.isfile(config_file): raise FileNotFoundError(f"Configuration file '{config_file}' not found")
    # prepare the map
    CONFIG = {}
    CONFIG["surveyed_directories"] = {}
    directories = {}
    # read the config file
    f = open(config_file, "r")
    for line in f.read().splitlines():
        # skip if the line does not look like "key = value"
        if not re.match(r"^\s*[^=]+\s*=\s*.+\s*$", line): continue
        # split the line and remove the spaces
        [key, value] = list(map(lambda item: item.strip(), line.split("=")))
        # store the values
        if key == "local.host": CONFIG[key] = value
        elif key == "local.port": CONFIG[key] = value
        elif key == "storage.path": CONFIG[key] = value
        elif key == "storage.host": CONFIG[key] = value
        elif key == "storage.port": CONFIG[key] = value
        elif key == "storage.user": CONFIG[key] = value
        elif key == "storage.public_key": CONFIG[key] = os.path.abspath(value)
        elif key == "refresh.rate": CONFIG[key] = int(value)
        elif key == "final.file": CONFIG[key] = value
        elif key == "progress.file": CONFIG[key] = os.path.abspath(value)
        elif key == "queue.file": CONFIG[key] = os.path.abspath(value)
        elif key == "rsync.bin.path": CONFIG[key] = os.path.abspath(value)
        elif key == "ssh.bin.path": CONFIG[key] = os.path.abspath(value)
        elif key == "version": CONFIG[key] = value
        # keys for survey
        elif key == "survey.enabled": CONFIG[key] = value.lower() == "true" or value.lower() == "on"
        elif key == "survey.depth" and str(value).isnumeric: CONFIG[key] = int(value)
        elif key == "survey.time" and re.match(r"^\d\d:\d\d$", value): CONFIG[key] = value
        elif match := re.search("survey\\.(.*)\\.dir", key, re.IGNORECASE): directories[match.group(1)] = {"dir": value}
        elif match := re.search("survey\\.(.*)\\.regex", key, re.IGNORECASE): directories[match.group(1)]["regex"] = value
        elif match := re.search("survey\\.(.*)\\.isfile", key, re.IGNORECASE): directories[match.group(1)]["isfile"] = value.lower() == "true" or value.lower() == "on"
    CONFIG["surveyed_directories"] = directories
    f.close()

def initialize(config_file):
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
    read_config_file(config_file)
    # test that files are actually found
    if not os.path.isfile(get_storage_key()): raise FileNotFoundError(f"Public key '{get_storage_key()}' not found")
    # create an empty file if the final file does not exist
    if not os.path.isfile(get_final_file()):
        CONFIG["final.file"] = ".cumulus.rsync"
        f = open(get_final_file(), "w")
        f.close()
    # add RSync and SSH to path
    if os.path.isdir(get_rsync_path()): os.environ["PATH"] = get_rsync_path() + os.pathsep + os.environ["PATH"]
    if os.path.isdir(get_ssh_path()): os.environ["PATH"] = get_ssh_path() + os.pathsep + os.environ["PATH"]
    # display a message if the survey mode is active
    if is_survey_activated():
        if get_survey_depth() < 1: CONFIG["survey.depth"] = 1
        if get_survey_depth() > 3: CONFIG["survey.depth"] = 3
        logger.warning("SURVEY MODE IS ACTIVE!")
        logger.warning(f"The following directories will be surveyed at {get_survey_time()}")

### GENERIC FUNCTIONS ###

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
    return f"{get_storage_user()}@{get_storage_host()}:{get_storage_path()}"
      
def extract_from_settings(settings):
    job_id = settings["job_id"]
    job_dir = settings["job_dir"]
    owner = settings["owner"]
    shared_files = json.loads(settings["files"]) # raw files
    local_files = json.loads(settings["local_files"]) # fasta files
    return job_id, job_dir, owner, shared_files, local_files

def wait(seconds = 15):
    time.sleep(seconds)

### FUNCTIONS FOR REMOTE SERVER ###

def is_controller_reachable():
    # send a blank file to the controller, just to test the connection
    cmd = f"rsync -e 'ssh -l {get_storage_user()} -i \"{get_storage_key()}\" -o \"StrictHostKeyChecking no\"' --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r \"{get_final_file()}\" \"{get_storage_host()}:{get_storage_path()}/tests\""
    # logger.debug(cmd)
    # return True if the controller is reachable
    return os.system(cmd) == 0

def get_rsync_command(file, job_dir):
    # Rsync options:
    # -r: recurse into directories
    # --size-only: skip files that have the same size, this replaces --ignore-existing that skips files with the same name even if they had different sizes
    # --exclude: do not send wal files, they shouldn't even be here
    # --progress: monitor progression of the transfer on stdout
    # -e: specify the remote shell to use
    #   -l: login
    #   -i: the path to the public key
    #   -o 'StrictHostKeyChecking no': do not ask if the key has to be trusted
    # --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r: make sure that directories have permission 755 and files 644
    options = f"-r --size-only --exclude='*-wal' --progress -e 'ssh -l {get_storage_user()} -i \"{get_storage_key()}\" -o \"StrictHostKeyChecking no\"' --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r"
    # determine the remote folder (either main storage, or job folder)
    remote_path = f"{get_storage_host()}:{get_storage_path()}/jobs/{job_dir}" if job_dir != "" else f"{get_storage_host()}:{get_storage_path()}/data"
    # log the action
    if os.path.isdir(file): logger.debug(f"Sending directory '{os.path.basename(file)}' to '{remote_path}'")
    else: logger.debug(f"Sending file '{os.path.basename(file)}' to '{remote_path}'")
    # cwrsync requires drives to be prepended (Windows only)
    if os.name == 'nt': file = re.sub(r"^([a-zA-Z]):", r"/cygdrive/\1", file.replace("\\", "/"))
    # return the command
    return f"rsync {options} \"{file}\" \"{remote_path}\" > \"{get_progress_file()}\""

def get_server_free_space():
    response = requests.get(f"http://{get_storage_host()}:{get_storage_port()}/diskusage")
    return response.json()[2]

def fail_job(job_id, error_message):
    # send a message to the server to fail the job
    logger.warning(error_message)
    try:
        r = requests.post(f"http://{get_storage_host()}:{get_storage_port()}/fail", data = {"job_id": job_id, "error_message": error_message})
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.warning("Could not send the message", e)

def is_enough_free_space_on_server(fake_free_space_for_test = None):
    global STORAGE_USAGE_LAST_CALL
    # if the last check was less than a minute ago, say it's ok (the time of last check is only recorded when it's successful)
    current_timestamp = time.time()
    if STORAGE_USAGE_LAST_CALL != 0 and current_timestamp - STORAGE_USAGE_LAST_CALL < STORAGE_USAGE_WAITING_TIME:
        return True
    else:
         # call the server
        free_space = get_server_free_space() if fake_free_space_for_test is None else fake_free_space_for_test
        # if the server has enough space, store the current time and return True
        if free_space > STORAGE_FREE_LIMIT:
            STORAGE_USAGE_LAST_CALL = current_timestamp
            return True
        else: return False

### PROGRESS FILE MANAGEMENT ###

def delete_progress_file():
    if os.path.exists(get_progress_file()): os.remove(get_progress_file())

def read_progress_file():
    # prepare values to return
    file = ""
    size = 0
    # prepare variables
    current_file = ""
    current_size = 0
    total_size = 0
    current_line = ""
    current_line_number = 0
    # open the file with a try/except block
    try:
        with open(get_progress_file()) as file:
            # read line by line
            for line in file:
                current_line = line # used in case of error
                current_line_number += 1
                line = line.rstrip()
                if line != "":
                    if line.startswith(" "):
                        # on lines indicating the progress, store the size that is given
                        current_size = int(line.split()[0].replace(".", "").replace(",", ""))
                    else:
                        # on lines indicating which file is being transferred (can be several when transferring a folder)
                        # add the last size that was recorded (so we do not add up the size at 25% and 50% for the same file)
                        total_size += current_size # this size should correspond to the size of the previous file
                        current_size = 0
                        current_file = line # store the name of the file currently transferred
        file = current_file.split("/")[0]
        size = total_size + current_size
    except Exception as e:
        # do not log if the exception is a FileNotFoundError, it means that the file does not exist yet
        if not isinstance(e, FileNotFoundError):
            logger.error(f"Error on line {current_line_number}: {current_line}")
            logger.error(e)
    return [file, size]
    

# def read_progress_file():
# 	current_file = ""
# 	current_size = 0
# 	total_size = 0
#     # open the file
# 	# if os.path.exists(PROGRESS_FILE): 
# 	if os.path.exists(get_progress_file()): 
#         # with open(PROGRESS_FILE) as file:
# 		with open(get_progress_file()) as file:
#             # read line by line
# 			for line in file:
# 				line = line.rstrip()
# 				if line != "":
# 					if line.startswith(" "):
#                         # on lines indicating the progress, store the size that is given
# 						current_size = int(line.split()[0].replace(".", ""))
# 					else:
#                         # on lines indicating which file is being transferred (can be several when transferring a folder)
#                         # add the last size that was recorded (so we do not add up the size at 25% and 50% for the same file)
# 						total_size += current_size # this size should correspond to the size of the previous file
# 						current_size = 0
# 						current_file = line # store the name of the file currently transferred
#         # return the basename of the file (or name of the folder) and the size corresponding to the complete amount of what has been transferred
# 		return [current_file.split("/")[0], total_size + current_size]
# 	else:
# 		return ["", 0]

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


reset_configuration()
