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
    """
    Retrieve the value associated with a given key from the CONFIG dictionary.

    Args:
        key (str): The key to look up in the CONFIG dictionary.

    Returns:
        Any: The value associated with the key if it exists, otherwise None.

    Logs:
        An error message if the key is not found in the CONFIG dictionary.
    """
    if key in CONFIG: return CONFIG[key]
    else: 
        logger.error(f"Key '{key}' not found in the configuration file")
        return None
def get_local_host(): return get_config_value("local.host")
def get_local_port(): return get_config_value("local.port")
def get_storage_host(): return get_config_value("storage.host")
def get_storage_path(is_test = False): 
    if is_test and os.getenv("CUMULUS_DEBUG"): return get_config_value("storage.path") + "/tests"
    else: return get_config_value("storage.path")
def get_storage_port(): return get_config_value("storage.port")
def get_storage_user(): return get_config_value("storage.user")
def get_storage_key(): return get_config_value("storage.public_key")
def get_refresh_rate(): return get_config_value("refresh.rate")
def get_final_file(): return get_config_value("final.file")
def get_progress_file(): return get_config_value("progress.file")
def get_queue_file(): return get_config_value("queue.file")
def get_remote_input_folder(): return get_config_value("remote.input.folder")
def get_rsync_path(): return get_config_value("rsync.bin.path")
def get_ssh_path(): return get_config_value("ssh.bin.path")
def get_version(): return get_config_value("version")
def get_survey_depth(): return get_config_value("survey.depth")
def get_survey_time(): return get_config_value("survey.time")
def get_surveyed_directories(): return get_config_value("surveyed_directories")
def is_survey_activated(): 
    """
    Checks whether the survey feature is activated based on configuration and time format.

    Returns:
        bool: True if the survey is enabled in the configuration, the survey time is set,
        and the survey time matches the "HH:MM" format; otherwise, False.
    """
    if get_config_value("survey.enabled") is None or not get_config_value("survey.enabled"): return False
    # check if the survey is activated
    if get_survey_time() is None: return False
    # check if survey_time is set and valid
    if not re.match(r"^\d\d:\d\d$", get_survey_time()): return False
    # otherwise, return True
    return True

def reset_configuration():
    """
    Resets the global CONFIG dictionary to its default configuration values.

    This function initializes or re-initializes the CONFIG global variable with default
    settings for local and storage hosts, ports, file paths, user credentials, refresh rates,
    and other operational parameters required by the cumulus_rsync utility.

    Global Variables:
        CONFIG (dict): The configuration dictionary that will be reset to default values.

    Configuration Keys Set:
        - "local.host": Hostname or IP address to listen on.
        - "local.port": Port to listen on.
        - "storage.host": Hostname of the cumulus server.
        - "storage.path": Remote path for data transfer.
        - "storage.port": Port on the storage server.
        - "storage.user": Username for remote connection.
        - "storage.public_key": Absolute path to the public key for server connection.
        - "refresh.rate": Interval (in seconds) for daemon wake-up.
        - "final.file": Marker file to indicate job completion.
        - "progress.file": File to track progress.
        - "queue.file": Filename for the transfer queue database.
        - "version": Version string (initially empty).
        - "rsync.bin.path": Path to the rsync binary.
        - "ssh.bin.path": Path to the ssh binary.
        - "survey.enabled": Boolean flag to enable/disable survey.
        - "survey.depth": Depth for directory survey.
        - "survey.time": Time for scheduled survey.
        - "surveyed_directories": Dictionary to track surveyed directories.
    """
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
    CONFIG["remote.input.folder"] = "input" # a subdirectory within the job folder where local files will be sent
    CONFIG["version"] = ""
    CONFIG["rsync.bin.path"] = "" # the path to the rsync binary
    CONFIG["ssh.bin.path"] = "" # the path to the ssh binary
    CONFIG["survey.enabled"] = False
    CONFIG["survey.depth"] = 1
    CONFIG["survey.time"] = "23:00"
    CONFIG["surveyed_directories"] = {}

def read_config_file(config_file):
    """
    Reads a configuration file and populates the global CONFIG dictionary with configuration values.

    The configuration file should contain key-value pairs in the format "key = value", one per line.
    Recognized keys include settings for local and storage hosts, ports, file paths, survey options, and more.
    Surveyed directories and their associated properties (directory path, regex, isfile) are also parsed and stored.

    Args:
        config_file (str): Path to the configuration file to read.

    Raises:
        FileNotFoundError: If the specified configuration file does not exist.

    Side Effects:
        Modifies the global CONFIG dictionary with the parsed configuration values.
    """
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
        # if key == "local.host": CONFIG[key] = value
        # elif key == "local.port": CONFIG[key] = value
        # elif key == "storage.path": CONFIG[key] = value
        # elif key == "storage.host": CONFIG[key] = value
        # elif key == "storage.port": CONFIG[key] = value
        # elif key == "storage.user": CONFIG[key] = value
        if key == "storage.public_key": CONFIG[key] = os.path.abspath(value)
        elif key == "refresh.rate": CONFIG[key] = int(value)
        # elif key == "final.file": CONFIG[key] = value
        elif key == "progress.file": CONFIG[key] = os.path.abspath(value)
        elif key == "queue.file": CONFIG[key] = os.path.abspath(value)
        # elif key == "remote.input.folder": CONFIG[key] = value
        elif key == "rsync.bin.path": CONFIG[key] = os.path.abspath(value)
        elif key == "ssh.bin.path": CONFIG[key] = os.path.abspath(value)
        # elif key == "version": CONFIG[key] = value
        # keys for survey
        elif key == "survey.enabled": CONFIG[key] = value.lower() == "true" or value.lower() == "on"
        elif key == "survey.depth" and str(value).isnumeric: CONFIG[key] = int(value)
        elif key == "survey.time" and re.match(r"^\d\d:\d\d$", value): CONFIG[key] = value
        elif match := re.search("survey\\.(.*)\\.dir", key, re.IGNORECASE): directories[match.group(1)] = {"dir": value}
        elif match := re.search("survey\\.(.*)\\.regex", key, re.IGNORECASE): directories[match.group(1)]["regex"] = value
        elif match := re.search("survey\\.(.*)\\.isfile", key, re.IGNORECASE): directories[match.group(1)]["isfile"] = value.lower() == "true" or value.lower() == "on"
        else: CONFIG[key] = value
    CONFIG["surveyed_directories"] = directories
    f.close()

def initialize(config_file):
    """
    Initializes the Cumulus Rsync utility with the provided configuration file.

    This function performs the following steps:
    1. Configures logging based on the presence of the "CUMULUS_DEBUG" environment variable.
       - If set, logs are output at DEBUG level to the console.
       - Otherwise, logs are written at INFO level to a rotating file handler.
    2. Reads the configuration file specified by `config_file`.
    3. Verifies that the required public key file exists; raises FileNotFoundError if not found.
    4. Ensures the final file exists; creates an empty one if it does not.
    5. Adds the RSync and SSH binary paths to the system PATH environment variable if their directories exist.
    6. If survey mode is activated:
       - Ensures the survey depth is within the allowed range (1 to 3).
       - Logs warnings indicating that survey mode is active and when directories will be surveyed.

    Args:
        config_file (str): Path to the configuration file to be loaded.

    Raises:
        FileNotFoundError: If the required public key file is not found.
    """
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
        if get_survey_depth() is None: CONFIG["survey.depth"] = 1
        elif get_survey_depth() < 1: CONFIG["survey.depth"] = 1
        elif get_survey_depth() > 3: CONFIG["survey.depth"] = 3
        logger.warning("SURVEY MODE IS ACTIVE!")
        logger.warning(f"The following directories will be surveyed at {get_survey_time()}")

### GENERIC FUNCTIONS ###

def get_size(file):
    """
    Calculate the size of a file or the total size of all files within a directory.

    Args:
        file (str): Path to the file or directory.

    Returns:
        int: Size in bytes. If a file is provided, returns its size. If a directory is provided,
             returns the cumulative size of all contained files, excluding symbolic links.

    Notes:
        - Symbolic links are ignored when calculating directory sizes.
        - If the path does not exist, an exception may be raised.
    """
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
    """
    Constructs and returns the storage information string in the format 'user@host:path'.

    Returns:
        str: A string containing the storage user, host, and path in the format 'user@host:path'.
    """
    return f"{get_storage_user()}@{get_storage_host()}:{get_storage_path()}"
      
def extract_from_settings(settings):
    """
    Extracts and returns job-related information from a settings dictionary.

    Args:
        settings (dict): A dictionary containing job configuration with the following keys:
            - "job_id" (str): The unique identifier for the job.
            - "job_dir" (str): The directory associated with the job.
            - "owner" (str): The owner of the job.
            - "files" (str): A JSON-encoded string representing shared files.
            - "local_files" (str): A JSON-encoded string representing local files (e.g., fasta files).

    Returns:
        tuple: A tuple containing:
            - job_id (str): The job identifier.
            - job_dir (str): The job directory.
            - owner (str): The job owner.
            - shared_files (Any): The decoded shared files object.
            - local_files (Any): The decoded local files object.
    """
    job_id = settings["job_id"]
    job_dir = settings["job_dir"]
    owner = settings["owner"]
    shared_files = json.loads(settings["files"]) # raw files
    local_files = json.loads(settings["local_files"]) # fasta files
    return job_id, job_dir, owner, shared_files, local_files

def wait(seconds = 15):
    """
    Pauses the execution of the program for a specified number of seconds.

    Args:
        seconds (int, optional): The number of seconds to wait. Defaults to 15.

    Returns:
        None
    """
    time.sleep(seconds)

### FUNCTIONS FOR REMOTE SERVER ###

def is_controller_reachable():
    """
    Checks if the controller is reachable by attempting to send a blank file using rsync over SSH.
    This function is called once to verify the connection to the controller before starting the main daemon.

    Returns:
        bool: True if the controller is reachable (rsync command succeeds), False otherwise.
    """
    # send a blank file to the controller, just to test the connection
    cmd = f"rsync -e 'ssh -l {get_storage_user()} -i \"{get_storage_key()}\" -o \"StrictHostKeyChecking no\"' --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r \"{get_final_file()}\" \"{get_storage_host()}:{get_storage_path()}/temp\""
    # logger.debug(cmd)
    # return True if the controller is reachable
    return os.system(cmd) == 0

def get_rsync_command(file, job_dir):
    """
    Constructs an rsync command to transfer a file or directory to a remote storage location, with specific options for permissions, progress monitoring, and SSH authentication.

    Args:
        file (str): The path to the local file or directory to be transferred.
        job_dir (str): The job-specific directory on the remote storage. If empty, defaults to the main data directory.

    Returns:
        str: The complete rsync command as a string, ready to be executed.

    Notes:
        - Uses SSH for remote shell with specified user and key.
        - Sets directory permissions to 755 and file permissions to 644 on the remote side.
        - Excludes files matching '*-wal'.
        - On Windows, converts drive letters to Cygwin-style paths for compatibility with cwrsync.
        - Logs the transfer action for debugging purposes.
        - Redirects rsync progress output to a progress file.
    """
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
    remote_path = f"{get_storage_host()}:{get_storage_path(True)}/jobs/{job_dir}" if job_dir != "" else f"{get_storage_host()}:{get_storage_path()}/data"
    # log the action
    if os.path.isdir(file): logger.debug(f"Sending directory '{os.path.basename(file)}' to '{remote_path}'")
    else: logger.debug(f"Sending file '{os.path.basename(file)}' to '{remote_path}'")
    # cwrsync requires drives to be prepended (Windows only)
    if os.name == 'nt': file = re.sub(r"^([a-zA-Z]):", r"/cygdrive/\1", file.replace("\\", "/"))
    # return the command
    return f"rsync {options} \"{file}\" \"{remote_path}\" > \"{get_progress_file()}\""

def get_server_free_space():
    """
    Retrieves the free disk space available on the server.

    Sends a GET request to the storage server's `/diskusage` endpoint and returns the free space value from the response.

    Returns:
        int or float: The amount of free disk space on the server, as provided by the third element in the JSON response.

    Raises:
        requests.RequestException: If the HTTP request fails.
        KeyError, IndexError: If the expected data is not present in the response.
    """
    response = requests.get(f"http://{get_storage_host()}:{get_storage_port()}/diskusage")
    return response.json()[2]

def fail_job(job_id, error_message):
    """
    Marks a job as failed by sending a failure message to the server and logging the error.
    This function is used when a job contains files that cannot be transferred, for instance if they are visible to the user but not readable by the server.

    Args:
        job_id (str): The unique identifier of the job to be marked as failed.
        error_message (str): The error message describing the reason for failure.

    Logs:
        - Logs the error message as a warning.
        - Logs a warning if the failure message could not be sent to the server.
    """
    # send a message to the server to fail the job
    logger.warning(error_message)
    try:
        r = requests.post(f"http://{get_storage_host()}:{get_storage_port()}/fail", data = {"job_id": job_id, "error_message": error_message})
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.warning("Could not send the message", e)

def is_enough_free_space_on_server(fake_free_space_for_test = None):
    """
    Checks if there is enough free space on the server.

    This function determines whether the server has sufficient free space by either:
    - Returning True if a successful check was performed less than STORAGE_USAGE_WAITING_TIME seconds ago.
    - Querying the server for available free space (or using a provided test value), and comparing it to STORAGE_FREE_LIMIT.

    If enough space is available, the timestamp of the last successful check is updated.

    Args:
        fake_free_space_for_test (int, optional): If provided, this value is used as the available free space instead of querying the server. Useful for testing.

    Returns:
        bool: True if there is enough free space on the server, False otherwise.
    """
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
    """
    Deletes the progress file if it exists.

    This function checks if the progress file exists on the filesystem.
    If it does, the file is removed. The path to the progress file is
    determined by the `get_progress_file()` function.
    """
    if os.path.exists(get_progress_file()): os.remove(get_progress_file())

def read_progress_file():
    """
    Reads the progress file generated during a file transfer operation and extracts the name of the last file being transferred and the total size transferred so far.

    Returns:
        list: A list containing two elements:
            - file (str): The name of the last file being transferred.
            - size (int): The total size (in bytes) transferred so far.

    Notes:
        - If the progress file does not exist, returns default values without logging an error.
        - In case of other exceptions, logs the error along with the line number and content where the error occurred.
    """
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
    
def get_progress_for_job(job_id, files):
    """
    Calculates the upload progress percentage for each file in a job.

    Args:
        job_id (int): The identifier of the job for which progress is being tracked.
        files (list of tuple): A list of tuples, each containing the file path (str) and file size (int).

    Returns:
        dict: A dictionary mapping each file's base name to its upload progress percentage (int).

    Notes:
        - The function reads the current file being uploaded and its progress from a progress file.
        - If a file is currently being uploaded, its progress is calculated as a percentage of its total size.
        - Files not currently being uploaded are assigned a progress of 0%.
        - Logs the progress information for the file currently being uploaded.
    """
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
