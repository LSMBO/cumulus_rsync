# Copyright or © or Copr. Alexandre BUREL for LSMBO / IPHC UMR7178 / CNRS (2024)
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

from flask import Flask
from flask import jsonify
from flask import request
from flask import send_file
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import threading
import time

app = Flask(__name__)
logger = logging.getLogger(__name__)
logging.basicConfig(
				#handlers=[RotatingFileHandler(filename = f"{__name__}.log", maxBytes = 100000, backupCount = 10)],
				level=logging.DEBUG,
				format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
				datefmt='%Y/%m/%d %H:%M:%S')

# default config
STORAGE_HOST = "localhost" # the host where the cumulus server is
STORAGE_PATH = "/storage" # the remote path where data will be sent
#STORAGE_PORT = 8800 # do not use a port already used on the controller (in this case by flask)
STORAGE_USER = "me" # the remote user name
STORAGE_KEY = os.path.abspath("cumulus.pem") # the public key to connect to the server, it has to be an absolute path
REFRESH_RATE = 15 # the number of seconds before the daemon wakes up again and checks if there is something in the queue
FINAL_FILE = os.path.abspath(".cumulus.rsync") # a blank file to transfer at the end of each job, to tell the controller that all the files have been transferred
PROGRESS_FILE = ".cumulus.progress"

# read the config file
f = open(__name__ + ".conf", "r")
for line in f.read().splitlines():
		[key, value] = list(map(lambda item: item.strip(), line.split("=")))
		if key == "storage.path": STORAGE_PATH = value
		elif key == "storage.host": STORAGE_HOST = value
		#elif key == "storage.port": STORAGE_PORT = value
		elif key == "storage.user": STORAGE_USER = value
		elif key == "storage.public_key": STORAGE_KEY = os.path.abspath(value)
		elif key == "refresh.rate": REFRESH_RATE = int(value)
		elif key == "final.file": FINAL_FILE = os.path.abspath(value)
		elif key == "progress.file": PROGRESS_FILE = os.path.abspath(value)
f.close()

# add RSync to path, (Windows only)
# TODO put the rsync path in the config file
# TODO or update the path in the bat file and in the sh file
if os.name == 'nt':
	RSYNC_PATH = os.getcwd() + "/cwrsync_6.3.0_x64_free/bin/"
	os.environ["PATH"] = RSYNC_PATH + ";" + os.environ["PATH"]

# Rsync options:
# -r: recurse into directories
# --ignore-existing: skip updating files that exist on receiver
# --exclude: do not send wal files, they shouldn't even be here
# --progress: monitor progression of the transfer on stdout
# -e: specify the remote shell to use
#   -l: login
#   -i: the path to the public key
#   -o 'StrictHostKeyChecking no': do not ask if the key has to be trusted
# TODO send the file even if it exists on receiver but with a different size
#RSYNC_OPTIONS = f"-r --ignore-existing --exclude='*-wal' -e 'ssh -l {STORAGE_USER} -p {STORAGE_PORT} -i \"{STORAGE_KEY}\" -o \"StrictHostKeyChecking no\"'"
RSYNC_OPTIONS = f"-r --ignore-existing --exclude='*-wal' --progress -e 'ssh -l {STORAGE_USER} -i \"{STORAGE_KEY}\" -o \"StrictHostKeyChecking no\"'"
# each time the user wants to send files, the files are put in a queue and a job id is returned; the queue and the id are not stored and will be reseted when the daemon is stopped
SEND_QUEUE = list()
# we use another queue to store the ids of the jobs canceled, so we do not have to worry about synchronizing the main queue between threads
CANCEL_QUEUE = list()
# global variables to store the progress of the file currently uploaded
CURRENT_JOB = ""
CURRENT_FILE = ""
#CURRENT_FILE_SIZE = 0
#CURRENT_PROGRESS = 0

logger.info(f"Cumulus RSync daemon is running, data will be sent to {STORAGE_USER}@{STORAGE_HOST}:{STORAGE_PATH}")

def daemon():
	while True:
		if len(SEND_QUEUE) > 0:
			# get the first and oldest entry in the queue
			job_id, owner, file, nb, job_dir, size = SEND_QUEUE[0]
			# do not send files that belong to cancelled jobs
			if not job_id in CANCEL_QUEUE:
				# make sure that a folder does not end with a slash
				if os.path.isdir(file) and (file.endswith("/") or file.endswith("\\")): file = file[0:-1]
				# determine the remote folder (either main storage, or job folder)
				remote_path = f"{STORAGE_HOST}:{STORAGE_PATH}/jobs/{job_dir}" if job_dir != "" else f"{STORAGE_HOST}:{STORAGE_PATH}/data"
				# send file with RSync
				if os.path.isdir(file): logger.info(f"Sending directory '{os.path.basename(file)}' to '{remote_path}'")
				else: logger.info(f"Sending file '{os.path.basename(file)}' to '{remote_path}'")
				# store the current file
				CURRENT_JOB = job_id
				CURRENT_FILE = file
				# cwrsync requires drives to be prepended (Windows only)
				if os.name == 'nt': file = re.sub(r"^([A-Z]):", r"/cygdrive/\1", file.replace("\\", "/"))
				# call RSync
				#logger.debug(f"rsync {RSYNC_OPTIONS} \"{file}\" \"{remote_path}\"")
				os.system(f"rsync {RSYNC_OPTIONS} \"{file}\" \"{remote_path}\" > {PROGRESS_FILE}")
				#logger.info(f"RSYNC: Transfer of '{file}' is finished, {len(SEND_QUEUE)} file(s) are left in the queue")
			#else:
			#		logger.info(f"Job {job_id} has been canceled")
			# remove the item from the list
			SEND_QUEUE.pop(0)
			# delete the progress file
			if os.path.exists(PROGRESS_FILE): os.remove(PROGRESS_FILE)
			# clean the cancel queue eventually: remove all the ids that are lower to the current job id
			job_id = int(job_id)
			for id in CANCEL_QUEUE:
				if id < job_id: CANCEL_QUEUE.remove(id)
		else:
			# wait for 15 seconds
			time.sleep(REFRESH_RATE)

@app.route("/")
def hello_world():
    return "OK"

def get_size(file):
	if os.path.isfile(file):
		return os.path.getsize(file)
	else:
		total_size = 0
		for dirpath, dirnames, filenames in os.walk(file):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				# skip if it is symbolic link
				if not os.path.islink(fp): total_size += os.path.getsize(fp)
		return total_size

@app.route("/send-rsync", methods=["POST"])
def send_rsync():
		# parameters are in request.form['param'] = value
		# parameters are just a list of files with absolute pathes
		settings = request.form
		job_id = settings["job_id"]
		job_dir = settings["job_dir"]
		owner = settings["owner"]
		#print(f"Calling /send-rsync from owner '{owner}' with job id {job_id}")
		logger.info(f"Receiving files to upload for job {job_id}")
		# for each file, add [job_id, job_owner, file_path] to the queue
		files = json.loads(settings["files"]) # raw files
		local_files = json.loads(settings["local_files"]) # fasta files
		nb = len(files) + len(local_files)
		for file in local_files:
			logger.info(f"Add '{os.path.basename(file)}' to the queue, it will be sent to {job_dir}")
			SEND_QUEUE.append([job_id, owner, file, nb, job_dir, get_size(file)])
		for file in files:
			logger.info(f"Add '{os.path.basename(file)}' to the queue, it will be shared for all jobs")
			SEND_QUEUE.append([job_id, owner, file, nb, "", get_size(file)])
		# send a blank file to the job folder to warn the controller that all the transfers are done for this job
		SEND_QUEUE.append([job_id, owner, FINAL_FILE, nb, job_dir, get_size(file)])
		return f"{nb} files have been added to the queue"

@app.route("/list-rsync")
def list_rsync():
		#return jsonify(["my-new-fake-maps.txt", "TP4808CMO_Slot2-1_1_4820_ABU.d", "Q_ABU_ValidRaw_ValidArchive.raw", "Q_ABU_ValidRaw_ValidArchive.d", "TP4823CMO_ABU_Slot2-17_1_4835.d"])
		files = []
		for job_id, _, file, _, job_dir, _ in SEND_QUEUE:
			# do not list the fasta files or the files that have been cancelled
			if job_dir == "" and not job_id in CANCEL_QUEUE:
				files.append(os.path.basename(file))
		# return the list of files, without duplicates
		return jsonify(list(dict.fromkeys(files)))

@app.route("/cancel-rsync/<string:owner>/<int:job_id>")
def cancel_rsync(owner, job_id):
		# use a different queue, to avoid removing elements already transferred and deleted from the queue (or use a async queue)
		logger.info(f"Receiving cancel order for job {job_id}")
		CANCEL_QUEUE.append(job_id)
		# return the number of file transfer canceled
		nb = len(list(filter(lambda job: list(job)[0] == job_id, SEND_QUEUE)))
		return f"{nb} transfers have been canceled"

def read_progress_file():
	current_file = ""
	current_size = 0
	total_size = 0
	if os.path.exists(PROGRESS_FILE): 
		with open(PROGRESS_FILE) as file:
			for line in file:
				line = line.rstrip()
				if line != "":
					if line.startswith(" "):
						current_size = int(line.split()[0].replace(".", ""))
					else:
						total_size += current_size
						current_size = 0
						current_file = line
		return [current_file.split("/")[0], total_size + current_size]
	else:
		return ["", 0]

@app.route("/test")
def test():
	[current_file, current_amount] = read_progress_file()
	logger.info(f"> Progress of '{current_file}': {current_amount}%")
	return f"'{current_file}': {current_amount*1}%"
	

@app.route("/progress-rsync/<string:owner>/<int:job_id>")
def progress_rsync(owner, job_id):
	logger.info(f"Monitoring progress for job {job_id} owned by {owner}")
	# read the progress file and extract the file being transferred and the amount of bytes transferred
	[current_file, current_amount] = read_progress_file()
	logger.info(f"> Progress of '{current_file}': {current_amount}")
	# prepare a dict for the results
	#progress = []
	progress = {}
	# for each file, set a size of 0 unless it is the file being transferred (then set the percentage)
	for file in SEND_QUEUE:
		id, username, filename, nb, _, size = file
		if job_id == int(id) and owner == username:
			if os.path.basename(filename) == current_file:
				#progress.append({filename: int(current_amount * 100 / size)})
				progress[filename]= int(current_amount * 100 / size)
			else:
				#progress.append({filename: 0})
				progress[filename]= 0
	# return the dict with the files that are still in the queue
	# the files not in that list will be considered as already transferred
	logger.info(jsonify(progress))
	return jsonify(progress)

# start the queue once all functions are defined
threading.Thread(target=daemon, args=(), daemon=True).start()
