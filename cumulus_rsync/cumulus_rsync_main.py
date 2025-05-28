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

from flask import Flask, jsonify, request
import logging
import os
import threading
import cumulus_rsync.cumulus_rsync_utils as utils
import cumulus_rsync.cumulus_rsync_db as db
import cumulus_rsync.cumulus_rsync_survey as survey

os.environ["CUMULUS_DEBUG"] = "1"
# prepare the main variables
app = Flask(__name__)
logger = logging.getLogger(__name__)

def daemon():
	"""
	Runs the Cumulus RSync daemon, which continuously monitors and processes file transfer jobs.

	The daemon performs the following tasks in a loop:
	- If survey mode is activated and it's time to survey, scans the specified directories for new files and adds them to the transfer queue.
		- The survey mode is used to automatically discover new files in specified directories and add them to the transfer queue.
		- This should strongly reduce the amount of time spent to transfer files to the server.
		- Ideally this should be done at night.
	- Checks if there are pending jobs in the queue:
		- For each job, ensures there is enough free storage space before proceeding.
		- Constructs and executes the rsync command to transfer the file.
		- Removes the job from the queue upon completion and deletes any associated progress files.
		- If the job was initiated by the survey process and all survey jobs are done, marks the survey as completed for the day.
	- Waits for a configurable interval when the queue is empty.

	Logging is performed at various stages to provide status updates and warnings.
	"""
	logger.info(f"Cumulus RSync daemon is running, data will be sent to {utils.get_storage_info()}")
	# start the main loop
	while True:
		# once a day, add all new files in the surveyed directories
		if utils.is_survey_activated() and survey.is_time_to_survey(utils.get_survey_time()):
			# TODO this was not tested live!!
			logger.info("Entering survey mode...")
			# get the new files
			files = survey.survey_directories(utils.get_surveyed_directories(), utils.get_survey_depth())
			# add the files to the queue with a fake job_id, no job_dir, fake owner and no local files
			db.add_to_queue(0, "", survey.SURVEY_OWNER, files, [], False)
			# reset the boolean
			# TODO this should only be done once all the surveyed files are transferred
			# survey.set_surveyed_today()
		# check if there is something to do
		if not db.is_queue_empty():
			# get the first and oldest entry in the queue
			entry_id, job_id, file, job_dir, owner = db.get_first_job_in_queue()
			# logger.debug(f"Job {job_id}: file '{file}' of size {size}")
			# make sure that there is enough space on the server
			i = 0
			while not utils.is_enough_free_space_on_server():
				logger.warning(f"Storage free space is below {utils.STORAGE_FREE_LIMIT // 2**30}GB, uploads are paused for now...")
				utils.wait(utils.STORAGE_FREE_LIMIT_SLEEP)
				i = i + 1
			if i > 0: logger.info("Storage free space is above the limit, uploads can resume now")
			# get the rsync command
			cmd = utils.get_rsync_command(file, job_dir)
			# call RSync
			logger.debug(cmd)
			# TODO do something if it fails
			os.system(cmd)
			# logger.info(f"RSYNC: Transfer of '{file}' is finished, {len(SEND_QUEUE)} file(s) are left in the queue")
			# remove the item from the list
			db.remove_entry_from_queue(entry_id)
			# delete the progress file
			utils.delete_progress_file()
			# if the job was a survey, reset the boolean if there is no more survey files in the queue
			if owner == survey.SURVEY_OWNER and len(db.get_jobs_per_owner(survey.SURVEY_OWNER)) == 0:
				survey.set_surveyed_today()
			# wait half a second
			utils.wait(0.5)
		else:
			# wait for 15 seconds
			utils.wait(utils.get_refresh_rate())

@app.route("/")
def config(): 
	"""
	Retrieve the current version of the application from the utils module.
	This route is also used to check if the application is running.

	Returns:
		str: The version string of the application.
	"""
	return utils.get_version()

@app.route("/send-rsync", methods=["POST"])
def send_rsync():
		"""
		Handles a POST request to add files to the rsync queue.

		Reads form data from the request, extracts job and file information,
		adds the specified files to the processing queue, and returns a message
		indicating how many files were added.

		Returns:
			str: A message indicating the number of files added to the queue.
		"""
		# read the POST form
		job_id, job_dir, owner, shared_files, local_files = utils.extract_from_settings(request.form)
		# add the files to the queue
		nb = db.add_to_queue(job_id, job_dir, owner, shared_files, local_files)
		# return the number of files added to the queue
		return f"{nb} files have been added to the queue"

@app.route("/list-rsync")
def list_rsync():
	"""
	Returns a JSON response containing the list of shared files currently in the queue, with duplicates removed.

	Returns:
		flask.Response: A JSON response with the list of unique shared files.
	"""
	# return the list of files, without duplicates
	return jsonify(db.list_shared_files_in_queue())

@app.route("/cancel-rsync/<string:owner>/<int:job_id>")
def cancel_rsync(owner, job_id):
	"""
	Cancels an rsync job if it exists in the queue and the requester is the owner.

	Args:
		owner (str): The identifier of the user requesting the cancellation.
		job_id (int): The unique identifier of the rsync job to cancel.

	Returns:
		str: A message indicating the result of the cancellation attempt. Possible messages include:
			- Confirmation of the number of transfers canceled.
			- Notification if the job does not exist in the queue.
			- Notification if the requester is not the owner of the job.
	"""
	if not db.is_job_in_queue(job_id):
		return f"Job {job_id} does not exist in the queue"
	elif db.get_job_owner(job_id) == owner:
		# use a different queue, to avoid removing elements already transferred and deleted from the queue (or use a async queue)
		logger.info(f"Receiving cancel order for job {job_id}")
		nb = db.cancel_job(job_id)
		# return the number of file transfer canceled
		return f"{nb} transfers have been canceled"
	else:
		return "You are not the owner of this job"
	
@app.route("/progress-rsync/<string:owner>/<int:job_id>")
def progress_rsync(owner, job_id):
	"""
	Monitor and report the progress of an rsync job for a specific owner.

	Args:
		owner (str): The owner of the job.
		job_id (int): The unique identifier of the rsync job.

	Returns:
		flask.Response: A JSON response containing a dictionary with the progress
		status of each file associated with the job. Files not listed are considered
		already transferred.
	"""
	# logger.info(f"Monitoring progress for job {job_id} owned by {owner}")
	# get the list of files for this job
	files = db.list_files_for_job(job_id, owner)
	# read the progress file and put the results in a dict
	progress_dict = utils.get_progress_for_job(job_id, files)
	# return the dict with the files that are still in the queue
	# the files not in that list will be considered as already transferred
	return jsonify(progress_dict)

def start():
	"""
	Starts the cumulus_rsync main process.

	This function performs the following steps:
	1. Loads the application configuration from 'cumulus_rsync.conf'.
	2. Checks if the server is reachable; logs an error and exits if not.
	3. Starts the daemon process in a background thread.
	4. Launches the Waitress WSGI server to serve the application using the local host and port from the configuration.
	"""
	from waitress import serve
	# load the configuration
	utils.initialize("cumulus_rsync.conf")
	# check that the controller can be reached
	if not utils.is_controller_reachable():
		logger.error("The controller is not reachable, exiting...")
		return
	# start the daemon
	threading.Thread(target=daemon, args=(), daemon=True).start()
	# start waitress WSGI server
	serve(app, host = utils.get_local_host(), port = utils.get_local_port())
