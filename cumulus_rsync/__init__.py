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

# prepare the main variables
app = Flask(__name__)
logger = logging.getLogger(__name__)

# load the configuration
utils.initialize("cumulus_rsync.conf")

def daemon():
	logger.info(f"Cumulus RSync daemon is running, data will be sent to {utils.get_storage_info()}")
	while True:
		# logger.debug(f"{len(SEND_QUEUE)} file(s) in the queue...")
		# if not utils.is_send_queue_empty():
		if not db.is_queue_empty():
			# get the first and oldest entry in the queue
			# job_id, file, job_dir = utils.get_first_job_in_queue()
			entry_id, job_id, file, job_dir = db.get_first_job_in_queue()
			# logger.debug(f"Job {job_id}: file '{file}' of size {size}")
			# do not send files that belong to cancelled jobs
			# if not utils.is_job_cancelled(job_id):
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
			# logger.debug(cmd)
			# TODO do something if it fails
			os.system(cmd)
			# logger.info(f"RSYNC: Transfer of '{file}' is finished, {len(SEND_QUEUE)} file(s) are left in the queue")
			# remove the item from the list
			# utils.remove_first_job_in_queue()
			db.remove_entry_from_queue(entry_id)
			# delete the progress file
			utils.delete_progress_file()
			# # clean the cancel queue eventually: remove all the ids that are lower to the current job id
			# utils.clean_cancel_queue(int(job_id))
		else:
			# wait for 15 seconds
			utils.wait()

@app.route("/")
def config(): return utils.VERSION

@app.route("/send-rsync", methods=["POST"])
def send_rsync():
		# read the POST form
		job_id, job_dir, owner, shared_files, local_files = utils.extract_from_settings(request.form)
		# add the files to the queue
		# nb = utils.add_to_queue(job_id, job_dir, owner, shared_files, local_files)
		nb = db.add_to_queue(job_id, job_dir, owner, shared_files, local_files)
		# return the number of files added to the queue
		return f"{nb} files have been added to the queue"

@app.route("/list-rsync")
def list_rsync():
	# return the list of files, without duplicates
	# return jsonify(utils.list_shared_files_in_queue())
	return jsonify(db.list_shared_files_in_queue())

@app.route("/cancel-rsync/<string:owner>/<int:job_id>")
def cancel_rsync(owner, job_id):
		if db.get_job_owner == owner:
			# use a different queue, to avoid removing elements already transferred and deleted from the queue (or use a async queue)
			logger.info(f"Receiving cancel order for job {job_id}")
			# nb = utils.cancel_job(job_id)
			nb = db.cancel_job(job_id)
			# return the number of file transfer canceled
			return f"{nb} transfers have been canceled"
		else:
			return "You are not the owner of this job"
	
@app.route("/progress-rsync/<string:owner>/<int:job_id>")
def progress_rsync(owner, job_id):
	# logger.info(f"Monitoring progress for job {job_id} owned by {owner}")
	# read the progress file and put the results in a dict
	# progress_dict = utils.get_progress_for_job(job_id, owner)
	# get the list of files for this job
	files = db.list_files_for_job(job_id, owner)
	# read the progress file and put the results in a dict
	progress_dict = utils.get_progress_for_job(job_id, files)
	# return the dict with the files that are still in the queue
	# the files not in that list will be considered as already transferred
	return jsonify(progress_dict)

# start the queue once all functions are defined
threading.Thread(target=daemon, args=(), daemon=True).start()
