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

import logging
import os
import sqlite3
import cumulus_rsync.cumulus_rsync_utils as utils

logger = logging.getLogger(__name__)

def connect():
    # connect to the database, create it if it does not exist yet
    cnx = sqlite3.connect(utils.get_queue_file(), isolation_level = None)
    cursor = cnx.cursor()
	# create the main table if it does not exist
    cursor.execute("""
		CREATE TABLE IF NOT EXISTS queue(
			id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            job_id INTEGER NOT NULL,
            owner TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            nb_files_in_job INTEGER NOT NULL,
			job_dir TEXT)
	""")
    cnx.commit()
    return cnx, cursor

def count_entries_in_queue():
    # connect to the database
    cnx, cursor = connect()
    # count the number of entries in the queue
    cursor.execute(f"SELECT COUNT(*) FROM queue")
    # the queue is empty if count is 0
    nb = cursor.fetchone()[0]
    # disconnect and return the value
    cnx.close()
    return nb

def is_job_in_queue(job_id):
    # connect to the database
    cnx, cursor = connect()
    # check if the job is in the queue
    cursor.execute(f"SELECT COUNT(*) FROM queue WHERE job_id = ?", (job_id,))
    # get the content
    nb = cursor.fetchone()[0]
    # disconnect and return the value
    cnx.close()
    return nb > 0

def is_queue_empty():
    return count_entries_in_queue() == 0

def get_first_job_in_queue():
    # connect to the database
    cnx, cursor = connect()
    # get the first entry in the queue
    cursor.execute(f"SELECT id, job_id, file_path, job_dir FROM queue ORDER BY id ASC LIMIT 1")
    # get the content
    entry_id, job_id, file_path, job_dir = cursor.fetchone()
    if job_dir is None: job_dir = ""
    # disconnect and return the content
    cnx.close()
    return entry_id, job_id, file_path, job_dir

def remove_entry_from_queue(entry_id):
    # connect to the database
    cnx, cursor = connect()
    # remove the entry from the queue
    cursor.execute(f"DELETE FROM queue WHERE id = ?", (entry_id,))
    cnx.commit()
    # disconnect and return the content
    cnx.close()
    # if the queue is empty after this operation, log the event
    if is_queue_empty(): logger.info("The queue is now empty")

def get_job_owner(job_id):
    # connect to the database
    cnx, cursor = connect()
    # get the owner of the job
    cursor.execute(f"SELECT owner FROM queue WHERE job_id = ? LIMIT 1", (job_id,))
    # get the content
    owner, = cursor.fetchone()
    # disconnect and return the content
    cnx.close()
    return owner

def add_to_queue(job_id, job_dir, owner, shared_files, local_files, insert_final_file = True):
    # connect to the database
    cnx, cursor = connect()
    # count the total number of files in this job
    nb = len(shared_files) + len(local_files)
    # count the number of files that are available
    nb_available = 0
    for file in local_files:
        if os.path.isfile(file) or os.path.isdir(file): nb_available += 1
    for file in shared_files:
        if os.path.isfile(file) or os.path.isdir(file): nb_available += 1
    if nb_available != nb:
        utils.fail_job(job_id, f"Job {job_id}: {nb - nb_available} files were not available, sending a request to set the status of the job to 'failed'")
        return 0
    else:
        # add the files to the queue
        for file in local_files:
            if os.path.isfile(file) or os.path.isdir(file):
                logger.debug(f"Add '{file}' to the queue, it will be sent to {job_dir}")
                cursor.execute(f"INSERT INTO queue VALUES (?, ?, ?, ?, ?, ?, ?)", (None, job_id, owner, file, utils.get_size(file), nb, job_dir))
                nb_available += 1
        for file in shared_files:
            if os.path.isfile(file) or os.path.isdir(file):
                logger.debug(f"Add '{file}' to the queue, it will be shared for all jobs")
                cursor.execute(f"INSERT INTO queue VALUES (?, ?, ?, ?, ?, ?, ?)", (None, job_id, owner, file, utils.get_size(file), nb, None))
                nb_available += 1
        # send a blank file to the job folder to warn the controller that all the transfers are done for this job
        if insert_final_file:
            cursor.execute(f"INSERT INTO queue VALUES (?, ?, ?, ?, ?, ?, ?)", (None, job_id, owner, utils.get_final_file(), utils.get_size(utils.get_final_file()), nb, job_dir))
        # commit the changes and return the number of added entries to the queue (minus the final file)
        cnx.commit()
        cnx.close()
        logger.info(f"Job {job_id}: {nb} files have been added to the queue")
        return nb

def list_files_for_job(job_id, owner):
    # connect to the database
    cnx, cursor = connect()
    # this function is called to check which files are stored on the server
    files = []
    results = cursor.execute(f"SELECT file_path, file_size FROM queue WHERE job_id = ? AND owner = ? AND file_path != ? ORDER BY id", (job_id, owner, utils.get_final_file()))
    for file, size in results:
        files.append([file, size])
    # disconnect and return the list of files
    cnx.close()
    return sorted(files)

def list_shared_files_in_queue():
    # connect to the database
    cnx, cursor = connect()
    # this function is called to check which files are stored on the server
    files = []
    results = cursor.execute(f"SELECT DISTINCT file_path FROM queue WHERE job_dir IS NULL AND file_path != ?", (utils.get_final_file(),))
    for file, in results:
        files.append(os.path.basename(file))
    # disconnect and return the sorted list of files
    cnx.close()
    return sorted(files)

def cancel_job(job_id):
    # connect to the database
    cnx, cursor = connect()
    # count the number of entries for this job
    cursor.execute(f"SELECT COUNT(*) FROM queue WHERE job_id = ?", (job_id,))
    nb = cursor.fetchone()[0]
    # remove all the entries for this job
    cursor.execute(f"DELETE FROM queue WHERE job_id = ?", (job_id,))
    cnx.commit()
    # disconnect and return the number of entries removed
    cnx.close()
    return nb
