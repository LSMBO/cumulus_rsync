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

import os
import cumulus_rsync.cumulus_rsync_utils as utils
import cumulus_rsync.cumulus_rsync_db as db

# utils.reset_configuration()
# utils.initialize("cumulus_rsync/test/cumulus_rsync.conf")

def test_connect():
    # delete the database if it exists;
    if os.path.isfile(utils.get_queue_file()): os.remove(utils.get_queue_file())
    # call the connect function
    cnx, _ = db.connect()
    # we can disconnect right away
    cnx.close()
    # the database should be created
    assert os.path.isfile(utils.get_queue_file())

def test_count_entries_in_queue():
    assert db.count_entries_in_queue() == 0

def test_is_queue_empty():
    # the queue should be empty
    assert db.is_queue_empty()

def test_add_to_queue():
    nb = db.add_to_queue(11, "path/to/job_dir", "test.user", ["cumulus_rsync/test/File1.raw","cumulus_rsync/test/File2.raw","cumulus_rsync/test/File3.raw"], ["cumulus_rsync/test/File.fasta"])
    assert nb == 4
    assert db.count_entries_in_queue() == 5

def test_get_first_job_in_queue():
    # add another job
    db.add_to_queue(7, "another/path/to/another/job_dir", "another.test.user", ["cumulus_rsync/test/File3.raw","cumulus_rsync/test/File2.raw","cumulus_rsync/test/File1.raw"], ["cumulus_rsync/test/File.fasta"])
    # get the first job
    _, job_id, file_path, job_dir = db.get_first_job_in_queue()
    # make sure that the results match the first job added, not the one we just added
    assert job_id == 11
    assert file_path == "cumulus_rsync/test/File.fasta"
    assert job_dir == "path/to/job_dir"

def test_remove_entry_from_queue():
    # get the id from the first job
    id, _, _, _ = db.get_first_job_in_queue()
    # remove the first job
    db.remove_entry_from_queue(id)
    # the id from the next first job would be a increment of the last id
    next_id, _, _, job_dir = db.get_first_job_in_queue()
    assert next_id == id + 1
    assert job_dir == ""
    # two jobs were added, each with 3 shared files, 1 local file, and 1 final file
    # one file was removed
    assert db.count_entries_in_queue() == 9

def test_get_job_owner():
    assert db.get_job_owner(7) == "another.test.user"
    assert db.get_job_owner(11) == "test.user"

def test_list_files_for_job():
    assert db.list_files_for_job(7, "test.user") == []
    assert db.list_files_for_job(11, "test.user") == [["cumulus_rsync/test/File1.raw", 2566], ["cumulus_rsync/test/File2.raw", 2566], ["cumulus_rsync/test/File3.raw", 2566]] # fasta file was removed

def test_list_shared_files_in_queue():
    assert db.list_shared_files_in_queue() == ["File1.raw", "File2.raw", "File3.raw"]

def test_cancel_job():
    db.cancel_job(7)
    assert db.count_entries_in_queue() == 4

def test_end_test():
    # this is the last test, remove the temp database
    if os.path.isfile(utils.get_queue_file()): os.remove(utils.get_queue_file())
    assert os.path.isfile(utils.get_queue_file()) == False
