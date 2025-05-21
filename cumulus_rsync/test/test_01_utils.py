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

import cumulus_rsync.cumulus_rsync_utils as utils
import os
import shutil

def test_initialization():
    assert utils.get_storage_host() == "localhost"
    utils.initialize("cumulus_rsync/test/cumulus_rsync.conf")
    assert utils.get_storage_host() == "127.0.0.1"
    assert len(utils.get_surveyed_directories()) == 2

def test_get_size():
    # test a file
    file_size = utils.get_size("cumulus_rsync/test/cumulus_rsync.conf")
    assert file_size > 200
    # test a folder
    folder_size = utils.get_size(".")
    assert folder_size > file_size

def test_get_storage_info():
    assert utils.get_storage_info() == "me@127.0.0.1:/storage"

def test_get_rsync_command_shared():
    options = f"-r --size-only --exclude='*-wal' --progress -e 'ssh -l {utils.get_storage_user()} -i \"{utils.get_storage_key()}\" -o \"StrictHostKeyChecking no\"' --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r"
    file = "path/to/file"
    remote_path = f"{utils.get_storage_host()}:{utils.get_storage_path()}/data"
    expected_cmd = f"rsync {options} \"{file}\" \"{remote_path}\" > \"{utils.get_progress_file()}\""
    assert utils.get_rsync_command(file, "") == expected_cmd

def test_get_rsync_command_local():
    job_dir = "job_123"
    options = f"-r --size-only --exclude='*-wal' --progress -e 'ssh -l {utils.get_storage_user()} -i \"{utils.get_storage_key()}\" -o \"StrictHostKeyChecking no\"' --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r"
    file = "path/to/file"
    remote_path = f"{utils.get_storage_host()}:{utils.get_storage_path()}/jobs/{job_dir}"
    expected_cmd = f"rsync {options} \"{file}\" \"{remote_path}\" > \"{utils.get_progress_file()}\""
    # print(expected_cmd)
    assert utils.get_rsync_command(file, job_dir) == expected_cmd

def test_is_enough_free_space_on_server():
    # test with a fake free space that is below the limit
    assert utils.is_enough_free_space_on_server(utils.STORAGE_FREE_LIMIT - 1000) == False
    # test with a fake free space that is above the limit, because it returns True it should record the time
    assert utils.is_enough_free_space_on_server(utils.STORAGE_FREE_LIMIT + 1000) == True
    # test with a fake free space that is below the limit, it should return False but because a True was returned just before it will say True
    utils.wait(1)
    assert utils.is_enough_free_space_on_server(utils.STORAGE_FREE_LIMIT - 1000) == True

def test_read_progress_file():
    # make a copy of the sample file
    shutil.copyfile("cumulus_rsync/test/.cumulus.progress.test", utils.get_progress_file())
    # read the file
    [file, progress] = utils.read_progress_file()
    # test the output
    assert file == "TP4806_Slot2-1_1_4818.d"
    assert progress == 94249694

def test_extract_from_settings():
    settings = {"job_id": "1", "job_dir": "Job_1", "owner": "test.user", "files": '["File1.raw","File2.raw","File3.raw"]', "local_files": '["File.fasta"]'}
    job_id, job_dir, owner, shared_files, local_files = utils.extract_from_settings(settings)
    assert job_id == "1"
    assert job_dir == "Job_1"
    assert owner == "test.user"
    assert len(shared_files) == 3
    assert len(local_files) == 1

def test_get_progress_for_job():
    files = [["cumulus_rsync/test/TP4806_Slot2-1_1_4818.d", 94249694 * 1.25]]
    dict = utils.get_progress_for_job(2, files)
    assert dict["TP4806_Slot2-1_1_4818.d"] == 80

def test_delete_progress_file():
    # test that file exists
    assert os.path.isfile(utils.get_progress_file())
    # delete it
    utils.delete_progress_file()
    # test that file no longer exists
    assert os.path.isfile(utils.get_progress_file()) == False
