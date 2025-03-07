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
import time
import cumulus_rsync.cumulus_rsync_survey as survey

def test_is_time_to_survey():
    assert survey.SURVEY_DONE == False
    # create a survey time 1 hour in the past
    survey_time_before = time.strftime("%H:%M", time.localtime(time.time() - 3600))
    # check the time before the survey time
    assert survey.is_time_to_survey(survey_time_before) == True
    # create a survey time 1 hour in the future
    survey_time_after = time.strftime("%H:%M", time.localtime(time.time() + 3600))
    # check the time after the survey time
    assert survey.is_time_to_survey(survey_time_after) == False

def test_set_surveyed_today():
    assert survey.SURVEY_DONE == False
    survey.set_surveyed_today()
    assert survey.SURVEY_DONE == True

def test_survey_directories():
    # create a fake survey map
    directories = { "test": {"dir": "cumulus_rsync/test", "isfile": True, "regex": "File.*\\.raw"} }
    files = survey.survey_directories(directories)
    assert len(files) == 0 # all the files are too old
    # create fake files
    open("cumulus_rsync/test/File4.raw", "w").close()
    open("cumulus_rsync/test/File4.d", "w").close()
    files = survey.survey_directories(directories)
    assert len(files) == 1 # only one file is recent
    # remove the fake files
    os.remove("cumulus_rsync/test/File4.raw")
    os.remove("cumulus_rsync/test/File4.d")
