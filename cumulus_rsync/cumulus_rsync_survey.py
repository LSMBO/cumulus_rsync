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
import re
import time

logger = logging.getLogger(__name__)

# this variable is local, not from the config file
SURVEY_DONE = False

# the functions will be called from the main.daemon() loop
def is_time_to_survey(survey_time):
    global SURVEY_DONE
    # reset the boolean if the day has changed since the last survey
    if SURVEY_DONE and time.strftime("%H:%M") < survey_time: SURVEY_DONE = False
    # return True if the time is right and the survey has not been done yet
    return time.strftime("%H:%M") >= survey_time and SURVEY_DONE == False

def set_surveyed_today():
    global SURVEY_DONE
    SURVEY_DONE = True

def survey_directories(directories):
    files = []
    # loop over the directories and return the files that appeared in the last 24 hours
    for name, data in directories.items():
        logger.info(f"Surveying directory {name}")
        # get the list of files
        for file in os.listdir(data["dir"]):
            # get the full path of the file
            file_path = os.path.join(data["dir"], file)
            # only consider the right type of data
            if data["isfile"] and not os.path.isfile(file_path): continue
            if not data["isfile"] and not os.path.isdir(file_path): continue
            # only consider the files that match the regex
            if not re.match(data["regex"], file): continue
            # only consider the files that are less than 24 hours old
            if os.path.getmtime(file_path) < time.time() - 24 * 3600: continue
            # add the full path to the list
            logger.info(f"Surveyed file will be uploaded to the server: '{file_path}'")
            files.append(file_path)
    return files
