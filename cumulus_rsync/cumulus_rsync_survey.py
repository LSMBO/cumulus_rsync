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
IS_SURVEY_DONE = False
MIN_AGE_IN_HOURS = 2
MAX_AGE_IN_HOURS = 36

# the functions will be called from the main.daemon() loop
def is_time_to_survey(survey_time):
    global IS_SURVEY_DONE
    # reset the boolean if the day has changed since the last survey
    if IS_SURVEY_DONE and time.strftime("%H:%M") < survey_time: IS_SURVEY_DONE = False
    # return True if the time is right and the survey has not been done yet
    logger.debug(f"Survey time: {survey_time}, current time: {time.strftime('%H:%M')}, survey done: {IS_SURVEY_DONE}")
    return time.strftime("%H:%M") >= survey_time and IS_SURVEY_DONE == False

def set_surveyed_today():
    global IS_SURVEY_DONE
    IS_SURVEY_DONE = True

# this function is only needed for tests
def change_file_min_age(min_age):
    global MIN_AGE_IN_HOURS
    MIN_AGE_IN_HOURS = min_age

# this function is only needed for tests
def change_file_max_age(max_age):
    global MAX_AGE_IN_HOURS
    MAX_AGE_IN_HOURS = max_age

def is_valid(file_path, isfile, regex):
    # do not consider folders if we are expecting files and vice versa
    if isfile and not os.path.isfile(file_path): 
        # print(f"File '{file_path}' is not a file")
        return False
    if not isfile and not os.path.isdir(file_path): 
        # print(f"File '{file_path}' is not a directory")
        return False
    # do not consider the files that do not match the regex
    if not re.match(regex, os.path.basename(file_path)): 
        # print(f"File '{file_path}' does not match the regex '{regex}'")
        return False
    # do not consider the files that are too older than 36 hours (only files from the last 24 hours will be actually sent)
    if MAX_AGE_IN_HOURS >= 0 and os.path.getmtime(file_path) < time.time() - MAX_AGE_IN_HOURS * 3600: 
        # print(f"File '{file_path}' is too old")
        return False
    # do not consider the files that are too recent (less than 2 hours) to avoid sending files that are still in acquisition
    if os.path.getmtime(file_path) > time.time() - MIN_AGE_IN_HOURS * 3600: 
        # print(f"File '{file_path}' is too recent")
        return False
    return True

def parse_folder(path, isfile, regex, depth, max_depth):
    files = []
    if depth < max_depth:
        for file in os.listdir(path):
            # get the full path of the file
            file_path = os.path.join(path, file)
            # add the file/folder if it is valid
            if is_valid(file_path, isfile, regex):
                logger.info(f"Surveyed file will be uploaded to the server: '{file_path}'")
                files.append(file_path)
            elif os.path.isdir(file_path):
                files += parse_folder(file_path, isfile, regex, depth + 1, max_depth)
    return files

def survey_directories(directories, depth = 1):
    files = []
    # loop over the directories and return the files that appeared in the last 36 hours
    for name, data in directories.items():
        logger.info(f"Surveying directory {name}")
        # get the list of files for this directory and its subdirectories
        files += parse_folder(data["dir"], data["isfile"], data["regex"], 0, depth)
    return files
