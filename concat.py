#!/usr/bin/env python

"""
Concatenate all csv files within a measurement type folder and place resulting
csv into a folder named `merged` within the NDAR folder.
This script requires 2 arguments, the root directory for all site data, and a
location to store a file containing the last time this script was runself.

Usage: python concat.py <root dir containing sites> <timestamp location>
eg: python concat.py /external_data/fitabase-data /external_data/fitabase-data/last_process.txt
"""

from __future__ import print_function
import csv
import sys
import os
import math
import pandas as pd
from time import time


def get_timestamp(file):
    """Get timestamp from last processing job.
    If timestamp file does not exist yet (ie on first run),
    then return None.
    """
    if os.path.isfile(file):
        fp = open(file, 'r')
        timestamp = float(fp.read())
        fp.close()
    else:
        timestamp = None
    return timestamp

def find_dirs(root_dir, timestamp, ignore_dirs):
    """Find all the required folders to proccess that contain data
    files created after the last time this concat script was run.
    """
    # Use DFS to get bottom most sub-dir as long at they are not in list of dirs to be exluded from
    sites = {os.path.join(root_dir, i) for i in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, i)) and i not in ignore_dirs}
    folders = set()

    for folder in sites:
        for root, dirs, files in os.walk(folder):
            if not dirs and os.path.basename(root) != "merged":
                folders.add(root)
    return folders

def column_to_index(folder_name):
    """Helper function to determine which column
    to use as index for combining dataframes.
    """
    if folder_name in {"30secondSleepStages", "heartrate_1min", "heartrate"}:
        return "Time"
    elif folder_name in {"minuteCaloriesNarrow", "minuteIntensitiesNarrow", "minuteMETsNarrow", "minuteStepsNarrow"}:
        return "ActivityMinute"
    elif folder_name == "minuteSleep":
        return "date"
    elif folder_name in {"sleepDay", "sleepStagesDay"}:
        return "SleepDay"
    elif folder_name in {"activitylogs", "sleepLogInfo", "sleepStageLogInfo"}:
        return "StartTime"
    elif folder_name == "battery":
        return "DateTime"
    elif folder_name == "dailyActivity":
        return "ActivityDate"
    elif folder_name == "dailySteps":
        return "ActivityDay"

def process_concat(folders, timestamp):
    """Find which data files were created since last job
    and concatenate them into 'merged.csv' within that folder.
    """
    for folder in folders:
        # Merged folder:
        ndar_folder = os.path.dirname(folder)
        merged_folder = os.path.join(ndar_folder, "merged")
        folder_name = os.path.basename(folder)
        merged_file_loc = os.path.join(merged_folder, folder_name + ".csv")
        if not os.path.isdir(merged_folder):
            os.makedirs(merged_folder)

        # Find which files to process - do all if merged file not found
        if os.path.isfile(merged_file_loc):
            files_to_process = [file for file in os.listdir(folder) if os.path.getmtime(os.path.join(folder, file)) > timestamp]
        else:
            files_to_process = [file for file in os.listdir(folder)]
        files_to_process.sort()
        # Stop if no new files
        if len(files_to_process) < 1:
            continue

        if os.path.isfile(merged_file_loc):
            merged_df = pd.read_csv(merged_file_loc)
        else:
            merged_df = pd.read_csv(os.path.join(folder, files_to_process[0]))
        # Get column name to merge on
        index_key = column_to_index(folder_name)
        for file in files_to_process:
            file_df = pd.read_csv(os.path.join(folder, file))
            merged_df = file_df.set_index(index_key, drop = False).combine_first(merged_df.set_index(index_key, drop = False))

        # Don't actually create the file if there is no data
        if merged_df.empty:
            merged_df.to_csv(merged_file_loc, index = False)

def log_timestamp(file, start_time):
    """Output the time this processing job began
    to the supplied timestamp file for next job.
    """
    fp = open(file, "w")
    fp.write(str(start_time))
    fp.close()

def main():
    """Main function to collate all the other functions into one call.
    """
    # Housekeeping
    if len(sys.argv) != 3:
        print("Error! Please specify the root location of sites and a file location to store processing timestamp. \nUsage: concatenate_data.py <root directory of fitabase data> <timestamp file location>", file = sys.stderr)
        exit(-1)
    # Set up varialbes for functions from system arguements
    root_dir = sys.argv[1]
    timestamp_file_loc = sys.argv[2]
    # Sub-folders to ignore
    pilot_studies = {"2018-Q2-pilot", "2018-Q3-testing"}
    # Record when this job started
    start_time = time()
    # Start job
    timestamp = get_timestamp(timestamp_file_loc)
    folders_to_process = find_dirs(root_dir, timestamp, pilot_studies)
    process_concat(folders_to_process, timestamp)
    log_timestamp(timestamp_file_loc, start_time)

if __name__ == "__main__":
    main()
