#!/usr/bin/env python
"""
When pointed at a directory, classify files into measures based on filename, 
load them, process the data, and output it or upload it to Redcap.

"""

# todo: add day of the week (for first day) fits_ss_day_of_week_day00

import pycurl, cStringIO, json, sys, re, os, math
import argparse
from array import array
from StringIO import StringIO
from time import sleep
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Just to must pandas warnings
pd.options.mode.chained_assignment = None

def parse_args():
    parser = argparse.ArgumentParser(
            description="Score all participants for a given site")
    parser.add_argument('site', default='VCU', help='ABCD site abbreviation')
    parser.add_argument('--force', '-f', help='Upload result to Redcap')
    parser.add_argument('--ignore-site', action='store_true',
            help='Score all participants, regardless of site (no Redcap pull')
    parser.add_argument('--input', '-i', default='test_data',
            help='Path to folder with Fitabase exports')
    parser.add_argument('--verbose', '-v')
    return parser.parse_args()


def redcap_site_subjects(token):
    buf = cStringIO.StringIO()
    # variables we need from REDCap
    data = {
        'token': tokens[site],
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'fields[0]': 'id_redcap',
        'fields[1]': 'redcap_event_name',
        'fields[2]': 'fitc_device_dte',
        'events[0]': 'baseline_year_1_arm_1',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'true',
        'returnFormat': 'json'
    }
    ch = pycurl.Curl()
    ch.setopt(ch.URL, 'https://abcd-rc.ucsd.edu/redcap/api/')
    ch.setopt(ch.HTTPPOST, data.items())
    ch.setopt(ch.WRITEFUNCTION, buf.write)
    ch.perform()
    ch.close()

    v = json.load( StringIO(buf.getvalue()) )
    buf.close()
    # We here we have a list of the valid participants for this site
    # out = v
    return v

def collect_fitabase_files_from_folder(folder):
    fitabase_files = {}
    # for root, dirs, files in os.walk("data_SubStudy"):
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".csv"):
                sp = file.split("_")
                pGUID = sp[0].strip()
                if sp[0] == "NDAR":
                    pGUID = pGUID + "_" + sp[1]
                    pGUID = pGUID.strip()
                if not(pGUID in fitabase_files):
                    fitabase_files[pGUID] = []
                fitabase_files[pGUID].append( { 'filename': os.path.join(root, file), 'pGUID': pGUID, 'processed': False } )
    return fitabase_files

# find sets of files for one participant that have the same date range, import those together
def dateRangeSet( files ):
    dateranges    = []
    daterangesets = []
    for entry in files:
        file = entry['filename']
        fn = os.path.basename(file)
        fn = os.path.splitext(fn)[0]
        # example: NDAR_INV0AU5R8NA_30secondSleepStages_20160308_20180408.csv
        fn = fn.split("_")
        daterange = fn[-2] + "_" + fn[-1]
        if not(daterange in dateranges):
            dateranges.append(daterange)
    # now sort the files into the different date ranges
    for thisdaterange in dateranges:
        l = []
        for entry in files:
            file = entry['filename']
            fn = os.path.basename(file)
            fn = os.path.splitext(fn)[0]
            # example: NDAR_INV0AU5R8NA_30secondSleepStages_20160308_20180408.csv
            fn = fn.split("_")
            daterange = fn[-2] + "_" + fn[-1]
            if daterange == thisdaterange:
                l.append(entry)
        daterangesets.append(l)    
    return daterangesets

# OK: remove seconds from datetime stamp
# don't use 1, 2, 3 (sleep) for METs Steps everything
# Intensities:
#    level 1 is 0, level 2 is 3 - 0 sedetary


# wearminutes 0 or smaller 600 (not larger 1200), put mean in there but count for week only > 600 (weekend/weekday)
# OK: METS / 10 .. why is value 31 or a decimals?

# week sum is not correct

# NEXT:

# 600 minutes per day, don't count the activity minutes for the week, weekend weekday, number of valid days
# sleep minutes, weekly averages don't include if < 300

# resting heart rate, All Daily Activity Export option "dailyRestingRate"

# difference between blank and zero
#   fits_ss_intensityminutes_sedentary_day01 
#   should start with 0, not have a value if there is no heart rate for that day
#   May 24th Train the Trainer

# does not get 600 minutes, second day

# how to create alerts
#   preferred method of contact in REDCap

def normalizeDate( t, column ):
    # normalize the Time entry to remove seconds
    t[column] = t[column].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p').strftime('%m/%d/%Y %H:%M'))
        
def normalizeDate30( t, column ):
    # normalize the Time entry to remove seconds / duplicate the SleepStage column to store the second entry in column+"30"
    # should be 00 or 30 to indicate which row is for which participant
    t['slice'] = t[column]
    t['slice'] = t['slice'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p').strftime('%S'))
    # get two tables for 00 and 30
    table1 = t[t['slice'] == "00"]
    # strip the seconds
    table1[column] = table1[column].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p').strftime('%m/%d/%Y %H:%M'))
    table2 = t[t['slice'] == "30"]
    # strip the seconds
    table2[column] = table2[column].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p').strftime('%m/%d/%Y %H:%M'))
    # rename to SleepStage30
    table2 = table2.rename(columns={'SleepStage': 'SleepStage30'})
    # now merge both by time to get one Time, a 'SleepStage' and a 'SleepStage30'
    table3 = pd.merge(table1, table2, left_on=column, right_on=column, how="left")
    # return the new table
    return table3[[column,'SleepStage','SleepStage30']]


        

def prepare_heart_rate(hrdata):
    # normalize the Time entry to remove seconds
    normalizeDate(hrdata, 'Time')

    # data is now in HR_Value instead of Value
    hrdata.rename(columns={'Value': 'HR_Value' }, inplace=True)
    
    # we should check the hrdata and remove any entries that stay the same for 6 or more consecutive minutes
    bunchStart = 0       # start index
    bunch = 0            # count number of same hr values
    bunchvalue = ""      # values that is the same
    removeThese = False  # got more than 6 in a row
    removeIdxArray = list()
    for index, row in hrdata.iterrows():
        if row['HR_Value'] != bunchvalue:
            if removeThese:
                # remove the rows from bunchStart to index-1
                removeIdxArray.extend(list(range(bunchStart, index)))
                # only remove elements at the end
                # hrdata.drop(hrdata.index[range(bunchStart,index)])
            bunchStart = index
            removeThese = False
            bunch = 0
            bunchvalue = row['HR_Value']
        else:
            bunch = bunch + 1
        if bunch >= 10:
            # same value for 10 or more rows
            removeThese = True
    if len(removeIdxArray) > 0:
        print("Found %d entries that need to be removed because they are more than 10 identical consecutive HR values for pGUID: %s [%s]" % (len(removeIdxArray), pGUID, site))
    hrdata.drop(hrdata.index[removeIdxArray])
    # lets remove entries not inside 50..210
    removeIdxArray = list()
    def getValidHR(idx):
        x = hrdata['HR_Value'][idx]
        if (x<50) or (x>210):
            removeIdxArray.append(idx)
    map(getValidHR, hrdata['HR_Value'].index) # apply to each row
    if len(removeIdxArray) > 0:
        print("Found %d entries not between 50..210HR for pGUID: %s [%s]" % (len(removeIdxArray), pGUID, site))

    #for index, row in hrdata.iterrows():
    #    if (row['HR_Value'] < 40) or (row['HR_Value'] > 210):
    #        removeIdxArray.append(index)
    hrdata.drop(hrdata.index[removeIdxArray])
    return hrdata


def run_pandas_prep(validsteps, date0, min_day=-1, max_day=22):
    validsteps.loc[:, 'Datetime'] = pd.to_datetime(validsteps['Time'], format='%m/%d/%Y %H:%M')
    validsteps['dayofweek'] = validsteps['Datetime'].dt.dayofweek
    validsteps['start_delta'] = validsteps['Datetime'] - date0
    validsteps['start_delta_days'] = validsteps['start_delta'].dt.days

    if min_day and max_day:
        return validsteps[(validsteps['start_delta_days'] > min_day) & 
                (validsteps['start_delta_days'] < max_day)]
    else:
        return validsteps


def get_valid_steps(timerange):
    # import the data
    try:
        heartrate = [item['filename'] for item in timerange if 'heartrate_1min' in item['filename']][0]
    except IndexError:
        print("Error: timerange without heartrate_1min " + str([item['filename'] for item in timerange]))
        return False
        
    hrdata = pd.read_csv(heartrate).pipe(prepare_heart_rate)
    
    steps = [item['filename'] for item in timerange if 'minuteStepsNarrow' in item['filename']][0]
    stdata = pd.read_csv(steps)
    normalizeDate(stdata, 'ActivityMinute') # remove the seconds fom this date
    # data is in Steps
    
    # metabolic equivalents (METs)
    mets = [item['filename'] for item in timerange if 'minuteMETsNarrow' in item['filename']][0]
    medata = pd.read_csv(mets)
    normalizeDate(medata, 'ActivityMinute')
    
    sl = [item['filename'] for item in timerange if 'minuteSleep' in item['filename']][0]
    sldata = pd.read_csv(sl)
    # rename the columns for the sleep valuea
    sldata.rename(columns={'value': 'sleep_value', 'logId': 'sleep_logId' }, inplace=True)
    normalizeDate(sldata, 'date')

    # add physical activity minuteIntensitiesNarrow (level 0, 1, 2, 3) -> get per day number in that activity state
    inte = [item['filename'] for item in timerange if 'minuteIntensitiesNarrow' in item['filename']][0]
    indata = pd.read_csv(inte)
    normalizeDate(indata, 'ActivityMinute')

    # add 30second sleep stages (SleepStage,SleepStage30)
    sleep30 = [item['filename'] for item in timerange if '30secondSleepStages' in item['filename']][0]
    sleep30data = pd.read_csv(sleep30)
    sleep30data = normalizeDate30(sleep30data, 'Time')
    
    #print("found heartrate file: " + heartrate)
    #print("found steps file: " + steps)
    # only get measures if the heart rate data exists for this minute
    validsteps1 = pd.merge(hrdata, stdata, left_on="Time", right_on="ActivityMinute", how="left")
    validsteps1 = validsteps1[['Time', 'HR_Value', 'Steps']]
    validsteps2 = pd.merge(hrdata, medata, left_on="Time", right_on="ActivityMinute", how="left")
    validsteps2 = validsteps2[['Time', 'METs']]
    validsteps3 = pd.merge(hrdata, sldata, left_on="Time", right_on="date",           how="left")
    validsteps3 = validsteps3[['Time', 'sleep_value', 'sleep_logId']]
    validsteps4 = pd.merge(hrdata, indata, left_on="Time", right_on="ActivityMinute", how="left")
    validsteps4 = validsteps4[['Time', 'Intensity']]
    validsteps5 = pd.merge(hrdata, sleep30data, left_on="Time", right_on="Time", how="left")
    validsteps5 = validsteps5[['Time', 'SleepStage', 'SleepStage30']]
    # merge all together, even if some data is missing
    validsteps  = pd.merge(validsteps1, validsteps2, left_on="Time", right_on="Time", how="outer")
    validsteps  = pd.merge(validsteps, validsteps3, left_on="Time", right_on="Time",  how="outer")
    validsteps  = pd.merge(validsteps, validsteps4, left_on="Time", right_on="Time",  how="outer")
    validsteps  = pd.merge(validsteps, validsteps5, left_on="Time", right_on="Time",  how="outer")

    return validsteps


def process_timerange(pGUID, timerange, date0): 
    """
    Load the raw data from per-subject files, return processed scores in a list 
    of Redcap upload-ready JSON-like dicts (i.e. each dict has keys id_redcap 
    and redcap_event_name alongside the keys for whatever scores were computed.

    pGUID: id_redcap
    timerange: list of dicts. Each dict has keys:
        - filename (path to CSV)
        - pGUID 
        - processed (bool) - already set to True by the time they're passed 
          into the function
    date0: Datetime on which the device was first worn; score day after
    """
    scores = []

    validsteps = get_valid_steps(timerange).pipe(run_pandas_prep, date0)
    # valid steps should filter by sleep value (don't count steps if the sleep value is 0 or above)
    
    #print("merged data for participant: " + pGUID + " is:")
    # print(validsteps.to_string())
    # relative to the date0 lets see what the days are that we can sum steps for
    # we need steps for week since date0, day of week, weekend, weekend
    sumStepsPerDay = {}
    sumHRPerDay    = {}
    sumHRPerOverall = {}
    sumHRPerSleep  = {}
    sumMETPerDay   = {}
    sumSleepPerDay = {}
    sumSleepStagePerDay = {}
    sumIntePerDay  = {}
    bedtimePerDay  = {}
    for index, row in validsteps.iterrows():
        dat = row['Time']
        date1 = datetime.strptime(dat, '%m/%d/%Y %H:%M') # datetime.strptime(dat, '%m/%d/%Y %I:%M:%S %p')
        dayreldate0 = (date1-date0).days
        # Monday is 0 .. Friday 4, Saturday 5, Sunday is 6
        weekday     = date1.weekday()
        #print("Found time stamp " + str(dayreldate0) + " days from the event date this is on weekday " + str(date1.weekday()))
        vname = dayreldate0
        #if (vname == 0) and (weekday == 0):
        #    print("Found weekday 0 value for vname " + str(vname) + " pGUID: " + pGUID )

        if (row['sleep_value'] == "") or math.isnan(float(row['sleep_value'])):  # do not count sleep as anything (0, 1 sleep, 2)
            # SUM STEPS
            if not(vname in sumStepsPerDay):
                if ('Steps' in row) and (row['Steps'] > 0):
                    sumStepsPerDay[vname] = { 'weekday': weekday, 'steps': row['Steps'] }
                else:
                    sumStepsPerDay[vname] = { 'weekday': weekday, 'steps': 0 }
            else:
                if ('Steps' in row) and (row['Steps'] > 0):
                    sumStepsPerDay[vname]['steps'] = sumStepsPerDay[vname]['steps'] + row['Steps']
            # SUM HR
            if not(vname in sumHRPerDay):
                if ('HR_Value' in row) and (row['HR_Value'] > 0):
                    sumHRPerDay[vname] = { 'weekday': weekday, 'HR': row['HR_Value'], 'N': 1 }
                else:
                    sumHRPerDay[vname] = { 'weekday': weekday, 'HR': 0, 'N': 0 }
            else:
                if ('HR_Value' in row) and (float(row['HR_Value']) > 0):
                    # print("HR: %d now add %d" % (sumHRPerDay[vname]['HR'], row['HR_Value'])) 
                    sumHRPerDay[vname]['HR'] = sumHRPerDay[vname]['HR'] + row['HR_Value']
                    sumHRPerDay[vname]['N']  = sumHRPerDay[vname]['N'] + 1
            # SUM METS
            if not(vname in sumMETPerDay):
                if ('METs' in row) and (row['METs'] > 0):
                    sumMETPerDay[vname] = { 'weekday': weekday, 'mets': row['METs']/10.0, 'N': 1 }
                else:
                    sumMETPerDay[vname] = { 'weekday': weekday, 'mets': 0, 'N': 0 }
            else:
                if ('METs' in row) and (row['METs'] > 0):
                    sumMETPerDay[vname]['mets'] = sumMETPerDay[vname]['mets'] + (row['METs']/10.0)
                    sumMETPerDay[vname]['N'] = sumMETPerDay[vname]['N'] + 1
            # SUM INTENSITIES
            if not(vname in sumIntePerDay):
                if ('Intensity' in row) and (row['Intensity'] != "") and not(math.isnan(float(row['Intensity']))):
                    sumIntePerDay[vname] = { 'weekday': weekday, 'intensity0': 0, 'intensity1': 0, 'intensity2': 0, 'intensity3': 0, 'N': 1 }
                else:
                    sumIntePerDay[vname] = { 'weekday': weekday, 'intensity0': 0, 'intensity1': 0, 'intensity2': 0, 'intensity3': 0, 'N': 0 }
            else:
                if ('Intensity' in row) and (row['Intensity'] != "") and not(math.isnan(float(row['Intensity']))):
                    vname2 = "intensity%d" % row['Intensity']
                    sumIntePerDay[vname][vname2] = sumIntePerDay[vname][vname2] + 1  # how many minutes of this sleep value
                    sumIntePerDay[vname]['N'] = sumIntePerDay[vname]['N'] + 1  # how many minutes of sleep for this day
        else:
            # calculate bedtimePerDay
            midday = timedelta(hours=12,minutes=0)
            h = datetime.strptime(row['Time'], '%m/%d/%Y %H:%M').hour
            m = datetime.strptime(row['Time'], '%m/%d/%Y %H:%M').minute
            # difference in minutes
            minutesFromMidday = (timedelta(hours=h,minutes=m) - midday).total_seconds() / 60
            minutesFromMidnight = (timedelta(hours=h,minutes=m) - timedelta(hours=0,minutes=0)).total_seconds() / 60
            if ('HR_Value' in row) and (row['HR_Value'] > 0) and (vname > -1) and (vname < 22):
                if not(vname in bedtimePerDay):
                    if minutesFromMidday > 0:
                        bedtimePerDay[vname] = { 'laydown': row['Time'], 'laydown_diff': minutesFromMidday,
                                                 'earliestSleep': row['Time'], 'earliestSleep_diff': minutesFromMidnight }
                    else:
                        bedtimePerDay[vname] = { 'laydown': '', 'laydown_diff': 0,
                                                 'earliestSleep': row['Time'], 'earliestSleep_diff': minutesFromMidnight }
                else:
                    if minutesFromMidday > 0:
                        # if we are in the afternoon find the earliest sleep time
                        if bedtimePerDay[vname]['laydown'] == '':
                            # fill in default values to have something
                            bedtimePerDay[vname]['laydown_diff'] = minutesFromMidday
                            bedtimePerDay[vname]['laydown'] = row['Time']
                            
                        if minutesFromMidday < bedtimePerDay[vname]['laydown_diff']:
                            # found earlier time (closer to noon), replace current time
                            bedtimePerDay[vname]['laydown_diff'] = minutesFromMidday
                            bedtimePerDay[vname]['laydown'] = row['Time']
                    else:
                        # if we are in the morning we can see if we have an earlier sleep time for the previous day (if going to sleep after midnight)
                        if minutesFromMidnight < bedtimePerDay[vname]['earliestSleep_diff']:
                            bedtimePerDay[vname]['earliestSleep_diff'] = minutesFromMidday
                            bedtimePerDay[vname]['earliestSleep'] = row['Time']
                                                    
            # SUM HR sleep
            if not(vname in sumHRPerSleep):
                if ('HR_Value' in row) and (row['HR_Value'] > 0):
                    sumHRPerSleep[vname] = { 'weekday': weekday, 'HR': row['HR_Value'], 'N': 1 }
                else:
                    sumHRPerSleep[vname] = { 'weekday': weekday, 'HR': 0, 'N': 0 }
            else:
                if ('HR_Value' in row) and (float(row['HR_Value']) > 0):
                    # print("HR: %d now add %d" % (sumHRPerDay[vname]['HR'], row['HR_Value'])) 
                    sumHRPerSleep[vname]['HR'] = sumHRPerSleep[vname]['HR'] + row['HR_Value']
                    sumHRPerSleep[vname]['N']  = sumHRPerSleep[vname]['N'] + 1
                    
        if not(vname in sumHRPerOverall):
            if ('HR_Value' in row) and (row['HR_Value'] > 0):
                sumHRPerOverall[vname] = { 'weekday': weekday, 'HR': row['HR_Value'], 'N': 1 }
            else:
                sumHRPerOverall[vname] = { 'weekday': weekday, 'HR': 0, 'N': 0 }
        else:
            if ('HR_Value' in row) and (float(row['HR_Value']) > 0):
                # print("HR: %d now add %d" % (sumHRPerDay[vname]['HR'], row['HR_Value'])) 
                sumHRPerOverall[vname]['HR'] = sumHRPerOverall[vname]['HR'] + row['HR_Value']
                sumHRPerOverall[vname]['N']  = sumHRPerOverall[vname]['N'] + 1

        # SUM SLEEP
        if not(vname in sumSleepPerDay):
            if ('sleep_value' in row) and (row['sleep_value'] != "") and not(math.isnan(float(row['sleep_value']))):
                sumSleepPerDay[vname] = { 'weekday': weekday, 'sleep1': 0, 'sleep2': 0, 'sleep3': 0, 'sleep4': 0, 'N': 1 }
            else:
                sumSleepPerDay[vname] = { 'weekday': weekday, 'sleep1': 0, 'sleep2': 0, 'sleep3': 0, 'sleep4': 0, 'N': 0 }
        else:
            if ('sleep_value' in row) and (row['sleep_value'] != "") and not(math.isnan(float(row['sleep_value']))):
                vname2 = "sleep%d" % row['sleep_value']
                sumSleepPerDay[vname][vname2] = sumSleepPerDay[vname][vname2] + 1  # how many minutes of this sleep value
                sumSleepPerDay[vname]['N'] = sumSleepPerDay[vname]['N'] + 1  # how many minutes of sleep for this day
        # SUM SLEEP STAGES (counts in .5)
        if not(vname in sumSleepStagePerDay):
            if ('SleepStage' in row) and (row['SleepStage'] != "") and (str(row['SleepStage']) != "nan"):
                sumSleepStagePerDay[vname] = { 'weekday': weekday, 'light': 0, 'wake': 0, 'rem': 0, 'deep': 0 }
                sumSleepStagePerDay[vname][row['SleepStage']] = sumSleepStagePerDay[vname][row['SleepStage']] + .5
            else:
                sumSleepStagePerDay[vname] = { 'weekday': weekday, 'light': 0, 'wake': 0, 'rem': 0, 'deep': 0 }
            if ('SleepStage30' in row) and (row['SleepStage30'] != "") and (str(row['SleepStage30']) != "nan"):
                sumSleepStagePerDay[vname][row['SleepStage30']] = sumSleepStagePerDay[vname][row['SleepStage30']] + .5
        else:
            if ('SleepStage' in row) and (row['SleepStage'] != "") and (str(row['SleepStage']) != "nan"):
                vname2 = row['SleepStage']
                sumSleepStagePerDay[vname][vname2] = sumSleepStagePerDay[vname][vname2] + .5  # how many minutes of this sleep value
            if ('SleepStage30' in row) and (row['SleepStage30'] != "") and (str(row['SleepStage30']) != "nan"):
                vname2 = row['SleepStage30']
                sumSleepStagePerDay[vname][vname2] = sumSleepStagePerDay[vname][vname2] + .5  # how many minutes of this sleep value

    #
    # Processing into JSON-friendly score items starts here
    #
    # (Given the DataFrame 
    #
    for idx, entry in bedtimePerDay.items():
        if (idx > -1) and (idx < 22):
            vname1 = "fits_ss_bedtime_day%02d" % (idx)
            timeString = ''
            if (bedtimePerDay[idx]['laydown'] == '') and ((idx+1) in bedtimePerDay):
                # if we don't have a laydown time we need to check in the next day if there is earliestSleep time instead
                # if we don't have a next day, leave the value empty (no day 22 information)
                timeString = datetime.strptime(bedtimePerDay[idx+1]['earliestSleep'], '%m/%d/%Y %H:%M').strftime('%H:%M')
            else:
                # if we do have a laydown time use it
                if bedtimePerDay[idx]['laydown'] != '':
                    timeString = datetime.strptime(bedtimePerDay[idx]['laydown'], '%m/%d/%Y %H:%M').strftime('%H:%M')
            scores.append({ 'id_redcap': u['id_redcap'],
                            'redcap_event_name': u['redcap_event_name'],
                            vname1: timeString })
                
    # get the weekday of the first day
    for idx, entry in sumHRPerDay.items():
        if (idx == 0):
            vname = "fits_ss_day_of_week_day00"
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: entry['weekday'] })        
                
    for idx, entry in sumStepsPerDay.items():
        # do not count day 0, start counting at day 1
        if (idx > -1) and (idx < 22):
            vname = "fits_ss_steps_day%02d" % idx
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: entry['steps'] })
    for idx, entry in sumMETPerDay.items():
        if (idx > -1) and (idx < 22):
            vname = "fits_ss_mets_day%02d" % idx
            vname2 = "fits_ss_metsminutes_day%02d" % idx
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: "%.2f" % (entry['mets']/(entry['N'] if entry['N']>0 else 1)), vname2: "%.2f" % entry['mets'] })
    for idx, entry in sumHRPerDay.items():
        if (idx > -1) and (idx < 22):
            vname = "fits_ss_hr_awake_day%02d" % idx
            vname2 = "fits_ss_wearminutes_day%02d" % idx
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: "%.2f" % (entry['HR']/(entry['N'] if entry['N']>0 else 1)), vname2: int(entry['N']) })
    for idx, entry in sumHRPerSleep.items():
        if (idx > -1) and (idx < 22):
            vname = "fits_ss_hr_sleep_day%02d" % idx
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: "%.2f" % (entry['HR']/(entry['N'] if entry['N']>0 else 1)) })
    for idx, entry in sumHRPerOverall.items():
        if (idx > -1) and (idx < 22):
            vname = "fits_ss_hr_overall_day%02d" % idx
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: "%.2f" % (entry['HR']/(entry['N'] if entry['N']>0 else 1)) })
    for idx, entry in sumIntePerDay.items():
        if (idx > -1) and (idx < 22):
            # here we use textual values instead of 0 to 3 which is used in the file
            vname20 = "fits_ss_intensityminutes_sedentary_day%02d" % idx
            vname21 = "fits_ss_intensityminutes_light_day%02d" % idx
            vname22 = "fits_ss_intensityminutes_moderate_day%02d" % idx
            vname23 = "fits_ss_intensityminutes_vigorous_day%02d" % idx
            # to total number of intensity minutes during daytime (remove for now)
            vname00 = "fits_ss_intensity_day%02d" % idx
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname20: entry['intensity0'], vname21: entry['intensity1'], vname22: entry['intensity2'], vname23: entry['intensity3'] })
    #
    # SLEEP on minute level
    #
    #for idx, entry in sumSleepPerDay.items():
    #    if (idx > -1) and (idx < 22):
    #        vname21 = "fits_ss_sleepminutes_asleep_day%02d" % idx
    #        vname22 = "fits_ss_sleepminutes_restless_day%02d" % idx
    #        vname23 = "fits_ss_sleepminutes_awake_day%02d" % idx
    #        vname00 = "fits_ss_sleepminutes_day%02d" % idx
    #        scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname00: int(entry['N']), vname21: entry['sleep1'], vname22: entry['sleep2'], vname23: entry['sleep3'] })
    #sumSleepPerWeek = {}
    #sumSleepPerWeekend = { 'sleep1': 0, 'N1': 0, 'sleep2': 0, 'N2': 0, 'sleep3': 0, 'N3': 0 }
    #sumSleepPerWeekday = { 'sleep1': 0, 'N1': 0, 'sleep2': 0, 'N2': 0, 'sleep3': 0, 'N3': 0 }
    #for idx, entry in sumSleepPerDay.items():
    #    if (idx > -1) and (idx < 22):
    #        # we should only count sleep days if they had more than 300 minutes of total sleep per day
    #        if entry['sleep1'] + entry['sleep2'] + entry['sleep3'] < 300:
    #            # skip this day during the calculations
    #            continue
    #        week = (idx-1) // 7
    #        if not(week in sumSleepPerWeek):
    #            # counting how many sleep minutes we have for each level and in total per week
    #            sumSleepPerWeek[week] = { 'sleep1': 0, 'N1': 0, 'sleep2': 0, 'N2': 0, 'sleep3': 0, 'N3': 0, 'total': 0, 'NT': 0 }
    #        else:
    #            sumSleepPerWeek[week]['sleep1'] = sumSleepPerWeek[week]['sleep1'] + entry['sleep1']
    #            sumSleepPerWeek[week]['N1'] = sumSleepPerWeek[week]['N1'] + 1
    #            sumSleepPerWeek[week]['sleep2'] = sumSleepPerWeek[week]['sleep2'] + entry['sleep2']
    #            sumSleepPerWeek[week]['N2'] = sumSleepPerWeek[week]['N2'] + 1
    #            sumSleepPerWeek[week]['sleep3'] = sumSleepPerWeek[week]['sleep3'] + entry['sleep3']
    #            sumSleepPerWeek[week]['N3'] = sumSleepPerWeek[week]['N3'] + 1
    #            sumSleepPerWeek[week]['total'] = sumSleepPerWeek[week]['total'] + entry['sleep1'] + entry['sleep2'] + entry['sleep3']
    #            sumSleepPerWeek[week]['NT'] = sumSleepPerWeek[week]['NT'] + 1
    #        if entry['weekday'] < 5:
    #            sumSleepPerWeekday['sleep1'] = sumSleepPerWeekday['sleep1'] + entry['sleep1']
    #            sumSleepPerWeekday['N1'] = sumSleepPerWeekday['N1'] + 1
    #            sumSleepPerWeekday['sleep2'] = sumSleepPerWeekday['sleep2'] + entry['sleep2']
    #            sumSleepPerWeekday['N2'] = sumSleepPerWeekday['N2'] + 1
    #            sumSleepPerWeekday['sleep3'] = sumSleepPerWeekday['sleep3'] + entry['sleep3']
    #            sumSleepPerWeekday['N3'] = sumSleepPerWeekday['N3'] + 1
    #        else:
    #            sumSleepPerWeekend['sleep1'] = sumSleepPerWeekend['sleep1'] + entry['sleep1']
    #            sumSleepPerWeekend['N1'] = sumSleepPerWeekend['N1'] + 1
    #            sumSleepPerWeekend['sleep2'] = sumSleepPerWeekend['sleep2'] + entry['sleep2']
    #            sumSleepPerWeekend['N2'] = sumSleepPerWeekend['N2'] + 1
    #            sumSleepPerWeekend['sleep3'] = sumSleepPerWeekend['sleep3'] + entry['sleep3']
    #            sumSleepPerWeekend['N3'] = sumSleepPerWeekend['N3'] + 1
    #vname1 = "fits_ss_sleep_asleepavg_weekend"
    #vname2 = "fits_ss_sleep_restlessavg_weekend"
    #vname3 = "fits_ss_sleep_awakeavg_weekend"
    #scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], 
    #                vname1: "%0.2f" % (sumSleepPerWeekend['sleep1']/(sumSleepPerWeekend['N1'] if sumSleepPerWeekend['N1'] > 0 else 1)), 
    #                vname2: "%0.2f" % (sumSleepPerWeekend['sleep2']/(sumSleepPerWeekend['N2'] if sumSleepPerWeekend['N2'] > 0 else 1)), 
    #                vname3: "%0.2f" % (sumSleepPerWeekend['sleep3']/(sumSleepPerWeekend['N3'] if sumSleepPerWeekend['N3'] > 0 else 1)) })
    #vname1 = "fits_ss_sleep_asleepavg_weekday"
    #vname2 = "fits_ss_sleep_restlessavg_weekday"
    #vname3 = "fits_ss_sleep_awakeavg_weekday"
    #scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], 
    #                vname1: "%0.2f" % (sumSleepPerWeekday['sleep1']/(sumSleepPerWeekday['N1'] if sumSleepPerWeekday['N1'] > 0 else 1)), 
    #                vname2: "%0.2f" % (sumSleepPerWeekday['sleep2']/(sumSleepPerWeekday['N2'] if sumSleepPerWeekday['N2'] > 0 else 1)), 
    #                vname3: "%0.2f" % (sumSleepPerWeekday['sleep3']/(sumSleepPerWeekday['N3'] if sumSleepPerWeekday['N3'] > 0 else 1)) })
    #for idx, entry in sumSleepPerWeek.items():
    #    if (idx > -1) and (idx < 3):
    #        vname1 = "fits_ss_sleep_asleepavg_week" + str(idx+1)
    #        vname2 = "fits_ss_sleep_restlessavg_week" + str(idx+1)
    #        vname3 = "fits_ss_sleep_awakeavg_week" + str(idx+1)
    #        vname4 = "fits_ss_sleep_combinedavg_week" + str(idx+1)
    #        scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], 
    #                        vname1: "%0.2f" % (sumSleepPerWeek[idx]['sleep1']/(sumSleepPerWeek[idx]['N1'] if sumSleepPerWeek[idx]['N1'] > 0 else 1)), 
    #                        vname2: "%0.2f" % (sumSleepPerWeek[idx]['sleep2']/(sumSleepPerWeek[idx]['N2'] if sumSleepPerWeek[idx]['N2'] > 0 else 1)), 
    #                        vname3: "%0.2f" % (sumSleepPerWeek[idx]['sleep3']/(sumSleepPerWeek[idx]['N3'] if sumSleepPerWeek[idx]['N3'] > 0 else 1)),
    #                        vname4: "%0.2f" % (sumSleepPerWeek[idx]['total']/(sumSleepPerWeek[idx]['NT'] if sumSleepPerWeek[idx]['NT'] > 0 else 1)) })
    
    #
    # SLEEP on 30 second level
    #
    for idx, entry in sumSleepStagePerDay.items():
        if (idx > -1) and (idx < 22):
            vname21 = "fits_ss_sleepminutes_light_day%02d" % idx
            vname22 = "fits_ss_sleepminutes_deep_day%02d" % idx
            vname23 = "fits_ss_sleepminutes_rem_day%02d" % idx
            vname24 = "fits_ss_sleepminutes_awake_day%02d" % idx
            vname00 = "fits_ss_sleepminutes_combined_day%02d" % idx
            scores.append({ 'id_redcap': u['id_redcap'],
                            'redcap_event_name': u['redcap_event_name'],
                            vname00: (float(entry['light']) + float(entry['deep']) + float(entry['rem']) + float(entry['wake'])),
                            vname21: float(entry['light']),
                            vname22: float(entry['deep']),
                            vname23: float(entry['rem']),
                            vname24: float(entry['wake']) })
    sumSleepStagePerWeek = {}
    sumSleepStagePerWeekend = { 'light': 0, 'lightN': 0, 'deep': 0, 'deepN': 0, 'rem': 0, 'remN': 0, 'wake': 0, 'wakeN': 0 }
    sumSleepStagePerWeekday = { 'light': 0, 'lightN': 0, 'deep': 0, 'deepN': 0, 'rem': 0, 'remN': 0, 'wake': 0, 'wakeN': 0 }
    for idx, entry in sumSleepStagePerDay.items():
        if (idx > -1) and (idx < 22):
            # we should only count sleep days if they had more than 300 minutes of total sleep per day
            if (entry['light'] + entry['deep'] + entry['rem'] + entry['wake']) < 300:
                # skip this day during the calculations
                continue
            week = (idx-1) // 7
            if not(week in sumSleepStagePerWeek):
                # counting how many sleep minutes we have for each level and in total per week
                sumSleepStagePerWeek[week] = { 'light': 0, 'lightN': 0, 'deep': 0, 'deepN': 0, 'rem': 0, 'remN': 0, 'wake': 0, 'wakeN': 0, 'total': 0, 'NT': 0 }
            else:
                sumSleepStagePerWeek[week]['light']  = sumSleepStagePerWeek[week]['light'] + entry['light']
                sumSleepStagePerWeek[week]['lightN'] = sumSleepStagePerWeek[week]['lightN'] + 1
                sumSleepStagePerWeek[week]['deep']   = sumSleepStagePerWeek[week]['deep'] + entry['deep']
                sumSleepStagePerWeek[week]['deepN']  = sumSleepStagePerWeek[week]['deepN'] + 1
                sumSleepStagePerWeek[week]['rem']    = sumSleepStagePerWeek[week]['rem'] + entry['rem']
                sumSleepStagePerWeek[week]['remN']   = sumSleepStagePerWeek[week]['remN'] + 1
                sumSleepStagePerWeek[week]['wake']   = sumSleepStagePerWeek[week]['wake'] + entry['wake']
                sumSleepStagePerWeek[week]['wakeN']  = sumSleepStagePerWeek[week]['wakeN'] + 1

                sumSleepStagePerWeek[week]['total']  = sumSleepStagePerWeek[week]['total'] + entry['light'] + entry['deep'] + entry['rem'] + entry['wake']
                sumSleepStagePerWeek[week]['NT']     = sumSleepStagePerWeek[week]['NT'] + 1
            if entry['weekday'] < 5:
                sumSleepStagePerWeekday['light']  = sumSleepStagePerWeekday['light'] + entry['light']
                sumSleepStagePerWeekday['lightN'] = sumSleepStagePerWeekday['lightN'] + 1
                sumSleepStagePerWeekday['deep']   = sumSleepStagePerWeekday['deep'] + entry['deep']
                sumSleepStagePerWeekday['deepN']  = sumSleepStagePerWeekday['deepN'] + 1
                sumSleepStagePerWeekday['rem']    = sumSleepStagePerWeekday['rem'] + entry['rem']
                sumSleepStagePerWeekday['remN']   = sumSleepStagePerWeekday['remN'] + 1
                sumSleepStagePerWeekday['wake']   = sumSleepStagePerWeekday['wake'] + entry['wake']
                sumSleepStagePerWeekday['wakeN']  = sumSleepStagePerWeekday['wakeN'] + 1
            else:
                sumSleepStagePerWeekend['light']  = sumSleepStagePerWeekend['light'] + entry['light']
                sumSleepStagePerWeekend['lightN'] = sumSleepStagePerWeekend['lightN'] + 1
                sumSleepStagePerWeekend['deep']   = sumSleepStagePerWeekend['deep'] + entry['deep']
                sumSleepStagePerWeekend['deepN']  = sumSleepStagePerWeekend['deepN'] + 1
                sumSleepStagePerWeekend['rem']    = sumSleepStagePerWeekend['rem'] + entry['rem']
                sumSleepStagePerWeekend['remN']   = sumSleepStagePerWeekend['remN'] + 1
                sumSleepStagePerWeekend['wake']   = sumSleepStagePerWeekend['wake'] + entry['wake']
                sumSleepStagePerWeekend['wakeN']  = sumSleepStagePerWeekend['wakeN'] + 1

    vname1 = "fits_ss_sleep_lightavg_weekend"
    vname2 = "fits_ss_sleep_deepavg_weekend"
    vname3 = "fits_ss_sleep_remavg_weekend"
    vname4 = "fits_ss_sleep_awakeavg_weekend"
    scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], 
                    vname1: "%0.2f" % ((sumSleepStagePerWeekend['light']/(sumSleepStagePerWeekend['lightN'] if sumSleepStagePerWeekend['lightN'] > 0 else 1))), 
                    vname2: "%0.2f" % ((sumSleepStagePerWeekend['deep']/(sumSleepStagePerWeekend['deepN'] if sumSleepStagePerWeekend['deepN'] > 0 else 1))), 
                    vname3: "%0.2f" % ((sumSleepStagePerWeekend['rem']/(sumSleepStagePerWeekend['remN'] if sumSleepStagePerWeekend['remN'] > 0 else 1))), 
                    vname4: "%0.2f" % ((sumSleepStagePerWeekend['wake']/(sumSleepStagePerWeekend['wakeN'] if sumSleepStagePerWeekend['wakeN'] > 0 else 1))) })
    vname1 = "fits_ss_sleep_lightavg_workday"
    vname2 = "fits_ss_sleep_deepavg_workday"
    vname3 = "fits_ss_sleep_remavg_workday"
    vname4 = "fits_ss_sleep_awakeavg_workday"
    scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], 
                    vname1: "%0.2f" % ((sumSleepStagePerWeekday['light']/(sumSleepStagePerWeekday['lightN'] if sumSleepStagePerWeekday['lightN'] > 0 else 1))), 
                    vname2: "%0.2f" % ((sumSleepStagePerWeekday['deep']/(sumSleepStagePerWeekday['deepN'] if sumSleepStagePerWeekday['deepN'] > 0 else 1))), 
                    vname3: "%0.2f" % ((sumSleepStagePerWeekday['rem']/(sumSleepStagePerWeekday['remN'] if sumSleepStagePerWeekday['remN'] > 0 else 1))), 
                    vname4: "%0.2f" % ((sumSleepStagePerWeekday['wake']/(sumSleepStagePerWeekday['wakeN'] if sumSleepStagePerWeekday['wakeN'] > 0 else 1))) })
    for idx, entry in sumSleepStagePerWeek.items():
        if (idx > -1) and (idx < 3):
            vname1 = "fits_ss_sleep_lightavg_week" + str(idx+1)
            vname2 = "fits_ss_sleep_deepavg_week" + str(idx+1)
            vname3 = "fits_ss_sleep_remavg_week" + str(idx+1)
            vname4 = "fits_ss_sleep_awakeavg_week" + str(idx+1)
            vname5 = "fits_ss_sleep_combinedavg_week" + str(idx+1)
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], 
                            vname1: "%0.2f" % ((sumSleepStagePerWeek[idx]['light']/(sumSleepStagePerWeek[idx]['lightN'] if sumSleepStagePerWeek[idx]['lightN'] > 0 else 1))), 
                            vname2: "%0.2f" % ((sumSleepStagePerWeek[idx]['deep']/(sumSleepStagePerWeek[idx]['deepN'] if sumSleepStagePerWeek[idx]['deepN'] > 0 else 1))), 
                            vname3: "%0.2f" % ((sumSleepStagePerWeek[idx]['rem']/(sumSleepStagePerWeek[idx]['remN'] if sumSleepStagePerWeek[idx]['remN'] > 0 else 1))),
                            vname4: "%0.2f" % ((sumSleepStagePerWeek[idx]['wake']/(sumSleepStagePerWeek[idx]['wakeN'] if sumSleepStagePerWeek[idx]['wakeN'] > 0 else 1))),
                            vname5: "%0.2f" % ((sumSleepStagePerWeek[idx]['total']/(sumSleepStagePerWeek[idx]['NT'] if sumSleepStagePerWeek[idx]['NT'] > 0 else 1))) })
           

    # sum of wear minutes per week (and average)
    sumHRDaysPerWeek = {}
    sumHRDaysPerWeekend = 0
    sumHRDaysPerWeekday = 0
    sumHRMinutesPerWeek = {}
    sumHRMinutesPerWeekday = 0
    sumHRMinutesPerWeekend = 0
    for idx, entry in sumHRPerDay.items():  # contains the total HR as HR and the number of minutes per day as N
        # don't count entries for days with less than 600 minutes of awake time
        if (int(entry['N']) < 600) or (int(entry['N']) > 1200):
            # skip this day
            continue

        if (idx > 0) and (idx < 22):    # here we don't want to count day 0
            week = (idx-1) // 7
            if entry['weekday'] < 5:
                sumHRDaysPerWeekday = sumHRDaysPerWeekday + 1
                sumHRMinutesPerWeekday = sumHRMinutesPerWeekday + entry['N']
            else:
                sumHRDaysPerWeekend = sumHRDaysPerWeekend + 1
                sumHRMinutesPerWeekend = sumHRMinutesPerWeekend + entry['N']
            if not(week in sumHRMinutesPerWeek):
                sumHRMinutesPerWeek[week] = entry['N']
            else:
                sumHRMinutesPerWeek[week] = sumHRMinutesPerWeek[week] + entry['N']

            if not(week in sumHRDaysPerWeek):
                sumHRDaysPerWeek[week] = { 'HR': entry['HR'], 'N': 1 }
            else:
                sumHRDaysPerWeek[week]['HR'] = sumHRDaysPerWeek[week]['HR'] + entry['HR']
                sumHRDaysPerWeek[week]['N']  = sumHRDaysPerWeek[week]['N'] + 1
    scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], "fits_ss_weardays_awake_weekend": sumHRDaysPerWeekend, "fits_ss_weardays_awake_workday": sumHRDaysPerWeekday })
    scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], "fits_ss_wearminutes_sum_weekend": sumHRMinutesPerWeekend,  "fits_ss_wearminutes_sum_workday": sumHRMinutesPerWeekday }) 
    for idx, d in sumHRDaysPerWeek.items():
        if (idx > -1) and (idx < 3):
            vname2 = "fits_ss_wearminutesavg_week" + str(idx+1)
            vname3 = "fits_ss_weardays_awake_week" + str(idx+1)
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname2: "%.2f" % (d['HR']/(d['N'] if d['N'] > 0 else 1) ), vname3: int(d['N']) })
    for idx, d in sumHRMinutesPerWeek.items():
        if (idx > -1) and (idx < 3):
            vname  = "fits_ss_wearminutes_sum_week" + str(idx+1)
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: d })

    sumStepsPerWorkDay = {}
    sumStepsPerWeekend = {}
    for idx, entry in sumStepsPerDay.items():
        if (idx > 0) and (idx < 22):
            # we would like to be 1..7 as week 1, 8..14 as week 2
            week = (idx-1) // 7                    
            if entry['weekday'] < 5:
                if not(week in sumStepsPerWorkDay):
                    sumStepsPerWorkDay[week] = entry['steps']
                else:
                    sumStepsPerWorkDay[week] = sumStepsPerWorkDay[week] + entry['steps']
            else:
                if not(week in sumStepsPerWeekend):
                    sumStepsPerWeekend[week] = entry['steps']
                else:
                    sumStepsPerWeekend[week] = sumStepsPerWeekend[week] + entry['steps']
    for idx, d in sumStepsPerWorkDay.items():
        if (idx > -1) and (idx < 3):
            vname = "fits_ss_steps_workday_week" + str(idx+1)
            vname2 = "fits_ss_steps"
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: d })
    for idx, d in sumStepsPerWeekend.items():
        if (idx > -1) and (idx < 3):
            vname = "fits_ss_steps_weekend_week" + str(idx+1)
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: d })

    #
    # calculate the sum of steps per week (and average)
    #
    sumStepsPerWeek = {}
    for idx, d in sumStepsPerDay.items():
        if (idx > 0) and (idx < 22):
            week = (idx-1) // 7
            if not(week in sumStepsPerWeek):
                sumStepsPerWeek[week] = { 'steps': d['steps'], 'N': 1 }
            else:
                sumStepsPerWeek[week]['steps'] = sumStepsPerWeek[week]['steps'] + d['steps']
                sumStepsPerWeek[week]['N'] = sumStepsPerWeek[week]['N'] + 1

    for idx, entry in sumStepsPerWeek.items():
        if (idx > -1) and (idx < 3):
            vname = "fits_ss_steps_week" + str(idx+1)
            vname2 = "fits_ss_stepsavg_week" + str(idx+1)
            scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], vname: entry['steps'], vname2: "%.2f" % (entry['steps']/(entry['N'] if entry['N'] > 0 else 1)) })

    #
    # calculate the sum of mets per week/weekday/weekend (and average)
    #
    sumMETPerWeek = {}
    sumMETPerWeekday = {}
    sumMETPerWeekend = {}
    sumTotalMETPerWeekend  = 0
    sumTotalMETPerWeekday  = 0
    sumTotalMETPerWeekendN = 0
    sumTotalMETPerWeekdayN = 0
    for idx, d in sumMETPerDay.items():
        if (idx > 0) and (idx < 22):
            week = (idx-1) // 7
            met_val = d['mets']/(d['N'] if d['N'] > 0 else 1)
            if not(week in sumMETPerWeek):
                sumMETPerWeek[week] = { 'mets': met_val, 'N': 1 }
            else:
                sumMETPerWeek[week]['mets'] = sumMETPerWeek[week]['mets'] + met_val
                sumMETPerWeek[week]['N'] = sumMETPerWeek[week]['N'] + 1
            if d['weekday'] < 5:
                if not(week in sumMETPerWeekday):
                    sumMETPerWeekday[week] = { 'mets': met_val, 'N': 1 }
                    sumTotalMETPerWeekday  = sumTotalMETPerWeekday + met_val
                    sumTotalMETPerWeekdayN  = sumTotalMETPerWeekdayN + 1
                else:
                    sumMETPerWeekday[week]['mets'] = sumMETPerWeekday[week]['mets'] + met_val
                    sumMETPerWeekday[week]['N']    = sumMETPerWeekday[week]['N'] + 1
                    sumTotalMETPerWeekday          = sumTotalMETPerWeekday + met_val
                    sumTotalMETPerWeekdayN         = sumTotalMETPerWeekdayN + 1
            else:
                if not(week in sumMETPerWeekend):
                    sumMETPerWeekend[week]  = { 'mets': met_val, 'N': 1 }
                    sumTotalMETPerWeekend   = sumTotalMETPerWeekend + met_val
                    sumTotalMETPerWeekendN  = sumTotalMETPerWeekendN + 1
                else:
                    sumMETPerWeekend[week]['mets'] = sumMETPerWeekend[week]['mets'] + met_val
                    sumMETPerWeekend[week]['N']    = sumMETPerWeekend[week]['N'] + 1
                    sumTotalMETPerWeekend   = sumTotalMETPerWeekend + met_val
                    sumTotalMETPerWeekendN  = sumTotalMETPerWeekendN + 1
    vname  = "fits_ss_metsavg_weekend"
    vname2 = "fits_ss_metsavg_workday"
    scores.append({
        'id_redcap': u['id_redcap'],
        'redcap_event_name': u['redcap_event_name'],
        vname: "%.2f" % (sumTotalMETPerWeekend/(sumTotalMETPerWeekendN if sumTotalMETPerWeekendN > 0 else 1)),
        vname2: "%.2f" % (sumTotalMETPerWeekday/(sumTotalMETPerWeekdayN if sumTotalMETPerWeekdayN > 0 else 1))
    })          
    for idx, entry in sumMETPerWeek.items():
        if (idx > -1) and (idx < 3):
            #vname = "fits_ss_mets_week" + str(idx+1)
            vname2 = "fits_ss_metsavg_week" + str(idx+1)
            scores.append({
                'id_redcap': u['id_redcap'],
                'redcap_event_name': u['redcap_event_name'],
                vname2: "%.2f" % (entry['mets']/(entry['N'] if entry['N'] > 0 else 1))                    
            })
    return scores
        

if __name__ == "__main__":
    args = parse_args()
    site = args.site
    with open('/var/www/html/code/php/tokens.json') as data_file:
        tokens = json.load(data_file)
        site_token = tokens[site]

    out = redcap_site_subjects(site_token)
    fitabase_files = collect_fitabase_files_from_folder(args.input)

    scores = []
    # walk over the participants for this site
    for u in out:
        pGUID = u['id_redcap']
        # search for scores for this pGUID
        if not(pGUID in fitabase_files):
            continue
        # mark this pGUID as getting processed
        for d in fitabase_files[pGUID]:
            d['processed'] = True
        print("importing data for %s [%s]" % (pGUID, site))
        date0 = u['fitc_device_dte']
        if date0 == '':
            print("Error: REDCap missing fitc_timestamp_v2 for participant " + pGUID + ". Data for this participant cannot be imported.")
            scores.append( { 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], 'fits_ss_import_error': "missing fitc_timestamp_v2" })
            continue
        # 2017-06-08 14:43

        date0 = datetime.strptime(date0, '%Y-%m-%d %H:%M')
        #print("date fitbit was given out: " + str(date0))
        
        # we need to ask what the correct event is for the data
        for timerange in dateRangeSet(fitabase_files[pGUID]):
            timerange_scores = process_timerange(pGUID, timerange, date0)
            if timerange_scores:
                scores.extend(timerange_scores)
            
            #print(json.dumps(scores))
        # scores.append({ 'id_redcap': u['id_redcap'], 'redcap_event_name': u['redcap_event_name'], "survey_handle": fake_email })

    # remove entries that are null or NaN
    for score in scores:
        for idx, d in score.items():
            try:
                if (d == None) or math.isnan(float(d)):
                    # set to empty string
                    score[idx] = ""
            except ValueError:
                pass
            except TypeError:
                print("d is not correct, its %s for key %s" % (str(d), idx))
        if not('fits_ss_import_error' in score):
            # overwrite any previous  value in this field (import is ok)
            score['fits_ss_import_error'] = "no error"
        
    # We can minimize the number of times we have to send data to REDCap if we collect the measure we want to update
    # based on the id_redcap and redcap_event_name. Whenever both are the same we can merge the measures.
    scores_sorted = sorted(scores, key=lambda d: (d['id_redcap'], d['redcap_event_name']))
    # print(json.dumps(scores_sorted))
    scores_combined = []
    scores_current = {}
    for scores in scores_sorted:
        if (len(scores_current.keys()) == 0):
            scores_current = scores.copy()
        if (scores_current['id_redcap'] == scores['id_redcap']) and (scores_current['redcap_event_name'] == scores['redcap_event_name']):
            scores_current.update(scores)
        else:
            scores_combined.append(scores_current)
            scores_current = scores.copy()
    scores_combined.append(scores_current)
    #print(json.dumps(scores_combined,indent=4))

    def chunks(l, n):
        # For item i in a range that is a length of l,
        for i in range(0, len(l), n):
            # Create an index range for l of n items:
            yield l[i:i+n]
            
    # now add the values to REDCap
    if args.force:
        print("Add scores to REDCap...")
        for score in chunks(scores_combined, 3):
            #print("try to add: " + json.dumps(score))
            buf = cStringIO.StringIO()
            data = {
                'token': tokens[site],
                'content': 'record',
                'format': 'json',
                'type': 'flat',
                'overwriteBehavior': 'normal',
                'data': json.dumps(score),
                'returnContent': 'count',
                'returnFormat': 'json'
            }
            ch = pycurl.Curl()
            ch.setopt(ch.URL, 'https://abcd-rc.ucsd.edu/redcap/api/')
            ch.setopt(ch.HTTPPOST, data.items())
            ch.setopt(ch.WRITEFUNCTION, buf.write)
            ch.perform()
            ch.close()
            print buf.getvalue()
            buf.close()
            # beauty sleep
            sleep(0.02)
    else:        
        print(json.dumps(scores_combined, indent=4))

    # list the files that we did not process
    for pGUID in fitabase_files:
        for entry in fitabase_files[pGUID]:
            if not(entry['processed']):
                print("Error: we did not process file: %s (example file %s) for site %s" %( pGUID, entry['filename'], site ))
                break
