#!/usr/bin/env python
"""
For all sites that we have tokens for, retrieve data on creation date of batches.
"""
import argparse
import datetime
from fitabase_api import FitabaseSite
import json
import logging as log
import os
import pandas as pd
import sys

# If executed from cron, paths are relative to PWD, so anything we need must 
# have an absolute path
CURRENT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)))
log.basicConfig(
        filename=os.path.join(CURRENT_DIR,
            "logs", os.path.basename(__file__) + ".log"), 
        format="%(asctime)s  %(levelname)10s  %(message)s",
        level=log.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser(
            description=__doc__)
    parser.add_argument('--batch-name', default=None,
            help='Only include stats for batches of this name.')
    parser.add_argument('--outfile', '-o', default=sys.stdout,
            help='Only include stats for batches of this name.')
    parser.add_argument('--convert-timezones', action='store_true',
            help="Change timezone from GMT to US/Pacific.")
    parser.add_argument('--verbose', '-v', action='store_true',
            help="Display / save logged INFO-level messages.")
    return parser.parse_args()

def export_info_to_columns(token):
    info = FitabaseSite(token).get_last_batch_export_info()
    return pd.Series(info)

if __name__ == "__main__":
    args = parse_arguments()
    if args.verbose:
        log.getLogger().setLevel(log.DEBUG)

    with open(os.path.join(CURRENT_DIR, 'fitabase_tokens.json')) as data_file:
        fitabase_tokens = json.load(data_file).get('tokens')
        fitabase_tokens = pd.DataFrame.from_records(fitabase_tokens, index='name')

    export_info = fitabase_tokens['token'].apply(export_info_to_columns)
    all_info = fitabase_tokens.loc[:, ['description']].join(export_info)
    if args.batch_name:
        all_info = all_info.loc[all_info['Name'] == args.batch_name]

    if args.convert_timezones:
        datelike_cols = ['ProcessingStarted', 'ProcessingCompleted']
        for col in datelike_cols:
            all_info.loc[:, col] = (
                    pd.to_datetime(all_info.loc[:, col], utc=True)
                    .dt.tz_convert('US/Pacific'))
        ## Why a loop, you might ask?
        ##
        ## The following doesn't work due to a bug in pandas: 
        ## https://github.com/pandas-dev/pandas/issues/20511
        # all_info.loc[:, datelike_cols] = all_info.loc[:, datelike_cols].apply(
        #         pd.to_datetime)
        # all_info.loc[:, col] = pd.to_datetime(all_info.loc[:, col]).dt.tz_localize('GMT')
        # all_info.loc[:, [col]] = pd.to_datetime(all_info[:, [col]])
        # all_info.loc[:, col] = all_info.loc[:, col].apply(pd.to_datetime)
        # all_info.loc[:, col] = all_info.loc[:, [col]].apply(pd.to_datetime)

    all_info.sort_values('ProcessingCompleted', inplace=True)
    all_info.to_csv(args.outfile)
