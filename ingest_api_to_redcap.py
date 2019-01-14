#!/usr/bin/env python
"""
Grab relevant info from Fitabase API and pass it along to ABCD Redcap.

If not invoked with --all, it will only update the information for participants 
who are indicated to have received a Fitbit, i.e. have fitc_device_dte.

Current variables of interest:

- fitc_fitabase_exists
- fitc_fitabase_profile_id
- fitc_last_sync_date
- fitc_last_battery_level
- ~~fitc_hr_start_date~~ (not extractable via API at this point)
"""

import argparse
import fitabase
import json
import logging as log
from notification import NotificationSubmission
import os
import pandas as pd
import redcap as rc
import requests
import sys

# NOTE: This setting was made _after_ I investigate where the supposed chained 
# assignment was happening and concluded that (1) it wasn't clear *how*, (2) it 
# wasn't affecting functionality.
pd.options.mode.chained_assignment = None

# If executed from cron, paths are relative to PWD, so anything we need must 
# have an absolute path
CURRENT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)))
log.basicConfig(
        filename=os.path.join(CURRENT_DIR, "logs", os.path.basename(__file__) + ".log"), 
        format="%(asctime)s  %(levelname)10s  %(message)s",
        level=log.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser(
            description=__doc__)
    parser.add_argument('site', nargs='+')
    parser.add_argument('--all', '-a', action='store_true',
            help="Update all records, even if they do not have a Fitbit distribution date")
    parser.add_argument('--dry-run', '-n', action='store_true',
            help="Print final dataframe instead of uploading it")
    parser.add_argument('--verbose', '-v', action='store_true',
            help="Display / save INFO-level messages, too.")
    return parser.parse_args()


def load_fitabase_data(api, pull_sync=False, name_subset=None):
    fit_devices = api.get_device_ids().set_index('Name')
    if name_subset is not None:
        fit_devices = fit_devices.loc[fit_devices.index.isin(name_subset)]
    if pull_sync and not fit_devices.empty:
        return fit_devices.join(api.get_all_tracker_sync_data(fit_devices,
            device_name='Charge 2'))
    else:
        return fit_devices


if __name__ == "__main__":
    REDCAP_URL = 'https://abcd-rc.ucsd.edu/redcap/api/'
    REDCAP_EVENT = '2_year_follow_up_y_arm_1'  # FIXME: Move to args?
    args = parse_arguments()
    if args.verbose:
        log.getLogger().setLevel(log.DEBUG)

    with open(os.path.join("/var/www/secure/", 'fitabase_tokens.json')) as data_file:
        fitabase_tokens = json.load(data_file).get('tokens')
        fitabase_tokens = pd.DataFrame.from_records(fitabase_tokens, index='name')
        # TODO: Could pass the list of keys as site choices for parse_arguments
    with open(os.path.join(CURRENT_DIR, '../../../secure/tokens.json')) as data_file:
        redcap_tokens = json.load(data_file)
        redcap_tokens = pd.DataFrame.from_dict(redcap_tokens, orient='index', columns=['token'])

    # No need to keep the call one site at a time - we can iterate through all
    for site in args.site:
        log.info("%s: Started processing", site)

        try:
            # Get device list from main Redcap project
            try:
                rc_token = redcap_tokens.loc[site, 'token']
            except KeyError:
                log.error('%s: Redcap token ID is not available!', site)
                continue
            rc_api = rc.Project(REDCAP_URL, rc_token)
            rc_fit_fields = ['fitc_device_dte']
            rc_devices = rc_api.export_records(
                    fields=rc_fit_fields + [rc_api.def_field],
                    events=[REDCAP_EVENT],  
                    export_data_access_groups=True,
                    df_kwargs={
                        'parse_dates': rc_fit_fields,
                        # Only setting record id field as index here, instead of it 
                        # *and* redcap_event_name, in order to facilitate easy join 
                        # with the Fitabase DataFrame
                        'index_col': [rc_api.def_field]},
                    format='df')

            if not args.all:
                rc_devices.dropna(subset=['fitc_device_dte'], inplace=True)
                if rc_devices.empty:
                    log.info("%s: No active devices at site", site)
                    continue
            rc_names = rc_devices.index.get_level_values('id_redcap').tolist()

            # Get device list from Fitabase
            try:
                fit_token = fitabase_tokens.loc[site, 'token']
            except KeyError:
                log.error('%s: Fitabase token ID is not available!', site)
                continue
            fit_api = fitabase.Project(fit_token)
            # TODO: Maybe subset based on available Redcap IDs? If ID is absent in 
            # Redcap, that maybe warrants a warning, but the data definitely won't 
            # be useful...
            fit_data = load_fitabase_data(fit_api, pull_sync=True, 
                    name_subset=rc_names)

            # Now, transform the Fitabase data into columns. Matches almost 1-to-1:
            join = rc_devices.join(fit_data).rename(columns={
                'SyncDateTracker': 'fitc_last_sync_date',
                'LatestBatteryLevelTracker': 'fitc_last_battery_level',
                'ProfileId': 'fitc_fitabase_profile_id',
                })

            # Note the .astype(int) - PyCAP apparently doesn't know to convert 
            # boolean pandas columns, and instead uploads strings, so we have to do 
            # this for it
            join.loc[:, 'fitc_fitabase_exists'] = pd.notnull(join['fitc_fitabase_profile_id']).astype(int)

            # For Redcap upload to work, redcap_event_name must be in the index
            join = join.reset_index().set_index(['id_redcap', 'redcap_event_name'])

            # The try block is necessary because in some cases, there won't be any 
            # Fitabase matches - thus no last_sync_date or last_battery_level. (We 
            # could explicitly test for them, but catching KeyError should be 
            # sufficiently specific.)
            try:
                # It's insane, but ABCD Redcap follows MDY convention
                # Also, the field is unvalidated text, so NaT wreaks havoc
                join['fitc_last_sync_date'] = (join['fitc_last_sync_date']
                        .dt.strftime('%m-%d-%Y %H:%M:%S')
                        .astype(str)
                        .replace('NaT', ''))
                join['fitc_last_battery_level'] = join['fitc_last_battery_level'].str.upper()

                # Only keep the columns of interest
                # (This removes both original Fitabase columns that we have no use for, 
                # and original Redcap columns that we don't need to rewrite.)
                join = join.loc[:, ['fitc_last_sync_date', 'fitc_last_battery_level', 
                    'fitc_fitabase_exists', 'fitc_fitabase_profile_id']]
            except KeyError as e:
                log.warn('%s: No corresponding Fitabase entries for any of %s.', site,
                        join.index.get_level_values('id_redcap').tolist())
                join = join.loc[:, ['fitc_fitabase_exists']]
            if args.dry_run:
                print(join.to_csv(sys.stdout))
            else:
                try:
                    out = rc_api.import_records(join, overwrite='overwrite', return_content='ids')
                    # TODO: Maybe compare out (which is a list of IDs) with 
                    # join.index.get_level_values('id_redcap') to see if any were 
                    # omitted?
                    log.info('%s: Successfully updated Redcap records for %s', site, out)
                except requests.RequestException as e:
                    # TODO: If exception happens, maybe retry record-by-record?
                    log.exception('%s: Error occurred during upload of %d records.', site, join.shape[0])

        except Exception as e:
            log.exception("%s: Uncaught exception occurred.", site)
            continue

    log.info('Ended run with invocation: %s', sys.argv)
