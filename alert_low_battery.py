#!/usr/bin/env python
"""
When appropriate, send out low-battery alerts to any participants at site(s).

It can be run with multiple sites, e.g.:

    ./alert_low_battery.py -fv UCSD CHLA UPMC UMICH

will trawl and notify any people from these sites (conditional on not creating 
a pile-on on the subject - see below).

For more details, run `./alert_low_battery.py -h`.

WARNING: The script will _not_ run in the shared abcd-report environment. To 
run it, you need to either:

    - `cd /var/www/html/applications/fitbit/ && source env/bin/activate` in 
      your shell prior to invoking it with `./alert_low_battery.py`, or
    - directly invoke the virtualenv Python to execute the script, e.g.:

    /var/www/html/applications/fitbit/env/bin/python alert_low_battery.py -f UCSD

Steps:
1. Get device list from Fitabase.
2. Get current device list from Redcap, filtering for extant fitc_device_dte 
   and extant Fitabase record.
3. Subsetting device list from Fitabase, pull sync + battery data with 
   get_all_tracker_sync_data.
4. Use NotificationSubmission to limit uploads to only participants who haven't 
   been contacted in the recent past.
"""

import argparse
import datetime
import json
import logging as log
from notification import NotificationSubmission
import os
import pandas as pd
import redcap as rc
import requests
from fitabase_api import FitabaseSite

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
            description=("Send direct alerts to site subjects with low Fitbit "
                         "battery levels."))
    parser.add_argument('site', nargs='+')
    parser.add_argument('--dry-run', '-n', action='store_true',
            help="Ensure that the NotificationSubmission aborts any upload.")
    parser.add_argument('--force', '-f', action='store_true',
            help="Without --force, no uploads will be attempted.")
    parser.add_argument('--verbose', '-v', action='store_true',
            help="Display / save INFO-level messages, too.")
    return parser.parse_args()

if __name__ == "__main__":
    REDCAP_URL = 'https://abcd-rc.ucsd.edu/redcap/api/'
    REDCAP_EVENT = '2_year_follow_up_y_arm_1'  # FIXME: Move to args?
    args = parse_arguments()
    if args.verbose:
        log.basicConfig(level=log.DEBUG)

    with open(os.path.join(CURRENT_DIR, 'fitabase_tokens.json')) as data_file:
        fitabase_tokens = json.load(data_file).get('tokens')
        fitabase_tokens = pd.DataFrame.from_records(fitabase_tokens, index='name')
        # TODO: Could pass the list of keys as site choices for parse_arguments
    with open(os.path.join(CURRENT_DIR, '../../code/php/tokens.json')) as data_file:
        redcap_tokens = json.load(data_file)
        redcap_tokens = pd.DataFrame.from_dict(redcap_tokens, orient='index', columns=['token'])

    # No need to keep the call one site at a time - we can iterate through all
    for site in args.site:
        # Get device list from Fitabase
        try:
            fit_token = fitabase_tokens.loc[site, 'token']
        except KeyError:
            log.error('%s: Fitabase token ID is not available!', site)
            continue
        fit_api = FitabaseSite(fit_token)
        fit_devices = fit_api.get_device_ids()
        fit_ids = fit_devices['Name']#.tolist()

        # Get device list from main Redcap project
        rc_token = redcap_tokens.loc[site, 'token']
        rc_api = rc.Project(REDCAP_URL, rc_token)
        rc_fit_fields = ['fitc_device_dte']
        rc_devices = rc_api.export_records(
                fields=rc_fit_fields + [rc_api.def_field],
                events=[REDCAP_EVENT],  
                export_data_access_groups=True,
                df_kwargs={
                    'parse_dates': rc_fit_fields,
                    'index_col': [rc_api.def_field]},
                format='df')
        # Subset to only devices that are on Fitabase and currently in the data 
        # collection period
        #
        # FIXME: Should also check that this is a positive Timedelta, in case 
        # someone's fitc_device_dte is set in the future? Although then the 
        # device will probably not be active / won't have sync data, so...
        rc_devices['now_collecting'] = (
                (pd.to_datetime('today') - rc_devices['fitc_device_dte']) 
                < pd.Timedelta(days=23))
        active_devices = (rc_devices.loc[rc_devices.index.isin(fit_ids) & 
                                         rc_devices['now_collecting']])
        if active_devices.empty:
            log.warn("%s: No active devices at site.", site)
            continue
        active_devices = active_devices.join(fit_devices.set_index('Name'))
        active_devices = active_devices.join(
                fit_api.get_all_tracker_sync_data(active_devices))
        active_devices['time_since_sync'] = (
                pd.to_datetime('today') - active_devices['SyncDateTracker'])

        # Tag a participant for a potential reminder if:
        # 
        # 1. the battery level is EMPTY + last sync was more than 6 hours ago,
        # 2. the battery level is LOW + last sync was more than 1 day ago.

        empty_idx = active_devices['LatestBatteryLevelTracker'] == 'Empty'
        low_idx   = active_devices['LatestBatteryLevelTracker'] == 'Low'
        more_than_6hr_ago = active_devices['time_since_sync'] > pd.Timedelta(hours=6)
        more_than_1d_ago  = active_devices['time_since_sync'] > pd.Timedelta(days=1)
        devices_to_notify = active_devices.loc[
                (empty_idx & more_than_6hr_ago) | (low_idx & more_than_1d_ago)]
        # TODO: Many of these conditions could be expressed as a 
        # pd.DataFrame.query, which means that they could live in a config file 
        # somewhere

        if devices_to_notify.empty:
            continue

        # Now, we need to create three versions of the notification; the 
        # external system decides which ones to send.
        # 
        # Here, we'll first create the shared attributes of the message, then 
        # infer the specifics based on the dict key in messages:
        messages = {
                'parent_en': "Your child's Fitbit is about to run out of battery. When off, the device cannot collect data. Please ask them to recharge it.",
                'parent_es': "Your child's Fitbit is about to run out of battery. When off, the device cannot collect data. Please ask them to recharge it.",
                'child_en':  "Your Fitbit is about to run out of battery. When off, the device cannot collect data. Please recharge it."}
        default = {
                'noti_subject_line': 'Please charge your Fitbit!',
                'noti_status': 1,
                'noti_purpose': 'send_charge_reminder',
                'noti_timestamp_create': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'noti_site_name': site,
                # 'noti_spanish_language': 0,
                # 'noti_recipient': 1,
                'noti_send_preferred_time': 0}  # 0: immediate, 1: daily

        # Setup: connect to the Notifications Redcap and retrieve past 
        # notifications for tagged participants
        ids_to_notify = devices_to_notify.index.get_level_values('id_redcap').tolist()
        notif_token = ''
        notif_api = rc.Project(REDCAP_URL, notif_token)

        try:
            notif_records = notif_api.export_records(records=ids_to_notify, 
                    forms=['notifications'],
                    format='df')
        except pd.errors.EmptyDataError as e:  # All tagged IDs have no priors
            notif_records = pd.DataFrame()


        # For each participant, process and upload the notifications we'd 
        # created
        for pGUID in ids_to_notify:
            notifications = []
            # For each message, merge defaults and specifics and appends them 
            # to the notifications list
            for recipient, message in messages.items():
                specifics = {
                        'record_id': pGUID,
                        'noti_text': message,
                        'noti_spanish_language': int(recipient.endswith('_es')),
                        'noti_recipient': int(not recipient.startswith('parent')) + 1}
                notifications.append(dict(default, **specifics))


            # NotificationSubmission provides three things:
            # 
            # 1. Processing logic (adding redcap_repeat_instrument, etc.)
            # 2. History-dependent stopping logic (given the previously sent 
            # alerts, should another one go out this early?)
            # 3. Upload logic.
            #
            # First, we wrap the bundle of messages in a single 
            # NotificationSubmission. (To work with NotificationSubmission, 
            # input must be an indexed DataFrame.)
            notifications_df = pd.DataFrame(notifications).set_index('record_id')
            submission = NotificationSubmission(notif_api, notifications_df, 
                    notif_records, dry_run=args.dry_run)
            # Now, we execute two checks:
            # 1. Check that participant has not received any alerts *of any 
            # kind* in the past two days
            any_alerts = submission.stop_if_early(timedelta=pd.Timedelta(days=2), 
                    check_current_purpose_only=False)
            # 2. Check that participant has not received a "charge your Fitbit" 
            # alert in the past week. Note that this check makes this script 
            # idempotent - if you re-run it, it will not recreate the alerts.
            battery_alerts = submission.stop_if_early(timedelta=pd.Timedelta(days=3), 
                    check_current_purpose_only=True)

            # Without --force, no uploads will be done. With --dry-run, upload 
            # will be attempted, but it will fail (as NotificationSubmission 
            # takes a dry_run argument that triggers its stopping logic).
            if not args.force:
                log.warning("%s, %s: To try to upload battery warning notification,"
                            " run with --force", site, pGUID)
            else:
                try:
                    submission.upload(create_redcap_repeating=True)
                    log.info("%s, %s: Battery warning notifications (%d versions) " 
                             "uploaded.", site, pGUID, len(notifications))
                except ValueError as e:
                    log.warning("%s, %s: Abort condition triggered. Why? "
                                "Dry run: %s; "
                                "Too early after battery alert: %s. "
                                "Too early after any alert: %s; "
                                "(ValueError: %s)." % (
                                    site, pGUID, args.dry_run, battery_alerts, 
                                    any_alerts, e))
