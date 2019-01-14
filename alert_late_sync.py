#!/usr/bin/env python
"""
TODO: 
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
import sys

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
            description="Send alerts to subjects with overlate sync.")
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
        log.getLogger().setLevel(log.DEBUG)

    with open('/var/www/secure/tokens.json') as data_file:
        redcap_tokens = json.load(data_file)
        redcap_tokens = pd.DataFrame.from_dict(redcap_tokens, orient='index', columns=['token'])

    with open(os.path.join(CURRENT_DIR, 'notifications_token.json')) as token_file:
        notif_token = json.load(token_file).get('token')
        notif_api = rc.Project(REDCAP_URL, notif_token)

    log.info('Started run with invocation: %s', sys.argv)

    # No need to keep the call one site at a time - we can iterate through all
    for site in args.site:
        # Get device list from main Redcap project
        rc_token = redcap_tokens.loc[site, 'token']
        rc_api = rc.Project(REDCAP_URL, rc_token)
        rc_fit_datefields = ['fitc_device_dte', 'fitc_last_sync_date'] 
        rc_fit_fields = ['fitc_last_battery_level', 'fitc_fitabase_exists', 
                'fitc_fitabase_profile_id', 'fitc_number_devices']
        rc_devices = rc_api.export_records(
                fields=rc_fit_datefields + rc_fit_fields + [rc_api.def_field],
                events=[REDCAP_EVENT],  
                export_data_access_groups=True,
                df_kwargs={
                    'parse_dates': rc_fit_datefields,
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
        active_devices = (rc_devices.loc[rc_devices['fitc_fitabase_exists'].astype(bool) & 
                                         rc_devices['now_collecting']])
        if active_devices.empty:
            log.warn("%s: No active devices at site.", site)
            continue
        else:
            log.info("%s: %d active devices at site.", site, active_devices.shape[0])
        active_devices['time_since_sync'] = (
                pd.to_datetime('today') - active_devices['fitc_last_sync_date'])

        more_than_3d_ago  = active_devices['time_since_sync'] > pd.Timedelta(days=3)
        # less_than_7d_ago  = active_devices['time_since_sync'] < pd.Timedelta(days=7)
        # devices_to_notify = active_devices.loc[more_than_3d_ago & less_than_7d_ago]
        devices_to_notify = active_devices.loc[more_than_3d_ago]

        if devices_to_notify.empty:
            continue

        # Now, we need to create three versions of the notification; the 
        # external system decides which ones to send.
        # 
        # Here, we'll first create the shared attributes of the message, then 
        # infer the specifics based on the dict key in messages:
        messages = {
                'parent_en': "%YOUTH_FIRST_NAME%: Please sync your fitbit for ABCD! Our records show that you did this more than {days} days ago. Do this every day so we don't lose your data. Do not reply to this message. Contact your site directly if you have questions.",
                'parent_es': "%YOUTH_FIRST_NAME%: Please sync your fitbit for ABCD! Our records show that you did this {days} days ago. Do this every day so we don't lose your data. Do not reply to this message. Contact your site directly if you have questions.",
                'child_en': "%YOUTH_FIRST_NAME%: Please sync your fitbit for ABCD! Our records show that you did this {days} days ago. Do this every day so we don't lose your data. Do not reply to this message. Contact your site directly if you have questions."}
        default = {
                'noti_subject_line': '%YOUTH_FIRST_NAME%: Please charge your Fitbit!',
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
                        'noti_text': message.format(days=devices_to_notify.loc[pGUID, 'time_since_sync'].days),
                        'noti_spanish_language': int(recipient.endswith('_es')),
                        'noti_recipient': int(not recipient.startswith('parent')) + 1}
                notifications.append(dict(default, **specifics))


            log.debug('%s, %s: %s', site, pGUID, notifications)
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
            any_alerts = submission.stop_if_early(timedelta=pd.Timedelta(days=1), 
                    check_current_purpose_only=False)
            # 2. Check that participant has not received a "charge your Fitbit" 
            # alert in the past week. Note that this check makes this script 
            # idempotent - if you re-run it, it will not recreate the alerts.
            specific_alerts = submission.stop_if_early(timedelta=pd.Timedelta(days=3), 
                    check_current_purpose_only=True)

            # Without --force, no uploads will be done. With --dry-run, upload 
            # will be attempted, but it will fail (as NotificationSubmission 
            # takes a dry_run argument that triggers its stopping logic).
            if not args.force:
                log.warning("%s, %s: To try to upload late sync notification,"
                            " run with --force", site, pGUID)
            else:
                try:
                    submission.upload(create_redcap_repeating=True)
                    log.info("%s, %s: Late sync notifications (%d versions) " 
                             "uploaded.", site, pGUID, len(notifications))
                except ValueError as e:
                    log.warning("%s, %s: Abort condition triggered. Why? "
                                "Dry run: %s; "
                                "Too early after late sync alert: %s. "
                                "Too early after any alert: %s; "
                                "(ValueError: %s)." % (
                                    site, pGUID, args.dry_run, specific_alerts, 
                                    any_alerts, e))

    log.info('Ended run with invocation: %s', sys.argv)
