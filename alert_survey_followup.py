#!/usr/bin/env python
"""
Send out alerts about unfilled surveys if the link has been sent more than 
three days ago.

It can be run with multiple sites, e.g.:

    ./alert_survey_followup.py -fv UCSD CHLA UPMC UMICH

will trawl and notify any people from these sites.

For more details, run `./alert_survey_followup.py -h`.
"""

import argparse
import datetime
from fitabase_api import FitabaseSite
import json
import logging as log
from notification import NotificationSubmission
import os
import pandas as pd
import redcap as rc
import requests
from utils import get_redcap_survey_url, apply_redcap_survey_url

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

    with open('/var/www/html/code/php/tokens.json') as data_file:
        redcap_tokens = json.load(data_file)
        redcap_tokens = pd.DataFrame.from_dict(redcap_tokens, orient='index', columns=['token'])

    with open(os.path.join(CURRENT_DIR, 'notifications_token.json')) as token_file:
        notif_token = json.load(token_file).get('token')
        notif_api = rc.Project(REDCAP_URL, notif_token)

    # No need to keep the call one site at a time - we can iterate through all
    for site in args.site:
        # Get device list from main Redcap project
        rc_token = redcap_tokens.loc[site, 'token']
        rc_api = rc.Project(REDCAP_URL, rc_token)
        rc_fit_datefields = ['fitc_device_dte', 'fitc_noti_generated_survey', 
                'fitc_noti_generated_sync', 'fitc_noti_generated_bat', 
                'fitc_last_dte_ra_contact', 'fitc_last_dte_daic_contact'] 
        rc_fit_fields = ['fitc_last_status_contact', 'fitc_number_devices',
                # First question on the survey:
                'fitpo_physical', 'fitpo_physical_p']
        rc_devices = rc_api.export_records(
                fields=rc_fit_datefields + rc_fit_fields + [rc_api.def_field],
                events=[REDCAP_EVENT],  
                export_data_access_groups=True,
                df_kwargs={
                    'parse_dates': rc_fit_datefields,
                    'index_col': [rc_api.def_field]},
                format='df')

        # Only look at devices that are done collecting within recent past
        rc_devices['time_since_start'] = (
                pd.to_datetime('today') - rc_devices['fitc_device_dte'])
        rc_devices['done_collecting'] = (
                (rc_devices['time_since_start'] >= pd.Timedelta(days=22)) &
                (rc_devices['time_since_start'] < pd.Timedelta(days=200)))
        done_devices = rc_devices.loc[rc_devices['done_collecting']]
        if done_devices.empty:
            log.warn("%s: No devices with finished collection at site.", site)
            continue
        else:
            log.info("%s: %d devices with finished collection at site.", site, 
                    done_devices.shape[0])
        done_devices['time_since_survey_receipt'] = (
                pd.to_datetime('today') - done_devices['fitc_noti_generated_survey'])

        # 1. TODO: If a participant is done but has not been sent a survey 
        #    link, do so (functionality currently implemented by 
        #    sendEndSurveys.py)
        # 2. If a participant has been sent a survey link, but survey has not 
        #    been submitted (because answer to first question is missing)

        survey_not_sent = pd.isnull(done_devices['fitc_noti_generated_survey'])
        survey_sent_3d  = done_devices['time_since_survey_receipt'] > pd.Timedelta(days=3)
        # fitc_noti_generated_survey is currently set by sendEndSurveys, not by 
        # the PII, so it shouldn't be regarded as gospel - instead, we can 
        # offload the work of determining whether a survey notification has 
        # been sent to NotificationSubmission.stop_if_early.
        missing_youth   = pd.isnull(done_devices['fitpo_physical'])
        missing_parent  = pd.isnull(done_devices['fitpo_physical_p'])

        # youth_to_notify = done_devices.loc[survey_sent_3d & missing_youth]
        # parent_to_notify = done_devices.loc[survey_sent_3d & missing_parent]
        # to_notify = done_devices.loc[(survey_not_sent | survey_sent_3d) 
        #         & (missing_parent | missing_youth)]
        to_notify = done_devices.loc[missing_parent | missing_youth]
        ids_to_notify = to_notify.index.get_level_values('id_redcap').tolist()

        # For now, we assume that parent can get all notifications.
        #
        # TODO: Are the notification to parent and youth separate? What if PII 
        # DB didn't have the contact for one or both of them? (We'd see that in 
        # the missing fitc_noti_generated_survey.)
        #
        # Cases:
        #
        # 1. Notification has been sent but:
        #       - Neither has completed the survey. -> re-send links to both
        #       - Youth has completed the survey. -> send parent link, send 
        #       youth a message to bother parent
        #       - Parent has completed the survey. -> send youth link, send 
        #       parent a message to bother the youth.
        # 2. Notification has not been sent and:
        #       - There are devices. -> ? maybe devices were added later?
        #       - There are no devices. -> throw an error for sites?

        # if youth_to_notify.empty and parent_to_notify:
        if to_notify.empty:
            log.info('%s: No done devices with incomplete follow-up.', site)
            continue

        to_notify['youth_link']  = to_notify.apply(apply_redcap_survey_url, 
                axis=1, rc_api=rc_api, survey='fitbit_postassessment_youth')
        to_notify['parent_link'] = to_notify.apply(apply_redcap_survey_url, 
                axis=1, rc_api=rc_api, survey='fitbit_postassessment_parent')

        try:
            notif_records = notif_api.export_records(records=ids_to_notify, 
                    forms=['notifications'],
                    format='df')
        except pd.errors.EmptyDataError as e:  # All tagged IDs have no priors
            notif_records = pd.DataFrame()

        STOCK_MESSAGE_EN = ("You have finished 21 days with the Fitbit! It is "
            "important to complete the following so you can receive your "
            "payment. 1) Send Fitbit device by mail with the pre-paid envelope."
            "Be sure to include the charger. 2) complete a questionnaire")

        messages = {
                'child_en':  STOCK_MESSAGE_EN + ": %s",
                'parent_en': STOCK_MESSAGE_EN + ". Parent: %s, Youth: %s",
                'parent_es': STOCK_MESSAGE_EN + ". Parent: %s, Youth: %s",
        }

        default = {
                'noti_subject_line': 'Please complete post-Fitbit survey!',
                'noti_status': 1, # 1: Created
                'noti_purpose': 'send_survey_reminder',
                'noti_timestamp_create': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'noti_site_name': site,
                'noti_recipient': 2,  # 1: Parent only, 2: Participant only, 3: Both
                'noti_send_preferred_time': 1}  # 0: immediate, 1: daily

        for pGUID in ids_to_notify:
            notifications = []
            # For each message, merge defaults and specifics and appends them 
            # to the notifications list
            for recipient, message in messages.items():
                youth_link = to_notify.loc[pGUID, 'youth_link']
                if recipient.startswith('parent'):
                    deliver_to = 2
                    parent_link = to_notify.loc[pGUID, 'parent_link']
                    filled_message = message % (parent_link, youth_link)
                else:
                    deliver_to = 1
                    filled_message = message % youth_link

                specifics = {
                        'record_id': pGUID,
                        'noti_text': filled_message,
                        'noti_spanish_language': int(recipient.endswith('_es')),
                        'noti_recipient': deliver_to}
                notifications.append(dict(default, **specifics))

            notifications_df = pd.DataFrame(notifications).set_index('record_id')
            submission = NotificationSubmission(notif_api, notifications_df, 
                    notif_records)
            # submission = NotificationSubmission(notif_api, notifications_df, 
            #         notif_records, dry_run=args.dry_run)

            # Only send the survey if no survey notification has been sent in 
            # the past 3 days
            survey_alerts = submission.stop_if_early(timedelta=pd.Timedelta(days=3), 
                    check_current_purpose_only=True, 
                    check_created_or_sent_only=True)

            number_devices = to_notify.loc[pGUID, 'fitc_number_devices']
            if pd.isnull(number_devices) or (number_devices == 0):
                log.warning("%s, %s: Generating notification, but no devices associated with account!", 
                            site, pGUID)

            if args.dry_run:
                if submission._NotificationSubmission__abort:
                    log.warning("%s, %s: Running with --dry-run; %d messages would abort: %s (reason: %s)", 
                            site, pGUID, len(notifications),
                            submission._NotificationSubmission__abort,
                            submission._NotificationSubmission__abort_reason)
                else:
                    log.warning("%s, %s: Running with --dry-run; %d messages would be sent.", 
                            site, pGUID, len(notifications))
            elif not args.force:
                log.warning("%s, %s: To try to upload %d end-survey notification(s),"
                            " run with --force", site, pGUID, len(notifications))
            else:
                try:
                    submission.upload(create_redcap_repeating=True)
                    log.info("%s, %s: End-survey notifications (%d versions) " 
                             "uploaded.", site, pGUID, len(notifications))
                except ValueError as e:
                    log.warning("%s, %s: Abort condition triggered. Why? "
                                "Dry run: %s; "
                                "Too early after survey notification: %s. "
                                "(ValueError: %s)." % (
                                    site, pGUID, args.dry_run, survey_alerts, e))
