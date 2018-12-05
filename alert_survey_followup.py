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
from notification import (NotificationSubmission, RECIPIENT_PARENT, 
        STATUS_CREATED, RECIPIENT_CHILD, RECIPIENT_BOTH, DELIVERY_NOW, 
        DELIVERY_MORNING)
import os
import pandas as pd
import redcap as rc
import requests
import sys
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
    parser.add_argument('--subjects', '-s', nargs='+', required=False,
            help="Only send out the notification for enumerated subjects.")
    parser.add_argument('--force', '-f', action='store_true',
            help="Without --force, no uploads will be attempted.")
    parser.add_argument('--verbose', '-v', action='store_true',
            help="Save DEBUG-level messages, too.")
    return parser.parse_args()


def process_site(rc_api, notif_api, site, dry_run=False, force_upload=False, 
        only_subjects=None):
    """
    Given API objects and parameters, create survey-link messages for the 
    participants who should receive them.

    Returns the list of IDs to be alerted.
    """
    # NOTE: fitc_noti_generated_survey is currently set by the alert-generating 
    # script, not by the PII, so it must not be regarded as gospel - instead, 
    # we can offload the work of determining whether a survey notification has 
    # been sent to NotificationSubmission.stop_if_early.
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
        return None
    else:
        log.info("%s: %d devices with finished collection at site.", site, 
                done_devices.shape[0])


    # Find all participants who are done, but either they or their parents did 
    # not complete the survey. (NotificationSubmission will decide whether 
    # they've been sent a notification.)
    youth_missing   = pd.isnull(done_devices['fitpo_physical'])
    parent_missing  = pd.isnull(done_devices['fitpo_physical_p'])

    # Save whether youth or parent survey is missing for later
    done_devices.loc[youth_missing, 'youth_missing'] = True
    done_devices.loc[parent_missing, 'parent_missing'] = True

    # Create a useful subset that only contains notifiable subjects
    to_notify = done_devices.loc[parent_missing | youth_missing]
    ids_to_notify = to_notify.index.get_level_values('id_redcap').tolist()

    if to_notify.empty:
        log.info('%s: No done devices with incomplete follow-up.', site)
        return None
    else:
        log.info('%s: %d devices with incomplete follow-up.', site, 
                len(ids_to_notify))

    # If only a subset of participants should be processed, remove them from 
    # the notifiable DataFrame now
    if only_subjects:
        ids_to_notify = [i for i in ids_to_notify if i in only_subjects]
        if len(ids_to_notify) == 0:
            log.info('%s: None of the devices with incomplete follow-up are in'
                ' --subjects; skipping', site)
            return None
        else:
            to_notify = to_notify.loc[ids_to_notify]
            log.info('%s: Trimmed device list to %d, out of %d IDs '
                'specified in --subjects', site, len(ids_to_notify), 
                len(only_subjects))

    # For final subset of notifiable participants, retrieve Redcap survey links
    to_notify['youth_link']  = to_notify.apply(apply_redcap_survey_url, 
            axis=1, rc_api=rc_api, survey='fitbit_postassessment_youth')
    to_notify['parent_link'] = to_notify.apply(apply_redcap_survey_url, 
            axis=1, rc_api=rc_api, survey='fitbit_postassessment_parent')

    # Get prior notifications generated for this final subset, too
    try:
        notif_records = notif_api.export_records(records=ids_to_notify, 
                forms=['notifications'],
                format='df')
    except pd.errors.EmptyDataError as e:  # All tagged IDs have no priors
        notif_records = pd.DataFrame()

    timestamp_now_mdy = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    timestamp_now_ymd = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    default = {
            'noti_subject_line': 'Please complete post-Fitbit survey!',
            'noti_purpose': 'send_survey_reminder',
            'noti_status': STATUS_CREATED,
            'noti_timestamp_create': timestamp_now_ymd,
            'noti_site_name': site,
            'noti_send_preferred_time': DELIVERY_MORNING}

    notified = []
    for pGUID in ids_to_notify:
        notifications = []
        # Get messages dependent on which surveys are missing, filled in with 
        # links for this pGUID
        messages = get_targeted_messages(
                youth_missing=to_notify.loc[pGUID, 'youth_missing'],
                parent_missing=to_notify.loc[pGUID, 'parent_missing'],
                youth_link=to_notify.loc[pGUID, 'youth_link'],
                parent_link=to_notify.loc[pGUID, 'parent_link'])

        # Convert messages to a DataFrame acceptable to NotificationSubmission
        notifications = process_messages(messages, default, record_id=pGUID)
        notifications_df = pd.DataFrame(notifications).set_index('record_id')
        submission = NotificationSubmission(notif_api, notifications_df, 
                notif_records, dry_run=dry_run)

        log.debug('%s, %s: %s', site, pGUID, notifications)

        # Only send the survey if no survey notification has been sent in 
        # the past 3 days
        survey_alerts = submission.stop_if_early(timedelta=pd.Timedelta(days=3), 
                check_current_purpose_only=True, 
                check_created_or_sent_only=True)

        # Only send the survey if RA has not reached out in past 2 days
        ra_alerts = submission.stop_if_too_early_after(
                timedelta=datetime.timedelta(days=2), 
                reference_time=to_notify.loc[pGUID, 'fitc_last_dte_ra_contact'])

        # Move on to next participants if submission is aborted. (If dry_run is 
        # True, then each participant will always be aborted.)
        if submission.is_aborted:
            log.warning("%s, %s: %d messages would abort. Reason: %s", 
                    site, pGUID, len(notifications), submission.abortion_reason)
            continue

        # Warn if the number of associated devices is dangerously low
        number_devices = to_notify.loc[pGUID, 'fitc_number_devices']
        if pd.isnull(number_devices) or (number_devices == 0):
            log.warning("%s, %s: Generating notification, but no devices "
                        "associated with account!", 
                        site, pGUID)
        elif number_devices == 1:
            log.warning("%s, %s: Only one device associated with account; "
                        "info loss possible.", 
                        site, pGUID)


        # Final step: either pretend-upload or real-upload.
        #
        # NOTE: On the pretend-upload route, pGUID will always be added to 
        # "successfully notified" return list. On the actual upload route, 
        # pGUID is only considered successful if the upload doesn't fail.
        if dry_run or not force_upload:
            log.warning("%s, %s: %d end-survey notification(s) would be sent if"
                        " script ran with --force", site, pGUID, 
                        len(notifications))
            notified.append(pGUID)
        else:
            try:
                # Write to Notifications Redcap
                submission.upload(create_redcap_repeating=True)
                log.info("%s, %s: End-survey notifications (%d versions) " 
                         "uploaded.", site, pGUID, len(notifications))
                notified.append(pGUID)

                # Write to main Redcap
                rc_api.import_records(
                        [{'id_redcap': pGUID, 
                          'redcap_event_name': REDCAP_EVENT,
                          'fitc_noti_generated_survey': timestamp_now_mdy}])
                log.info("%s, %s: Alert generation timestamp loaded to Redcap.",
                         site, pGUID)
            except ValueError as e:
                log.warning("%s, %s: Abort condition triggered. Why? "
                            "Dry run: %s; "
                            "Too early after survey notification: %s. "
                            "Too early after last human contact: %s. "
                            "(ValueError: %s)." % (
                                site, pGUID, dry_run, 
                                survey_alerts, ra_alerts, e))
    return notified


def get_targeted_messages(youth_missing, parent_missing, youth_link, parent_link):
    """
    Produce different messages based on whose survey is absent.
    """
    if youth_missing and parent_missing:
        message_base = ("You have finished 21 days with the Fitbit! It is "
            "important to complete the following so you can receive your "
            "payment. 1) Send Fitbit device by mail with the pre-paid envelope."
            "Be sure to include the charger. 2) complete a questionnaire")
        messages = {
                'child_en':  "%s: %s" % (message_base, youth_link),
                'parent_en': "%s. Parent: %s, Youth: %s" % (message_base, 
                    parent_link, youth_link),
                'parent_es': "%s. Parent: %s, Youth: %s" % (message_base, 
                    parent_link, youth_link),
                }
    elif youth_missing:
        messages = {
                'parent_en': ("Thank you for completing your questionnaire! In "
                    "order to receive your ABCD payment, your child must also "
                    "complete their questionnaire: %s") % youth_link,
                'parent_es': ("Thank you for completing your questionnaire! In "
                    "order to receive your ABCD payment, your child must also "
                    "complete their questionnaire: %s") % youth_link,
                'child_en': ("Thank you for finishing your 21 days with the "
                    "Fitbit! In order to receive your ABCD payment, you must "
                    "complete the questionnaire: %s" % youth_link),
                }
    elif parent_missing:
        messages = {
                'child_en': ("Thank you for completing your questionnaire! In "
                    "order to receive your ABCD payment, your parent "
                    "must complete their questionnaire. If they haven't "
                    "received a text with the link to the questionnaire, "
                    "please contact your ABCD site."),
                'parent_en': ("Your child has completed the post-Fitbi survey."
                    "In order to receive your ABCD payment, you must "
                    "complete the questionnaire too: %s" % parent_link),
                'parent_es': ("Your child has completed the post-Fitbit survey."
                    "In order to receive your ABCD payment, you must "
                    "complete the questionnaire too: %s" % parent_link),
                }
    else:
        raise ValueError('Neither youth nor parent missing; this code path '
            'should never be executed')
        return None

    return messages


def process_messages(messages, defaults, **kwargs):
    """
    Given a {RECIPIENT}_{LANG} -> message dict, combine with defaults to create 
    a DataFrame-like dict for NotificationSubmission to ingest.
    """
    # For each message, merge defaults and specifics and appends them to the 
    # notifications list
    notifications = []
    for recipient, message in messages.items():
        if recipient.startswith('parent'):
            deliver_to = RECIPIENT_PARENT
        else:
            deliver_to = RECIPIENT_CHILD

        specifics = {
                'noti_text': message,
                'noti_spanish_language': int(recipient.endswith('_es')),
                'noti_recipient': deliver_to}

        notification = defaults.copy()
        notification.update(specifics)
        notification.update(kwargs)
        # notification = dict(dict(defaults, **specifics), **kwargs)
        notifications.append(notification)

    return notifications
    # return pd.DataFrame(notifications).set_index('record_id')


if __name__ == "__main__":
    REDCAP_URL = 'https://abcd-rc.ucsd.edu/redcap/api/'
    REDCAP_EVENT = '2_year_follow_up_y_arm_1'  # FIXME: Move to args?
    args = parse_arguments()
    if args.verbose:
        log.getLogger().setLevel(log.DEBUG)
    log.debug('Started run with invocation: %s', sys.argv)

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

        notified = process_site(rc_api, notif_api, site, 
                dry_run=args.dry_run, force_upload=args.force, 
                only_subjects=args.subjects)

        if notified:
            upload_run = args.force and not args.dry_run
            action = 'will be' if upload_run else 'would (but will not) be'
            log.info('%s: Processing over, subjects that %s notified are: %s',
                    site, action, ", ".join(notified))
        else:
            log.info('%s: Processing over, no subjects to notify', site)

    log.debug('Ended run with invocation: %s', sys.argv)
