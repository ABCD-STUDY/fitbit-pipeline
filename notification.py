import datetime
import numpy as np
import pandas as pd
import redcap

# NOTE: Should previous_notifications be an class attribute rather than an 
# instance attribute?
STATUS_CREATED = 1
STATUS_SENT = 2
STATUS_FAILURE = 3
STATUS_RESOLVED = 4
STATUS_NO_CONTACT = 5
STATUS_LANG_MISMATCH = 6

RECIPIENT_PARENT = 1
RECIPIENT_CHILD  = 2
RECIPIENT_BOTH   = 3

DELIVERY_NOW = 0
DELIVERY_MORNING = 1

class NotificationSubmission(object):
    """
    Abstraction around the submission of a direct-to-participant notification.

    Allows recency checks that will abort upload if attempted.

    Imagined use case, in which the submission is uploaded unless there's been 
    another user alert in the past three days, or cause-specific user alert in 
    the past six:

    ```python
    for group, data in new_notifications.groupby('record_id'):
        n = NotificationSubmission(api, data)
        n.previous_notifications = old_notifications
        n.stop_if_early(pd.Timedelta(days=3)
        n.stop_if_early(pd.Timedelta(days=6), check_current_purpose_only=True)
        n.upload()
    ```
    """


    def __init__(self, api, submission_df, previous_notifications=None, dry_run=False):
        """
        Seed the submission with content and connectivity.

        If dry_run is provided, the submission will be automatically aborted.

        If previous_notifications (expected direct output of 
        `api.export_records(forms='notifications', format='df')`) is *not* 
        present, it will be pulled for the subject if/when needed.
        """
        self.__abort = False
        self.__previous_notifications = None
        self.previous_notifications = previous_notifications

        # Submission_df is already expected with Redcap-compatible columns
        assert isinstance(api, redcap.Project)

        # Check that only one subject is in the submission
        record_ids = submission_df.index.unique().tolist()
        assert len(record_ids) == 1, "submission_df can only contain one subject"
        self.record_id = record_ids[0]

        # Check that only one purpose is in the submission
        purposes = submission_df['noti_purpose'].unique().tolist()
        assert len(purposes) == 1, "submission_df can only contain one purpose"
        self.purpose = purposes[0]

        self.api = api
        self.submission = submission_df

        if dry_run:
            self.__abort = True


    def stop_if_early(self, timedelta=None, check_current_purpose_only=False):
        """
        Check if too little time has elapsed since previous messages have been 
        sent.

        If the current messages would be too early, returns True and sets 
        self.__abort to True. Otherwise, returns False.
        """
        previous_notifications = self.previous_notifications

        # Validate arguments
        if timedelta is not None:
            if not (isinstance(timedelta, pd.Timedelta) or 
                    isinstance(timedelta, datetime.timedelta)):
                raise TypeError("timedelta must be either pandas.Timedelta "
                                "or datetime.timedelta!")

        # Now, we chip away at the previous_notifications subset to see if, 
        # after subsetting for the timedelta and purpose, any violations remain
        try:
            notifications_subject = previous_notifications.loc[self.record_id, :]
        except KeyError:
            return False

        if check_current_purpose_only:
            notifications_subject = notifications_subject.loc[
                    notifications_subject['noti_purpose'] == self.purpose]

        if timedelta:
            # TODO: Should we compare to creation timestamp or send timestamp?
            created_within_timedelta = (pd.to_datetime('today') - 
                    notifications_subject['noti_timestamp_create'] < timedelta)
            sent_within_timedelta = (pd.to_datetime('today') - 
                    notifications_subject['noti_sent_timestamp'] < timedelta)
            notifications_subject = notifications_subject.loc[created_within_timedelta]
                    

        # After all this, if any messages remained in the set, the current 
        # messages constitute spam and must be abandoned.
        if not notifications_subject.empty:
            self.__abort = True
            return True
        else:
            return False


    def upload(self, create_redcap_repeating=False):
        """
        If no stops have been set, upload the submission to Redcap.
        """
        if self.__abort:
            raise ValueError("Submission doesn't meet upload requirements")
            return False
        else:
            if create_redcap_repeating:
                self.add_redcap_repeat_instance(force=True)
            return self.api.import_records(self.submission)


    def add_redcap_repeat_instance(self, form_name="notifications", lookup=None, force=False):
        # force: rewrite current redcap_instrument_* even if it exists
        # lookup: use this DataFrame to get the last redcap_repeat_instance
        if lookup is not None:
            next_instance = lookup.loc[self.record_id] + 1
        else:
            try:
                next_instance = self.previous_notifications.loc[
                        self.record_id, 'redcap_repeat_instance'].max() + 1
            except KeyError:  # record_id doesn't exist
                next_instance = 1
        if (not force and 
                ('redcap_repeat_instrument' in self.submission.columns) and 
                ('redcap_repeat_instance' in self.submission_columns) and
                pd.notnull(self.submission.loc[:, 
                    ['redcap_repeat_instrument', 'redcap_repeat_instance']]
                    .all())):
            return
        else:
            row_count = self.submission.shape[0]
            self.submission.loc[:, 'redcap_repeat_instrument'] = 'notifications'
            self.submission.loc[:, 'redcap_repeat_instance'] = np.arange(next_instance, next_instance + row_count)
            return


    @property
    def previous_notifications(self):
        if self.__previous_notifications is not None:
            return self.__previous_notifications
        else:
            try:
                self.previous_notification = self.api.export_records(
                        records=[self.record_id], 
                        forms=['notifications'],
                        format='df')
            except pd.errors.EmptyDataError as e:
                self.previous_notifications = pd.DataFrame()
            return self.__previous_notifications

    @previous_notifications.setter
    def previous_notifications(self, value):
        if value is None:
            return
        if not isinstance(value, pd.DataFrame):
            raise TypeError('previous_notifications must be a pandas DataFrame')
        if value.empty:
            self.__previous_notifications = value
            return

        # Modify the previous_notifications DataFrame
        needed_cols = ['noti_purpose', 'noti_status', 'noti_sent_timestamp', 'noti_timestamp_create',
                'redcap_repeat_instrument', 'redcap_repeat_instance']
        for col in needed_cols:
            assert col in value.columns
        self.__previous_notifications = value.loc[value['redcap_repeat_instrument'] == 'notifications']
        datetime_cols = ['noti_timestamp_create', 'noti_sent_timestamp']
        for col in datetime_cols:
            self.__previous_notifications.loc[:, col] = pd.to_datetime(self.__previous_notifications[col])
        if value.index.names[0] != "record_id":
            self.__previous_notifications.set_index('record_id', inplace=True)
