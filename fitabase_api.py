import cStringIO
import dateutil
import json
import pandas as pd
import pycurl
from StringIO import StringIO
from time import sleep
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode
from zipfile import ZipFile

class FitabaseSite(object):
    """
    FitabaseSite exposes the API actions for a specific Fitabase profile.
    """
    # FIXME: Need to read up on resource freeing with StringIO + ZipFile etc., 
    # and figure out how that works with resources that are returned to an 
    # external scope.

    def __init__(self, token, url='https://api.fitabase.com/v1/'):
        """
        Initialize the object with info underlying all API calls.

        Each site has separate unique API token. 
        """
        self.token = token
        self.url = url


    def get_device_ids(self, format='df'):
        """
        For the site identified by the token, return all available Device IDs,
        names, and creation dates.

        In Fitabase API terminology, "profile" is shorthand for Connected
        Device Profile.
        """
        devices = self._make_request('Profiles/', format=format)
        return devices


    def get_tracker_sync_data(self, device_id, format='json', sleep_interval=0):
        """
        For a single tracker ID, return its last sync and battery level.
        """
        # => {'SyncDateTracker': iso8601 or None,
        #     'LatestBatteryLevelTracker': 'High', 'Medium', 'Low', 'Empty', or 
        #     None}
        sleep(sleep_interval)
        data = self._make_request('Sync/Latest/%s' % device_id, format="json")
        keys_of_interest = ['SyncDateTracker', 'LatestBatteryLevelTracker']  
        #, 'LatestDeviceNameTracker'])
        data = {k: data[k] for k in keys_of_interest}

        if data['SyncDateTracker']:
            data['SyncDateTracker'] = dateutil.parser.parse(data['SyncDateTracker'])

        if format == 'json':
            return data
        elif format == 'df':
            return pd.Series(data)


    def get_all_tracker_sync_data(self, device_ids=None, sleep_interval=0.1):
        """
        For all device IDs passed in the DataFrame, retrieve the last sync time 
        and battery level and return in an equally-indexed DataFrame.

        (Equal indexing means that you can extend the existing DataFrame with 
        something like `df.join(api.get_all_tracker_sync_data(device_ids=df))`, 
        thus easily combining the input and output.)

        Warning: This queries each device ID individually, so consider pruning 
        device_ids in order to avoid flooding the API.
        """
        if not isinstance(device_ids, pd.DataFrame) and device_ids is not None:
            raise TypeError("device_ids must be a pandas DataFrame, as "
                    "returned by get_device_ids(format='df').")
        if device_ids is None:
            device_ids = self.get_device_ids(format='df')
        return device_ids['ProfileId'].apply(
                self.get_tracker_sync_data,
                sleep_interval=sleep_interval,
                format='df')


    def get_device_last_sync(self, device_id, sleep_interval=0):
        """
        DEPRECATED: For a single device ID, return the last sync as datetime.

        Deprecated in favor of `get_tracker_sync_data`.
        """
        # Returns: {'SyncDate': iso8601 or None}
        sleep(sleep_interval)
        data = self._make_request('Sync/Latest/%s' % device_id)
        last_sync = data['SyncDate']
        if last_sync:
            return dateutil.parser.parse(last_sync)
        else:
            return None



    def get_all_devices_with_sync(self, device_ids=None, sleep_interval=0.1):
        """
        DEPRECATED: For all device IDs passed in the DataFrame, retrieve the 
        last sync time.

        Deprecated in favor of `get_all_tracker_sync_data`.

        Warning: This queries each device ID individually, so take care not to
        flood the API.
        """
        if not isinstance(device_ids, pd.DataFrame) and device_ids is not None:
            raise TypeError("device_ids must be a pandas DataFrame, as "
                            "returned by get_device_ids(format='df').")
        if device_ids is None:
            device_ids = self.get_device_ids(format='df')
        device_ids['LastSync'] = device_ids['ProfileId'].apply(
                self.get_device_last_sync, 
                sleep_interval=sleep_interval)
        return device_ids


    def export_batch(self, batch_id):
        """
        Given a valid batch ID, get the raw stream of the zip file containing the batched export
        """
        # TODO: Instead of get_batch_export_zipfile, it might make sense to 
        # have a format parameter here?
        assert isinstance(batch_id, basestring)
        batch_stream = self._make_request(
                'BatchExport/Download/%s' % batch_id,
                method="post",
                format='raw')
        # This exports the full raw stream of a zipfile
        return batch_stream


    def get_last_batch_export_info(self, format='json'):
        """
        Extract all info about the latest batch export.
        """
        batch_info = self._make_request('BatchExport/Latest', 
                format=format)
        return batch_info


    def get_last_batch_export_id(self):
        """
        Extract just the DownloadDataBatchId from the BatchExport/Latest API
        response.
        """
        batch_info = self.get_last_batch_export_info(format="json")
        batch_id = batch_info.get('DownloadDataBatchId')
        if batch_id is None:
            raise IOError('Could not retrieve ID of last regular batch;'
                          'does Fitabase know to create them?')
        else:
            return batch_id


    def get_batch_export_zipfile(self, batch_id=None):
        """
        Return the result of self.export_batch as a ZipFile object.
        """
        if not batch_id:
            batch_id = self.get_last_batch_export_id()
        batch_stream = self.export_batch(batch_id=batch_id)
        if not batch_stream:
            raise IOError('FitabaseSite.export_batch did not return a zip stream')
            return None
        return ZipFile(batch_stream)


    def export_batch_to_directory(self, batch_id=None, path=None):
        """
        Extract the result of self.export_batch to a chosen path

        DEPRECATED: Should be handled outside of the API object.

        (Note that the script doesn't check that the path exists, or that you
        have write permissions to it.)
        """
        batch_zip = self.get_batch_export_zipfile(batch_id)
        batch_zip.extractall(path=path)


    def _make_request(self, api_path, method="get", format="json", **header_data):
        """
        Helper function for all API requests. Outputs raw StringIO, JSON, or DataFrame.
        
        In general, each request will specify a URL subpath; the subpath will
        typically include any parameters, so header_data will typically be
        empty, except for site API token.

        Currently, there are no safeguards if jsonification is not successful.
        """
        # Buffer to save the curl output into
        buf = cStringIO.StringIO()

        # Any header data to put in the curl request, always with token
        data = {
            'Ocp-Apim-Subscription-Key': self.token
        }
        data.update(header_data)
        ch = pycurl.Curl()
        ch.setopt(ch.URL, self.url + api_path)

        assert method in ('get', 'post')
        if method == 'get':
            ch.setopt(ch.HTTPGET, True)
        elif method == 'post':
            ch.setopt(ch.POSTFIELDS, urlencode(data))
        # FIXME: Should be urlencoded?
        ch.setopt(ch.HTTPHEADER, ['%s: %s' % (k, v) for k, v in data.items()])
        ch.setopt(ch.WRITEFUNCTION, buf.write)
        ch.perform()
        ch.close()

        out_raw = StringIO(buf.getvalue())
        buf.close()

        assert format in ("raw", "json", "df")
        if format == "json":
            return json.load(out_raw)
        elif format == "df":
            return pd.DataFrame(json.load(out_raw))
        else:
            return out_raw
