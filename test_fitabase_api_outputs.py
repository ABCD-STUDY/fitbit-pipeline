"""
This file tests the consistency of the Fitabase API, not the way API is exposed
/ abstracted over by the Python constructs.
"""
import json
import os
import pandas as pd
import pytest

# TODO: Should have a JSON with per-site sample device ID
# TODO: Should use tdda/great_expectations to test shape AND content of API 
# returns
# with open(os.path.join(os.path.dirname(__file__), '../fitabase_tokens.json')) as data_file:
with open('fitabase_tokens.json') as data_file:
    fitabase_tokens = json.load(data_file).get('tokens')
    fitabase_tokens = pd.DataFrame.from_records(fitabase_tokens, index='name')
    TEST_TOKEN = fitabase_tokens.loc['UCSD', 'token']

@pytest.fixture
def api(test_token=TEST_TOKEN):
    from fitabase import Project
    return Project(token=test_token)._make_request

@pytest.fixture
def device_id():
    return '5822ade8-218c-4cef-b11c-12051df061f5'

def test_api_get_device_list(api):
    result = api('Profiles/')
    assert len(result) > 0
    expected_keys = set(['ProfileId', 'Name', 'CreatedDate'])
    for line in result:
        for key in expected_keys:
            assert key in line

def test_api_get_device_sync(api, device_id):
    result = api('Sync/Latest/%s' % device_id)
    expected_keys = set(['SyncDate', 'LatestBatteryLevel', 'LatestDeviceName',  # across trackers and scales
        'SyncDateTracker', 'LatestBatteryLevelTracker', 'LatestDeviceNameTracker',  # trackers only
        'Devices'])
    for key in expected_keys:
        assert key in result

def test_api_get_last_batch_export(api):
    result = api('BatchExport/Latest')
    expected_keys = set(['DownloadDataBatchId', 'StartDate', 'EndDate', 'Name',
        'ProcessingStarted', 'ProcessingCompleted'])
    for key in expected_keys:
        assert key in result

@pytest.mark.skip
def test_api_post_last_batch_export(api):
    pass

def test_api_get_daily_activity(api, device_id, start_date='11-01-2018', end_date='11-30-2018'):
    result = api('DailyActivity/%s/%s/%s' % (device_id, start_date, end_date))
    assert len(result) > 0
    expected_keys =   ["ActivityDate",
            "TotalDistance",
            "TrackerDistance",
            "LoggedActivitiesDistance",
            "VeryActiveDistance",
            "ModeratelyActiveDistance",
            "LightActiveDistance",
            "SedentaryActiveDistance",
            "VeryActiveMinutes",
            "FairlyActiveMinutes",
            "LightlyActiveMinutes",
            "SedentaryMinutes",
            "TotalSteps",
            "Calories",
            "Floors",
            "CaloriesBMR",
            "MarginalCalories",
            "RestingHeartRate",
            "GoalCaloriesOut",
            "GoalDistance",
            "GoalFloors",
            "GoalSteps"]
    for line in result:
        for key in expected_keys:
            assert key in line
