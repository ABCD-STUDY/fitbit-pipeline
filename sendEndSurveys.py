#!/usr/bin/env python
#
# Send the survey's out to participants and parents that have used fitbit devices for 22 days (day 23)
#
# Summary of functionality:
#
# 1. Based on participants in Fitabase, retrieve from Redcap a list 
#    of device issuance dates + survey notification timestamps.
# 2. If (now - fitc_device_dte).days == 23 and no 
#    fitc_noti_generated_survey, then prepare to send a reminder.
# 3. Get youth and parent survey links from Redcap.
# 4. Push notifications to ABCD Notifications Redcap.
# 5. Push timestamp of notification generation to ABCD Study Redcap 
#    as fitc_noti_generated_survey.

import pycurl, cStringIO, json, sys, re, time
import datetime
from StringIO import StringIO


with open('fitabase_tokens.json') as data_file:
    ftokens = json.load(data_file)

with open('notifications_token.json') as token_file:
    notif_token = json.load(token_file).get('token')

# default
site = "UCSD"
# do not add to REDCap
force = ""

site = sys.argv[1]
if len(sys.argv) == 3:
    force = sys.argv[2]

# each site has its own token for access
ftoken = ''
for s in ftokens['tokens']:
    if s['name'] == site:
        ftoken = s['token']
if ftoken == '':
    print('Error: this site does not have a token')
    sys.exit(-1)
    
buf = cStringIO.StringIO()
# variables we need from REDCap
data = {
    'Ocp-Apim-Subscription-Key': ftoken
}
ch = pycurl.Curl()
ch.setopt(ch.URL, 'https://api.fitabase.com/v1/Profiles/')
ch.setopt(ch.HTTPGET, True)
ch.setopt(ch.HTTPHEADER, ['%s: %s' % (k, v) for k, v in data.items()])
ch.setopt(ch.WRITEFUNCTION, buf.write)
ch.perform()
ch.close()

v = json.load( StringIO(buf.getvalue()) )
buf.close()

# print out the profiles on fitabase - one for each participant
# print(json.dumps(v))
# [{"ProfileId": "a99f06d2-4264-49a1-80d5-3b794a507d96", "Name": "Test 9/25", "CreatedDate": "2018-09-25T19:47:50.017"}]

# what is the list of participants that have a device right now? fitc_device_dte
with open('../../code/php/tokens.json') as data_file:
    tokens = json.load(data_file)

buf = cStringIO.StringIO()
data = {
    'token': tokens[site],
    'content': 'record',
    'format': 'json',
    'type': 'flat',
    'fields[0]': 'id_redcap',
    'fields[1]': 'fitc_device_dte',
    'fields[2]': 'fitc_noti_generated_survey',
    'fields[3]': 'redcap_event_name',
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

va = json.load( StringIO(buf.getvalue()) )
buf.close()

participants = []
for d in va:
    #if d['id_redcap'] != "NDAR_INVTEST00001":
    #    continue
    # a participant is current if his/her Device distribution date is in the last 22 days
    if d['fitc_device_dte'] != '':
        # 2017-10-22 14:22
        date1 = datetime.datetime.strptime(d['fitc_device_dte'], '%Y-%m-%d %H:%M')
        date2 = datetime.datetime.now()
        delta = (date2 - date1).days
        #print("pGUID: ", d['id_redcap'], " delta: ", delta, " \"", d['fitc_device_dte'], "\" ", d['redcap_event_name'])
        if (delta == 23) and (d['fitc_noti_generated_survey'] == ""):
            # only add this participant if we have reached day 23 and there is no survey notification yet
            participants.append([d['id_redcap'], d['fitc_device_dte'], delta, d['fitc_noti_generated_survey'], d['redcap_event_name']])

print(json.dumps(participants))

for d in participants:
    #
    # create the survey
    #
    pGUID = d[0]
    event = d[4]
        
    next_noti_id = 0
    # what is the next redcap_repeat_instance that is not yet used up?
    buf = cStringIO.StringIO()
    data = {
        'token': notif_token,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'records[0]': pGUID,
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    ch = pycurl.Curl()
    ch.setopt(ch.URL, 'https://abcd-rc.ucsd.edu/redcap/api/')
    ch.setopt(ch.HTTPPOST, data.items())
    ch.setopt(ch.WRITEFUNCTION, buf.write)
    ch.perform()
    ch.close()
    ri = json.load( StringIO(buf.getvalue()) )
    buf.close()            
    print("existing notifications: ", json.dumps(ri))
    
    # what is the next repeat instance we can use?
    for v in ri:
        if 'redcap_repeat_instrument' in v and v['redcap_repeat_instrument'] != 'notifications':
            continue
        if 'redcap_repeat_instance' in v and v['redcap_repeat_instance'] != '':
            num = int(v['redcap_repeat_instance'])
            if num > next_noti_id:
                next_noti_id = num
    next_noti_id = next_noti_id + 1
    print("the next free record id is: %d" % next_noti_id)

    # lets get a survey link for the parent and one for the child (for the fitbit_postassessment_youth/parent instruments)
    buf = cStringIO.StringIO()
    data = {
        'token': tokens[site],
        'content': 'surveyLink',
        'format': 'json',
        'instrument': 'fitbit_postassessment_youth',
        'event': event,
        'record': pGUID,
        'returnFormat': 'json'
    }
    ch = pycurl.Curl()
    ch.setopt(ch.URL, 'https://abcd-rc.ucsd.edu/redcap/api/')
    ch.setopt(ch.HTTPPOST, data.items())
    ch.setopt(ch.WRITEFUNCTION, buf.write)
    ch.perform()
    ch.close()
    #print buf.getvalue()
    ysurvey = buf.getvalue()
    buf.close()

    buf = cStringIO.StringIO()
    data = {
        'token': tokens[site],
        'content': 'surveyLink',
        'format': 'json',
        'instrument': 'fitbit_postassessment_parent',
        'event': event,
        'record': pGUID,
        'returnFormat': 'json'
    }
    ch = pycurl.Curl()
    ch.setopt(ch.URL, 'https://abcd-rc.ucsd.edu/redcap/api/')
    ch.setopt(ch.HTTPPOST, data.items())
    ch.setopt(ch.WRITEFUNCTION, buf.write)
    ch.perform()
    ch.close()
    #print buf.getvalue()
    psurvey = buf.getvalue()
    buf.close()
    
    noti_youth     = "Hi %%YOUTH_FIRST_NAME%%, you have finished 21 days with the Fitbit! It is important to complete the following so you can receive your payment. 1) Send Fitbit device by mail with the pre-paid envelope. Be sure to include the charger. 2) complete a questionnaire: %s" % ysurvey
    noti_parent_en = "Hi %%YOUTH_FIRST_NAME%%, you have finished 21 days with the Fitbit! It is important to complete the following so you can receive your payment. 1) Send Fitbit device by mail with the pre-paid envelope. Be sure to include the charger. 2) complete a questionnaire. Parent: %s, Youth: %s" % (psurvey, ysurvey)
    noti_parent_es = "Hi %%YOUTH_FIRST_NAME%%, you have finished 21 days with the Fitbit! It is important to complete the following so you can receive your payment. 1) Send Fitbit device by mail with the pre-paid envelope. Be sure to include the charger. 2) complete a questionnaire. Parent: %s, Youth: %s" % (psurvey, ysurvey)
    notifications = []
    notifications.append({
        'record_id': pGUID,
        'noti_subject_line': '%YOUTH_FIRST_NAME%: ABCD Fitbit survey reminder',
        'noti_text': noti_youth,
        'noti_spanish_language': 0,
        'noti_purpose': 'send_survey_reminder',
        'noti_status': 1,
        'noti_timestamp_create': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'noti_site_name': site,
        'noti_recipient': 2,
        'noti_send_preferred_time': 1,
        'redcap_repeat_instrument': 'notifications',
        'redcap_repeat_instance': next_noti_id 
    })
    notifications.append({
        'record_id': pGUID,
        'noti_subject_line': 'ABCD Fitbit survey reminder for %YOUTH_FIRST_NAME%',
        'noti_text': noti_parent_en,
        'noti_spanish_language': 0,
        'noti_status': 1,
        'noti_purpose': 'send_survey_reminder',
        'noti_timestamp_create': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'noti_site_name': site,
        'noti_recipient': 1,
        'noti_send_preferred_time': 1,
        'redcap_repeat_instrument': 'notifications',
        'redcap_repeat_instance': next_noti_id+1
    })
    notifications.append({
        'record_id': pGUID,
        'noti_subject_line': 'ABCD Fitbit survey reminder for %YOUTH_FIRST_NAME%',
        'noti_text': noti_parent_es,
        'noti_spanish_language': 1,
        'noti_status': 1,
        'noti_purpose': 'send_survey_reminder',
        'noti_timestamp_create': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'noti_site_name': site,
        'noti_recipient': 1,
        'noti_send_preferred_time': 1,
        'redcap_repeat_instrument': 'notifications',
        'redcap_repeat_instance': next_noti_id +2   
    })
    
    # create the notifications in REDCap
    buf = cStringIO.StringIO()
    data = {
        'token': notif_token,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'overwriteBehavior': 'normal',
        'forceAutoNumber': 'false',
        'data': json.dumps(notifications),
        'returnContent': 'count',
        'returnFormat': 'json'
    }
    print(json.dumps(data))
    if force == "-f":
        ch = pycurl.Curl()
        ch.setopt(ch.URL, 'https://abcd-rc.ucsd.edu/redcap/api/')
        ch.setopt(ch.HTTPPOST, data.items())
        ch.setopt(ch.WRITEFUNCTION, buf.write)
        ch.perform()
        ch.close()
        print buf.getvalue()
    buf.close()     
        
    # mark in REDCap that we generated a notification in fitc_noti_generated_sync
    buf = cStringIO.StringIO()
    data = {
        'token': tokens[site],
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'overwriteBehavior': 'overwrite',
        'data': json.dumps([{ 'id_redcap': pGUID, 'redcap_event_name': event, 'fitc_noti_generated_survey': datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S") }]),
        'returnContent': 'count',
        'returnFormat': 'json'
        #'record_id': hashlib.sha1().hexdigest()[:16]
    }
    print(json.dumps(data))
    if force == "-f":
        ch = pycurl.Curl()
        ch.setopt(ch.URL, 'https://abcd-rc.ucsd.edu/redcap/api/')
        ch.setopt(ch.HTTPPOST, data.items())
        ch.setopt(ch.WRITEFUNCTION, buf.write)
        ch.perform()
        ch.close()
        print buf.getvalue()
    buf.close()
            
