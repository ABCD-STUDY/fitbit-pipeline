#!/usr/bin/env python
#
# find out if we have to alert participants/parents because a user forgot to sync their device
#

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
    'fields[2]': 'fitc_noti_generated_sync',
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
        if delta > 0 and delta <= 22:
            # only add this participant if the last time we contacted them is before the last sync date
            participants.append([d['id_redcap'], d['fitc_device_dte'], delta, d['fitc_noti_generated_sync'], d['redcap_event_name']])

print(json.dumps(participants))

for d in v:
    lastContactDate = ''
    redcap_event_name = ''
    sync_days_ago = ''
    found = False
    for p in participants:
        if p[0] == d['Name']:
            found = True
            lastContactDate = p[3]
            redcap_event_name = p[4]
            sync_days_ago = p[2]
            break
    if not found:
        continue
    
    # for each of the profiles we would like to get the sync data
    print("Check data for %s (ProfileId: %s)" % ( d['Name'], d['ProfileId']))
    pGUID = d['Name']
    
    buf = cStringIO.StringIO()
    # variables we need from REDCap
    data = {
        'Ocp-Apim-Subscription-Key': ftoken
    }
    ch = pycurl.Curl()
    ch.setopt(ch.URL, 'https://api.fitabase.com/v1/Sync/Latest/%s' % d['ProfileId'])
    ch.setopt(ch.HTTPGET, True)
    ch.setopt(ch.HTTPHEADER, ['%s: %s' % (k, v) for k, v in data.items()])
    ch.setopt(ch.WRITEFUNCTION, buf.write)
    ch.perform()
    ch.close()
    
    out = json.load( StringIO(buf.getvalue()) )
    buf.close()
    # ok, got some data now for this profileId

    # profileId: a99f06d2-4264-49a1-80d5-3b794a507d96 returned: {"SyncDate": "2018-09-25T13:19:18"}
    if not out['SyncDate']:
        # no sync date yet
        print("No sync date yet for: ",pGUID)
    else:
        # New data will contain additionally to the SyncDate for each device a BatteryLevel indicator
        #{
        #    "SyncDate": "2018-11-15T21:42:48",
        #    "LatestBatteryLevel": "High",
        #    "LatestDeviceName": "Versa",
        #    "Devices": [{
        #        "DeviceName": "Versa",
        #        "BatteryLevel": "High",
        #        "LastSync": "2018-11-15T21:42:48"
        #    }, {
        #        "DeviceName": "Alta HR",
        #        "BatteryLevel": "High",
        #        "LastSync": "2018-11-15T14:49:55"
        #    }]
        #}
        
        lastSyncDate = out['SyncDate']
        date1 = datetime.datetime.strptime(lastSyncDate, '%Y-%m-%dT%H:%M:%S')
        date2 = datetime.datetime.now()

        #
        # store the sync date if there was one in REDCap
        #
        buf = cStringIO.StringIO()
        data = {
            'token': tokens[site],
            'content': 'record',
            'format': 'json',
            'type': 'flat',
            'overwriteBehavior': 'overwrite',
            'data': json.dumps([{ 'id_redcap': pGUID, 'redcap_event_name': redcap_event_name, 'fitc_last_sync_date': date1.strftime("%m-%d-%Y %H:%M:%S") }]),
            'returnContent': 'count',
            'returnFormat': 'json'
            #'record_id': hashlib.sha1().hexdigest()[:16]
        }
        print("submit a sync date to REDCap:")
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
        #
        # store done
        #
        
        if lastContactDate != '':
            date3 = datetime.datetime.strptime(lastContactDate, '%m-%d-%Y %H:%M:%S')
        else:
            date3 = ''
            
        delta = date2 - date1
        if date3 != '':
            delta2 = date3 - date1
            if delta2.days >= 0:
                # we have a contact date after the last sync date! Don't create another alert
                print("contact date is after the sync date (%d), don't create another alert" % delta2.days)
                continue
        else:
            # we don't have a last contact date, assume we will send a message
            print("no reported contact date found for %s in REDCap" % pGUID)
            
                
        alert = False
        if delta.days >= 3:
            print("more than 3 days since last sync")
            alert = True
        else:
            print("less than 3 days since last sync")
            alert = False
            
        print("pGUID: %s profileId: %s returned: %s alert: %s" % (pGUID, d['ProfileId'], json.dumps(out), alert))

        if alert:
            # generate 3 notifications, one for child, one for parent and one for spanish parent
            # Post-Assessment Link:
            #
            # I suggest the following SMS notification:
            #
            # You have finished 21 days with the Fitbit! Please check your e-mail for directions on how to return the device, complete a questionnaire, and receive your payment! To complete your questionnaire, click here:
            # Parent: LINK
            # Youth: LINK
            # And the following email notification:
            # 
            # Thank you for completing 21 days with the Fitbit! It is important to complete the following so you can receive your $20 payment.
            #
            # Send Fitbit device by mail with the pre-paid envelope. Be sure to include the charger.
            #   Complete these short questionnaires.
            #   Parent: LINK
            #   Youth: LINK
            #   As soon as we receive the above, the ABCD site let you know when to expect your payment. Thank you for participating in the Fitbit part of the study, and we hope you enjoyed using it! Feel free to contact the ABCD Site with any questions.


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
            
            noti_youth     = "Hi %%YOUTH_FIRST_NAME%%, please sync your fitbit for ABCD! Our records show that you did this %d days ago. Do this every day so we don't lose your data. Do not reply to this message. Contact your site directly if you have questions." % sync_days_ago
            noti_parent_en = "%%YOUTH_FIRST_NAME%%: Please sync your fitbit for ABCD! Our records show that you did this %d days ago. Do this every day so we don't lose your data. Do not reply to this message. Contact your site directly if you have questions." % sync_days_ago
            noti_parent_es = "%%YOUTH_FIRST_NAME%%: Please sync your fitbit for ABCD! Our records show that you did this %d days ago. Do this every day so we don't lose your data. Do not reply to this message. Contact your site directly if you have questions." % sync_days_ago
            notifications = []
            notifications.append({
                'record_id': pGUID,
                'noti_subject_line': '%%YOUTH_FIRST_NAME%%: ABCD Fitbit sync reminder',
                'noti_text': noti_youth,
                'noti_spanish_language': 0,
                'noti_purpose': 'send_sync_reminder',
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
                'noti_subject_line': 'ABCD Fitbit sync reminder for %%YOUTH_FIRST_NAME%%',
                'noti_text': noti_parent_en,
                'noti_spanish_language': 0,
                'noti_status': 1,
                'noti_purpose': 'send_sync_reminder',
                'noti_timestamp_create': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'noti_site_name': site,
                'noti_recipient': 1,
                'noti_send_preferred_time': 1,
                'redcap_repeat_instrument': 'notifications',
                'redcap_repeat_instance': next_noti_id+1
            })
            notifications.append({
                'record_id': pGUID,
                'noti_subject_line': 'ABCD Fitbit sync reminder for %%YOUTH_FIRST_NAME%%',
                'noti_text': noti_parent_es,
                'noti_spanish_language': 1,
                'noti_status': 1,
                'noti_purpose': 'send_sync_reminder',
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
                'data': json.dumps([{ 'id_redcap': pGUID, 'redcap_event_name': redcap_event_name, 'fitc_noti_generated_sync': datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S") }]),
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
            
