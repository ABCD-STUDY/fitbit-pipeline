import requests

def get_redcap_survey_url(rc_api, subject_id, survey, event):
    """
    Implement the missing PyCAP functionality to retrieve survey URI via API.
    """
    survey_link_request = rc_api._Project__basepl('surveyLink')
    survey_link_request.update(rc_api._kwargs())
    survey_link_request.update({
        'instrument': survey,
        'event': event,
        'record': subject_id
    })
    response = requests.post(rc_api.url, survey_link_request)
    survey_link = response.content

    return survey_link
