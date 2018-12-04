import requests

def get_redcap_survey_url(rc_api, subject_id, survey, event):
    """
    Implement the missing PyCAP functionality to retrieve survey URI via API.

    rc_api: a redcap.Project instance
    subject_id (str): the primary Redcap record ID
    survey (str): name of the survey
    event (str): valid redcap_event_name 
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


def apply_redcap_survey_url(row, rc_api, survey):
    """
    row: pandas.Series, as passed by pandas.DataFrame.apply(axis=1)
    rc_api: a redcap.Project instance
    survey (str): name of the survey
    """
    if isinstance(row.name, tuple):
        if len(row.name) == 2:
            return get_redcap_survey_url(rc_api, subject_id=row.name[0], 
                    survey=survey, event=row.name[1])
        elif len(row.name) == 1:
            return get_redcap_survey_url(rc_api, subject_id=row.name[0], 
                    survey=survey, event=row['redcap_event_name'])
    else:
        return get_redcap_survey_url(rc_api, subject_id=row.name, 
                survey=survey, event=row['redcap_event_name'])
