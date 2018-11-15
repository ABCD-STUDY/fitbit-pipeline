# How to connect to fitabase
This project is not in use for the substudy data. Check the auto-scoring fitbit folder for the project that is used to import fitbit data for the substudy.


## API documentation to connect to fitabase setup

The API keys for each site are stored in fitabase_tokens.json. They come from the app.fitabase.com page (user hbartsch). Each site's API key has to be enabled to get access to the different APIs that are available for the project.

### Sync service

In order to get sync information we need a 'profileId'. We get the profileId from the Profiles API. Here an example (UCSD):

```
GET https://api.fitabase.com/v1/Profiles/ HTTP/1.1
Host: api.fitabase.com
Ocp-Apim-Subscription-Key: 
```
Response:
```
[]
```

Given a profileId we can get the Sync information for that profile.

```
@ECHO OFF

curl -v -X GET "https://api.fitabase.com/v1/Sync/Latest/{profileId}"
-H "Ocp-Apim-Subscription-Key: {subscription key}"
--data-ascii "{body}"
```
