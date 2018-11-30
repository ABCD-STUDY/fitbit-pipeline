This takes the pilot study scoring code from 
/var/www/html/applications/auto-scoring/fitabase/, which was not 
version-controlled, and introduces it here. The main change has been 
hard-coding a different origin folder.

To get the correct score, the script is run with `./old_score.py SRI`. That 
processes the files in `test_data/`, and should output to stdout the content of 
`test_output.txt`. (The processed data itself should correspond to the subset 
    thereof, kept in `test_output.json`.)

The `SRI` parameter is because the test subject, NDAR_INVZ2KNBAKB, belongs to 
the SRI site - and the `old_score.py` script checks with Redcap about the files 
it is allowed to score.
