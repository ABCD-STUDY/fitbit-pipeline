#!/bin/bash
# Send all errors captured in the folder's logs directory to the maintainers' 
# addresses.
#
# Note that both time period and To: addresses are presently hard-coded. That 
# need not be the case, but multiple kinds of arguments in Bash is out of scope 
# for now.

# Use bash strict mode - see 
# http://redsymbol.net/articles/unofficial-bash-strict-mode/
#
# set -x for debugging
set -euo pipefail

# 1. Get current folder
local_dir=$(dirname "$0")
yesterday=`date +'%Y-%m-%d' -d '-1 days'`

# 2. Execute the grep search - turn off strict error processing for this, since 
#    the grep might not actually find anything
set +e
# errors_yesterday=$(grep "$yesterday" "${local_dir}"/logs/*.py.log | egrep '(ERROR|CRITICAL)')
# Using while some standard things are still classified as errors
errors_yesterday=$(grep "$yesterday" "${local_dir}"/logs/*.py.log | egrep '(ERROR|CRITICAL)' | grep -v 'Eligible for notification')
error_count=$(echo "$errors_yesterday" | wc -l)
set -e

# if ! (( $error_count )); then
if [ "$errors_yesterday" == "" ]; then
  # Substitute the message but still send it
  error_count=0
  errors_yesterday="No errors detected for ${yesterday}."
fi

# to='"Simon Podhajsky" <simon.podhajsky@sri.com>, "Ramon Quitales" <ramon.quitales@sri.com>' # doesn't send, for some reason
to='simon.podhajsky@sri.com, ramon.quitales@sri.com'
subject="$error_count alert/ingest errors on ${yesterday}" 
email="To: ${to}
Subject: ${subject}

${errors_yesterday}"

echo "$email" | /usr/sbin/sendmail -v -t
# echo "$email" | sendmail -t
