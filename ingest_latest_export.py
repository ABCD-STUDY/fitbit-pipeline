#!/usr/bin/env python
"""
For selected site(s), download and save/extract latest batch export archive.

By default, export is into SITE/SUBJECT/CURRENT_DATE; this can be modified with 
--target-dir, --no-subject-subdirs and --no-date-subdirs, respectively.
"""
import argparse
import datetime
from fitabase_api import FitabaseSite
import itertools
import json
import logging as log
import os
import pandas as pd
import re
import zipfile

# If executed from cron, paths are relative to PWD, so anything we need must 
# have an absolute path
CURRENT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)))
log.basicConfig(
        filename=os.path.join(CURRENT_DIR,
            "logs", os.path.basename(__file__) + ".log"), 
        format="%(asctime)s  %(levelname)10s  %(message)s",
        level=log.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser(
            description="Ingest the latest batch export for the site.")
    parser.add_argument('site', nargs='+')

    dir_choices = parser.add_mutually_exclusive_group()
    dir_choices.add_argument('--root-dir', 
            default='/external_data/fitabase-data/',
            help="Root directory; each site extracted into its subdirectory.")
    dir_choices.add_argument('--target-dir', '-t', 
            default=None,
            help="Alternative dir to extract the batch into.")

    parser.add_argument('--no-subject-subdirs',
            action='store_true',
            help="Extract all files as-is (rather than creating a "
                 "subdirectory for each subject's files)")
    # TODO: Instead of using current date, should be extracting date range from 
    # the files, like it does with IDs
    parser.add_argument('--no-date-subdirs',
            action='store_true',
            help="Skip creating subdirs with current YYYYMMDD.")

    # TODO: Could also offer the option of creating per-instrument 
    # subdirectories instead of per-date subdirectories.

    parser.add_argument('--no-download', '-n', action='store_true',
            help="Check but do not download.")
    parser.add_argument('--no-extract', '-x', action='store_true',
            help="Do not extract the files from the zip.")
    parser.add_argument('--verbose', '-v', action='store_true',
            help="Display / save logged INFO-level messages.")
    return parser.parse_args()


def ensure_directory(root, *args):
    """
    Return valid directory path and, if it does not exist, create it
    """
    export_dir = os.path.join(root, *args)
    if not os.path.isdir(export_dir):  # makedirs -> OSError if leaf dir exists
        os.makedirs(export_dir)  # could still raise OSError for permissions
    return export_dir


def group_files_into_directories(file_list, subject_pattern=r'^NDAR_[^_]+'):
    """
    Separate a list of strings into groups based on a regex pattern match. 

    Returns an itertools.Groupby iterable of form group_key -> [member_list].

    (Note that re.match only matches the beginning of the string. If group_key 
    is located elsewhere in the string, re.search or equivalent should be used 
    instead.)
    """
    def get_subject_id(haystack):
        match = re.match(subject_pattern, haystack)
        if not match:
            return None
        else:
            return match.group(0)

    return itertools.groupby(file_list, get_subject_id)


if __name__ == "__main__":
    args = parse_arguments()
    if args.verbose:
        log.basicConfig(level=log.DEBUG)

    with open(os.path.join(CURRENT_DIR, 'fitabase_tokens.json')) as data_file:
        fitabase_tokens = json.load(data_file).get('tokens')
        fitabase_tokens = pd.DataFrame.from_records(fitabase_tokens, index='name')

    for site in args.site:
        try:
            fit_token = fitabase_tokens.loc[site, 'token']
        except KeyError:
            log.error('%s: Fitabase token ID is not available!', site)
            continue
        fit_api = FitabaseSite(fit_token)
        last_batch = fit_api.get_last_batch_export_info()
        try:
            last_id = last_batch.get('DownloadDataBatchId')
        except (AttributeError, IOError) as e:
            log.error('%s: Last batch ID not available.', site)
            continue

        if args.no_download:
            log.info('%s, %s: Not downloading', site, last_id)
            continue

        # Determine what the base directory is and create it if needed
        if args.target_dir:
            target_dir = ensure_directory(args.target_dir)
        else:
            target_dir = ensure_directory(args.root_dir, site)

        ymd_string = datetime.datetime.now().strftime('%Y%m%d')  # .utcnow()?

        # Save the whole zip file if required...
        if args.no_extract:
            site_zip_stream = fit_api.export_batch(last_id)

            file_name = "%s_%s.zip" % (ymd_string, last_id[:6])
            target_file = os.path.join(target_dir, file_name)
            with file(target_file, mode='wb') as zf:
                zf.write(site_zip_stream.read())

            log.info('%s, %s: Saving zip file without extraction to %s',
                     site, last_id, target_file)
            continue

        # ...otherwise, extracting files one way or another:
        with fit_api.get_batch_export_zipfile(last_id) as site_zip:
            if args.no_subject_subdirs:
                if not args.no_date_subdirs:
                    target_dir = ensure_directory(target_dir, ymd_string)
                site_zip.extractall(path=target_dir)
                log.info('%s, %s: Extracting all files as-is to %s', 
                        site, last_id, target_dir)
            else:
                # Group files by participant, then extract them into subdirs
                all_files = site_zip.namelist()
                files_by_dir = group_files_into_directories(all_files)
                for subdir, files in files_by_dir:
                    # If subject ID did not match the pattern, then the file 
                    # was not grouped:
                    if subdir is not None:
                        subject_dir = ensure_directory(target_dir, subdir)
                    else:
                        subject_dir = target_dir

                    # Add date subdir unless requested otherwise
                    if not args.no_date_subdirs:
                        subject_dir = ensure_directory(subject_dir, ymd_string)

                    # Extract all files in the group to the designated subdir
                    for f in files:
                        site_zip.extract(f, path=subject_dir)

                log.info('%s, %s: Extracted files into per-subject folders in '
                         '%s, no date subdirs = %s',
                         site, last_id, target_dir, args.no_date_subdirs)
