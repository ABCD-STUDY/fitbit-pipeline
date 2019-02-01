# Approach 1: x.csv and x.correct.csv, with pd.testing.assert_frame_equal(x, y)
#
# To make this approach powerful, automatic discovery of paired files is useful
import glob
import os
import pandas as pd
import re

def get_csv_pairs(fpath='', regex_ending=r'\.correct\.csv$'):
    csv_files = glob.glob(os.path.join(fpath, '*.csv'))
    
    outputs = [x for x in csv_files if re.search(regex_ending, x)]
    
#     # FIXME: This will fail if there's no counterpart to .correct - should match instead
#     inputs  = [re.sub(r'\.correct\.csv$', '.csv', x) for x in outputs]
#     return zip(inputs, outputs)

    input_dict = {x: re.sub(regex_ending, '.csv', x) for x in outputs}
    return [(file_in, file_out) for file_out, file_in in input_dict.items()]

def run_test(file_pair, fn):
    input_file  = pd.read_csv(file_pair[0])
    output_file = pd.read_csv(file_pair[1])
    pd.testing.assert_frame_equal(
        fn(input_file),
        output_file
    )

# This sort of looks like what pytest does better. 
# Can I maybe generate test_{core_filename} functions on the fly, so that
# pytest can eat them? How does pytest detect them? And can they be 
# generated this way?
#
# Maybe something with decorators?
def run_test_on_pairs(file_pairs, fn, verbose=False):
    for file_pair in file_pairs:
        try:
            run_test(file_pair, fn)
            if verbose:
                print('Test passed for', file_pair)
        except AssertionError:
            if verbose:
                print('Test failed for', file_pair)
            else:
                raise
