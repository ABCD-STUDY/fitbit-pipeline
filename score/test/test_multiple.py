import os
from pair_helper import get_csv_pairs, run_test_on_pairs

def save_multiple_of_a_into_b(df):
    df['b'] = 2 * df['a']
    return df

def save_inverse_of_a_into_b(df):
    df['b'] = 2 / df['a']
    return df

if __name__ == '__main__':
    CURRENT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)))
    file_pairs = get_csv_pairs(os.path.join(CURRENT_DIR, 'test_multiple/'))
    # NOTE: By removing '.py$' from os.path.realpath(__file__), we could obtain 
    # the directory with the test cases automatically?
    run_test_on_pairs(file_pairs, save_multiple_of_a_into_b, verbose=True)
    run_test_on_pairs(file_pairs, save_inverse_of_a_into_b, verbose=True)
