import pytest
import subprocess

def test_score_output(capfd):
    subprocess.call(['./old_score.py', 'SRI'])
    captured = capfd.readouterr()
    with open('test_output.txt', 'r') as f:
        assert captured.out == f.read()
