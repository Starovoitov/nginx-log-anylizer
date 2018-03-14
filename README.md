Example of using of log_analyzer.py:

    python log_analyzer.py -c c.conf --ts_file=/tmp/ts --debug_file=/tmp/analyzer_job.log

File c.conf - an example of config file and could be found together with the other items in log analyzer folder
More talkative information about additional options and config fields could be found by using

    python log_analyzer.py -h


Example of running unittests:

    python -m unittest test_log_analyzer