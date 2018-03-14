#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import gzip
import re
import os.path
from shutil import copyfile
from datetime import datetime
import sys
import getopt
import logging
import time as timer


class Stats:
    """Enumeration of processing stats"""
    def __init__(self):
        return
    count = 0
    count_perc = 1
    time_avg = 2
    time_max = 3
    time_med = 4
    time_perc = 5
    time_sum = 6


class Data:
    """Enumeration of processing url stats data structure fields"""
    def __init__(self):
        return
    url = 0
    val = 1


# default config
config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "REPORT_TEMPLATE": "report.html",
    "LOG_DIR": "./log",
    "LOG_NAME": "test.log",
    "TS_FILE": "log_analyzer.ts",
    "DEBUG_LOG": None,
    "ERROR_THRESHOLD": 0.5
}


# global counters
total_requests = 0
failed_requests = 0
total_time = 0
unrecognized_lines = 0
total_lines = 0


def median(lst):
    """Returns a median of request processing time sample"""
    n = len(lst)
    if n < 1:
            return None
    if n % 2 == 1:
            return sorted(lst)[n//2]
    else:
            return sum(sorted(lst)[n//2-1:n//2+1])/2.0


def shift_max_time(url_stats_frame, new_time):
    """Compares given time of url processing to previous max time. Returns max time of processing"""
    if new_time - url_stats_frame[Stats.time_max] > 0:
        url_stats_frame[Stats.time_max] = new_time
    return url_stats_frame[Stats.time_max]


def add_time_to_median_sample(url_stats_frame, new_time):
    """Add url time processing to it's sample of time processing"""
    url_stats_frame[Stats.time_med].append(new_time)
    return url_stats_frame[Stats.time_med]


def increase_total_time_per_url(url_stats_frame, new_time):
    """Increases total time of processing of given url"""
    url_stats_frame[Stats.time_sum] += new_time
    return url_stats_frame[Stats.time_sum]


def increase_url_hits(url_stats_frame):
    """Increases number of requests to given url"""
    url_stats_frame[Stats.count] += 1
    return url_stats_frame[Stats.count]


def get_url_time_status(line):
    """Returns url, time of url processing and HTTP status from nginx log record"""
    url = re.search(r'\S+(?=(\sHTTP/1\.[210]))', line)
    time = re.search(r'\d+\.\d+$', line)
    status = re.search(r'(?<=(HTTP/1\.[210]\"\s))\d{3}', line)
    if url and time and status:
        url = url.group()
        time = float(time.group())
        status = int(status.group())
        return url, time, status
    return None, None, None


def process_line(line, url_stats):
    """Processes incoming line from nginx log"""
    global total_requests, total_time, unrecognized_lines, failed_requests
    url, time, status = get_url_time_status(line)
    if url and status:
        total_requests += 1
        total_time += time
        if url not in url_stats:
            try:
                # new url, never been met before
                url_stats[url] = [1, 1.0/total_requests, time, time, [time], 100.0, time]
                return url, url_stats[url][:]
            except MemoryError:
                logging.error("Can't process so large quantity of urls, miss any new")
        else:
            increase_url_hits(url_stats[url])
            add_time_to_median_sample(url_stats[url], time)
            shift_max_time(url_stats[url], time)
            increase_total_time_per_url(url_stats[url], time)
            if status >= 400:
                failed_requests += 1
            return url, url_stats[url][:]
    else:
        unrecognized_lines += 1
        return None, None


def fill_stats(url_data):
    """Calculate stats not beeing processed immediately after record reading like median of time processing"""
    global total_time, total_requests
    for url in url_data:
        try:
            # rate of url requests in general count
            url_data[url][Stats.count_perc] = float(url_data[url][Stats.count]) / total_requests
            # average time of url request processing
            url_data[url][Stats.time_avg] = float(url_data[url][Stats.time_sum]) / (url_data[url][Stats.count])
            # median time of processing
            url_data[url][Stats.time_med] = median(url_data[url][Stats.time_med])
            # total amount of time has been used by url
            url_data[url][Stats.time_perc] = float(url_data[url][Stats.time_sum]) / total_time
            pass
        except ZeroDivisionError:
            logging.error("request with 0 time of processing - account as failed request or bad nginx logging")
        finally:
            pass
    logging.info("stats have been processed")
    return


def xreadlines(log_path, url_stats):
    """Reads lines one by one from nginx log and calls a function handling containing stats"""
    global total_lines
    if log_path.endswith(".gz"):
        log = gzip.open(log_path, 'rb')
    else:
        log = open(log_path)
    for line in log:
        url, value = process_line(line, url_stats)
        total_lines += 1
        if url and value:
            yield url, value
    log.close()


def render_report(stats, report_dir, report_template):
    """Creates a report from existing template"""
    report_name = "report-" + datetime.strftime(datetime.now(), '%Y%m%d') + ".html"
    try:
        copyfile(report_template, report_dir + '/' + report_name)
    except IOError:
        logging.error("report template doesn't exist in given place")
        return
    try:
        report_file = open(report_dir + '/' + report_name, "r+")
        template = report_file.read()
        report_file.seek(0)
        report_file.write(re.sub("\$table_json", str(stats), template))
        report_file.close()
        logging.info("report has been created from given template")
    except IOError:
        logging.exception("unexpected exception")
        raise
    return


def get_upper_records_by_sum_time(stats, upper_records):
    """Returns a list of url stats with highest time of processing"""
    upper_stats = []
    sorted_stats = sorted(stats.items(), key=lambda e: e[Data.val][Stats.time_sum], reverse=True)
    logging.info("output of the most frequent urls")
    for record in range(0, upper_records, 1):
        try:
            upper_stats.append(
                    {"url": sorted_stats[record][Data.url],
                     "count": sorted_stats[record][Data.val][Stats.count],
                     "count_perc": sorted_stats[record][Data.val][Stats.count_perc],
                     "time_avg": sorted_stats[record][Data.val][Stats.time_avg],
                     "time_max": sorted_stats[record][Data.val][Stats.time_max],
                     "time_med": sorted_stats[record][Data.val][Stats.time_med],
                     "time_perc": sorted_stats[record][Data.val][Stats.time_perc],
                     "time_sum": sorted_stats[record][Data.val][Stats.time_sum]}
                            )
            logging.info(sorted_stats[record])
        except IndexError:
            logging.error("Number of frequent urls is lesser than sample size - stopping search")
            break
    return upper_stats


def get_last_log(log_list):
    """Gets log with latest date"""
    if log_list:
        log_dates = {}
        for log in log_list:
            date = re.search(r'(?<=-)\d{8}', log)
            if date:
                log_dates[log] = int(date.group())
        if log_dates:
            return max(log_dates, key=log_dates.get)
    return


def get_current_log_name(dir_path, log_pattern):
    """Gets the latest yet mot parsed nginx log from log folder among files satisfying log pattern"""
    try:
        log_list = [f for f in os.listdir(dir_path) if re.match('^' + log_pattern + r'-\d{8}.*$', f)]
        if not log_list:
            logging.info("parsing log is not found - no files suitable for given pattern")
            return
        latest_log = get_last_log(log_list)
        logging.info("the latter log has been determined - %s", latest_log)
        return dir_path + '/' + latest_log
    except OSError:
        logging.error("No such directory: %s", dir_path)
        return


def mark_as_handled(log_path):
    """Marks processing log as already handled thus not requiring to be parsed repeatedly in the future"""
    os.rename(os.path.dirname(log_path) + '/' + os.path.basename(log_path),
              os.path.dirname(log_path) + '/' + 'handled_' + os.path.basename(log_path))
    logging.info("parsed log is marked as handled - %s", os.path.dirname(log_path) + '/' + os.path.basename(log_path))
    return


def write_ts(conf):
    """Writes timestamp into TS_FILE"""
    try:
        ts = open(conf["TS_FILE"], "w")
        ts.write(str(datetime.now()))
        ts.close()
        logging.info("ts file is created - %s\n", conf["TS_FILE"])
    except IOError:
        logging.error("can't create ts file by requested path")
    return


def read_config(config_filename, conf):
    """Reads given config and joins it's settings to the default. New settings overlaps default"""
    status = "not determined"
    try:
        config_file = open(config_filename, 'r')
        content = config_file.read()
        properties = content.split("\n")
        for config_line in properties:
            if config_line:
                name, value = config_line.split("=")
                if name == 'REPORT_SIZE':
                    conf["REPORT_SIZE"] = int(value)
                elif name == 'REPORT_DIR':
                    conf["REPORT_DIR"] = value
                elif name == 'REPORT_TEMPLATE':
                    conf["REPORT_TEMPLATE"] = value
                elif name == 'LOG_DIR':
                    conf["LOG_DIR"] = value
                elif name == 'LOG_NAME':
                    conf["LOG_NAME"] = value
                elif name == 'TS_FILE':
                    conf["TS_FILE"] = value
                elif name == 'DEBUG_FILE':
                    conf["DEBUG_FILE"] = value
                elif name == 'ERROR_THRESHOLD':
                    conf["ERROR_THRESHOLD"] = float(value)
        config_file.close()
        status = "config is parsed - " + config_filename
    except IOError:
        status = "Can't read config file"
    finally:
        return conf, status


def print_help():
    """Prints a brief info about input parameters"""
    print "c, (--config) - config file. If not used then settings from default config are put instead:\
    REPORT_SIZE= 1000\n\
    REPORT_DIR=./reports\n\
    REPORT_TEMPLATE=report.html\n\
    LOG_DIR=./log\n\
    LOG_NAME=nginx-access-ui.log\n\
    TS_FILE=log_analyzer.ts\n\
    DEBUG_LOG=None\n\
    ERROR_THRESHOLD=0.5\n\
    where REPORT_SIZE - number of urls using the biggest time of processing, REPORT_DIR - a folder where final report is " \
          "saved, REPORT_TEMPLATE - place of template for creating a final report, LOG_NAME - pattern for parsing " \
          "nginx logs. It is supposed they might have a data in format %YYYY%M%D,  LOG_DIR - folder for nginx logs," \
          "TS_FILE - file with timestamp of processing ending, DEBUG_LOG - log_analyzer's log (output into console if" \
          "not set, ERROR_THRESHOLD - ratio of unrecognized/recognized lines in nginx logs. The script stops it's " \
          "work if this parameter is exceeded) "
    print "r, (--report_dir) - a folder where final report is saved"
    print "l, (--log_dir) - folder for nginx logs"
    print "n, (--log_generic_name) - pattern for parsing nginx logs. It is supposed they might have a data in " \
          "format %YYYY%M%D, so the log with latest date is taken for processing"
    print "h, (--help) - output of this instruction"
    print "t, (--report_template) - place of template for creating a final report"
    print "m (--ts_file) - file with timestamp of processing ending"
    print "d, (--debug_file) - log_analyzer's log (output into console if not set"
    print "e, (--error_threshold) - ratio of unrecognized/recognized lines in nginx logs. " \
          "The script stops it's work if this parameter is exceeded)"
    return


def main(conf, conf_status):
    global total_requests, total_time, unrecognized_lines
    start = timer.time()
    url_stats = {}
    try:
        logging.basicConfig(filename=conf["DEBUG_LOG"], level=logging.INFO,
                            format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S',
                            filemode='w')
        logging.info(conf_status)
        log_path = get_current_log_name(conf["LOG_DIR"], conf["LOG_NAME"])

        if not log_path:
            logging.error("No new files for parsing")
            return

        url_stats = {url: value for url, value in xreadlines(log_path, url_stats)}

        if float(unrecognized_lines) / total_lines > conf["ERROR_THRESHOLD"]:
            logging.error("Unrecognizable log format")
            return

        fill_stats(url_stats)
        stats = get_upper_records_by_sum_time(url_stats, conf["REPORT_SIZE"])
        render_report(stats, conf["REPORT_DIR"], conf["REPORT_TEMPLATE"])
        mark_as_handled(log_path)
    except KeyboardInterrupt:
        logging.error("Interrupted")
    except IOError:
        logging.exception("unexpected exception")
        raise
    finally:
        end = timer.time()
        write_ts(conf)
        logging.info("Found %d requests getting response for %f sec. %d among them are different urls. "
                     "%d have failed. Unrecognized lines: %d/%d It used %f sec for the script.", total_requests,
                     total_time, len(url_stats), failed_requests, unrecognized_lines, total_lines, end - start)
    pass


if __name__ == "__main__":
    config_status = "Default config in use"
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'c:r:l:n:s:h:t:m:d:e', ['config=', 'report_dir=', 'log_dir=',
                                                                         'log_generic_name=', 'report_size=', 'help',
                                                                         'report_template=', 'ts_file=', 'debug_file=',
                                                                         'error_threshold='])
    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit(2)
        elif opt in ('-c', '--config'):
            config, config_status = read_config(arg.strip('='), config)
        elif opt in ('-r', '--report_dir'):
            config["REPORT_DIR"] = arg.strip('=')
        elif opt in ('-l', '--log_dir'):
            config["LOG_DIR"] = arg.strip('=')
        elif opt in ('-n', '--log_generic_name'):
            config["LOG_NAME"] = arg.strip('=')
        elif opt in ('-s', '--report_size'):
            config["REPORT_SIZE"] = int(arg.strip('='))
        elif opt in ('-t', '--report_template'):
            config["REPORT_TEMPLATE"] = arg.strip('=')
        elif opt in ('-m', '--ts_file'):
            config["TS_FILE"] = arg.strip('=')
        elif opt in ('-d', '--debug_file'):
            config["DEBUG_LOG"] = arg.strip('=')
        elif opt in ('-e', '--error_threshold'):
            config["ERROR_THRESHOLD"] = float(arg.strip('='))
        else:
            pass

    main(config, config_status)
