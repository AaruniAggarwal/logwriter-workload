#!/usr/bin/env python3
# -*- coding: utf8 -*-

import argparse
import hashlib
import logging
import sys
import time

from datetime import datetime


def readlogfile(fo):
    """
    Read and validate given log file.
    """
    logging.info("opened log file: %s", fo.name)
    validation_fail = False
    error_in_fo = False
    prev_line = None
    for line in fo:
        try:
            timestamp, data = line.split()
        except ValueError:
            logging.error("failed to read line: %s", line.encode('unicode-escape'))
            validation_fail = True
            error_in_fo = True
            continue
        if len(timestamp) != 26:
            logging.error("incorrect timestamp format: %s", timestamp.encode('unicode-escape'))
            validation_fail = True
            error_in_fo = True
            continue
        if prev_line is not None:
            checksum = hashlib.sha256(prev_line.encode('utf8')).hexdigest()
            if checksum == data:
                logging.debug("line at %s verified", timestamp)
            else:
                logging.error(
                    "line at %s doesn't provide good digest",
                    timestamp)
                validation_fail = True
                error_in_fo = True
        prev_line = line

    if error_in_fo:
        logging.error("log file is corrupted: %s", fo.name)
    else:
        logging.info("log file is ok: %s", fo.name)
    error_in_fo = False

    return validation_fail


def main():
    ap = argparse.ArgumentParser(description="logreader validation script")
    ap.add_argument(
        "logfile",
        nargs='+',
        help="filepath of a log file to read and verify")
    ap.add_argument(
        "-t",
        "--duration",
        type=int,
        metavar='T',
        default=5,
        help="time duration in minutes you want to run this [default is 5 minute]")
    ap.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="set log level to DEBUG")
    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    retcode = 0

    if args.duration < 0:
        print("error: negative time duration specified", file=sys.stderr)
        return 2

    # if number of reruns is zero, we are going to run validation just once
    
    time_before = datetime.now()
    time_passed = 0
    time_after = None
    i = 0
    while True:
        logging.info("Run number: %d", i)

        if time_passed >= args.duration:
            logging.info("Done reading!")
            break
        
        for filename in args.logfile:
            try:
                with open(filename, "r") as fo:
                    failed = readlogfile(fo)
                    if failed:
                        retcode = 1
            except IOError as ex:
                logging.error(ex)
                retcode = 1

        time_after = datetime.now()
        time_passed = (time_after - time_before).total_seconds()/60
        logging.info("Time passed %s", time_passed)
        
    return retcode

if __name__ == '__main__':
    sys.exit(main())
