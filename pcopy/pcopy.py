# !/usr/bin/env python3

import argparse
import logging
import logging.handlers
import math
import os
import platform
import sys

from wekalib import sthreads


def configure_logging(logger, verbosity):
    loglevel = logging.INFO     # default logging level

    # default message formats
    console_format = "%(message)s"
    syslog_format =  "%(levelname)s:%(message)s"

    syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"

    if verbosity == 1:
        loglevel = logging.DEBUG
        console_format = "%(levelname)s:%(message)s"
        syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
    elif verbosity > 1:
        loglevel = logging.DEBUG
        console_format = "%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
        syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"

    # create handler to log to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(console_format))
    logger.addHandler(console_handler)

    # create handler to log to syslog
    logger.info(f"setting syslog on {platform.platform()}")
    if platform.platform()[:5] == "macOS":
        syslogaddr = "/var/run/syslog"
    else:
        syslogaddr = "/dev/log"
    syslog_handler = logging.handlers.SysLogHandler(syslogaddr)
    syslog_handler.setFormatter(logging.Formatter(syslog_format))

    # add syslog handler to root logger
    if syslog_handler is not None:
        logger.addHandler(syslog_handler)

    # set default loglevel
    logger.setLevel(loglevel)


    logging.getLogger("wekalib.sthreads").setLevel(logging.ERROR)
    logging.getLogger("wekalib.circular").setLevel(logging.ERROR)

def copyrange(sfp, dfp, range_start, range_end, blocksize):
    sfp.seek(range_start, 0)
    dfp.seek(range_start, 0)

    while sfp.tell() < range_end:
        buffer = sfp.read(blocksize)
        dfp.write(buffer)

def is_power_of_two(n):
    """Return True if n is a power of two."""
    if n <= 0:
        return False
    else:
        return n & (n - 1) == 0


def optimal_blocksize(blocksize):
    # determine power-of-2 blocksize between 4k and 1mb
    bin_blocksize = bin(blocksize)[2:]
    num_bits = len(bin_blocksize)
    if num_bits >= 21: # more than 1mb
        optimal = 1024**2
    elif num_bits <15: # less than 4k
        optimal = 4096
    else:
        optimal = ((blocksize >> (num_bits-1)) << (num_bits-1))
    return optimal


def optimal_threads(total_blocks, minthreads, maxthreads):
    # target blocks per thread = 100? 500? 1000?
    target_bpt = 100
    test_bpt = total_blocks / maxthreads
    print(f"test_bpt = {test_bpt}")

    if test_bpt >= target_bpt:
        return maxthreads

    return optimal_threads(total_blocks,minthreads,int(maxthreads*.5))


if __name__ == '__main__':

    maxthreads = 50
    print(f"maxthreads = {maxthreads}")

    # parse arguments
    progname = sys.argv[0]
    parser = argparse.ArgumentParser(description='A threaded cp command')
    parser.add_argument("-v", "--verbosity", action="count", default=0, help="increase output verbosity")

    parser.add_argument('source', metavar='source', type=str, help='file to copy to dest')
    parser.add_argument('dest', metavar='dest', type=str, help='destination location')

    args = parser.parse_args()

    # set the root logger
    log = logging.getLogger()
    configure_logging(log, args.verbosity)

    source_stats = os.stat(args.source)

    # determine optimal proposed_blocksize
    if source_stats.st_size < 4096:
        # why are you using this tool?  Duh!
        blocksize = source_stats.st_size
        smallfile = True
    else:
        smallfile = False
        blocksize = optimal_blocksize(int(math.ceil(source_stats.st_size / maxthreads)))

    # determine ranges/threads
    end = source_stats.st_size
    total_blocks = int(math.trunc(source_stats.st_size / blocksize)) # +1 partial block

    threads = optimal_threads(total_blocks, 1, maxthreads)
    blocks_per_thread = int(total_blocks / threads)

    print(f"file size = {source_stats.st_size}, blocksize = {blocksize}, bpt = {blocks_per_thread}, threads = {threads}")

    # determine ranges
    startblock = 0
    ranges = list()
    while startblock < total_blocks:
        ranges.append(startblock)
        startblock += blocks_per_thread +1

    print(ranges)

    threads = sthreads.simul_threads(threads)

    with open(args.source, "rb") as source_fp:
        with open(args.dest, "wb") as dest_fp:
            # both were able to be opened/created
            for range_start in ranges:
                thread = threads.new(copyrange, source_fp, dest_fp, range_start,
                                          range_start + blocks_per_thread, blocksize)
            threads.run()

            # get that last bit
            buffer = source_fp.read()
            dest_fp.write(buffer)



