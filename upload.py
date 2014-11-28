#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import multiprocessing
from collections import defaultdict
import random

from hashsync.utils import sha1sum, traverse_directory, strip_leading
from hashsync.connection import connect, get_bucket
from hashsync.objectlist import ObjectList
from hashsync.manifest import Manifest
from hashsync.transfer import upload_file

import logging
log = logging.getLogger(__name__)


def init_worker():
    "Ignore SIGINT for process workers"
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def process_directory(dirname, jobs, dryrun=False):
    if not dryrun:
        bucket = get_bucket()
        object_list = ObjectList(bucket)
        object_list.load()
    else:
        object_list = ObjectList(None)

    pool = multiprocessing.Pool(jobs, initializer=init_worker)

    upload_jobs = upload_directory(dirname, object_list, pool, dryrun=dryrun)

    stats = defaultdict(int)
    m = Manifest()
    # Wait for jobs
    for job, filename, h in upload_jobs:
        if job:
            try:
                # Specify a timeout for .get() to allow us to catch
                # KeyboardInterrupt.
                state = job.get(86400)
                stats[state] += 1
            except KeyboardInterrupt:
                log.error("KeyboardInterrupt - exiting")
                exit(1)
            except Exception:
                log.exception("error processing %s", filename)
                raise
        else:
            stats['skipped'] += 1

        stripped = strip_leading(dirname, filename)
        perms = os.stat(filename).st_mode & 0777
        m.add(h, stripped, perms)

    log.info("stats: %s", dict(stats))

    # Shut down pool
    pool.close()
    pool.join()

    return m


def upload_directory(dirname, object_list, pool, dryrun=False):
    # On my system generating the hashes serially over 86MB of data with a
    # cold disk cache finishes in 1.9s. With a warm cache it
    # finishes in 0.45s.
    # Trying to do this in parallel results in these values:
    #   n   warm    cold
    #   1   0.55s   1.9s
    #   2   0.56s   1.1s
    #   3   0.66s   0.82
    #   4   0.66s   0.82
    # The only time parallelization wins is on a cold disk cache;
    # no need to try and parallize this part.
    jobs = []
    for filename, h in traverse_directory(dirname, sha1sum):
        # re-process .1% of objects here
        # to ensure that objects get their last modified date refreshed
        # this avoids all objects expiring out of the manifest at the same time
        r = random.randint(0, 999)
        if h in object_list and r != 0:
            log.debug("skipping %s - already in manifest", filename)
            jobs.append((None, filename, h))
            continue

        # TODO: Handle packing together smaller files
        if not dryrun:
            keyname = "objects/{}".format(h)
            job = pool.apply_async(upload_file, (filename, keyname))
            jobs.append((job, filename, h))
        else:
            jobs.append((None, filename, h))

        # Add the object to the local manifest so we don't try and
        # upload it again
        object_list.add(h)

    return jobs


def dupes_report(manifest):
    files_by_hash = defaultdict(list)
    for h, filename in manifest.files:
        s = os.path.getsize(filename)
        files_by_hash[s, h].append(filename)

    dupe_size = 0
    for (size, h), filenames in sorted(files_by_hash.iteritems()):
        if len(filenames) > 1:
            sn = size * (len(filenames) - 1)
            log.info("%i %s", sn, filenames)
            dupe_size += sn
    log.info("%i in total duplicate files", dupe_size)


def main():
    import argparse
    import sys
    import gzip

    parser = argparse.ArgumentParser()
    # TODO: These aren't required if no-upload is set
    parser.add_argument("-r", "--region", dest="region", required=True)
    parser.add_argument("-b", "--bucket", dest="bucket_name", required=True)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN, default=logging.INFO)
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)
    parser.add_argument("-j", "--jobs", dest="jobs", type=int, help="how many simultaneous uploads to do", default=8)
    parser.add_argument("-o", "--output", dest="output", help="where to output manifet, use '-' for stdout", default="manifest.gz")
    parser.add_argument("-z", "--compress-manifest", dest="compress_manifest",
                        help="compress manifest output (default if outputting to a file)",
                        action="store_true")
    parser.add_argument("--no-compress-manifest", dest="compress_manifest",
                        help="don't compress manifest output (default if outputting to stdout)",
                        action="store_false")
    parser.add_argument("--no-upload", dest="dryrun", action="store_true", default=False)
    parser.add_argument("--report-dupes", dest="report_dupes", action="store_true", default=False, help="report on duplicate files")
    parser.add_argument("dirname", help="directory to upload")

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s - %(message)s")
    # Make boto shut up
    # TODO: Add -v -v support to set this to DEBUG?
    logging.getLogger('boto').setLevel(logging.INFO)

    if not args.dryrun:
        connect(args.region, args.bucket_name)

    manifest = process_directory(args.dirname, args.jobs, dryrun=args.dryrun)

    if args.output == '-':
        output_file = sys.stdout
    else:
        output_file = open(args.output, 'wb')
        # Enable compression by default if we're writing out to a file
        if args.compress_manifest is None:
            args.compress_manifest = True

    if args.compress_manifest:
        output_file = gzip.GzipFile(fileobj=output_file, mode='wb')

    manifest.save(output_file)

    if args.report_dupes:
        dupes_report(manifest)

if __name__ == '__main__':
    main()
