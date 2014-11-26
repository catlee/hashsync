#!/usr/bin/env python
import os
import time
from multiprocessing import Process, Queue
from Queue import Empty
from collections import defaultdict
import random

import boto.s3

from hashsync.utils import parse_date, sha1sum, traverse_directory, strip_leading
from hashsync.compression import maybe_compress
from hashsync.objectlist import ObjectList
from hashsync.manifest import Manifest

import logging
log = logging.getLogger(__name__)


def upload_file(filename, keyname, bucket, reduced_redundancy=True, refresh_mintime=86400, compress_minsize=1024):
    """
    Uploads file to bucket

    Arguments:
        filename (str):    path to local file
        keyname  (str):    key name to store object
        bucket   (boto.s3.Bucket): S3 bucket to upload object to
        reduced_redundancy (bool): whether to use reduced redundancy storage; defaults to True
        refresh_mintime (int): minimum time before refreshing the last_modified time of the key; defaults to 86400 (one day)
        compress_minsize (int): minimum size to try compressing the file; defaults to 1024

    Returns:
        state (str):       one of "skipped", "refreshed", "uploaded"
    """
    filesize = os.path.getsize(filename)
    if filesize == 0:
        log.debug("skipping 0 byte file; no need to upload it")
        return "skipped"

    key = bucket.get_key(keyname)
    if key:
        log.debug("we already have %s last-modified: %s", keyname, key.last_modified)
        # If this was uploaded recently, we can skip uploading it again
        # If the last-modified is old enough, we copy the key on top of itself
        # to refresh the last-modified time.
        if parse_date(key.last_modified) > time.time() - refresh_mintime:
            log.debug("skipping %s since it was uploaded recently, but not in manifest", filename)
            return "skipped"
        else:
            log.info("refreshing %s at %s", filename, keyname)
            key.copy(bucket.name, key.name, reduced_redundancy=reduced_redundancy)
            return "refreshed"
    else:
        key = bucket.new_key(keyname)

    log.debug("compressing %s", filename)

    fobj, was_compressed = maybe_compress(filename, compress_minsize)
    if was_compressed:
        key.set_metadata('Content-Encoding', 'gzip')

    log.info("uploading %s to %s", filename, keyname)
    key.set_contents_from_file(fobj, policy='public-read', reduced_redundancy=reduced_redundancy)
    return "uploaded"


class Worker(Process):
    daemon = True

    def __init__(self, bucket, in_queue, out_queue):
        self.bucket = bucket
        self.in_queue = in_queue
        self.out_queue = out_queue

        Process.__init__(self)

    def run(self):
        bucket = self.bucket
        in_queue = self.in_queue
        out_queue = self.out_queue

        while True:
            try:
                item = in_queue.get(timeout=10)
                if not item:
                    print "no item; exiting"
                    return

                filename, keyname = item
                h = os.path.basename(keyname)
                result = upload_file(filename, keyname, bucket)
                out_queue.put((result, filename, h))
            except Empty:
                print "timeout; exiting"
                return


def process_directory(dirname, bucket, jobs, dryrun=False):
    if not dryrun:
        object_list = ObjectList(bucket)
        object_list.load()
    else:
        object_list = ObjectList(None)

    # TODO: using a regular pool (with processes) here results in really poor
    # connection pooling behaviour, but using a threadpool means we can't
    # compress across more than 1 core.
    # This is because the connection (along with its pool) is being pickled
    # for each task, so each file to process gets a new connection pool!
    #pool = multiprocessing.pool.Pool(jobs)

    in_queue = Queue()
    out_queue = Queue()

    workers = [Worker(bucket, in_queue, out_queue) for _ in range(jobs)]

    [w.start() for w in workers]

    try:
        upload_directory(dirname, object_list, in_queue, out_queue, dryrun=dryrun)

        stats = defaultdict(int)
        m = Manifest()

        # TODO: Shut down the workers
        while not in_queue.empty() or not out_queue.empty():
            state, filename, h = out_queue.get()
            stats[state] += 1

            stripped = strip_leading(dirname, filename)
            perms = os.stat(filename).st_mode & 0777
            m.add(h, stripped, perms)

        log.info("stats: %s", dict(stats))

        return m
    except BaseException:
        [w.terminate() for w in workers]
        raise


def upload_directory(dirname, object_list, in_queue, out_queue, dryrun=False):
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
    for filename, h in traverse_directory(dirname, sha1sum):
        # re-process .1% of objects here
        # to ensure that objects get their last modified date refreshed
        # this avoids all objects expiring out of the manifest at the same time
        keyname = "objects/{}".format(h)
        if h in object_list and random.randint(0, 999) != 0:
            log.debug("skipping %s - already in manifest", filename)
            out_queue.put(('skipped', filename, h))
            continue

        # TODO: Handle packing together smaller files
        if not dryrun:
            # TODO: running in a subprocess here results in poor connection
            # pool behaviour; not sure why
            #job = pool.apply_async(upload_file, (filename, keyname, bucket))
            #jobs.append((job, filename, h))
            in_queue.put((filename, keyname))
        else:
            out_queue.put(('skipped', filename, h))

        # Add the object to the local manifest so we don't try and
        # upload it again
        object_list.add(h)


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

    parser = argparse.ArgumentParser()
    # TODO: These aren't required if no-upload is set
    parser.add_argument("-r", "--region", dest="region", required=True)
    parser.add_argument("-b", "--bucket", dest="bucket_name", required=True)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN, default=logging.INFO)
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)
    parser.add_argument("-j", "--jobs", dest="jobs", type=int, help="how many simultaneous uploads to do", default=8)
    parser.add_argument("-o", "--output", dest="output", help="where to output manifet, use '-' for stdout")
    parser.add_argument("-z", "--compress-manifest", dest="compress_manifest", help="compress manifest output", action="store_true")
    parser.add_argument("--no-upload", dest="dryrun", action="store_true", default=False)
    parser.add_argument("--report-dupes", dest="report_dupes", action="store_true", default=False, help="report on duplicate files")
    parser.add_argument("dirname", help="directory to upload")

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s - %(message)s")
    # Make boto shut up
    # TODO: Add -v -v support to set this to DEBUG?
    logging.getLogger('boto').setLevel(logging.INFO)

    if not args.dryrun:
        conn = boto.s3.connect_to_region(args.region)
        bucket = conn.get_bucket(args.bucket_name)
    else:
        bucket = None

    manifest = process_directory(args.dirname, bucket, args.jobs, dryrun=args.dryrun)

    manifest.save(args.output, compress=args.compress_manifest)

    if args.report_dupes:
        dupes_report(manifest)

if __name__ == '__main__':
    main()
