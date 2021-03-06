#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os
import random
import multiprocessing
from collections import defaultdict

from hashsync.connection import get_bucket
from hashsync.utils import parse_date, traverse_directory, sha1sum, strip_leading
from hashsync.compression import maybe_compress
from hashsync.objectlist import ObjectList
from hashsync.manifest import Manifest
from hashsync import config

import logging
log = logging.getLogger(__name__)


def upload_file(filename, keyname, reduced_redundancy=True):
    """
    Uploads the specified file to the bucket returned by hashsync.connection.get_bucket().

    Arguments:
        filename (str):    path to local file
        keyname  (str):    key name to store object
        reduced_redundancy (bool): whether to use reduced redundancy storage; defaults to True

    Returns:
        state (str):       one of "skipped", "refreshed", "uploaded"
    """
    filesize = os.path.getsize(filename)
    # TODO: inline small files
    if filesize == 0:
        log.debug("skipping 0 byte file; no need to upload it")
        return "inlined"

    bucket = get_bucket()
    key = bucket.get_key(keyname)
    if key:
        log.debug("we already have %s last-modified: %s", keyname, key.last_modified)
        # If this was uploaded recently, we can skip uploading it again
        # If the last-modified is old enough, we copy the key on top of itself
        # to refresh the last-modified time.
        if parse_date(key.last_modified) > time.time() - config.REFRESH_MINTIME:
            log.debug("skipping %s since it was uploaded recently, but not in manifest", filename)
            return "checked"
        else:
            log.info("refreshing %s at %s", filename, keyname)
            # TODO: This can fail if we've just deleted this key
            key.copy(bucket.name, key.name, reduced_redundancy=reduced_redundancy)
            return "refreshed"
    else:
        key = bucket.new_key(keyname)

    log.debug("compressing %s", filename)

    fobj, was_compressed = maybe_compress(filename)
    if was_compressed:
        key.set_metadata('Content-Encoding', 'gzip')

    log.info("uploading %s to %s", filename, keyname)
    key.set_contents_from_file(fobj, policy='public-read', reduced_redundancy=reduced_redundancy)
    return "uploaded"


def _init_worker():
    "Ignore SIGINT for process workers"
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def upload_directory(dirname, jobs, dryrun=False):
    """
    Uploads the specified directory to the bucket returned by hashsync.connection.get_bucket()

    Arguments:
        dirname (str): local directory name to upload
        jobs (int): how many uploads to do in parallel
        dryrun (bool): if True, don't actually upload anything (default: False)

    Returns:
        A hashsync.manifest.Manifest object
    """
    if not dryrun:
        bucket = get_bucket()
        object_list = ObjectList(bucket)
        object_list.load()
    else:
        object_list = ObjectList(None)

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
    pool = multiprocessing.Pool(jobs, initializer=_init_worker)
    jobs = []
    for filename, h in traverse_directory(dirname, sha1sum):
        # re-process some objects here to ensure that objects get their last
        # modified date refreshed. this avoids all objects expiring out of the
        # manifest at the same time
        r = random.randint(0, config.REFRESH_EVERY_NTH_OBJECTS)
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

    retval = []
    stats = defaultdict(int)
    size_by_state = defaultdict(int)
    m = Manifest()
    for job, filename, h in jobs:
        if job:
            # Specify a timeout for .get() to allow us to catch
            # KeyboardInterrupt.
            state = job.get(config.MAX_UPLOAD_TIME)
        else:
            state = 'skipped'

        stripped = strip_leading(dirname, filename)
        st = os.stat(filename)
        perms = st.st_mode & 0o777
        size = st.st_size
        m.add(h, stripped, perms)
        retval.append((state, filename, h))
        stats[state] += 1
        size_by_state[state] += size

    # Shut down pool
    pool.close()
    pool.join()

    log.info("stats: %s", dict(stats))
    log.info("size stats: %s", dict(size_by_state))
    return m
