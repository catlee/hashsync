#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os

from hashsync.connection import get_bucket
from hashsync.utils import parse_date
from hashsync.compression import maybe_compress

import logging
log = logging.getLogger(__name__)


def upload_file(filename, keyname, reduced_redundancy=True, refresh_mintime=86400, compress_minsize=1024):
    """
    Uploads the specified file to the bucket returned by hashsync.connection.get_bucket().

    Arguments:
        filename (str):    path to local file
        keyname  (str):    key name to store object
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

    bucket = get_bucket()
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
