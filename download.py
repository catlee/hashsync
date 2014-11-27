#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import tempfile

import boto.s3

from hashsync.utils import traverse_directory, sha1sum, copy_stream, strip_leading, SHA1SUM_ZERO
from hashsync.manifest import Manifest
from hashsync.compression import decompress_stream

import logging
log = logging.getLogger(__name__)


def mkdirs(d):
    if not os.path.exists(d):
        os.makedirs(d)


def touch(filename):
    dirname = os.path.dirname(filename)
    mkdirs(dirname)
    with open(filename, 'wb'):
        os.utime(filename, None)


class FileCache(object):
    def __init__(self, cachedir, verify=False):
        self.cachedir = os.path.abspath(cachedir)
        self.verify = verify

    def makepath(self, h):
        bits = "{0}/{1}/{2}".format(h[0], h[1], h)
        return os.path.join(self.cachedir, bits)

    def __contains__(self, h):
        p = self.makepath(h)
        return os.path.exists(p)

    def copy_from_cache(self, h, dest):
        log.info("Copying %s to %s", h, dest)
        dirname = os.path.dirname(dest)
        mkdirs(dirname)

        src = self.makepath(h)
        shutil.copyfile(src, dest)


# This is a standalone function rather than an instance method above so that it
# can be called via multiprocessing more easily
def download_key(bucket, keyname, dst):
    log.info("Downloading %s to %s", keyname, dst)
    k = bucket.get_key(keyname)

    if not k:
        log.error("couldn't find %s", keyname)
        raise ValueError("couldn't find %s" % keyname)

    dirname = os.path.dirname(dst)
    mkdirs(dirname)

    with open(dst, 'wb') as f:
        if k.content_encoding == 'gzip':
            # Download to a tmpfile first
            tmp = tempfile.TemporaryFile()
            copy_stream(k, tmp)
            tmp.seek(0)
            decompress_stream(tmp, f)
        else:
            copy_stream(k, f)


def main():
    import multiprocessing
    import argparse
    from collections import defaultdict

    parser = argparse.ArgumentParser()
    # TODO: These aren't required if no-upload is set
    parser.add_argument("-r", "--region", dest="region", required=True)
    parser.add_argument("-b", "--bucket", dest="bucket_name", required=True)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN, default=logging.INFO)
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)
    parser.add_argument("-j", "--jobs", dest="jobs", type=int, help="how many simultaneous downloads to do", default=8)
    parser.add_argument("-o", "--output", dest="output", help="where to output manifet, use '-' for stdout")
    parser.add_argument("--cache-dir", dest="cache_dir", help="where to cache objects locally", required=True)
    parser.add_argument("manifest", help="manifest to load")
    parser.add_argument("destdir", help="target directory to populate")

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s - %(message)s")
    # Make boto shut up
    # TODO: Add -v -v support to set this to DEBUG?
    logging.getLogger('boto').setLevel(logging.INFO)

    conn = boto.s3.connect_to_region(args.region)
    bucket = conn.get_bucket(args.bucket_name)

    m = Manifest()
    m.load(open(args.manifest, 'rb'))
    manifest_files = {(h, filename) for (h, filename, perms) in m.files}

    # Set of (h, filename) objects we have locally
    local_files = set()

    destdir = args.destdir

    if os.path.exists(destdir):
        for filename, h in traverse_directory(destdir, sha1sum):
            stripped = strip_leading(destdir, filename)
            local_files.add((h, stripped))

    # Remove files that aren't in the manifest
    to_remove = local_files - manifest_files
    for h, filename in to_remove:
        log.info("Removing %s %s", h, filename)
        os.unlink(os.path.join(destdir, filename))

    ok = local_files & manifest_files
    for h, filename in ok:
        log.debug("OK %s %s", h, filename)

    # TODO: Handle updating permissions
    to_add = manifest_files - local_files
    cache = FileCache(args.cache_dir)

    pool = multiprocessing.Pool(args.jobs)

    download_jobs = []
    files_by_hash = defaultdict(list)

    for h, filename in to_add:
        dest = os.path.join(destdir, filename)
        if h in files_by_hash:
            # We're already fetching this, make a note of the additional
            # filename
            files_by_hash[h].append(dest)
        elif h == SHA1SUM_ZERO:
            # Zero byte file!
            touch(dest)
        elif h not in cache:
            cache_filename = cache.makepath(h)
            keyname = "objects/{}".format(h)
            job = pool.apply_async(download_key, (bucket, keyname, cache_filename))
            files_by_hash[h].append(dest)
            download_jobs.append((job, h))
        else:
            cache.copy_from_cache(h, dest)

    for job, h in download_jobs:
        job.get()
        for dest in files_by_hash[h]:
            cache.copy_from_cache(h, dest)

    pool.close()
    pool.join()

if __name__ == '__main__':
    main()
