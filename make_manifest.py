#!/usr/bin/env python
from collections import defaultdict
import time
import multiprocessing

from hashsync.utils import parse_date
from hashsync.objectlist import ObjectList
from hashsync.connection import connect, get_bucket
from hashsync import config

import logging
log = logging.getLogger(__name__)


def delete_objects(keys):
    log.info("Deleting %i keys", len(keys))
    # Sleep a bit to work around request limits
    time.sleep(5)
    get_bucket().delete_keys(keys)


class Reaper(object):
    def __init__(self, max_objects=1000):
        self.max_objects = max_objects
        self.pool = multiprocessing.Pool(8)
        self.to_delete = []
        self.jobs = []

    def delete(self, key):
        self.to_delete.append(key)
        if len(self.to_delete) >= self.max_objects:
            job = self.pool.apply_async(delete_objects, (self.to_delete,))
            self.jobs.append(job)
            self.to_delete = []

    def stop(self):
        for job in self.jobs:
            job.get(86400)

        self.pool.close()
        self.pool.join()


def delete_old_keys(too_old):
    now = time.time()

    bucket = get_bucket()

    object_list = ObjectList(bucket)
    object_list.load()

    new_object_list = ObjectList(bucket)

    objects_by_key = defaultdict(list)
    bucket = get_bucket()
    log.info("Listing objects; deleting old keys...")
    reaper = Reaper()

    for o in bucket.list_versions():
        if hasattr(o, 'DeleteMarker'):
            continue
        if o.key.startswith("objectlist"):
            continue

        d = parse_date(o.last_modified)
        h = o.key.split("/")[-1]
        objects_by_key[o.key].append((d, o.version_id))
        if d >= too_old:
            new_object_list.add(h)
            continue

        if h not in object_list and d <= (now - config.PURGE_TIME):
            # Delete old objects
            reaper.delete((o.key, o.version_id))

    #  Delete duplicate versions of objects?
    log.info("deleting duplicate keys...")
    for key, versions in objects_by_key.iteritems():
        versions.sort()
        # Delete all but the newest
        for d, v in versions[:-1]:
            reaper.delete((key, v))

    reaper.stop()

    return new_object_list


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--region", dest="region", required=True)
    parser.add_argument("-b", "--bucket", dest="bucket_name", required=True)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN, default=logging.INFO)
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)
    parser.add_argument("cutoff", type=int, nargs="?",
                        help="cutoff time (timestamp); objects older than this will be considered for deletion; defaults to one week ago",
                        default=time.time() - 7 * 86400)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s - %(message)s")
    # Make boto shut up
    # TODO: Add -v -v support to set this to DEBUG?
    logging.getLogger('boto').setLevel(logging.INFO)

    connect(args.region, args.bucket_name)

    too_old = args.cutoff

    object_list = delete_old_keys(too_old)
    object_list.save()

if __name__ == '__main__':
    main()
