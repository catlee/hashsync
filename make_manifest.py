#!/usr/bin/env python
from collections import defaultdict
import time
import multiprocessing

from hashsync.utils import parse_date
from hashsync.objectlist import ObjectList
from hashsync.connection import connect, get_bucket

import logging
log = logging.getLogger(__name__)


def delete_objects(keys, reason):
    log.info("Deleting %i %s keys", len(keys), reason)
    # Sleep a bit to work around request limits
    time.sleep(5)
    get_bucket().delete_keys(keys)


def delete_old_keys(too_old):
    pool = multiprocessing.Pool(8)

    bucket = get_bucket()

    object_list = ObjectList(bucket)
    object_list.load()

    new_object_list = ObjectList(bucket)

    objects_by_key = defaultdict(list)
    delete_jobs = []
    bucket = get_bucket()
    log.info("Listing objects...")
    to_delete = []
    for o in bucket.list_versions():
        if hasattr(o, 'DeleteMarker'):
            continue
        if o.key.startswith("objectlist"):
            continue

        d = parse_date(o.last_modified)
        h = o.key.split("/")[-1]
        objects_by_key[o.key].append((d, o.version_id))
        if d < too_old:
            if h not in object_list:
                # Delete old objects
                to_delete.append((o.key, o.version_id))

                if len(to_delete) >= 1000:
                    delete_job = pool.apply_async(delete_objects, (to_delete, "old"))
                    delete_jobs.append(delete_job)
                    to_delete = []
        else:
            new_object_list.add(h)

    if to_delete:
        delete_job = pool.apply_async(delete_objects, (to_delete, "old"))
        delete_jobs.append(delete_job)

    to_delete = []
    #  Delete duplicate versions of objects?
    for key, versions in objects_by_key.iteritems():
        versions.sort()
        # Delete all but the newest
        for d, v in versions[:-1]:
            to_delete.append((key, v))
            if len(to_delete) >= 1000:
                delete_job = pool.apply_async(delete_objects, (to_delete, "dupe"))
                delete_jobs.append(delete_job)
                to_delete = []

    if to_delete:
        delete_job = pool.apply_async(delete_objects, (to_delete, "dupe"))
        delete_jobs.append(delete_job)

    for job in delete_jobs:
        job.get(86400)

    pool.close()
    pool.join()

    return new_object_list


def main():
    import argparse

    parser = argparse.ArgumentParser()
    # TODO: These aren't required if no-upload is set
    parser.add_argument("-r", "--region", dest="region", required=True)
    parser.add_argument("-b", "--bucket", dest="bucket_name", required=True)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN, default=logging.INFO)
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s - %(message)s")
    # Make boto shut up
    # TODO: Add -v -v support to set this to DEBUG?
    logging.getLogger('boto').setLevel(logging.INFO)

    connect(args.region, args.bucket_name)

    too_old = time.time() - 5 * 86400  # 5 days

    object_list = delete_old_keys(too_old)
    object_list.save()

if __name__ == '__main__':
    main()
