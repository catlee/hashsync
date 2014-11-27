#!/usr/bin/env python
from collections import defaultdict
from hashsync.utils import parse_date
from hashsync.objectlist import ObjectList
import time
import multiprocessing

import logging
log = logging.getLogger(__name__)


BUCKET = None


def delete_object(key, version_id, reason):
    log.info("Deleting %s %s %s", reason, key, version_id)
    BUCKET.delete_key(key, version_id=version_id)


def delete_old_keys(too_old):
    pool = multiprocessing.Pool(8)

    object_list = ObjectList(BUCKET)
    object_list.load()

    new_object_list = ObjectList(BUCKET)

    objects_by_key = defaultdict(list)
    delete_jobs = []
    for o in BUCKET.list_versions():
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
                delete_job = pool.apply_async(delete_object, (o.key, o.version_id, "old"))
                delete_jobs.append(delete_job)
        else:
            new_object_list.add(h)

    #  Delete duplicate versions of objects?
    for key, versions in objects_by_key.iteritems():
        versions.sort()
        # Delete all but the newest
        for d, v in versions[:-1]:
            delete_job = pool.apply_async(delete_object, (key, v, "dupe"))
            delete_jobs.append(delete_job)

    for job in delete_jobs:
        job.get()

    pool.close()
    pool.join()

    return new_object_list


def main():
    import boto.s3
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

    conn = boto.s3.connect_to_region(args.region)
    global BUCKET
    BUCKET = conn.get_bucket(args.bucket_name)

    too_old = time.time() - 5 * 86400  # 5 days

    object_list = delete_old_keys(too_old)
    object_list.save()

if __name__ == '__main__':
    main()
