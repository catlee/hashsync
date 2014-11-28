#!/usr/bin/env python
# -*- coding: utf-8 -*-
import boto.s3

# Global bucket we're using
# It's easiest to use a global object here so we can maintain one connection
# pool per process
BUCKET = None


def connect(region, bucket_name):
    """
    Connect to the specified bucket in the given region.

    Arguments:
        region (str): Amazon region name
        bucket_name(str): Name of bucket

    Returns:
        boto.s3.Bucket object

    Also sets the global BUCKET object in this module
    """
    global BUCKET
    conn = boto.s3.connect_to_region(region)
    conn.region_name = region
    BUCKET = conn.get_bucket(bucket_name)
    return BUCKET


def get_bucket():
    """
    Returns the bucket object previously conencted to with connect()
    """
    return BUCKET
