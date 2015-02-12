#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
config settings for hashsync
these are some general constants for various functions
"""

# Minimum filesize to try compressing
COMPRESS_MINSIZE = 1024

# Files larger than this many bytes will use a temporary file on disk to
# compress; files smaller than this are compressed in memory
COMPRESS_INMEM_SIZE = 104857600

# Minimum time before refreshing the last_modified time of the key
REFRESH_MINTIME = 86400

# one out of every N objects should be randomly refreshed per upload
REFRESH_EVERY_NTH_OBJECTS = 10000

# how long we'll wait before giving up on an upload
MAX_UPLOAD_TIME = 3600

# time to wait before actually deleting old objects from the bucket
PURGE_TIME = 86400 * 30
