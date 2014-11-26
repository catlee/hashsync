#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hashsync.compression import gzip_decompress, gzip_compress

import logging
log = logging.getLogger(__name__)


class ObjectList(object):
    """
    Handle getting/uploading list of objects from s3 bucket
    """
    def __init__(self, bucket, keyname="objectlist"):
        # Set of object hashes we know about
        self.objects = set()
        self.bucket = bucket
        self.keyname = keyname

    def load(self):
        remote_objects = self.bucket.get_key(self.keyname)

        if remote_objects:
            data = remote_objects.get_contents_as_string()
            if remote_objects.content_encoding == 'gzip':
                data = gzip_decompress(data)

            objects = data.split("\n")
            self.objects.update(objects)
            log.info("loaded %i old objects from %s/%s", len(objects), self.bucket.name, self.keyname)

        return self.objects

    def save(self):
        manifest_data = "\n".join(sorted(self.objects))

        manifest = self.bucket.new_key(self.keyname)
        manifest.set_metadata('Content-Encoding', 'gzip')
        manifest.set_contents_from_string(gzip_compress(manifest_data))
        log.info("wrote %i objects to manifest %s/%s", len(self.objects), self.bucket.name, self.keyname)

    def __contains__(self, h):
        return h in self.objects

    def add(self, h):
        self.objects.add(h)
