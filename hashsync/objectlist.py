#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from hashsync.compression import gzip_decompress, gzip_compress, GZIP_MAGIC

import logging
log = logging.getLogger(__name__)


class ObjectList(object):
    """
    Handle getting/uploading list of objects from s3 bucket
    """
    cache_file = ".objectlist"

    def __init__(self, bucket, keyname="objectlist"):
        # Set of object hashes we know about
        self.objects = set()
        self.bucket = bucket
        self.keyname = keyname

    def load_cache(self, etag):
        try:
            with open(self.cache_file, 'r') as fp:
                cached_objects = json.load(fp)
                if cached_objects['etag'] != etag:
                    return False
                self.objects.update(cached_objects['objects'])
                log.info("loaded %i old objects from cache", len(cached_objects['objects']))
                return True
        except IOError:
            return False
        except ValueError:
            return False

    def load_remote(self, key):
        data = key.get_contents_as_string()
        if key.content_encoding == 'gzip' or data.startswith(GZIP_MAGIC):
            data = gzip_decompress(data)
        data = data.decode("ascii")

        objects = data.split("\n")
        self.objects.update(objects)
        log.info("loaded %i old objects from %s/%s", len(objects), self.bucket.name, self.keyname)

    def load(self):
        remote_objects = self.bucket.get_key(self.keyname)

        if not remote_objects:
            return self.objects

        if not self.load_cache(remote_objects.etag):
            self.load_remote(remote_objects)
            self.save_cache(remote_objects.etag)

        return self.objects

    def save_cache(self, etag):
        try:
            with open(self.cache_file, 'w') as fp:
                cached_objects = {'etag': etag}
                cached_objects['objects'] = list(self.objects)
                json.dump(cached_objects, fp)
                return True
        except IOError:
            return False

    def save(self):
        manifest_data = "\n".join(sorted(self.objects))
        manifest_data = manifest_data.encode("ascii")

        manifest = self.bucket.new_key(self.keyname)
        manifest.set_metadata('Content-Encoding', 'gzip')
        manifest.set_contents_from_string(gzip_compress(manifest_data))
        log.info("wrote %i objects to manifest %s/%s", len(self.objects), self.bucket.name, self.keyname)

    def __contains__(self, h):
        return h in self.objects

    def add(self, h):
        self.objects.add(h)
