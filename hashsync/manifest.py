#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
from collections import defaultdict

from hashsync.compression import GZIP_MAGIC, gzip_decompress

import logging
log = logging.getLogger(__name__)


# TODO: Do we want to handle directories here?
class Manifest(object):
    """
    A Manifest describes a set of files along with their hashes and permissions
    """
    def __init__(self):
        # List of hash, filename, permission tuples
        self.files = []

    def add(self, h, filename, perms):
        """
        Adds a file to the manifest

        Arguments:
            h (str): the sha1 has of the file
            filename (str): the filename, usually relative to some top level directory
            perms (int): integer representation of file permissions
        """
        self.files.append((h, filename, perms))

    def save(self, output_file):
        """
        Outputs the manifest to a file object. Permissions are output in octal representation.

        Arguments:
            output_file (file object): the file object to write the manifest to
        """
        data = json.dumps(self.files, output_file, indent=2)
        data = data.encode("utf8")
        output_file.write(data)

    def load(self, input_file):
        """
        Loads a manifest from a file object

        Arguments:
            input_file (file_object): the file object to read the manifest from
        """
        data = input_file.read()
        if data.startswith(GZIP_MAGIC):
            data = gzip_decompress(data)
        data = data.decode("utf8")

        for h, filename, perms in json.loads(data):
            self.add(h, filename, perms)

    def report_dupes(self):
        """
        Report information about duplicate files in the manifest
        """
        files_by_hash = defaultdict(list)
        for h, filename in self.files:
            s = os.path.getsize(filename)
            files_by_hash[s, h].append(filename)

        dupe_size = 0
        for (size, h), filenames in sorted(files_by_hash.iteritems()):
            if len(filenames) > 1:
                sn = size * (len(filenames) - 1)
                log.info("%i %s", sn, filenames)
                dupe_size += sn
        log.info("%i in total duplicate files", dupe_size)
