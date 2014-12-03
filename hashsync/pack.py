#!/usr/bin/env python
# -*- coding: utf-8 -*-
import tempfile
import tarfile
import os

import logging
log = logging.getLogger(__name__)

# How will we name the tarfile?
# By the hash of its contents?
# Hash the hashes of the member files?


class Packer(object):
    def __init__(self, pack_maxsize):
        self.pack_maxsize = pack_maxsize

        self.current_tarfile = None
        self.current_tempfile = None
        # Set up our first tarball
        self.cycle()

    def cycle(self):
        if self.current_tarfile:
            self.current_tarfile.close()
        old_tempfile = self.current_tempfile

        self.current_tempfile = tempfile.TemporaryFile()
        self.current_tarfile = tarfile.open(mode='w:gz', fileobj=self.current_tempfile, format=tarfile.PAX_FORMAT)
        self.current_size = 0
        self.current_members = []

        return old_tempfile

    def add(self, filename, h):
        """
        Returns either a file, a pack object, or None.
        Returns a file when the file is too large for a pack
        Returns a pack when the current pack is full
        Returns None when the file has been added to the pack
        """
        s = os.path.getsize(filename)
        # Is this file too big to ever fit in a pack?
        if s > self.pack_maxsize:
            return filename

        retval = None
        # Does overflow the current pack? If so, start a new tarball and return
        # the current one
        if self.current_size + s > self.pack_maxsize:
            retval = self.cycle()

        self.current_tarfile.add(filename, arcname=h, recursive=False)
        self.current_size += s
        self.current_members.append((filename, h))
        return retval

    def close(self):
        self.current_tarfile.close()
        if len(self.current_members) > 0:
            return self.current_tempfile
        return None
