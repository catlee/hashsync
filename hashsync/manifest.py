#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gzip
import sys


# TODO: Do we want to handle permissions and directories here?
class Manifest(object):
    """
    A Manifest describes a set of files along with their hashes
    """
    def __init__(self):
        # List of filename, hash tuples
        self.files = []

    def add(self, h, filename):
        self.files.append((h, filename))

    def save(self, output_file, compress=True):
        if output_file in (None, "-"):
            manifest_outfile = sys.stdout
        elif compress:
            manifest_outfile = gzip.GzipFile(output_file, 'wb')
        else:
            manifest_outfile = open(output_file, 'wb')

        for h, filename in self.files:
            manifest_outfile.write("%s %s\n" % (h, filename))
        manifest_outfile.close()

    def load(self, input_file, compressed=True):
        # TODO: Detect if the file is compressed or not
        if compressed:
            f = gzip.GzipFile(input_file, 'rb')
        else:
            f = open(input_file, 'rb')

        with f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                h, filename = line.split(" ", 1)
                self.add(h, filename)
