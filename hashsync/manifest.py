#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json


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

        #for h, filename, perms in self.files:
            #line = u"{0} {1} 0{2:o}\n".format(h, filename, perms)
            #output_file.write(line.encode("utf8"))

    def load(self, input_file):
        """
        Loads a manifest from a file object

        Arguments:
            input_file (file_object): the file object to read the manifest from
        """
        data = input_file.read().decode("utf8")

        for h, filename, perms in json.loads(data):
            self.add(h, filename, perms)
