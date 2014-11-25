#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib
import calendar
import time
import email.utils
from functools import partial
import os

import logging
log = logging.getLogger(__name__)


def traverse_directory(dirname, action):
    """
    Call action() on all files under dirname. For each file under dirname,
    (filename, action(filename)) will be yielded.

    Arguments:
        dirname (str):  directory name to traverse
        action (callable): function to call

    Yields:
        (filename, result) tuples
    """
    for root, dirs, files in os.walk(dirname):
        dirs.sort()
        for f in sorted(files):
            filename = os.path.join(root, f)
            yield filename, action(filename)


def parse_date(s):
    try:
        return calendar.timegm(time.strptime(s[:19], '%Y-%m-%dT%H:%M:%S'))
    except ValueError:
        return calendar.timegm(email.utils.parsedate_tz(s))


def iterfile(fobj, blocksize=1024 ** 2):
    """
    Yields blocks of data from fobj
    """
    for block in iter(partial(fobj.read, blocksize), b''):
        yield block


def copy_stream(src, dst):
    """
    Copies data from src fileobj to dst fileobj
    """
    for block in iterfile(src):
        dst.write(block)


def sha1sum(filename):
    """
    Calculates the sha1sum of a file

    Arguments:
        filename (str): path to local file

    Returns:
        40 byte hex string representing the hash of the file
    """
    h = hashlib.new('sha1')
    with open(filename, 'rb') as fp:
        for block in iterfile(fp):
            h.update(block)
    return h.hexdigest()


def strip_leading(head, path):
    "Strips head from path, ending up with a relative path"
    n = len(head)
    if not head.endswith("/"):
        n += 1
    return path[n:]
