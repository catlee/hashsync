#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
compression module for hashsync
"""

import gzip
import os
from io import BytesIO
import tempfile

from hashsync.utils import iterfile


def compress_stream(src, dst):
    """
    Compresses data from file object src and writes it to file object dst

    Arguments:
        src (file object): stream to read data from. must support a
                           read(blocksize) method
        dst (file object): stream to write compressed data to. must support a
                           write(block) method

    Returns:
        None
    """
    with gzip.GzipFile(fileobj=dst, mode='wb') as gz:
        for block in iterfile(src):
            gz.write(block)


def decompress_stream(src, dst):
    """
    Decompresses data from file object src and writes it to file object dst

    Arguments:
        src (file object): stream to read compressed data from. must support a
                           read(blocksize) method
        dst (file object): stream to copy data to. must support a .write(block)
                           method

    Returns:
        None
    """
    with gzip.GzipFile(fileobj=src, mode='rb') as gz:
        for block in iterfile(gz):
            dst.write(block)


# TODO: Clean up these methods...there are too many variants doing the same
# thing
def compress_file(filename, in_memsize=104857600):
    """
    gzip compress a file, and return a file object with the compressed results

    Arguments:
        filename (str): filename to compress
        in_memsize (int): files larger than this many bytes use a temporary
                          file on disk to compress to; files smaller than this
                          are compressed in memory

    Returns:
        (compressed_size, file obj): a tuple of the size of the compressed data
        and a file object seeked to the beginning of the compressed data.
        A file object containing the compressed contents.
    """
    filesize = os.path.getsize(filename)
    # Use a temporary file to compress files more than 100MB
    src = open(filename, 'rb')
    if filesize > in_memsize:
        dst = tempfile.TemporaryFile()
    else:
        dst = BytesIO()

    compress_stream(src, dst)
    size = dst.tell()
    dst.seek(0)
    return size, dst


def gzip_compress(data):
    f = BytesIO()
    gz = gzip.GzipFile(mode='wb', fileobj=f)
    gz.write(data)
    gz.close()
    f.seek(0)
    return f


def gzip_decompress(data):
    f = BytesIO(data)
    gz = gzip.GzipFile(mode='rb', fileobj=f)
    return gz.read()
