#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_compression
----------------------------------

Tests for `hashsync.compression` module.
"""

import unittest
import os

from io import BytesIO, UnsupportedOperation

from hashsync.compression import compress_stream, decompress_stream, compress_file, maybe_compress, gzip_compress, gzip_decompress

HELLO_WORLD = b'\x1f\x8b\x08\x00\x9b\xff\x74\x54\x00\x03\xcb\x48\xcd\xc9\xc9\x57\x28\xcf\x2f\xca\x49\x01\x00\x85\x11\x4a\x0d\x0b\x00\x00\x00'

GZIP_MAGIC = b'\x1f\x8b'


class TestCompression(unittest.TestCase):
    test_file = os.path.join(os.path.dirname(__file__), 'test_data')

    def test_compress_stream(self):
        src = BytesIO(b"hello world")
        dst = BytesIO()
        compress_stream(src, dst)

        compressed_data = dst.getvalue()
        # test gzip magic
        self.assertEqual(compressed_data[:2], GZIP_MAGIC)
        self.assertEquals(gzip_decompress(compressed_data), b'hello world')

    def test_decompress_stream(self):
        src = BytesIO(HELLO_WORLD)
        dst = BytesIO()

        decompress_stream(src, dst)

        self.assertEqual(dst.getvalue(), b"hello world")

    def test_compress_file(self):
        # Check that small files are compressed in memory
        size, fobj = compress_file(__file__, in_memsize=1024 ** 3)
        with self.assertRaises(UnsupportedOperation):
            fobj.fileno()
        self.assertEqual(fobj.read(2), GZIP_MAGIC)

        # And that large files are compresed using a temporary file
        size, fobj = compress_file(__file__, in_memsize=0)
        self.assertTrue(fobj.fileno())
        self.assertEqual(fobj.read(2), GZIP_MAGIC)

    def test_maybe_compress(self):
        # Check that small files aren't compressed at all
        fobj, was_compressed = maybe_compress(self.test_file, compress_minsize=1024)
        self.assertFalse(was_compressed)

        # Check that small files aren't compressed if the compressed result is
        # larger than the original
        fobj, was_compressed = maybe_compress(self.test_file, compress_minsize=0)
        self.assertFalse(was_compressed)

        # And that larger files are compressed
        fobj, was_compressed = maybe_compress(__file__, compress_minsize=0)
        self.assertTrue(was_compressed)
        self.assertEqual(fobj.read(2), GZIP_MAGIC)

    def test_gzip_compress(self):
        compressed_data = gzip_compress(b'hello world')

        # test gzip magic
        self.assertEqual(compressed_data[:2], GZIP_MAGIC)

        self.assertEquals(gzip_decompress(compressed_data), b'hello world')

    def test_gzip_decompress(self):
        data = gzip_decompress(HELLO_WORLD)
        self.assertEquals(data, b'hello world')


if __name__ == '__main__':
    unittest.main()
