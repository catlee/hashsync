#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_compression
----------------------------------

Tests for `hashsync.compression` module.
"""

import unittest

from io import BytesIO

from hashsync.compression import compress_stream, decompress_stream


class TestCompression(unittest.TestCase):
    def test_compress(self):
        src = BytesIO(b"hello world")
        dst = BytesIO()
        compress_stream(src, dst)

        compressed_data = dst.getvalue()
        self.assertEqual(compressed_data[:2], b'\x1f\x8b')

        new = BytesIO()

        dst.seek(0)
        decompress_stream(dst, new)

        self.assertEqual(new.getvalue(), b"hello world")


if __name__ == '__main__':
    unittest.main()
