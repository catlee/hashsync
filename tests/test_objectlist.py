#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_objectlist
----------------------------------

Tests for `hashsync.objectlist` module.
"""

import unittest

import boto
import moto

from hashsync.objectlist import ObjectList
from hashsync.compression import gzip_decompress, gzip_compress


class TestObjectList(unittest.TestCase):
    def test_add(self):
        o = ObjectList(None)
        o.add("hash1")
        o.add("hash2")

        self.assertIn("hash1", o)
        self.assertIn("hash2", o)
        self.assertNotIn("hash3", o)

    @moto.mock_s3
    def test_save(self):
        conn = boto.connect_s3()
        bucket = conn.create_bucket('test-bucket')

        o = ObjectList(bucket)
        o.add("hash1")
        o.save()

        key = bucket.get_key(o.keyname)
        data = key.get_contents_as_string()
        data = gzip_decompress(data)

        self.assertEqual(data, b'hash1')

    @moto.mock_s3
    def test_load(self):
        conn = boto.connect_s3()
        bucket = conn.create_bucket('test-bucket')
        key = bucket.new_key('objectlist')
        key.set_contents_from_string(b'hash1\nhash2\n')

        o = ObjectList(bucket)
        o.load()

        self.assertIn('hash1', o)
        self.assertIn('hash2', o)
        self.assertNotIn("hash3", o)

    @moto.mock_s3
    def test_load_compressed(self):
        conn = boto.connect_s3()
        bucket = conn.create_bucket('test-bucket')
        key = bucket.new_key('objectlist')
        key.set_metadata('Content-Encoding', 'gzip')
        key.set_contents_from_string(gzip_compress(b'hash1\nhash2\n'))

        o = ObjectList(bucket)
        o.load()

        self.assertIn('hash1', o)
        self.assertIn('hash2', o)
        self.assertNotIn("hash3", o)
