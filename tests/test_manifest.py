#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_manifest
----------------------------------

Tests for `hashsync.manifest` module.
"""

import unittest

from io import BytesIO

from hashsync.manifest import Manifest


class TestManifest(unittest.TestCase):
    def test_add(self):
        m = Manifest()
        m.add('hashhashhash', u'dirname/filename', 0o644)

        self.assertEqual(m.files, [('hashhashhash', 'dirname/filename', 0o644)])

    def test_load(self):
        manifest_data = BytesIO(b'''
[
    ["hash1", "dirname/foo", 420],
    ["hash2", "dirname/bar", 493]
]
''')
        m = Manifest()
        m.load(manifest_data)
        self.assertEqual(m.files, [
            ('hash1', u'dirname/foo', 0o644),
            ('hash2', u'dirname/bar', 0o755),
        ])

    def test_save(self):
        m = Manifest()
        m.add('hash1', u'dirname/\N{SNOWMAN}', 0o644)

        dst = BytesIO()

        m.save(dst)

        data = dst.getvalue()
        # Strip out whitespace
        data = data.replace(b'\n', b'')
        data = data.replace(b' ', b'')

        self.assertEqual(data, b"""\
[["hash1","dirname/\\u2603",420]]""")

    def test_spaces(self):
        m = Manifest()
        m.add('hash1', u'dirname/file with space.txt', 0o755)

        dst = BytesIO()
        m.save(dst)
        dst.seek(0)

        m = Manifest()
        m.load(dst)
        self.assertEqual(m.files, [
            ('hash1', u'dirname/file with space.txt', 0o755),
        ])

    def test_unicode(self):
        m = Manifest()
        m.add('hash1', u'dirname/\N{SNOWMAN}.txt', 0o755)

        dst = BytesIO()
        m.save(dst)
        dst.seek(0)

        m = Manifest()
        m.load(dst)
        self.assertEqual(m.files, [
            ('hash1', u'dirname/☃.txt', 0o755),
        ])
