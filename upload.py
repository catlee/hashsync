#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hashsync.connection import connect
from hashsync.transfer import upload_directory

import logging
log = logging.getLogger(__name__)


def main():
    import argparse
    import sys
    import gzip

    parser = argparse.ArgumentParser()
    # TODO: These aren't required if no-upload is set
    parser.add_argument("-r", "--region", dest="region", required=True)
    parser.add_argument("-b", "--bucket", dest="bucket_name", required=True)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN, default=logging.INFO)
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)
    parser.add_argument("-j", "--jobs", dest="jobs", type=int, help="how many simultaneous uploads to do", default=8)
    parser.add_argument("-o", "--output", dest="output", help="where to output manifet, use '-' for stdout", default="manifest.gz")
    parser.add_argument("-z", "--compress-manifest", dest="compress_manifest",
                        help="compress manifest output (default if outputting to a file)",
                        action="store_true", default=None)
    parser.add_argument("--no-compress-manifest", dest="compress_manifest",
                        help="don't compress manifest output (default if outputting to stdout)",
                        action="store_false")
    parser.add_argument("--no-upload", dest="dryrun", action="store_true", default=False)
    parser.add_argument("--report-dupes", dest="report_dupes", action="store_true", default=False, help="report on duplicate files")
    parser.add_argument("dirname", help="directory to upload")

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s - %(message)s")
    # Make boto shut up
    # TODO: Add -v -v support to set this to DEBUG?
    logging.getLogger('boto').setLevel(logging.INFO)

    if not args.dryrun:
        connect(args.region, args.bucket_name)

    manifest = upload_directory(args.dirname, args.jobs, dryrun=args.dryrun)

    if args.output == '-':
        output_file = sys.stdout
    else:
        output_file = open(args.output, 'wb')
        # Enable compression by default if we're writing out to a file
        if args.compress_manifest is None:
            args.compress_manifest = True

    if args.compress_manifest:
        output_file = gzip.GzipFile(fileobj=output_file, mode='wb')

    manifest.save(output_file)

    if args.report_dupes:
        manifest.report_dupes()

if __name__ == '__main__':
    main()
