import argparse
from contextlib import ExitStack
import gzip
import logging
import os.path
import sys
import zstandard as zstd

log = logging.getLogger(__name__)

def parse_args(argv):
    parser = argparse.ArgumentParser(prog=argv[0])
    parser.add_argument('input_file')
    parser.add_argument('output_file')
    return parser.parse_args(argv[1:])

def main(argv=sys.argv):
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)-15s %(levelname)-8s [%(name)s] %(message)s',
    )

    cctx = zstd.ZstdCompressor(level=10)
    with ExitStack() as stack:
        in_size = os.path.getsize(args.input_file)
        in_fp = stack.enter_context(gzip.open(args.input_file, 'rb'))
        out_fp = stack.enter_context(open(args.output_file, 'wb'))
        _, write_size = cctx.copy_stream(in_fp, out_fp)
        log.info(
            f'read {in_size} bytes, wrote {write_size} bytes, '
            f'ratio={write_size / in_size}'
        )

if __name__ == '__main__':
    sys.exit(main() or 0)
