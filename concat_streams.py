import argparse
import logging
import sys
import zstandard as zstd

log = logging.getLogger(__name__)

def parse_args(argv):
    parser = argparse.ArgumentParser(prog=argv[0])
    parser.add_argument('input_files', nargs='+')
    return parser.parse_args(argv[1:])

def main(argv=sys.argv):
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)-15s %(levelname)-8s [%(name)s] %(message)s',
    )

    cctx = zstd.ZstdCompressor(level=10)
    dctx = zstd.ZstdDecompressor()
    out_fp = sys.stdout.buffer
    for path in args.input_files:
        with open(path, 'rb') as fp:
            with dctx.stream_reader(fp) as reader:
                cctx.copy_stream(reader, out_fp)

if __name__ == '__main__':
    sys.exit(main() or 0)
