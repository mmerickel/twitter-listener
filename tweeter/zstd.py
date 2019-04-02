from contextlib import ExitStack
import gzip
import io
import zstandard as zstd

log = __import__('logging').getLogger(__name__)

def readlines(fp, *, filter_empty_lines=True):
    dctx = zstd.ZstdDecompressor()
    stream_reader = dctx.stream_reader(fp)
    stream = io.TextIOWrapper(stream_reader, encoding='utf8')
    for line in stream:
        if filter_empty_lines:
            line = line.strip()
            if line:
                yield line
        else:
            yield line

def writer(fp, *, level=10):
    cctx = zstd.ZstdCompressor(level=level)
    compressor = cctx.stream_writer(fp)
    return compressor

def concat_streams(out_fp, streams, *, level=10):
    cctx = zstd.ZstdCompressor(level=level)
    dctx = zstd.ZstdDecompressor()
    bytes_out = 0
    for stream in streams:
        with dctx.stream_reader(stream) as reader:
            _, write_bytes = cctx.copy_stream(reader, out_fp)
            bytes_out += write_bytes
    return bytes_out

def compress_streams(out_fp, streams, *, level=10):
    cctx = zstd.ZstdCompressor(level=level)
    bytes_in, bytes_out = 0, 0
    for stream in streams:
        read_bytes, write_bytes = cctx.copy_stream(stream, out_fp)
        bytes_in += read_bytes
        bytes_out += write_bytes
    return bytes_in, bytes_out

def main_concat(cli, args):
    with cli.output_file(args.output_file, text=False) as out_fp:
        def streams():
            for path in args.input_files:
                log.debug('concatenating stream="{path}"')
                with cli.input_file(path, text=False) as in_fp:
                    yield in_fp
        stream_iter = streams()
        bytes_out = concat_streams(out_fp, stream_iter, level=args.level)
    log.info(f'wrote {bytes_out} bytes')

def main_decompress(cli, args):
    with ExitStack() as stack:
        out_fp = stack.enter_context(cli.output_file(args.output_file, text=False))
        in_fp = stack.enter_context(cli.input_file(args.input_file, text=False))

        dctx = zstd.ZstdDecompressor()
        bytes_in, bytes_out = dctx.copy_stream(in_fp, out_fp)
    log.info(
        f'read {bytes_in} bytes, wrote {bytes_out} bytes, '
        f'ratio={bytes_out/bytes_in}'
    )

def main_from_gz(cli, args):
    with ExitStack() as stack:
        out_fp = stack.enter_context(cli.output_file(args.output_file, text=False))
        in_fp = stack.enter_context(cli.input_file(args.input_file, text=False))
        gz_in_fp = stack.enter_context(gzip.open(in_fp, mode='rb'))

        bytes_in, bytes_out = compress_streams(out_fp, [gz_in_fp])
    log.info(
        f'read {bytes_in} bytes, wrote {bytes_out} bytes, '
        f'ratio={bytes_out/bytes_in}'
    )
