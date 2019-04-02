import io
import zstandard as zstd

log = __import__('logging').getLogger(__name__)

def readlines(fp):
    dctx = zstd.ZstdDecompressor()
    stream_reader = dctx.stream_reader(fp)
    stream = io.TextIOWrapper(stream_reader, encoding='utf8')
    for line in stream:
        line = line.strip()
        if line:
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
