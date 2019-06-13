from subparse import command

from .settings import asduration

def generic_options(parser):
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--profile', default='profile.yml')

@command('.stream', 'api:stream')
def stream(parser):
    """
    Listen to the twitter firehouse.

    Tweets are streamed out to zstd-compressed files, one tweet per line.

    A new file is created when the stream is interrupted due to an issue or
    when a SIGHUP is received locally. The resulting files can then be
    concatenated together and/or ingested into the database for querying.

    """
    parser.add_argument('filter_file')
    parser.add_argument('output_path_prefix')

@command('.updater', 'api:update')
def updater(parser):
    """
    Refresh statistics on stale tweets.

    Query the database for tweets that have not been refreshed lately.
    The updated tweets are output to zstd-compressed files, one tweet per line.

    """
    parser.add_argument('--db', required=True)
    parser.add_argument('--min-age', type=asduration, default='7d')
    parser.add_argument('--min-id', type=int)

@command('.search', 'api:search')
def search(parser):
    """
    Query the twitter api by search term.

    """
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('query')

@command('.ingest', 'db:ingest')
def ingest(parser):
    """
    Pull in tweets from zstd-compressed files into a database.

    """
    parser.add_argument('--db', required=True)
    parser.add_argument('input_files', nargs='+')

@command('.zstd:main_concat', 'zstd:concat')
def zstd_concat(parser):
    """
    Concatenate zstd files together into a single zstd file.

    """
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('--level', type=int, default=10)
    parser.add_argument('input_files', nargs='+')

@command('.zstd:main_compress', 'zstd:compress')
def zstd_compress(parser):
    """
    Compress a file using zstd.

    """
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('--level', type=int, default=10)
    parser.add_argument('input_file')

@command('.zstd:main_decompress', 'zstd:decompress')
def zstd_decompress(parser):
    """
    Decompress a zstd file.

    """
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('input_file')

@command('.zstd:main_from_gz', 'zstd:from-gz')
def zstd_from_gz(parser):
    """
    Convert a gzip file to zstd format.

    """
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('--level', type=int, default=10)
    parser.add_argument('input_file')
