from subparse import command

def generic_options(parser):
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--profile', default='profile.yml')

@command('.listener')
def listener(parser):
    parser.add_argument('filter_file')
    parser.add_argument('output_path_prefix')

@command('.search')
def search(parser):
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('query')

@command('.ingest', 'db:ingest')
def ingest(parser):
    parser.add_argument('--db', required=True)
    parser.add_argument('input_files', nargs='+')

@command('.zstd:main_concat', 'zstd:concat')
def zstd_concat(parser):
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('--level', type=int, default=10)
    parser.add_argument('input_files', nargs='+')

@command('.zstd:main_compress', 'zstd:compress')
def zstd_compress(parser):
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('--level', type=int, default=10)
    parser.add_argument('input_file')

@command('.zstd:main_decompress', 'zstd:decompress')
def zstd_decompress(parser):
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('input_file')

@command('.zstd:main_from_gz', 'zstd:from-gz')
def zstd_from_gz(parser):
    parser.add_argument('-o', '--output-file', default='-')
    parser.add_argument('--level', type=int, default=10)
    parser.add_argument('input_file')
