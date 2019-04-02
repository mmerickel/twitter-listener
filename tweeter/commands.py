from subparse import command

@command('.listener')
def listener(parser):
    parser.add_argument('filter_file')
    parser.add_argument('output_path_prefix')
