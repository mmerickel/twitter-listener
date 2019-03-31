import argparse
import gzip
import json
import logging
import sys

log = logging.getLogger(__name__)

def parse_tweet_stream(path):
    with gzip.open(path, 'rt', encoding='utf8') as fp:
        for line in fp:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except Exception as ex:
                    log.exception(ex)

def save_json(tweets, path):
    with open(path, 'w') as fp:
        json.dump(list(tweets), fp, sort_keys=True, indent=2)

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

    tweets = parse_tweet_stream(args.input_file)
    save_json(tweets, args.output_file)

if __name__ == '__main__':
    sys.exit(main() or 0)
