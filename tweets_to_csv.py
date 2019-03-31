import argparse
import csv
import io
import json
import logging
import sys
import zstandard as zstd

log = logging.getLogger(__name__)

def parse_tweet_stream(path):
    with open(path, 'rb') as fp:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(fp)
        stream = io.TextIOWrapper(stream_reader, encoding='utf8')
        for line in stream:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except Exception as ex:
                    log.exception(ex)

def save_tweet_csv(tweets, path):
    with open(path, 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow([
            'id',
            'created_at',
            'text',
            'user_name',
            'user_screen_name',
            'friends_count',
            'followers_count',
        ])
        for tweet in tweets:
            writer.writerow([
                tweet.get('id'),
                tweet.get('created_at'),
                tweet.get('text'),
                tweet.get('user', {}).get('name'),
                tweet.get('user', {}).get('screen_name'),
                tweet.get('user', {}).get('friends_count'),
                tweet.get('user', {}).get('followers_count'),
            ])

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
    save_tweet_csv(tweets, args.output_file)

if __name__ == '__main__':
    sys.exit(main() or 0)
