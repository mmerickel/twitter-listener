import argparse
import json
import logging
import sys
import tweepy
import yaml

log = logging.getLogger(__name__)

def parse_args(argv):
    parser = argparse.ArgumentParser(prog=argv[0])
    parser.add_argument('--profile', default='profile.yml')
    parser.add_argument('query')
    return parser.parse_args(argv[1:])

def main(argv=sys.argv):
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)-15s %(levelname)-8s [%(name)s] %(message)s',
    )

    with open(args.profile, 'r', encoding='utf8') as fp:
        profile = yaml.safe_load(fp)

    auth = tweepy.OAuthHandler(
        profile['consumer_key'],
        profile['consumer_secret'],
    )
    auth.set_access_token(
        profile['access_token'],
        profile['access_token_secret'],
    )

    api = tweepy.API(auth, wait_on_rate_limit=True)
    last_since_id = 0
    while True:
        results = api.search(args.query, since_id=last_since_id)
        for result in results:
            print(json.dumps(result._json) + '\r\n')
        if results[-1].id > last_since_id:
            last_since_id = results[-1].id
        else:
            break

if __name__ == '__main__':
    sys.exit(main() or 0)
