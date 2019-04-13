import json
import logging
import tweepy

log = logging.getLogger(__name__)

def main(cli, args):
    profile = cli.profile

    auth = tweepy.OAuthHandler(
        profile['twitter']['consumer_key'],
        profile['twitter']['consumer_secret'],
    )
    auth.set_access_token(
        profile['twitter']['access_token'],
        profile['twitter']['access_token_secret'],
    )

    api = tweepy.API(auth, wait_on_rate_limit=True)
    last_since_id = 0
    with cli.output_file(args.output_file) as fp:
        while True:
            results = api.search(args.query, since_id=last_since_id)
            for result in results:
                fp.write(json.dumps(result._json))
            if results[-1].id > last_since_id:
                last_since_id = results[-1].id
            else:
                break
