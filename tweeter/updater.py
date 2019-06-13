from datetime import datetime
import logging
from sqlalchemy import orm
import tweepy

from . import model

log = logging.getLogger(__name__)

def grouper(items, chunk_size):
    start = 0
    while True:
        end = start + chunk_size
        chunk = items[start:end]
        if not chunk:
            break
        yield chunk
        start = end

def main(cli, args):
    profile = cli.profile
    dbmaker = model.connect(args.db, pool=True)

    auth = tweepy.OAuthHandler(
        profile['twitter']['consumer_key'],
        profile['twitter']['consumer_secret'],
    )
    auth.set_access_token(
        profile['twitter']['access_token'],
        profile['twitter']['access_token_secret'],
    )

    max_ts = datetime.utcnow() - args.min_age

    db = dbmaker()
    stale_id_q = (
        db.query(model.Tweet.id)
        .filter(model.Tweet.updated_at < max_ts)
        .order_by(model.Tweet.id.asc())
    )

    if args.min_id is not None:
        stale_id_q = stale_id_q.filter(model.Tweet.id >= args.min_id)

    stale_ids = [id for id, in stale_id_q]
    db.close()
    log.info(f'found {len(stale_ids)} stale tweets')

    api = tweepy.API(auth, wait_on_rate_limit=True)
    for chunk in grouper(stale_ids, 100):
        log.info(f'starting from id={chunk[0]}')
        now = datetime.utcnow()
        statuses = api.statuses_lookup(chunk)
        db = dbmaker()
        try:
            tweets_by_id = {
                tw.id: tw
                for tw in (
                    db.query(model.Tweet)
                    .filter(model.Tweet.id.in_(s.id for s in statuses))
                    .options(orm.load_only(
                        'id',
                        'updated_at',
                        'favorite_count',
                        'quote_count',
                        'reply_count',
                        'retweet_count',
                    ))
                )
            }
            for s in statuses:
                raw = s._json
                tw = tweets_by_id[s.id]
                tw.updated_at = now
                tw.favorite_count = raw.get('favorite_count')
                tw.quote_count = raw.get('quote_count')
                tw.reply_count = raw.get('reply_count')
                tw.retweet_count = raw.get('retweet_count')
            log.debug(f'updated {len(statuses)} tweets')
            db.commit()
        finally:
            db.close()
