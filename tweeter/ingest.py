import attr
from datetime import datetime
from email.utils import parsedate
import json
from sqlalchemy import orm
import typing

from . import model
from . import zstd

log = __import__('logging').getLogger(__name__)

def parse_datetime(value):
    return datetime(*(parsedate(value)[:6]))

def tweet_from_object(obj, *, updated_at=None):
    created_at = parse_datetime(obj['created_at'])
    if updated_at is None:
        updated_at = created_at
    tw = model.Tweet(
        id=obj['id'],
        created_at=created_at,
        updated_at=updated_at,
        text=obj['text'],
        source=obj['source'],
        lang=obj['lang'],
        user_id=obj['user']['id'],
        user_description=obj['user']['description'],
        user_verified=obj['user']['verified'],
        user_followers_count=obj['user']['followers_count'],
        user_friends_count=obj['user']['friends_count'],
        user_listed_count=obj['user']['listed_count'],
        user_statuses_count=obj['user']['statuses_count'],
        user_favorites_count=obj['user']['favourites_count'],
        user_created_at=parse_datetime(obj['user']['created_at']),

        in_reply_to_tweet_id=obj['in_reply_to_status_id'],
        in_reply_to_user_id=obj['in_reply_to_user_id'],

        favorite_count=obj.get('favorite_count'),
        quote_count=obj.get('quote_count'),
        reply_count=obj.get('reply_count'),
        retweet_count=obj.get('retweet_count'),
    )
    for url in obj.get('entities', {}).get('urls', []):
        tw.text = tw.text.replace(url['url'], url['expanded_url'])
    for media in obj.get('entities', {}).get('media', []):
        tw.text = tw.text.replace(media['url'], media['media_url'])
    for url in obj.get('extended_entities', {}).get('urls', []):
        tw.text = tw.text.replace(url['url'], url['expanded_url'])
    for media in obj.get('extended_entities', {}).get('media', []):
        tw.text = tw.text.replace(media['url'], media['media_url'])
    extended_tweet = obj.get('extended_tweet')
    if extended_tweet:
        tw.text = extended_tweet.get('full_text') or tw.text
        for url in extended_tweet.get('entities', {}).get('urls', []):
            tw.text = tw.text.replace(url['url'], url['expanded_url'])
        for media in extended_tweet.get('entities', {}).get('media', []):
            tw.text = tw.text.replace(media['url'], media['media_url'])
        for url in extended_tweet.get('extended_entities', {}).get('urls', []):
            tw.text = tw.text.replace(url['url'], url['expanded_url'])
        for media in extended_tweet.get('extended_entities', {}).get('media', []):
            tw.text = tw.text.replace(media['url'], media['media_url'])
    quote_obj = obj.get('quoted_status')
    if quote_obj:
        tw.quoted_tweet_id = quote_obj['id']
    rt_obj = obj.get('retweeted_status')
    if rt_obj:
        tw.rt_tweet_id = rt_obj['id']
    return tw

def user_from_object(obj):
    u = model.User()
    u.id = obj['id']
    u.nick = obj['screen_name']
    return u

@attr.s(slots=True, auto_attribs=True)
class Context:
    db: typing.Any
    tweets_by_id: set = attr.Factory(dict)
    user_ids: set = attr.Factory(set)
    new_tweet_count: int = 0
    new_user_count: int = 0

def prepare_ctx(ctx):
    ctx.tweets_by_id = {
        tw.id: tw
        for tw in (
            ctx.db.query(model.Tweet)
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
    log.debug(f'loaded {len(ctx.tweets_by_id)} tweet ids')
    ctx.user_ids = {id for id, in ctx.db.query(model.User.id)}
    log.debug(f'loaded {len(ctx.user_ids)} user ids')

def add_tweet(ctx, tw):
    prev_tw = ctx.tweets_by_id.get(tw.id)
    if prev_tw is None:
        ctx.tweets_by_id[tw.id] = tw
        ctx.db.add(tw)
        ctx.new_tweet_count += 1

    elif tw.updated_at > prev_tw.updated_at:
        prev_tw.updated_at = tw.updated_at
        prev_tw.favorite_count = tw.favorite_count
        prev_tw.quote_count = tw.quote_count
        prev_tw.reply_count = tw.reply_count
        prev_tw.retweet_count = tw.retweet_count
        tw = prev_tw
    return tw

def add_user(ctx, u):
    if u.id in ctx.user_ids:
        return
    ctx.user_ids.add(u.id)
    ctx.db.add(u)
    ctx.new_user_count += 1

def add_tweets_from_status(ctx, msg, updated_at=None):
    tw = tweet_from_object(msg, updated_at=updated_at)
    add_tweet(ctx, tw)

    u = user_from_object(msg['user'])
    add_user(ctx, u)

    # forward the updated_at value from the original tweet into recursive
    # additions such that the quote/retweet objects attached to this tweet
    # are updated with the new tweet's time since the quote/retweet objects
    # should be an updated version (so statistics reflect now versus their
    # created_at time)
    if tw.quoted_tweet_id:
        add_tweets_from_status(ctx, msg['quoted_status'], tw.updated_at)

    if tw.rt_tweet_id:
        add_tweets_from_status(ctx, msg['retweeted_status'], tw.updated_at)

def main(cli, args):
    db = cli.connect_db(args.db)

    ctx = Context(db=db)
    prepare_ctx(ctx)

    total_messages = 0
    for path in args.input_files:
        log.debug(f'reading file={path}')
        with cli.input_file(path, text=False) as fp:
            for line in zstd.iter_lines(fp):
                total_messages += 1
                try:
                    msg = json.loads(line)
                    add_tweets_from_status(ctx, msg)
                except Exception as ex:
                    breakpoint()
                    log.error(f'failed parsing line={line}, error={ex}')
                    continue

    log.debug(f'processed {total_messages} messages')
    log.info(f'added {ctx.new_tweet_count} tweets')
    log.info(f'added {ctx.new_user_count} users')
