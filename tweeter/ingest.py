import attr
from datetime import datetime
from email.utils import parsedate
import json

from . import model
from . import zstd

log = __import__('logging').getLogger(__name__)

def parse_datetime(value):
    return datetime(*(parsedate(value)[:6]))

def tweet_from_object(obj):
    tw = model.Tweet(
        id=obj['id'],
        created_at=parse_datetime(obj['created_at']),
        text=obj['text'],
        source=obj['source'],
        lang=obj['lang'],
        user_id=obj['user']['id'],
        in_reply_to_tweet_id=obj['in_reply_to_status_id'],
        in_reply_to_user_id=obj['in_reply_to_user_id'],
    )
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

@attr.s
class Context:
    db = attr.ib()
    tweet_ids = attr.ib(factory=set)
    user_ids = attr.ib(factory=set)
    new_tweet_count = attr.ib(default=0)
    new_user_count = attr.ib(default=0)

def prepare_ctx(ctx):
    ctx.tweet_ids = {id for id, in ctx.db.query(model.Tweet.id)}
    ctx.user_ids = {id for id, in ctx.db.query(model.User.id)}
    log.debug(f'loaded {len(ctx.tweet_ids)} tweet ids')
    log.debug(f'loaded {len(ctx.user_ids)} user ids')

def maybe_add_tweet(ctx, tw):
    if tw.id in ctx.tweet_ids:
        return
    ctx.tweet_ids.add(tw.id)
    ctx.db.add(tw)
    ctx.new_tweet_count += 1

def maybe_add_user(ctx, u):
    if u.id in ctx.user_ids:
        return
    ctx.user_ids.add(u.id)
    ctx.db.add(u)
    ctx.new_user_count += 1

def add_tweets(ctx, msg):
    tw = tweet_from_object(msg)
    maybe_add_tweet(ctx, tw)

    u = user_from_object(msg['user'])
    maybe_add_user(ctx, u)

    if tw.quoted_tweet_id:
        add_tweets(ctx, msg['quoted_status'])

    if tw.rt_tweet_id:
        add_tweets(ctx, msg['retweeted_status'])

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
                    add_tweets(ctx, msg)
                except Exception as ex:
                    log.error(f'failed parsing line={line}, error={ex}')
                    continue

    log.debug(f'processed {total_messages} messages')
    log.info(f'added {ctx.new_tweet_count} tweets')
    log.info(f'added {ctx.new_user_count} users')
