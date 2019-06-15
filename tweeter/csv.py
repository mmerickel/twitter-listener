import csv

from . import model

def main(cli, args):
    db = cli.connect_db(args.db)

    q = (
        db.query(model.Tweet, model.User)
        .join(
            model.User,
            model.User.id == model.Tweet.user_id,
        )
        .order_by(model.Tweet.created_at.asc())
    )

    with cli.output_file(args.output_file, text=True) as fp:
        writer = csv.writer(fp)
        writer.writerow([
            'id',
            'created_at (utc)',
            'user_id',
            'user_nick',
            'in_reply_to_tweet_id',
            'in_reply_to_user_id',
            'quoted_tweet_id',
            'retweet_tweet_id',
            'counted_at (utc)',
            'favorite_count',
            'retweet_count',
            'reply_count',
            'quote_count',
            'text',
        ])
        for tweet, user in q:
            writer.writerow([
                tweet.id,
                tweet.created_at.isoformat(),
                tweet.user_id,
                user.nick,
                tweet.in_reply_to_tweet_id,
                tweet.in_reply_to_user_id,
                tweet.quoted_tweet_id,
                tweet.rt_tweet_id,
                tweet.updated_at,
                tweet.favorite_count,
                tweet.retweet_count,
                tweet.reply_count,
                tweet.quote_count,
                tweet.text,
            ])
