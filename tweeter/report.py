import csv
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import sqlalchemy as sa

from . import model

def main_csv(cli, args):
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
            'lang',
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
                tweet.updated_at.isoformat(),
                tweet.favorite_count,
                tweet.retweet_count,
                tweet.reply_count,
                tweet.quote_count,
                tweet.lang,
                tweet.text,
            ])

def main_plot(cli, args):
    db = cli.connect_db(args.db)

    date_col = sa.func.date(model.Tweet.created_at).label('date')
    tweets = (
        db.query(
            date_col,
            sa.func.count().label('count'),
        )
        .filter(model.Tweet.created_at.between(
            datetime(2019, 3, 20),
            datetime(2019, 5, 10),
        ))
        .filter(model.Tweet.in_reply_to_tweet_id.is_(None))
        .group_by(date_col)
        .order_by(date_col.asc())
        .all()
    )
    dates = [t.date for t in tweets]
    counts = [t.count for t in tweets]

    fig, ax = plt.subplots()
    x = np.arange(len(dates))
    plt.bar(x, counts)
    plt.xticks(x, dates)
    with cli.output_file(args.output_file, text=False) as fp:
        plt.savefig(fp, format='png')
