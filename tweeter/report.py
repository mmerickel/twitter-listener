from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import os.path
import pandas as pd
import sqlalchemy as sa

from . import model

def main(cli, args):
    db = cli.connect_db(args.db)

    format = args.format
    if not format:
        _, ext = os.path.splitext(args.output_file)
        if ext == '.csv':
            format = 'csv'
        elif ext == '.xlsx':
            format = 'excel'
        else:
            cli.abort('could not guess file format from extension')

    if format == 'csv':
        format_is_text = True
    elif format == 'excel':
        format_is_text = False
    else:
        cli.abort('unrecognized file format')

    Tweet = model.Tweet
    User = model.User

    as_txt = lambda col: sa.cast(col, sa.Text).label(col.name)
    q = (
        db.query(
            as_txt(Tweet.id),
            Tweet.created_at,
            as_txt(Tweet.user_id),
            User.nick,
            Tweet.user_description,
            Tweet.user_verified,
            Tweet.user_followers_count,
            Tweet.user_friends_count,
            Tweet.user_listed_count,
            Tweet.user_statuses_count,
            Tweet.user_favorites_count,
            Tweet.user_created_at,
            as_txt(Tweet.in_reply_to_tweet_id),
            as_txt(Tweet.in_reply_to_user_id),
            as_txt(Tweet.quoted_tweet_id),
            as_txt(Tweet.rt_tweet_id),
            Tweet.updated_at,
            Tweet.favorite_count,
            Tweet.retweet_count,
            Tweet.reply_count,
            Tweet.quote_count,
            Tweet.lang,
            Tweet.text,
        )
        .join(
            model.User,
            model.User.id == model.Tweet.user_id,
        )
        .order_by(model.Tweet.created_at.asc())
    )

    df = model.query_to_pandas(q)

    with cli.output_file(args.output_file, text=format_is_text) as fp:
        if format == 'csv':
            df.to_csv(fp, index=False)
        elif format == 'excel':
            with pd.ExcelWriter(
                fp,
                engine='xlsxwriter',
                options=dict(
                    strings_to_numbers=False,
                ),
            ) as writer:
                df.to_excel(writer, index=False)

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
        # .filter(model.Tweet.in_reply_to_tweet_id.is_(None))
        .filter(model.Tweet.rt_tweet_id.is_(None))
        .filter(sa.func.lower(model.Tweet.text).like('%#saam%'))
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
