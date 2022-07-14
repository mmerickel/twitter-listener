import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import orm
from sqlalchemy.schema import (
    Column,
)
from sqlalchemy.types import (
    DateTime,
    BigInteger,
    Boolean,
    Text,
)

log = __import__('logging').getLogger(__name__)

Base = declarative_base()
metadata = Base.metadata

class Tweet(Base):
    __tablename__ = 'tweet'

    id = Column(BigInteger(), primary_key=True)
    created_at = Column(DateTime(), nullable=False, index=True)
    text = Column(Text(), nullable=False)
    source = Column(Text(), nullable=False)
    lang = Column(Text())

    user_id = Column(BigInteger(), nullable=False, index=True)
    user_description = Column(Text())
    user_verified = Column(Boolean)
    user_followers_count = Column(BigInteger())
    user_friends_count = Column(BigInteger())
    user_listed_count = Column(BigInteger())
    user_statuses_count = Column(BigInteger())
    user_favorites_count = Column(BigInteger())
    user_created_at = Column(DateTime(), nullable=False)

    in_reply_to_tweet_id = Column(BigInteger(), index=True)
    in_reply_to_user_id = Column(BigInteger(), index=True)

    quoted_tweet_id = Column(BigInteger(), index=True)
    rt_tweet_id = Column(BigInteger(), index=True)

    updated_at = Column(DateTime(), nullable=False, index=True)
    quote_count = Column(BigInteger())
    reply_count = Column(BigInteger())
    retweet_count = Column(BigInteger())
    favorite_count = Column(BigInteger())

class User(Base):
    __tablename__ = 'user'

    id = Column(BigInteger(), primary_key=True)
    nick = Column(Text(), nullable=False)

def connect(path, *, migrate=True, pool=False):
    log.debug('opening database at path=%s', path)
    engine = sa.create_engine('sqlite:///' + path)
    engine.execute('PRAGMA foreign_keys=ON')
    if migrate:
        log.debug('running database migrations')
        metadata.create_all(bind=engine)
        log.debug('done running migrations')
    dbmaker = orm.sessionmaker(bind=engine)
    if pool:
        return dbmaker
    return dbmaker()

def close(db, *, rollback=False):
    if rollback:
        log.warn('rolling back database changes')
        db.rollback()
    else:
        db.commit()
    db.close()
    log.debug('closed database connection')

def query_to_pandas(q):
    import pandas as pd

    engine = q.session.bind
    return pd.read_sql(q.statement.compile(dialect=engine.dialect), engine)
