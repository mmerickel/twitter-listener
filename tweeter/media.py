import csv
import logging
import os
import re
import requests
from sqlalchemy import orm

from . import model

log = logging.getLogger(__name__)


def main_download(cli, args):
    profile = cli.profile
    dbmaker = model.connect(args.db, pool=True)
    db = dbmaker()

    img_re = re.compile(r'(?P<link>https?://(?:\S+\.)?twimg\.com/\S+)', re.IGNORECASE)

    with open('media.csv', 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(['tweet id', 'name', 'url', 'error'])

        for tweet in (
            db.query(model.Tweet)
            .execution_options(stream_results=True, max_row_buffer=1000)
            .filter(model.Tweet.text.ilike('%http%.twimg.com/%'))
        ):
            for url in img_re.findall(tweet.text):
                _, name = url.rsplit('/', 1)
                if os.path.exists(name):
                    log.debug(f'already have file={name}, skipping')
                    continue
                error = try_download_media(tweet, url, name)
                writer.writerow([tweet.id, name, url, error or ''])


def try_download_media(tweet, url, path):
    print(url)
    try:
        r = requests.get(url)
        r.raise_for_status()
    except Exception as ex:
        log.exception(f'failed to download url={url} for tweet={tweet.id}')
        return str(ex)
    with open(path, 'wb') as fp:
        fp.write(r.content)
