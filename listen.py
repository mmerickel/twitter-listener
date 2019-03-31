import argparse
from datetime import datetime
import gzip
import logging
import os
import signal
import sys
import tweepy
from twilio.rest import Client as TwilioClient
import yaml

log = logging.getLogger(__name__)

class FileOutputStreamListener(tweepy.StreamListener):
    fp = None

    def __init__(self, path):
        super().__init__()
        self.path = path

    def on_connect(self):
        """
        Called when the streaming connection is established.

        """
        log.info('connected')
        signal.signal(signal.SIGHUP, self.on_sighup)

    def on_error(self, status_code):
        """
        Called instead of on_connect.

        """
        log.error(f'received error status={status_code}')

    def on_timeout(self):
        """
        Called when the connection times out.

        """
        log.error(f'received timeout')

    def on_exception(self, e):
        """
        Called when the stream is interrupted.

        This is the only way the stream is closed.

        """
        signal.signal(signal.SIGHUP, signal.SIG_DFL)
        log.exception('received exception while streaming')
        self.cleanup()

    def keep_alive(self):
        """
        The streaming periodically contains empty lines to keep the
        connection open.

        """
        log.info('received keep-alive')

    def on_data(self, data):
        if self.fp is None:
            log.info(f'opening path={self.path}')
            self.fp = gzip.open(self.path, 'at', encoding='utf8')
        self.fp.write(data.strip() + '\n')

    def on_sighup(self, *args):
        self.cleanup()

        path, ext = os.path.splitext(self.path)
        now = datetime.utcnow()
        path = f'{path}.{now:%Y%m%d.%H%M%S}{ext}'
        os.rename(self.path, path)
        log.info(f'received SIGHUP, rotated file to path={path}')

    def cleanup(self):
        if self.fp is not None:
            self.fp.close()
            self.fp = None

def parse_args(argv):
    parser = argparse.ArgumentParser(prog=argv[0])
    parser.add_argument('--profile', default='profile.yml')
    parser.add_argument('config_file')
    parser.add_argument('output_path')
    return parser.parse_args(argv[1:])

def main(argv=sys.argv):
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)-15s %(levelname)-8s [%(name)s] %(message)s',
    )

    with open(args.profile, 'r', encoding='utf8') as fp:
        profile = yaml.safe_load(fp)

    with open(args.config_file, 'r', encoding='utf8') as fp:
        config = yaml.safe_load(fp)

    auth = tweepy.OAuthHandler(
        profile['twitter']['consumer_key'],
        profile['twitter']['consumer_secret'],
    )
    auth.set_access_token(
        profile['twitter']['access_token'],
        profile['twitter']['access_token_secret'],
    )

    twilio = TwilioClient(
        profile['twilio']['account_sid'],
        profile['twilio']['auth_token'],
    )

    while True:
        listener = FileOutputStreamListener(args.output_path)
        stream = tweepy.Stream(auth, listener)
        try:
            stream.filter(**config, stall_warnings=True)
        except Exception as ex:
            log.info('restarting after receiving exception')
            try:
                twilio.messages.create(
                    body=(
                        f'Received twitter-listen exception '
                        f'type={type(ex).__name__} '
                        f'message={str(ex)}'
                    ),
                    from_=profile['twilio']['source_phone_number'],
                    to=profile['twilio']['target_phone_number'],
                )
            except Exception:
                log.exception('squashing error sending sms')
        except KeyboardInterrupt:
            log.info('received SIGINT, stopping')
            listener.cleanup()
            break
        else:
            log.info('restarting')

if __name__ == '__main__':
    sys.exit(main() or 0)
