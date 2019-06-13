import attr
from datetime import datetime, timedelta
import logging
import signal
import tweepy
from twilio.rest import Client as TwilioClient
import typing
import yaml

from . import zstd

log = logging.getLogger(__name__)

@attr.s(frozen=True, slots=True, auto_attribs=True)
class Stream:
    path: str
    fp: typing.BinaryIO
    writer: typing.Any

    def write(self, bytes):
        return self.writer.write(bytes)

    def close(self):
        self.writer.close()
        self.fp.close()

class FileOutputStreamListener(tweepy.StreamListener):
    stream = None
    closed = False
    last_report_at = None
    num_records_since_report = 0
    report_every = timedelta(seconds=1)

    def __init__(self, path_prefix):
        super().__init__()
        self.path_prefix = path_prefix

    def on_connect(self):
        """
        Called when the streaming connection is established.

        """
        log.info('connected')
        signal.signal(signal.SIGHUP, self.on_sighup)
        self.last_report_at = datetime.utcnow()
        self.num_records_since_report = 0

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
        exc_info = (type(e), e, e.__traceback__)
        log.exception('received exception while streaming', exc_info=exc_info)
        self.cleanup()

    def keep_alive(self):
        """
        The streaming periodically contains empty lines to keep the
        connection open.

        """
        log.info('received keep-alive')

    def on_data(self, data):
        if self.closed:
            log.info('dropping message, listener closed')
            return False

        now = datetime.utcnow()
        self.num_records_since_report += 1

        if self.stream is None:
            now = datetime.utcnow()
            path = f'{self.path_prefix}.{now:%Y%m%d.%H%M%S}.zstd'
            log.info(f'opening path={path}')
            fp = open(path, mode='ab')
            writer = zstd.writer(fp)
            self.stream = Stream(
                path=path,
                fp=fp,
                writer=writer,
            )
        self.stream.write(data.strip().encode('utf8') + b'\n')

        if now - self.last_report_at >= self.report_every:
            self.report(now=now)

    def on_sighup(self, *args):
        log.info(f'received SIGHUP, rotating')
        self.cleanup()

    def report(self, now=None):
        if now is None:
            now = datetime.utcnow()
        dt = now - self.last_report_at

        log.info(
            f'received {self.num_records_since_report} records since '
            f'{dt.total_seconds():.2f} seconds ago'
        )
        self.last_report_at = now
        self.num_records_since_report = 0

    def cleanup(self):
        if self.stream is not None:
            log.info(f'closing path={self.stream.path}')
            self.stream.close()
            self.stream = None

    def close(self):
        self.closed = True
        self.cleanup()

def main(cli, args):
    profile = cli.profile

    with open(args.filter_file, 'r', encoding='utf8') as fp:
        filters = yaml.safe_load(fp)

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
        listener = FileOutputStreamListener(args.output_path_prefix)
        stream = tweepy.Stream(auth, listener)

        stopping = False
        def on_sigterm(*args):
            nonlocal stopping
            log.info('received SIGTERM, stopping')
            stopping = True
            listener.close()
            stream.disconnect()
        try:
            signal.signal(signal.SIGTERM, on_sigterm)
            stream.filter(**filters, stall_warnings=True)
        except Exception as ex:
            log.info('restarting after receiving exception')
            try:
                twilio.messages.create(
                    body=(
                        f'Received twitter-listen exception '
                        f'type={type(ex).__qualname__} args={ex}'
                    ),
                    from_=profile['twilio']['source_phone_number'],
                    to=profile['twilio']['target_phone_number'],
                )
            except Exception:
                log.exception('squashing error sending sms')
        except KeyboardInterrupt:
            log.info('received SIGINT, stopping')
            break
        else:
            if stopping:
                break
            log.info('restarting')
        finally:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            listener.close()
