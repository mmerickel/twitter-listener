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

class TweetStream(tweepy.Stream):
    stream = None
    last_report_at = None
    num_records_since_report = 0
    report_interval = timedelta(seconds=5)

    def __init__(self, *args, path_prefix, report_interval=None, **kw):
        super().__init__(*args, **kw)
        self.path_prefix = path_prefix
        if report_interval is not None:
            self.report_interval = report_interval

    def on_connect(self):
        """
        Called when the streaming connection is established.

        """
        log.info('connected')
        signal.signal(signal.SIGHUP, self.on_sighup)
        self.last_report_at = datetime.utcnow()
        self.num_records_since_report = 0

    def on_request_error(self, status_code):
        """
        Called when a non-200 HTTP status code is received.

        """
        log.error(f'received error status={status_code}')

    def on_connection_error(self):
        """
        Called when the connection times out.

        """
        log.error('received timeout')

    def on_exception(self, e):
        """
        Called when the stream cannot recover from an error.

        """
        exc_info = (type(e), e, e.__traceback__)
        log.exception('received exception while streaming', exc_info=exc_info)

    def on_keep_alive(self):
        """
        The streaming periodically contains empty lines to keep the
        connection open.

        """
        log.info('received keep-alive')

    def on_data(self, data):
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
        self.stream.write(data)
        self.stream.write(b'\n')

        if now - self.last_report_at >= self.report_interval:
            self.report(now=now)

    def on_disconnect(self):
        """
        Called when the stream has disconnected.

        """
        signal.signal(signal.SIGHUP, signal.SIG_DFL)
        self.rotate()
        self.report()

    def on_sighup(self, *args):
        log.info('received SIGHUP, rotating')
        self.rotate()

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

    def rotate(self):
        if self.stream is not None:
            log.info(f'closing path={self.stream.path}')
            self.stream.close()
            self.stream = None

def main(cli, args):
    profile = cli.profile

    with open(args.filter_file, 'r', encoding='utf8') as fp:
        filters = yaml.safe_load(fp)

    stream_profile = profile.get('stream', {})
    report_interval = stream_profile.get('report_interval')
    if report_interval is not None:
        report_interval = timedelta(seconds=report_interval)

    twilio = TwilioClient(
        profile['twilio']['account_sid'],
        profile['twilio']['auth_token'],
    )

    while True:
        stream = TweetStream(
            profile['twitter']['consumer_key'],
            profile['twitter']['consumer_secret'],
            profile['twitter']['access_token'],
            profile['twitter']['access_token_secret'],
            path_prefix=args.output_path_prefix,
            report_interval=report_interval,
        )

        stopping = False
        def on_sigterm(*args):
            nonlocal stopping
            log.info('received SIGTERM, stopping')
            stopping = True
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
            stream.disconnect()
