from cached_property import cached_property
from contextlib import contextmanager
import logging
import subparse
import sys
import yaml

from . import commands

class AbortCLI(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code

class App:
    stdin = sys.stdin
    stdout = sys.stdout
    stderr = sys.stderr

    def __init__(self, profile_file):
        self.profile_file = profile_file
        self.dbs = []

    def out(self, msg):
        if not msg.endswith('\n'):
            msg = msg + '\n'
        self.stdout.write(msg)

    def error(self, msg):
        if not msg.endswith('\n'):
            msg = msg + '\n'
        self.stderr.write(msg)

    def abort(self, error, code=1):
        self.error(error)
        raise AbortCLI(error, code)

    @cached_property
    def profile(self):
        with open(self.profile_file, 'r', encoding='utf8') as fp:
            return yaml.safe_load(fp)

    @contextmanager
    def input_file(self, path, *, text=True):
        if path == '-':
            if text:
                yield sys.stdin
            else:
                yield sys.stdin.buffer
        else:
            mode = 'rb' if not text else 'r'
            with open(path, mode) as fp:
                yield fp

    @contextmanager
    def output_file(self, path, *, text=True):
        if path == '-':
            if text:
                yield sys.stdout
            else:
                yield sys.stdout.buffer
        else:
            mode = 'wb' if not text else 'w'
            with open(path, mode) as fp:
                yield fp

    def connect_db(self, path, commit_on_close=True):
        from . import model
        db = model.connect(path)
        self.dbs.append(dict(db=db, commit_on_close=commit_on_close))
        return db

    def commit(self, exc=None):
        from . import model
        while self.dbs:
            info = self.dbs.pop(0)
            db = info['db']
            model.close(db, rollback=exc or not info['commit_on_close'])

def context_factory(cli, args, with_db=False):
    if getattr(args, 'reload', False):
        import hupper
        reloader = hupper.start_reloader(
            __name__ + '.main',
            shutdown_interval=30,
        )
        reloader.watch_files([args.profile])

    app = App(args.profile)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)-15s %(levelname)-8s [%(name)s] %(message)s',
    )

    try:
        yield app
    except Exception as ex:
        app.commit(exc=ex)
        raise
    else:
        app.commit()

def main(argv=sys.argv):
    cli = subparse.CLI(prog='tweeter', context_factory=context_factory)
    cli.add_generic_options(commands.generic_options)
    cli.load_commands(commands)
    try:
        return cli.run()
    except AbortCLI as ex:
        return ex.code
