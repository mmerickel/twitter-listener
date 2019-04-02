from cached_property import cached_property
import logging
import subparse
import sys
import yaml

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

def context_factory(cli, args):
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

    return app

def generic_options(parser):
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--profile', default='profile.yml')

def main(argv=sys.argv):
    cli = subparse.CLI(prog='tweeter', context_factory=context_factory)
    cli.add_generic_options(generic_options)
    cli.load_commands('.commands')
    try:
        return cli.run()
    except AbortCLI as ex:
        return ex.code
