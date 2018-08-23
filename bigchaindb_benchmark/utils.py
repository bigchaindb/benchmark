from time import time
import sys


def ts():
    return round(int(time() * 1e3))


def start(parser, argv, scope, callback_before=None):
    """Utility function to execute a subcommand.

    The function will look up in the ``scope``
    if there is a function called ``run_<parser.args.command>``
    and will run it using ``parser.args`` as first positional argument.

    Args:
        parser: an ArgumentParser instance.
        argv: the list of command line arguments without the script name.
        scope (dict): map containing (eventually) the functions to be called.

    Raises:
        NotImplementedError: if ``scope`` doesn't contain a function called
                             ``run_<parser.args.command>``.
    """
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        raise SystemExit()

    # look up in the current scope for a function called 'run_<command>'
    # replacing all the dashes '-' with the lowercase character '_'
    func = scope.get('run_' + args.command.replace('-', '_'))

    # if no command has been found, raise a `NotImplementedError`
    if not func:
        raise NotImplementedError('Command `{}` not yet implemented'.
                                  format(args.command))

    if args.peer is None:
        args.peer = ['http://localhost:9984,localhost:27017']

    if args.requests < args.processes:
        args.processes = args.requests
        args.requests_per_worker = 1
    else:
        args.requests_per_worker = args.requests // args.processes

    if args.auth:
        app_id, app_key = args.auth.split(':')
        args.auth = {'app_id': app_id,
                     'app_key': app_key}
    else:
        args.auth = {}

    if callback_before:
        callback_before(args)

    return func(args)


class unpack:

    def __init__(self, f):
        self.f = f

    def __call__(self, args):
        return self.f(*args)
