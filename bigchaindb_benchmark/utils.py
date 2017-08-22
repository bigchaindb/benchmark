"""Utility functions and basic common arguments for
``argparse.ArgumentParser``."""

import multiprocessing as mp


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

    args.multiprocess = getattr(args, 'multiprocess', False)

    if args.multiprocess is False:
        args.multiprocess = 1
    elif args.multiprocess is None:
        args.multiprocess = mp.cpu_count()

    if callback_before:
        callback_before(args)

    return func(args)
