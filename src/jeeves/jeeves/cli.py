import sys
import click
from pathlib import PosixPath
# from jeeves.commands.libs.command import import_file
import importlib

CONTEXT_SETTINGS = dict(auto_envvar_prefix='COMPLEX')


class Environment(object):

    def __init__(self):
        self.verbose = True
        self.home = PosixPath.cwd()

    def log(self, msg, *args):
        """Logs a message to stderr."""
        if args:
            msg %= args
        click.echo(msg, file=sys.stderr)

    def vlog(self, msg, *args):
        """Logs a message to stderr only if verbose is enabled."""
        if self.verbose:
            self.log(msg, *args)


pass_environment = click.make_pass_decorator(Environment, ensure=True)
cmd_folder = PosixPath(PosixPath.cwd()) / 'jeeves' / 'commands'
cmd_folder = cmd_folder.expanduser()


class ComplexCLI(click.MultiCommand):

    def list_commands(self, ctx):
        rv = []
        for filename in cmd_folder.glob('*.py'):
            filename = str(filename)
            if '__init__.py' in filename:
                continue

            cmd = filename.partition('/commands/')[2][:-3]
            rv.append(cmd)
        rv.sort()
        return rv

    def get_command(self, ctx, name):
        try:
            mod = importlib.import_module('jeeves.commands.' + name)
        except ImportError:
            return
        return mod.cli


@click.command(cls=ComplexCLI, context_settings=CONTEXT_SETTINGS)
@click.option('-v', '--verbose', is_flag=True,
              help='Enables verbose mode.')
@pass_environment
def cli(ctx, verbose):
    ctx.verbose = verbose
