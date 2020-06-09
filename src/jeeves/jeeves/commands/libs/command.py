import importlib
import sys


def import_file(f):
    """
    This has been a beast to figure out.
    @author Noah Goodrich
    @date 9/18/2019
    See the following links for inspiration and understanding of what the hell this even does.
    * https://manikos.github.io/how-pythons-import-machinery-works
    * https://stackoverflow.com/a/41595552/20178
    * https://stackoverflow.com/a/50182008/20178
    """
    modname = ".".join(f[:-3].split('/')[-2:])
    spec = importlib.util.spec_from_file_location(modname, f)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sys.modules[spec.name] = mod
    return mod
