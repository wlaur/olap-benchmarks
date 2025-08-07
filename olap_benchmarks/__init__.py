try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = "0.0.dev"
