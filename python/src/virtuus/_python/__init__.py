import os as _os


def _read_version() -> str:
    _here = _os.path.dirname(_os.path.abspath(__file__))
    _version_file = _os.path.normpath(
        _os.path.join(_here, "..", "..", "..", "..", "VERSION")
    )
    with open(_version_file) as _f:
        return _f.read().strip()


__version__: str = _read_version()
