"""fsspec filesystem implementation for ScoutFS with tape staging support.

This module provides a filesystem implementation that works with ScoutFS, including
support for staging files from tape storage and progress tracking.

Usage:
    import fsspec

    # Open a file on ScoutFS (automatically stages from tape if needed)
    fs = fsspec.filesystem("scoutfs", host="myhost.example.com")
    with fs.open("/path/to/file.nc") as f:
        data = f.read()

    # Check if a file is online (not on tape)
    is_online = fs.is_online("/path/to/file.nc")

    # Manually stage a file
    fs.stage("/path/to/file.nc")
"""

from fsspec_scoutfs.filesystem import ScoutFSFileSystem

__version__ = "0.1.0"
__all__ = ["ScoutFSFileSystem"]
