# fsspec-scoutfs

An [fsspec](https://filesystem-spec.readthedocs.io/) filesystem implementation for ScoutFS with tape staging support.

## Installation

```bash
pip install fsspec-scoutfs
```

Or with pixi:

```bash
pixi add fsspec-scoutfs
```

## Usage

```python
import fsspec

# Open a filesystem connection
fs = fsspec.filesystem("scoutfs", host="myhost.example.com")

# Check if a file is online (not on tape)
is_online = fs.is_online("/path/to/file.nc")

# Open a file (automatically stages from tape if needed)
with fs.open("/path/to/file.nc") as f:
    data = f.read()

# Manually stage a file
fs.stage("/path/to/file.nc")

# Release a file back to tape
fs.release("/path/to/file.nc")

# Check staging queue status
print(fs.queues)
```

## Configuration

The filesystem can be configured via environment variables:

- `SCOUTFS_API_URL`: Base URL for the ScoutFS HSM API (default: `https://hsm.dmawi.de:8080/v1`)
- `SCOUTFS_USERNAME`: API username (default: `filestat`)
- `SCOUTFS_PASSWORD`: API password (default: `filestat`)

Or via the `scoutfs_config` parameter:

```python
fs = fsspec.filesystem(
    "scoutfs",
    host="myhost.example.com",
    scoutfs_config={
        "api_url": "https://custom-hsm.example.com:8080/v1",
        "username": "myuser",
        "password": "mypass",
    }
)
```

## Features

- Transparent tape staging when opening files
- Automatic online/offline status detection
- Async batch status checking for multiple files
- Queue monitoring for staging operations
- Configurable staging timeout
- fsspec callback support for progress tracking

## License

MIT
