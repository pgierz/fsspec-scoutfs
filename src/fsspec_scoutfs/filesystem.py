"""ScoutFS filesystem implementation.

This module provides a filesystem implementation that works with ScoutFS, including
support for staging files from tape storage and progress tracking.
"""

import datetime
import os
import time
import warnings
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import fsspec
import fsspec.implementations.sftp
import requests
from fsspec.callbacks import Callback
from fsspec.registry import register_implementation
from loguru import logger

import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Disable urllib3 warnings globally for ScoutFS SSL connections
urllib3.disable_warnings()
urllib3.disable_warnings(InsecureRequestWarning)
warnings.filterwarnings("ignore", category=InsecureRequestWarning)

try:
    import requests.packages.urllib3 as requests_urllib3

    requests_urllib3.disable_warnings()
    requests_urllib3.disable_warnings(InsecureRequestWarning)
except (ImportError, AttributeError):
    pass


class ScoutFSFileSystem(fsspec.implementations.sftp.SFTPFileSystem):
    """Filesystem implementation for ScoutFS with tape staging support.

    This class extends SFTPFileSystem to add support for ScoutFS-specific features
    like file staging from tape storage and progress tracking.

    Parameters
    ----------
    host : str
        The hostname to connect to via SFTP/SSH
    scoutfs_config : dict, optional
        Configuration for ScoutFS API access. Keys:
        - api_url: Base URL for ScoutFS API (default: https://hsm.dmawi.de:8080/v1)
        - token: Pre-existing authentication token
        - username: API username (default: filestat)
        - password: API password (default: filestat)
    **kwargs
        Additional arguments passed to SFTPFileSystem

    Examples
    --------
    >>> import fsspec
    >>> fs = fsspec.filesystem("scoutfs", host="levante.dkrz.de")
    >>> fs.is_online("/path/to/file.nc")
    True
    >>> with fs.open("/path/to/file.nc") as f:
    ...     data = f.read()
    """

    protocol = ("scoutfs", "sftp", "ssh")

    def __init__(self, host: str, **kwargs):
        """Initialize the ScoutFS filesystem.

        Args:
            host: The host to connect to
            **kwargs: Additional arguments passed to SFTPFileSystem
        """
        self._scoutfs_config = kwargs.pop("scoutfs_config", {})

        # Support environment variable configuration
        if "api_url" not in self._scoutfs_config:
            self._scoutfs_config["api_url"] = os.environ.get(
                "SCOUTFS_API_URL", "https://hsm.dmawi.de:8080/v1"
            )
        if "username" not in self._scoutfs_config:
            self._scoutfs_config["username"] = os.environ.get("SCOUTFS_USERNAME", "filestat")
        if "password" not in self._scoutfs_config:
            self._scoutfs_config["password"] = os.environ.get("SCOUTFS_PASSWORD", "filestat")

        # Extract warning filter configuration (kept for compatibility)
        kwargs.pop("warning_filters", None)

        super().__init__(host, **kwargs)

    @contextmanager
    def _filtered_warnings(self):
        """Context manager to apply warning filters for this filesystem instance.

        Since warnings are now suppressed globally, this is a no-op but kept
        for API compatibility.
        """
        yield

    # --- ScoutFS API Methods ---

    def _scoutfs_generate_token(self) -> str:
        """Generate a new authentication token from the ScoutFS API.

        Returns:
            str: The authentication token
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        data = {
            "acct": self._scoutfs_config.get("username", "filestat"),
            "pass": self._scoutfs_config.get("password", "filestat"),
        }

        with self._filtered_warnings():
            response = requests.post(
                f"{self._scoutfs_api_url}/security/login",
                headers=headers,
                json=data,
                verify=False,
                timeout=30,
            )
        response.raise_for_status()
        return response.json().get("response")

    @property
    def _scoutfs_token(self) -> str:
        """Get the current authentication token, generating a new one if needed."""
        if "token" not in self._scoutfs_config:
            self._scoutfs_config["token"] = self._scoutfs_generate_token()
        return self._scoutfs_config["token"]

    @property
    def _scoutfs_api_url(self) -> str:
        """Get the base URL for the ScoutFS API."""
        return self._scoutfs_config.get("api_url", "https://hsm.dmawi.de:8080/v1")

    def _scoutfs_get_filesystems(self) -> Dict[str, Any]:
        """Get information about all available filesystems from the ScoutFS API.

        Returns:
            dict: Filesystem information including mount points and fsids
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        with self._filtered_warnings():
            response = requests.get(
                f"{self._scoutfs_api_url}/filesystems",
                headers=headers,
                verify=False,
                timeout=30,
            )
        response.raise_for_status()
        return response.json()

    def _resolve_path(self, path: str) -> str:
        """Resolve symlinks in a remote path via SFTP realpath.

        Uses the paramiko SFTPClient.normalize() call which invokes
        realpath on the server. Results are cached to avoid repeated
        round-trips for paths under the same prefix.

        Args:
            path: The remote file path (may contain symlinks)

        Returns:
            str: The fully resolved path
        """
        if not hasattr(self, "_resolve_cache"):
            self._resolve_cache: Dict[str, str] = {}

        if path in self._resolve_cache:
            return self._resolve_cache[path]

        try:
            resolved = self.ftp.normalize(path)
        except (OSError, IOError):
            # If normalize fails (e.g. file doesn't exist yet), return as-is
            resolved = path

        self._resolve_cache[path] = resolved

        if resolved != path:
            logger.debug("Resolved symlink: {} -> {}", path, resolved)

        return resolved

    def _get_fsid_for_path(self, path: str) -> str:
        """Get the filesystem ID for a given path.

        Resolves symlinks via SFTP before matching against ScoutFS mount
        points, so paths like ``/hs/projects/...`` (symlink) correctly
        match mounts like ``/hs/D-P/...`` (real path).

        Args:
            path: The file path to look up

        Returns:
            str: The filesystem ID

        Raises:
            ValueError: If no matching filesystem is found or multiple match
        """
        resolved_path = self._resolve_path(path)

        fsid_response = self._scoutfs_get_filesystems()
        matching_fsids = []
        for fsid_info in fsid_response.get("fsids", []):
            if resolved_path.startswith(fsid_info["mount"]):
                matching_fsids.append(fsid_info)

        if len(matching_fsids) == 0:
            raise ValueError(
                f"No ScoutFS filesystem found for path '{path}' "
                f"(resolved: '{resolved_path}'). "
                f"Available mounts: {[f['mount'] for f in fsid_response.get('fsids', [])]}"
            )
        elif len(matching_fsids) > 1:
            raise ValueError(
                f"Multiple ScoutFS filesystems match path '{path}' "
                f"(resolved: '{resolved_path}'): {matching_fsids}"
            )

        return matching_fsids[0]["fsid"]

    def _scoutfs_file(self, path: str) -> Dict[str, Any]:
        """Get file information from the ScoutFS API.

        Args:
            path: Path to the file (symlinks are resolved automatically)

        Returns:
            dict: File information including online/offline status
        """
        resolved = self._resolve_path(path)
        fsid = self._get_fsid_for_path(path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        with self._filtered_warnings():
            response = requests.get(
                f"{self._scoutfs_api_url}/file?fsid={fsid}&path={resolved}",
                headers=headers,
                verify=False,
                timeout=30,
            )
        response.raise_for_status()
        return response.json()

    def _scoutfs_request(self, command: str, path: str) -> Dict[str, Any]:
        """Make a request to the ScoutFS API.

        Args:
            command: The API command (e.g., "stage", "release")
            path: Path to the file (symlinks are resolved automatically)

        Returns:
            dict: API response
        """
        resolved = self._resolve_path(path)
        fsid = self._get_fsid_for_path(path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        with self._filtered_warnings():
            response = requests.post(
                f"{self._scoutfs_api_url}/request/{command}?fsid={fsid}&path={resolved}",
                headers=headers,
                json={"path": resolved},
                verify=False,
                timeout=30,
            )
        response.raise_for_status()
        return response.json()

    def _scoutfs_queues(self) -> Dict[str, Any]:
        """Get information about the staging queues.

        Returns:
            dict: Queue information
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        with self._filtered_warnings():
            response = requests.get(
                f"{self._scoutfs_api_url}/queues",
                headers=headers,
                verify=False,
                timeout=30,
            )
        response.raise_for_status()
        return response.json()

    @property
    def queues(self) -> Dict[str, Any]:
        """Get information about the staging queues."""
        return self._scoutfs_queues()

    def stage(self, path: str) -> Dict[str, Any]:
        """Stage a file from tape to disk.

        Args:
            path: Path to the file to stage

        Returns:
            dict: The API response from the staging request
        """
        return self._scoutfs_request("stage", path)

    def release(self, path: str) -> Dict[str, Any]:
        """Release a file back to tape.

        Args:
            path: Path to the file to release

        Returns:
            dict: The API response from the release request
        """
        return self._scoutfs_request("release", path)

    def info(self, path: str, **kwargs) -> Dict[str, Any]:
        """Get information about a file or directory.

        This extends the base info() method to add ScoutFS-specific information.

        Args:
            path: Path to the file or directory
            **kwargs: Additional arguments passed to parent class

        Returns:
            dict: File information including ScoutFS-specific details
        """
        robj = super().info(path, **kwargs)

        # Add ScoutFS-specific information
        try:
            scoutfs_file = self._scoutfs_file(path)
            robj["scoutfs_info"] = {
                "/file": scoutfs_file,
                "/batchfile": None,
            }
        except Exception as e:
            logger.debug(f"Failed to get ScoutFS info for {path}: {e}")
            robj["scoutfs_info"] = {
                "/file": {"error": str(e)},
                "/batchfile": None,
            }

        return robj

    def is_online(self, path: str) -> bool:
        """Check if a file is online (not on tape).

        Args:
            path: Path to the file to check

        Returns:
            bool: True if the file is online, False otherwise
        """
        try:
            info = self.info(path)
            scoutfs_info = info.get("scoutfs_info", {}).get("/file", {})
            online_blocks = scoutfs_info.get("onlineblocks", "")
            offline_blocks = scoutfs_info.get("offlineblocks", "")

            # Convert to int, defaulting to 0 for empty strings
            if online_blocks != "":
                online_blocks = int(online_blocks)
            else:
                online_blocks = 0

            if offline_blocks != "":
                offline_blocks = int(offline_blocks)
            else:
                offline_blocks = 0

            return online_blocks > 0 and offline_blocks == 0

        except Exception as e:
            logger.debug(f"Error checking if {path} is online: {e}")
            # If we can't determine the status, assume the file is online
            # to avoid unnecessary staging attempts
            return True

    async def is_online_async(self, path: str) -> bool:
        """Asynchronously check if a file is online (not on tape).

        Args:
            path: Path to the file to check

        Returns:
            bool: True if the file is online, False otherwise
        """
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.is_online, path)

    async def check_online_status_batch(
        self, paths: List[str], max_concurrent: int = 10
    ) -> Dict[str, bool]:
        """Asynchronously check online status for multiple files concurrently.

        Args:
            paths: List of file paths to check
            max_concurrent: Maximum number of concurrent status checks

        Returns:
            dict: Dictionary mapping paths to their online status (True/False)
        """
        import asyncio

        semaphore = asyncio.Semaphore(max_concurrent)

        async def check_with_semaphore(path: str):
            async with semaphore:
                return path, await self.is_online_async(path)

        tasks = [check_with_semaphore(path) for path in paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        status_dict = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in batch status check: {result}")
                continue
            path, is_online = result
            status_dict[path] = is_online

        return status_dict

    def open(
        self,
        path: str,
        mode: str = "r",
        stage_before_opening: bool = True,
        timeout: Optional[int] = None,
        callback: Optional[Callback] = None,
        **kwargs,
    ):
        """Open a file, optionally staging it from tape first.

        Args:
            path: Path to the file to open
            mode: File mode ('r', 'w', etc.)
            stage_before_opening: If True, stage the file before opening
            timeout: Maximum time to wait for staging (in seconds)
            callback: Optional fsspec callback for progress tracking
            **kwargs: Additional arguments passed to the parent class

        Returns:
            A file-like object

        Raises:
            TimeoutError: If staging times out
            FileNotFoundError: If the file doesn't exist
        """
        if "w" in mode or not stage_before_opening:
            return super().open(path, mode=mode, callback=callback, **kwargs)

        try:
            file_info = self.info(path)
        except FileNotFoundError:
            if "w" not in mode and "a" not in mode:
                raise
            return super().open(path, mode=mode, callback=callback, **kwargs)

        if self.is_online(path):
            return super().open(path, mode=mode, callback=callback, **kwargs)

        if callback:
            callback.set_description(f"Staging {path}")

        self.stage(path)

        timeout_dt = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout if timeout is not None else 180
        )

        while True:
            if self.is_online(path):
                break

            if datetime.datetime.now() > timeout_dt:
                raise TimeoutError(f"Timeout while waiting for file {path} to be staged")

            if callback:
                callback.relative_update(0)

            time.sleep(1)

        return super().open(path, mode=mode, callback=callback, **kwargs)


# Register the implementation with fsspec
register_implementation("scoutfs", ScoutFSFileSystem, clobber=True)
