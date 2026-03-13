"""Tests for ScoutFSFileSystem."""

import pytest


def test_import():
    """Test that the package can be imported."""
    from fsspec_scoutfs import ScoutFSFileSystem

    assert ScoutFSFileSystem is not None


def test_protocol():
    """Test that the protocol is correctly defined."""
    from fsspec_scoutfs import ScoutFSFileSystem

    assert "scoutfs" in ScoutFSFileSystem.protocol


def test_fsspec_registration():
    """Test that the filesystem is registered with fsspec."""
    import fsspec

    # Import to trigger registration
    import fsspec_scoutfs  # noqa: F401

    assert "scoutfs" in fsspec.registry
