"""Smoke test: the project skeleton is importable."""

import claimslake


def test_package_is_importable():
    assert claimslake.__doc__ is not None
