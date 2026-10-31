#!/usr/bin/env python3
"""Install only this product's signed, verified macOS app bundle."""
from __future__ import annotations

import sys
from pathlib import Path

from build_desktop import APP_NAME, BUNDLE_ID, OWNER_MARKER, owned_bundle
from install_security import InstallSecurityError, install_signed_bundle


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "dist" / f"{APP_NAME}.app"
DESTINATION = Path.home() / "Applications" / f"{APP_NAME}.app"


def main() -> int:
    if sys.platform != "darwin":
        raise SystemExit("Install the native application on macOS.")
    try:
        backup = install_signed_bundle(SOURCE, DESTINATION, owner_check=owned_bundle)
    except (InstallSecurityError, OSError) as exc:
        raise SystemExit(f"Install failed: {exc}") from exc
    if backup is not None:
        print(f"Previous app preserved at {backup}")
    print(DESTINATION)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
