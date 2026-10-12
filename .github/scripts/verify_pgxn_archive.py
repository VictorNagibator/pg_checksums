#!/usr/bin/env python3
"""Validate the archive produced by `make dist` before it is published.

Usage: verify_pgxn_archive.py <archive.zip> <version>

Checks that the archive is packed under the pg_checksums-<version>/ prefix that
PGXN expects, contains every file the distribution needs, carries no build
artifacts, and that the META.json inside it describes the same version.
"""

import json
import sys
import zipfile


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2

    archive, version = sys.argv[1], sys.argv[2]
    prefix = f"pg_checksums-{version}/"

    required = {
        "META.json",
        "Makefile",
        "Changes",
        "LICENSE",
        "README.md",
        "pg_checksums.control",
        f"pg_checksums--{version}.sql",
        "pg_checksums.c",
        "pg_checksums.h",
    }

    try:
        with zipfile.ZipFile(archive) as zf:
            files = {
                name[len(prefix):]
                for name in zf.namelist()
                if name.startswith(prefix) and not name.endswith("/")
            }

            if not files:
                print(
                    f"error: {archive} has no entries under {prefix}",
                    file=sys.stderr,
                )
                return 1

            missing = sorted(required - files)
            if missing:
                print(f"error: archive is missing {missing}", file=sys.stderr)
                return 1

            junk = sorted(
                name
                for name in files
                if name.endswith((".o", ".so", ".zip", ".log"))
                or name.startswith(("results/", "tmp_check/"))
            )
            if junk:
                print(
                    f"error: archive contains build artifacts: {junk}",
                    file=sys.stderr,
                )
                return 1

            meta = json.loads(zf.read(prefix + "META.json"))
    except (OSError, zipfile.BadZipFile, KeyError, ValueError) as exc:
        print(f"error: cannot read {archive}: {exc}", file=sys.stderr)
        return 1

    if meta.get("version") != version:
        print(
            f"error: META.json version is {meta.get('version')!r}, expected {version!r}",
            file=sys.stderr,
        )
        return 1

    provided = meta.get("provides", {}).get("pg_checksums", {}).get("version")
    if provided != version:
        print(
            f"error: META.json provides.pg_checksums.version is {provided!r}, "
            f"expected {version!r}",
            file=sys.stderr,
        )
        return 1

    if meta.get("provides", {}).get("pg_checksums", {}).get("file") not in files:
        print(
            "error: META.json points at a SQL file that is not in the archive",
            file=sys.stderr,
        )
        return 1

    print(f"{archive}: OK ({len(files)} files, version {version})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
