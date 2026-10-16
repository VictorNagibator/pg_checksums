#!/usr/bin/env python3
"""Print the Changes entry for a version, formatted as GitHub release notes.

Usage: changelog_for_version.py <version> [changes-file]

The Changes file holds one section per release, in the usual CPAN/PostgreSQL
style:

    1.1.0   2026-10-17

        - Something changed.

    1.0.0   2026-10-12

        - First release.

A section starts on the line whose first token is exactly <version> and runs
until the next section header (a line starting in column zero with a
version-looking token). The section body is dedented so that GitHub renders it
as a bullet list rather than a code block.

Exits with status 1 when there is no section for the requested version.
"""

import re
import sys
import textwrap

# A section header: a version-looking token at the start of the line, followed
# by whitespace (typically a date) and nothing else.
HEADER = re.compile(r"^([0-9][0-9A-Za-z.\-]*)(?:\s.*)?$")


def extract(text, version):
    lines = text.splitlines()

    start = None
    for index, line in enumerate(lines):
        match = HEADER.match(line)
        if match and match.group(1) == version:
            start = index + 1
            break

    if start is None:
        return None

    end = len(lines)
    for index in range(start, len(lines)):
        if HEADER.match(lines[index]):
            end = index
            break

    body = list(lines[start:end])
    while body and not body[0].strip():
        body.pop(0)
    while body and not body[-1].strip():
        body.pop()

    return textwrap.dedent("\n".join(body))


def main():
    if len(sys.argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        return 2

    version = sys.argv[1]
    path = sys.argv[2] if len(sys.argv) == 3 else "Changes"

    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        print(f"error: cannot read {path}: {exc}", file=sys.stderr)
        return 1

    entry = extract(text, version)
    if entry is None:
        print(f"error: no entry for version {version} in {path}", file=sys.stderr)
        return 1

    if not entry:
        print(f"error: entry for version {version} in {path} is empty", file=sys.stderr)
        return 1

    print(entry)
    return 0


if __name__ == "__main__":
    sys.exit(main())
