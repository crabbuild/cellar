#!/usr/bin/env python3
"""Generate registry READMEs with links pinned to the crate release tag."""
import argparse
from pathlib import Path
import re
import sys
import tomllib
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
CRATES = ('cellar-logic', 'cellar-native', 'cellar-ltx', 'cellar', 'cellar-axum')
REPOSITORY = 'https://github.com/crabbuild/cellar'


def registry_readme(source, tag):
    def target(raw, image=False):
        url = urlsplit(raw)
        if url.scheme or url.netloc or raw.startswith('#'):
            return raw
        path = (source.parent / url.path).resolve()
        if not path.is_relative_to(ROOT) or not path.exists():
            raise ValueError(f'{source.relative_to(ROOT)}: invalid local link {raw}')
        relative = path.relative_to(ROOT).as_posix()
        suffix = ('?' + url.query if url.query else '') + ('#' + url.fragment if url.fragment else '')
        if image:
            return f'https://raw.githubusercontent.com/crabbuild/cellar/{tag}/{relative}{suffix}'
        kind = 'tree' if path.is_dir() else 'blob'
        return f'{REPOSITORY}/{kind}/{tag}/{relative}{suffix}'

    text = source.read_text()
    text = re.sub(r'(!?\[[^\]]*\])\(([^)]+)\)',
                  lambda match: f'{match[1]}({target(match[2], match[1].startswith("!"))})', text)
    text = re.sub(r'(<img\b[^>]*\bsrc=")([^"]+)(")',
                  lambda match: match[1] + target(match[2], True) + match[3], text)
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Check generated READMEs without writing')
    args = parser.parse_args()
    version = tomllib.loads((ROOT / 'crates/cellar/Cargo.toml').read_text())['package']['version']
    tag = f'v{version}'
    errors = []
    for name in CRATES:
        directory = ROOT / 'crates' / name
        package = tomllib.loads((directory / 'Cargo.toml').read_text())['package']
        if package['version'] != version:
            errors.append(f'{name}: release version differs from {version}')
        if package.get('readme') != 'README.crates.io.md':
            errors.append(f'{name}: registry README is not configured')
        output = directory / 'README.crates.io.md'
        content = registry_readme(directory / 'README.md', tag)
        if args.check:
            if not output.is_file() or output.read_text() != content:
                errors.append(f'{name}: registry README is stale')
        else:
            output.write_text(content)
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    print(f'{"Checked" if args.check else "Generated"} {len(CRATES)} registry READMEs for {tag}.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
