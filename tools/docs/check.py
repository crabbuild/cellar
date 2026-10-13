#!/usr/bin/env python3
"""Validate real local documentation links, catalog coverage and SVG references."""
import json
from html.parser import HTMLParser
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
ERRORS = []
CHECKED = 0

class References(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.ids = set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            self.ids.add(attrs['id'])
        for key in ('href', 'src', 'data'):
            if key in attrs:
                self.links.append(attrs[key])

def markdown_ids(path):
    ids = set()
    counts = {}
    for line in path.read_text().splitlines():
        match = re.match(r'^#{1,6}\s+(.+)', line)
        if not match:
            continue
        title = match[1].strip().lower().replace('`', '')
        slug = re.sub(r'[^\w\- ]', '', title).replace(' ', '-')
        n = counts.get(slug, 0)
        counts[slug] = n + 1
        ids.add(slug if not n else f'{slug}-{n}')
    return ids

def check_link(source, raw):
    global CHECKED
    raw = raw.strip().strip('<>')
    url = urlsplit(raw)
    if url.scheme or url.netloc:
        return
    target = (source.parent / unquote(url.path)).resolve() if url.path else source
    CHECKED += 1
    if not target.exists():
        ERRORS.append(f'{source.relative_to(ROOT)}: missing {raw}')
        return
    if url.fragment and target.is_file():
        if target.suffix == '.md':
            ids = markdown_ids(target)
        elif target.suffix == '.svg':
            ids = {el.get('id') for el in ET.parse(target).iter() if el.get('id')}
        elif target.suffix == '.html':
            parser = References(); parser.feed(target.read_text()); ids = parser.ids
        else:
            return
        if unquote(url.fragment) not in ids:
            ERRORS.append(f'{source.relative_to(ROOT)}: missing anchor {raw}')

def main():
    catalog = json.loads((DOCS / 'catalog.json').read_text())
    listed = [d['path'] for d in catalog['documents']]
    actual = {str(p.relative_to(DOCS)) for p in DOCS.rglob('*.md')}
    if len(listed) != len(set(listed)):
        ERRORS.append('Duplicate document paths in catalog')
    if set(listed) != actual:
        ERRORS.append(f'Catalog coverage mismatch: {set(listed) ^ actual}')
    allowed_roles = {'builder', 'platform', 'operations', 'team'}
    for doc in catalog['documents']:
        if not doc['title'] or not doc['summary'] or not doc['roles'] or set(doc['roles']) - allowed_roles:
            ERRORS.append(f'Invalid catalog metadata: {doc["path"]}')
    for path in [ROOT / 'README.md', *DOCS.rglob('*.md')]:
        prose = re.sub(r'```.*?```', '', path.read_text(), flags=re.S)
        for raw in re.findall(r'!?\[[^\]]*\]\(([^)]+)\)', prose):
            check_link(path, raw)
    for path in DOCS.glob('*.html'):
        parser = References(); parser.feed(path.read_text())
        for raw in parser.links:
            check_link(path, raw)
        if '/*CATALOG*/null' in path.read_text():
            ERRORS.append('Explorer has no generated catalog')
    for path in (DOCS / 'diagrams').glob('*.svg'):
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError as error:
            ERRORS.append(f'{path.name}: invalid XML: {error}')
            continue
        if root.tag != '{http://www.w3.org/2000/svg}svg' or not root.get('viewBox'):
            ERRORS.append(f'{path.name}: missing SVG namespace/viewBox')
        ids = [el.get('id') for el in root.iter() if el.get('id')]
        if len(ids) != len(set(ids)):
            ERRORS.append(f'{path.name}: duplicate IDs')
        for el in root.iter():
            for key, value in el.attrib.items():
                if key.endswith('href'):
                    check_link(path, value)
                for reference in re.findall(r'url\(#([^)]*)\)', value):
                    if reference not in ids:
                        ERRORS.append(f'{path.name}: missing definition {reference}')
        if path.stem.endswith('-flow'):
            for required in ['step-0','step-5','failure-node','detail','prev','next','reset','failure']:
                if required not in ids:
                    ERRORS.append(f'{path.name}: missing interaction target {required}')
            if root.get('role') != 'group':
                ERRORS.append(f'{path.name}: interaction hidden by image-only semantics')
    for flow in catalog['flows']:
        check_link(DOCS / 'index.html', f'diagrams/{flow["id"]}-flow.svg')
        check_link(DOCS / 'index.html', flow['guide'])
    if ERRORS:
        print('\n'.join(ERRORS), file=sys.stderr)
        return 1
    print(f'Validated {len(actual)} documents, {len(catalog["flows"])} flows and {CHECKED} local references.')
    return 0

if __name__ == '__main__':
    sys.exit(main())
