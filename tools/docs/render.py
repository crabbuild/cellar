#!/usr/bin/env python3
"""Optional 2x PNG previews via a local agent-browser Chromium session.

No Rust/application dependency. Requires agent-browser already installed.
SVG sources remain primary and work without this preview utility.
"""
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
SESSION = 'cellar-docs-render-' + ROOT.parent.name

def command(*args):
    return subprocess.run(['agent-browser', '--session', SESSION, *args],
                          check=True, capture_output=True, text=True, timeout=60)

def main():
    try:
        for source in sorted((ROOT / 'docs/diagrams').glob('*.svg')):
            _, _, width, height = map(float, ET.parse(source).getroot().get('viewBox').split())
            command('open', source.as_uri())
            command('set', 'viewport', str(round(width * 2)), str(round(height * 2)))
            output = source.with_name(source.stem + '@2x.png')
            command('screenshot', str(output))
            print(f'Rendered {output.relative_to(ROOT)} at 2x', flush=True)
    finally:
        command('close')

if __name__ == '__main__':
    main()
