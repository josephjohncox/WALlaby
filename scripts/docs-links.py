#!/usr/bin/env python3
"""Check local links and fragments in a built MkDocs site."""

from __future__ import annotations

import argparse
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.ids: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if value := values.get("id"):
            self.ids.add(value)
        if tag == "a" and (name := values.get("name")):
            self.ids.add(name)
        if tag == "a" and (href := values.get("href")):
            self.links.append(href)
        if tag in {"img", "script"} and (src := values.get("src")):
            self.links.append(src)
        if tag == "link" and (href := values.get("href")):
            self.links.append(href)


def parse_page(path: Path) -> PageParser:
    parser = PageParser()
    parser.feed(path.read_text(encoding="utf-8"))
    return parser


def target_path(site: Path, page: Path, raw_path: str) -> Path:
    path = unquote(raw_path)
    if path.startswith("/"):
        path = path.removeprefix("/WALlaby/").lstrip("/")
        candidate = site / path
    else:
        candidate = page.parent / path
    if path.endswith("/") or candidate.is_dir():
        candidate /= "index.html"
    return candidate.resolve()


def main() -> int:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("site", type=Path)
    args = arg_parser.parse_args()
    site = args.site.resolve()
    pages = {path.resolve(): parse_page(path) for path in site.rglob("*.html")}
    failures: list[str] = []

    for page, parsed in pages.items():
        for link in parsed.links:
            parts = urlsplit(link)
            if parts.scheme in {"http", "https", "mailto", "tel", "data"} or parts.netloc:
                continue
            if not parts.path:
                target = page
            else:
                target = target_path(site, page, parts.path)
            if not target.exists():
                failures.append(f"{page.relative_to(site)}: missing {link}")
                continue
            if parts.fragment and target.suffix == ".html":
                target_page = pages.get(target)
                if target_page is None:
                    target_page = parse_page(target)
                    pages[target] = target_page
                if unquote(parts.fragment) not in target_page.ids:
                    failures.append(
                        f"{page.relative_to(site)}: missing fragment {link}"
                    )

    if failures:
        print("\n".join(sorted(set(failures))))
        return 1
    print(f"checked {len(pages)} HTML pages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
