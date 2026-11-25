#!/usr/bin/env python3
"""Require actual detector graph evidence for both Go modules and the uv lock."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

REQUIRED = ("go.mod", "terraform/provider/go.mod", "uv.lock")


def verify(document: dict, root: Path) -> dict[str, int]:
    graphs = document.get("dependencyGraphs")
    if not isinstance(graphs, dict):
        raise ValueError("detector output lacks dependencyGraphs")
    normalized = {}
    for location, graph in graphs.items():
        path = Path(location)
        if path.is_absolute():
            try:
                location = path.resolve().relative_to(root.resolve()).as_posix()
            except ValueError:
                continue
        else:
            location = path.as_posix()
        normalized[location] = graph
    counts = {}
    for manifest in REQUIRED:
        entry = normalized.get(manifest)
        graph = entry.get("graph") if isinstance(entry, dict) else None
        if not isinstance(graph, dict) or not graph:
            raise ValueError(f"missing or empty detected dependency graph: {manifest}")
        counts[manifest] = len(graph)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        counts = verify(json.loads(args.manifest.read_text()), args.root)
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"dependency coverage verification failed: {error}\n")
    print(json.dumps({"verified_manifest_graphs": counts}, sort_keys=True))


if __name__ == "__main__":
    main()
