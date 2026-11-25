"""Accept only the standard axiom report and the intended false-property error."""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

LANE = Path(__file__).resolve().parents[1]
ROOT = LANE.parents[1]
THEOREM = "Perennial.github_com.josephjohncox.wallaby.internal.reconcilepolicy.Decide.wp_proof"
ALLOWED_AXIOMS = {"propext", "Classical.choice", "Quot.sound"}


def audit() -> None:
    artifacts = LANE / "artifacts"
    report = (artifacts / "axioms.txt").read_text()
    match = re.fullmatch(r"'" + re.escape(THEOREM) + r"' depends on axioms:\s*\[(.*?)\]\s*", report, re.S)
    if match is None:
        raise ValueError("missing or malformed axiom report for the actual program-logic theorem")
    axioms = {name.strip() for name in match[1].split(",") if name.strip()}
    if not axioms <= ALLOWED_AXIOMS:
        raise ValueError(f"nonstandard axiom dependency: {sorted(axioms - ALLOWED_AXIOMS)}")
    negative = (artifacts / "negative.txt").read_text()
    fixture = LANE / "negative" / "FalseAdopt.lean.in"
    intended_line = [i for i, line in enumerate(fixture.read_text().splitlines(), 1) if line.strip() == "rfl"]
    errors = re.findall(r"^artifacts/FalseAdopt\.lean:(\d+):\d+: error: (.*)$", negative, re.M)
    if len(intended_line) != 1 or len(errors) != 1 or int(errors[0][0]) != intended_line[0]:
        raise ValueError("negative control did not fail only at the intended final equality proof")
    if errors[0][1] != "Tactic `rfl` failed: The left-hand side":
        raise ValueError("negative control failed for a tactic/import/tool reason")
    if not all(text in negative for text in ("not definitionally equal", "W64 1", "W64 2", "⊢ W64 1 = W64 2")):
        raise ValueError("negative control lacks the concrete Retry-versus-Adopt contradiction")
    try:
        manifest = json.loads((LANE / "lake-manifest.json").read_text())
    except (OSError, ValueError) as error:
        raise ValueError("cannot read the pinned Lake manifest") from error
    revisions = {}
    for package in manifest["packages"]:
        checkout = LANE / ".lake" / "packages" / package["name"]
        actual = subprocess.run(["git", "-C", str(checkout), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        if actual != package["rev"]:
            raise ValueError(f"dependency revision drift: {package['name']}")
        subprocess.run(["git", "-C", str(checkout), "diff", "--exit-code", "HEAD"], check=True, capture_output=True)
        revisions[package["name"]] = actual
    paths = [
        ROOT / "internal/reconcilepolicy/decision.go",
        LANE / "WallabyGoose/Code/github_com/josephjohncox/wallaby/internal/reconcilepolicy.lean",
        LANE / "WallabyGoose/Spec/Decide.lean",
        LANE / "WallabyGoose/Proof/Decide.lean",
        fixture, LANE / "lean-toolchain", LANE / "lake-manifest.json",
    ]
    hashes = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    evidence = {"result": "PROVED_CONDITIONAL_WP", "theorem": THEOREM,
                "axioms": sorted(axioms), "dependencies": revisions, "sha256": hashes,
                "negative": "actual generated Decide(1,false), false postcondition Adopt=2, rejected at rfl",
                "boundary": "Partial correctness under Goose semantics/interpreter and generated FuncUnfold assumptions; no adequacy, crash proof, I/O or coordinator proof"}
    (artifacts / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({"result": evidence["result"], "evidence": str(artifacts / "evidence.json")}))


if __name__ == "__main__":
    try:
        audit()
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"goose proof audit failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
