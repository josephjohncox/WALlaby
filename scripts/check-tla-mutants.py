#!/usr/bin/env python3
"""Require real safety counterexamples for DDL reconciliation-model mutants.

This tests adequacy of the bounded abstract model, not Go implementation
refinement. The companion Go-overlay gate tests production-code sensitivity.
"""

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import tempfile


def patch_action(source: str, action: str, old: str, new: str) -> str:
    start = source.index(action + " ==")
    end = source.find("\n\n", start)
    if end == -1:
        raise RuntimeError(f"missing action end: {action}")
    body = source[start:end]
    if body.count(old) != 1:
        raise RuntimeError(f"mutation anchor drift in {action}: want exactly one")
    return source[:start] + body.replace(old, new, 1) + source[end:]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", required=True, type=pathlib.Path)
    parser.add_argument("--evidence", type=pathlib.Path, default=pathlib.Path(".cache/protocol/tla-mutations.json"))
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parent.parent
    jar = args.jar.resolve(strict=True)
    source = (root / "specs/DDLExecution.tla").read_text()
    config = (root / "specs/DDLExecution.cfg").read_text()
    mutations = [
        ("unknown-grants-execution", "ReconcileIndeterminate(d)",
         r"/\ mayApply' = mayApply \ {d}", r"/\ mayApply' = mayApply \cup {d}",
         "IndeterminateFailsClosed"),
        ("unknown-grants-receipt", "ReconcileIndeterminate(d)",
         r"/\ knownApplied' = knownApplied \ {d}", r"/\ knownApplied' = knownApplied \cup {d}",
         "IndeterminateFailsClosed"),
        ("receipt-without-observation", "RecordReceipt(d)",
         r"/\ d \in knownApplied", r"/\ TRUE",
         "ReceiptRequiresExternalCommit"),
    ]
    evidence = {"scope": "bounded abstract DDL model, not Go refinement",
                "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
                "jar_sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
                "mutants": []}
    with tempfile.TemporaryDirectory(prefix="wallaby-tla-mutants-") as temporary:
        directory = pathlib.Path(temporary)
        (directory / "DDLExecution.cfg").write_text(config)

        def check(text: str, name: str) -> subprocess.CompletedProcess:
            (directory / "DDLExecution.tla").write_text(text)
            return subprocess.run(
                ["java", "-XX:+UseParallelGC", "-cp", str(jar), "tlc2.TLC",
                 "-coverage", "1", "-metadir", str(directory / name),
                 "-config", "DDLExecution.cfg", "DDLExecution.tla"],
                cwd=directory, capture_output=True, text=True, timeout=60, check=False)

        baseline = check(source, "baseline")
        output = baseline.stdout + baseline.stderr
        if baseline.returncode != 0 or "No error has been found" not in output:
            raise RuntimeError("TLC baseline is not green:\n" + output)
        coverage = re.search(r"<ReconcileIndeterminate\b[^>]*>:\s*(\d+):(\d+)", output)
        if coverage is None or int(coverage.group(2)) == 0:
            raise RuntimeError("vacuous baseline: ReconcileIndeterminate never enabled:\n" + output)
        evidence["indeterminate_transitions"] = int(coverage.group(2))
        for name, action, old, new, invariant in mutations:
            result = check(patch_action(source, action, old, new), name)
            output = result.stdout + result.stderr
            # Syntax errors, crashes, timeouts and deadlocks do not count as a
            # killed safety mutant. TLC 1.7.4's safety-violation exit is 12.
            violation_message = " ".join(("Invariant", invariant, "is", "violated"))
            if result.returncode != 12 or violation_message not in output:
                raise RuntimeError(f"mutant {name} not killed by {invariant}; exit={result.returncode}\n{output}")
            evidence["mutants"].append({"name": name, "invariant": invariant, "exit_code": 12})
            (root / ".cache/protocol").mkdir(parents=True, exist_ok=True)
            (root / ".cache/protocol" / f"tla-{name}.txt").write_text(output)
            print(f"killed {name} with {invariant}")
    if (root / "specs/DDLExecution.tla").read_text() != source:
        raise RuntimeError("model source changed during the gate")
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.evidence.write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"TLC mutation gate: {len(mutations)}/{len(mutations)} safety mutants killed; nonzero indeterminate coverage")


if __name__ == "__main__":
    main()
