#!/usr/bin/python3
"""Non-mutating Gobra proof runner for reconcilepolicy.Decide.

Fails closed when the pinned toolchain, the frozen executable subject, or a
negative control is missing or wrong. Does not skip a missing tool.
"""

from __future__ import annotations

import hashlib
import json
import os
import pwd
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import NoReturn

ROOT = Path(__file__).resolve().parents[2]
LANE = Path(__file__).resolve().parent
SUBJECT = ROOT / "internal" / "reconcilepolicy" / "decision.go"
try:
    PINS = json.loads((LANE / "pins.json").read_text())
except (OSError, ValueError) as error:
    print(f"gobra-verify: cannot load proof pins: {error}", file=sys.stderr)
    raise SystemExit(1) from error
OUT = LANE / "out"
CACHE = ROOT / ".cache"
GOBRA_SRC = CACHE / "gobra-src"
JAR = GOBRA_SRC / "target" / "scala-2.13" / "gobra.jar"


def pinned_download(prefix: str, suffix: str) -> tuple[str, dict]:
    matches = [
        (name, meta)
        for name, meta in PINS["downloads"].items()
        if name.startswith(prefix) and name.endswith(suffix)
    ]
    if len(matches) != 1:
        fail(f"expected exactly one download {prefix}*{suffix}, found {[name for name, _ in matches]}")
    return matches[0]


def pinned_z3() -> Path:
    archive, _ = pinned_download("z3-", ".zip")
    return CACHE / "toolchain" / "z3" / archive[: -len(".zip")] / "bin" / "z3"


# The proof must not inherit caller-controlled tool selection or configuration
# (JAVA_HOME, JAVA_TOOL_OPTIONS, GIT_DIR and similar). Every subprocess gets this
# fixed environment; every executable is a pinned absolute path, except the
# system git used only to read the pinned checkout revisions.
TOOL_ENV = {"PATH": "/usr/bin:/bin", "LC_ALL": "C"}


def tool_version(binary: Path, flag: str) -> tuple[int, str]:
    proc = subprocess.run(
        [str(binary), flag],
        check=False,
        text=True,
        capture_output=True,
        env=TOOL_ENV,
    )
    return proc.returncode, proc.stdout + proc.stderr


def mise_java_root() -> Path:
    """Return mise's Java install directory for the invoking account.

    The account database, not HOME or JAVA_HOME, supplies the base so no
    environment variable can redirect the pinned Java executable.
    """
    return Path(pwd.getpwuid(os.getuid()).pw_dir) / ".local" / "share" / "mise" / "installs" / "java"


def resolve_java() -> Path:
    """Return the one pinned mise Temurin install, or fail closed.

    pins.json names the exact install directory (the value of the java tool in
    mise.toml). The directory must be a real directory, java must resolve inside
    it, and its reported version must match. There is no override.
    """
    install_name = PINS["java"]["install"]
    version = PINS["java"]["version"]
    if Path(install_name).name != install_name or version not in install_name:
        fail(f"java.install must be one directory name containing {version}: {install_name!r}")
    install = mise_java_root() / install_name
    if install.is_symlink() or not install.is_dir():
        fail(f"pinned Java install {install_name} is not a real mise install directory; run mise install")
    candidate = install / "bin" / "java"
    if not candidate.is_file() or not candidate.resolve().is_relative_to(install.resolve()):
        fail(f"pinned Java install has no contained java binary: {candidate}")
    code, text = tool_version(candidate, "-version")
    if code != 0 or version not in text:
        fail(f"pinned Java is not {version}: {text.strip()}")
    return candidate


FROZEN_EXECUTABLE = """package reconcilepolicy
const (
\tWait   uint64 = 0
\tRetry  uint64 = 1
\tAdopt  uint64 = 2
\tReject uint64 = 3
)
func Decide(observation uint64, failed bool) uint64 {
\tif failed {
\t\treturn Wait
\t}
\tif observation == 1 {
\t\treturn Retry
\t}
\tif observation == 2 {
\t\treturn Adopt
\t}
\tif observation == 0 {
\t\treturn Wait
\t}
\treturn Reject
}
"""

REQUIRED_SPEC = [
    "ensures failed ==> res == Wait",
    "ensures !failed && observation == 0 ==> res == Wait",
    "ensures observation == 0 ==> res == Wait",
    "ensures !failed && observation == 1 ==> res == Retry",
    "ensures !failed && observation == 2 ==> res == Adopt",
    "ensures !failed && observation != 0 && observation != 1 && observation != 2 ==> res == Reject",
    "ensures res == Wait ==> (failed || observation == 0)",
    "ensures res == Retry ==> (!failed && observation == 1)",
    "ensures res == Adopt ==> (!failed && observation == 2)",
    "ensures res == Reject ==> (!failed && observation != 0 && observation != 1 && observation != 2)",
    "ensures res == Wait || res == Retry || res == Adopt || res == Reject",
    "ensures 0 <= res && res <= 3",
    "ensures Wait == 0 && Retry == 1 && Adopt == 2 && Reject == 3",
    "decreases",
]

FORBIDDEN_SPEC = re.compile(
    r"\b(assume|axiom|admit|sorry|trust|trusted|import|requires)\b",
    re.IGNORECASE,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def fail(message: str) -> NoReturn:
    print(f"gobra-verify: {message}", file=sys.stderr)
    raise SystemExit(1)


def executable_lines(source: str) -> str:
    kept = []
    for line in source.splitlines():
        stripped = line.strip()
        if stripped == "" or stripped.startswith("//"):
            continue
        if "//" in line:
            fail(f"inline comment would hide an executable token: {line!r}")
        kept.append(line)
    return "\n".join(kept) + "\n"


def spec_lines(source: str) -> list[str]:
    found = []
    for line_no, line in enumerate(source.splitlines(), 1):
        stripped = line.strip()
        if not stripped.startswith("// @"):
            continue
        payload = stripped[len("// @") :].strip()
        if FORBIDDEN_SPEC.search(payload):
            fail(f"forbidden Gobra clause at {SUBJECT}:{line_no}: {payload}")
        if payload not in REQUIRED_SPEC:
            fail(f"unexpected Gobra clause at {SUBJECT}:{line_no}: {payload}")
        found.append(payload)
    if found != REQUIRED_SPEC:
        fail("Gobra clauses are missing, reordered, or duplicated")
    return found


def translate(executable: str, spec: list[str]) -> str:
    signature = "func Decide(observation uint64, failed bool) uint64 {"
    named = "func Decide(observation uint64, failed bool) (res uint64) {"
    if executable.count(signature) != 1:
        fail("frozen signature is not unique; refusing to bind res")
    body = executable.replace(signature, named, 1)
    head, func, tail = body.partition(named)
    if func == "" or not tail.endswith("}\n"):
        fail("could not split the translated subject")
    # head is the package clause and constants. tail is the unchanged body.
    rendered = head + "\n".join(spec) + "\n" + func + tail
    if tail != executable.split(signature, 1)[1]:
        fail("translation changed the function body")
    return rendered


def git_rev(path: Path) -> str:
    if not path.exists():
        fail(f"missing git checkout: {path}")
    proc = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        check=False,
        text=True,
        capture_output=True,
        env=TOOL_ENV,
    )
    if proc.returncode != 0:
        fail(f"git rev-parse failed in {path}: {proc.stderr.strip()}")
    return proc.stdout.strip()


def require_file(path: Path, expected: str | None = None) -> str:
    if not path.is_file():
        fail(f"missing required file: {path}")
    digest = sha256(path)
    if expected is not None and digest != expected:
        fail(f"checksum mismatch for {path}: {digest} != {expected}")
    return digest


def require_tools() -> dict:
    try:
        java = resolve_java()
        z3 = pinned_z3()
        if not z3.is_file() or not os.access(z3, os.X_OK):
            fail(f"pinned Z3 is missing or not executable: {z3}")
        # Admit the solver by checksum before any execution of it.
        z3_hash = require_file(z3, PINS["z3"]["binary_sha256"])
        jar_hash = require_file(JAR, PINS["executed_gobra_jar_sha256"])
        sbt_name, sbt_meta = pinned_download("sbt-", ".tgz")
        z3_name, z3_meta = pinned_download("z3-", ".zip")
        antlr_name, antlr_meta = pinned_download("antlr-", ".jar")
        archive_hashes = {
            "sbt": require_file(CACHE / "dl" / sbt_name, sbt_meta["sha256"]),
            "z3_zip": require_file(CACHE / "dl" / z3_name, z3_meta["sha256"]),
            "antlr": require_file(GOBRA_SRC / ".genparser" / antlr_name, antlr_meta["sha256"]),
        }
        revs = {
            "gobra": git_rev(GOBRA_SRC),
            "viperserver": git_rev(GOBRA_SRC / "viperserver"),
            "carbon": git_rev(GOBRA_SRC / "viperserver" / "carbon"),
            "silicon": git_rev(GOBRA_SRC / "viperserver" / "silicon"),
            "silver_carbon": git_rev(GOBRA_SRC / "viperserver" / "carbon" / "silver"),
            "silver_silicon": git_rev(GOBRA_SRC / "viperserver" / "silicon" / "silver"),
        }
        expected = {
            "gobra": PINS["gobra"]["commit"],
            "viperserver": PINS["submodules"]["viperserver"],
            "carbon": PINS["submodules"]["carbon"],
            "silicon": PINS["submodules"]["silicon"],
            "silver_carbon": PINS["submodules"]["silver"],
            "silver_silicon": PINS["submodules"]["silver"],
        }
        for name, rev in revs.items():
            if rev != expected[name]:
                fail(f"{name} revision drift: {rev} != {expected[name]}")
        java_code, java_text = tool_version(java, "-version")
        if java_code != 0 or PINS["java"]["version"] not in java_text:
            fail(f"pinned Java version not reported by {java}: {java_text}")
        z3_code, z3_text = tool_version(z3, "--version")
        if z3_code != 0 or PINS["z3"]["version"] not in z3_text:
            fail(f"pinned Z3 version not reported: {z3_text}")
        return {
            "java": java_text.strip(),
            "java_executable": str(java),
            "z3": z3_text.strip(),
            "z3_executable": str(z3),
            "z3_binary_sha256": z3_hash,
            "jar_sha256": jar_hash,
            "archives": archive_hashes,
            "revisions": revs,
        }
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        fail(f"cannot validate pinned proof tools: {error}")


def run_gobra(subject: Path, java: Path, z3: Path) -> dict:
    command = [
        str(java),
        "-Xss128m",
        "-jar",
        str(JAR),
        "-i",
        str(subject),
        "--overflow",
        "--z3Exe",
        str(z3),
    ]
    if "--overflow" not in command or command[command.index("--z3Exe") + 1] != str(z3):
        fail("internal command builder dropped overflow checking or the pinned Z3")
    started = time.perf_counter()
    proc = subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=True,
        timeout=120,
        env=TOOL_ENV,
    )
    elapsed = time.perf_counter() - started
    combined = proc.stdout + proc.stderr
    return {
        "command": command,
        "exit_code": proc.returncode,
        "seconds": round(elapsed, 3),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "combined": combined,
    }


def assert_verified(result: dict) -> None:
    text = result["combined"]
    if result["exit_code"] != 0:
        fail(f"baseline Gobra exit {result['exit_code']}\n{text}")
    if "found 0 errors" not in text:
        fail(f"baseline did not report success\n{text}")
    if PINS["gobra"]["commit"] not in text:
        fail("baseline banner did not report the pinned Gobra commit")
    if "Postcondition might not hold" in text or "Exception" in text:
        fail(f"baseline contained a verification or tool error\n{text}")


def assert_rejected(result: dict, needle: str, label: str) -> None:
    text = result["combined"]
    if result["exit_code"] == 0 or "found 0 errors" in text:
        fail(f"{label} was accepted; negative control did not reject")
    if result["exit_code"] != 1:
        fail(f"{label} failed for a tool reason, exit {result['exit_code']}\n{text}")
    if "Postcondition might not hold" not in text or needle not in text:
        fail(f"{label} was not rejected for the intended postcondition\n{text}")
    tool_failure = "Exception" in text or "error while parsing" in text.lower()
    if "An unknown Exception" in text or "terminated abnormally" in text or "Error while generating" in text:
        fail(f"{label} hit a tool failure rather than a proof failure\n{text}")
    if tool_failure:
        fail(f"{label} tool failure\n{text}")


def write_case(name: str, text: str) -> Path:
    path = OUT / "generated" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def main() -> None:
    started = time.perf_counter()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "evidence.json").unlink(missing_ok=True)
    if not SUBJECT.is_file():
        fail(f"missing subject {SUBJECT}")
    source = SUBJECT.read_text()
    executable = executable_lines(source)
    if executable != FROZEN_EXECUTABLE:
        fail("executable subject drifted from the frozen Decide body and constants")
    spec = spec_lines(source)
    translated = translate(executable, spec)
    tools = require_tools()
    java = Path(tools["java_executable"])
    z3 = Path(tools["z3_executable"])
    baseline_path = write_case("decide.gobra", translated)
    false_text = translated.replace(
        "func Decide",
        "ensures res == 4\nfunc Decide",
        1,
    )
    false_path = write_case("decide-false-postcondition.gobra", false_text)
    mutant_text = translated.replace("\treturn Reject\n", "\treturn Wait\n", 1)
    if mutant_text == translated:
        fail("semantic mutant did not change the subject")
    mutant_path = write_case("decide-reject-mutant.gobra", mutant_text)

    baseline = run_gobra(baseline_path, java, z3)
    (OUT / "baseline.log").write_text(baseline["stdout"] + baseline["stderr"])
    assert_verified(baseline)
    false_post = run_gobra(false_path, java, z3)
    (OUT / "false-postcondition.log").write_text(false_post["stdout"] + false_post["stderr"])
    assert_rejected(false_post, "Assertion res == 4 might not hold.", "false postcondition")
    mutant = run_gobra(mutant_path, java, z3)
    (OUT / "mutant.log").write_text(mutant["stdout"] + mutant["stderr"])
    assert_rejected(
        mutant,
        "Assertion !failed && observation != 0 && observation != 1 && observation != 2 ==> res == Reject might not hold.",
        "semantic mutant",
    )

    evidence = {
        "result": "PROVED",
        "subject": "internal/reconcilepolicy/decision.go Decide",
        "member": "reconcilepolicy.Decide",
        "backend": "Gobra v26.09 / Silicon / Z3 4.16.0",
        "platform": "native macOS arm64",
        "overflow_checks": True,
        "source_sha256": sha256(SUBJECT),
        "executable_sha256": sha256_text(executable),
        "translation_sha256": sha256_text(translated),
        "clauses": REQUIRED_SPEC,
        "theorems": [
            "error precedence: failed ==> res == Wait",
            "indeterminate fail-closed: observation == 0 ==> res == Wait",
            "completeness and soundness of Wait, Retry, Adopt, and Reject",
            "result range 0..3 and wire constants 0,1,2,3",
            "termination via an empty decreases clause",
        ],
        "translation_boundary": "The production result is unnamed. The generated subject binds that single result to res and changes no other token.",
        "trusted_boundaries": [
            "Gobra parser, gobrafy-equivalent comment lifting performed by this runner, type checker, and Viper encoding",
            "Silicon symbolic execution and Z3 4.16.0",
            "Gobra uint64 is UnsignedInteger64, bounds [0, 2^64-1]; --overflow asserts integer subexpressions are inside those bounds",
            "No Go compiler, host Z3 5.4.1, pgx, or delivery coordinator is in this proof",
            "No subject axiom, assume, admit, or trusted contract",
        ],
        "tools": tools,
        "runs": {
            "baseline": {k: baseline[k] for k in ("command", "exit_code", "seconds")},
            "false_postcondition": {k: false_post[k] for k in ("command", "exit_code", "seconds")},
            "semantic_mutant": {k: mutant[k] for k in ("command", "exit_code", "seconds")},
        },
        "wall_seconds": round(time.perf_counter() - started, 3),
    }
    (OUT / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({"result": "PROVED", "evidence": str(OUT / "evidence.json"), "wall_seconds": evidence["wall_seconds"]}))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        fail(f"proof execution failed (not a rejected control): {error}")
