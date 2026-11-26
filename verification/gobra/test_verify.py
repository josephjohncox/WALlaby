"""Security tests for the Gobra proof runner's tool selection and subprocess use.

Run: mise exec -- python -m unittest discover -s verification/gobra -p 'test_*.py'
These tests use fake executables only; they do not run Gobra or touch .cache.
"""

from __future__ import annotations

import ast
import os
import stat
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest import mock

import verify

VERSION = verify.PINS["java"]["version"]
INSTALL = verify.PINS["java"]["install"]


def write_executable(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class ResolveJavaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve() / "java"
        self.root.mkdir()
        patch = mock.patch.object(verify, "mise_java_root", return_value=self.root)
        patch.start()
        self.addCleanup(patch.stop)

    def fake_java(self, install: str, version: str) -> Path:
        java = self.root / install / "bin" / "java"
        write_executable(java, f'echo "openjdk version {version}" >&2\n')
        return java

    def test_selects_only_pinned_install(self) -> None:
        java = self.fake_java(INSTALL, VERSION)
        self.assertEqual(verify.resolve_java(), java)

    def test_java_home_is_not_an_override(self) -> None:
        rogue = Path(self.tmp.name) / "rogue" / "bin" / "java"
        write_executable(rogue, f'echo "openjdk version {VERSION}" >&2\n')
        with mock.patch.dict(os.environ, {"JAVA_HOME": str(rogue.parent.parent)}), self.assertRaises(SystemExit):
            verify.resolve_java()

    def test_other_installs_are_never_scanned(self) -> None:
        self.fake_java("temurin-25.0.5+8.0.LTS-other", VERSION)
        self.fake_java("temurin-latest", VERSION)
        with self.assertRaises(SystemExit):
            verify.resolve_java()

    def test_wrong_reported_version_is_rejected(self) -> None:
        self.fake_java(INSTALL, "21.0.1+12")
        with self.assertRaises(SystemExit):
            verify.resolve_java()

    def test_symlinked_install_is_rejected(self) -> None:
        elsewhere = Path(self.tmp.name) / "elsewhere"
        write_executable(elsewhere / "bin" / "java", f'echo "openjdk version {VERSION}" >&2\n')
        (self.root / INSTALL).symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaises(SystemExit):
            verify.resolve_java()

    def test_java_binary_escaping_the_install_is_rejected(self) -> None:
        outside = Path(self.tmp.name) / "outside-java"
        write_executable(outside, f'echo "openjdk version {VERSION}" >&2\n')
        link = self.root / INSTALL / "bin" / "java"
        link.parent.mkdir(parents=True)
        link.symlink_to(outside)
        with self.assertRaises(SystemExit):
            verify.resolve_java()

    def test_install_name_must_be_one_pinned_component(self) -> None:
        for bad in ("../" + INSTALL, "sub/" + INSTALL, "temurin-21"):
            pins = {**verify.PINS, "java": {**verify.PINS["java"], "install": bad}}
            with self.subTest(install=bad), mock.patch.object(verify, "PINS", pins), self.assertRaises(SystemExit):
                verify.resolve_java()


class SubprocessBoundaryTest(unittest.TestCase):
    def test_run_gobra_command_and_environment(self) -> None:
        captured = {}

        def fake_run(command, **kwargs):
            captured.update(command=command, **kwargs)
            return subprocess.CompletedProcess(command, 0, "", "")

        java = Path("/pinned/bin/java")
        z3 = Path("/pinned/bin/z3")
        subject = Path("/subject/decide.gobra")
        rogue = {"JAVA_TOOL_OPTIONS": "-javaagent:/rogue.jar", "JAVA_HOME": "/rogue"}
        with mock.patch.dict(os.environ, rogue), mock.patch.object(subprocess, "run", fake_run):
            verify.run_gobra(subject, java, z3)
        command = captured["command"]
        self.assertEqual(command[0], str(java))
        self.assertEqual(command[command.index("-jar") + 1], str(verify.JAR))
        self.assertEqual(command[command.index("-i") + 1], str(subject))
        self.assertIn("--overflow", command)
        self.assertEqual(command[command.index("--z3Exe") + 1], str(z3))
        self.assertIs(captured["env"], verify.TOOL_ENV)
        self.assertNotIn("JAVA_TOOL_OPTIONS", captured["env"])
        self.assertNotIn("JAVA_HOME", captured["env"])
        self.assertFalse(captured.get("shell", False))

    def test_every_subprocess_run_pins_the_environment(self) -> None:
        tree = ast.parse(Path(verify.__file__).read_text())
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "run"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "subprocess"
        ]
        self.assertEqual(len(calls), 3)
        for call in calls:
            env = {keyword.arg: keyword.value for keyword in call.keywords}.get("env")
            self.assertEqual(ast.unparse(env) if env is not None else None, "TOOL_ENV")

    def test_runner_never_reads_caller_environment(self) -> None:
        source = Path(verify.__file__).read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in {"environ", "getenv"}:
                self.fail(f"verify.py reads the caller environment at line {node.lineno}")
        self.assertNotIn('"JAVA_HOME"', source)

    def test_git_revision_ignores_git_dir_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repos = []
            for name in ("pinned", "decoy"):
                path = Path(tmp) / name
                subprocess.run(["git", "init", "-q", str(path)], check=True)
                subprocess.run(
                    ["git", "-C", str(path), "-c", "user.name=t", "-c", "user.email=t@example.com",
                     "commit", "-q", "--allow-empty", "-m", name],
                    check=True,
                )
                repos.append(path)
            expected = subprocess.run(
                ["git", "-C", str(repos[0]), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
            ).stdout.strip()
            with mock.patch.dict(os.environ, {"GIT_DIR": str(repos[1] / ".git")}):
                self.assertEqual(verify.git_rev(repos[0]), expected)


class SolverAdmissionTest(unittest.TestCase):
    @staticmethod
    def pinned_revision(path: Path) -> str:
        relative = path.relative_to(verify.GOBRA_SRC).parts
        submodules = verify.PINS["submodules"]
        names = {(): verify.PINS["gobra"]["commit"], ("viperserver",): submodules["viperserver"]}
        names[("viperserver", "carbon")] = submodules["carbon"]
        names[("viperserver", "silicon")] = submodules["silicon"]
        names[("viperserver", "carbon", "silver")] = submodules["silver"]
        names[("viperserver", "silicon", "silver")] = submodules["silver"]
        return names[relative]

    def test_tampered_solver_never_reaches_subprocess(self) -> None:
        """Every other admission passes, so only the Z3 checksum can stop the run."""
        real_require_file = verify.require_file
        with tempfile.TemporaryDirectory() as tmp:
            z3 = Path(tmp) / "z3"
            write_executable(z3, "exit 0\n")
            executed: list[list[str]] = []

            def record_run(command, **kwargs):
                executed.append([str(part) for part in command])
                return subprocess.CompletedProcess(command, 0, VERSION + " " + verify.PINS["z3"]["version"], "")

            def require_file(path, expected=None):
                return real_require_file(path, expected) if path == z3 else "admitted"

            with (
                mock.patch.object(verify, "resolve_java", return_value=Path("/pinned/bin/java")),
                mock.patch.object(verify, "pinned_z3", return_value=z3),
                mock.patch.object(verify, "require_file", require_file),
                mock.patch.object(verify, "git_rev", self.pinned_revision),
                mock.patch.object(subprocess, "run", record_run),
                self.assertRaises(SystemExit),
            ):
                verify.require_tools()
            self.assertNotIn(str(z3), [command[0] for command in executed])

    def test_z3_hash_is_checked_before_version_execution(self) -> None:
        source = Path(verify.__file__).read_text()
        body = source[source.index("def require_tools") :]
        self.assertLess(body.index('PINS["z3"]["binary_sha256"]'), body.index('tool_version(z3,'))


class ProofContractTest(unittest.TestCase):
    """The security change must not weaken what the proof checks."""

    def test_java_install_matches_mise_pin(self) -> None:
        config = tomllib.loads((verify.ROOT / "mise.toml").read_text())
        self.assertEqual(verify.PINS["java"]["install"], config["tools"]["java"])

    def test_pins_keep_hashes_and_revisions(self) -> None:
        pins = verify.PINS
        self.assertEqual(pins["gobra"]["commit"], "d2c5af5450540c502855e53f4a5af1ba7630c41f")
        self.assertEqual(
            pins["executed_gobra_jar_sha256"],
            "d3d9b2427781033779584e6a6bf5e21deef14dc394ba51c62520ae1c3c90d241",
        )
        self.assertEqual(
            pins["z3"]["binary_sha256"],
            "edae32f9e37ea4b5bb35310d72f0e352d0dc07626cac4e9e30bc1ea9a5bc8efb",
        )
        self.assertEqual(len(pins["downloads"]), 3)

    def test_subject_contract_is_full_uint64(self) -> None:
        self.assertIn("func Decide(observation uint64, failed bool) uint64 {", verify.FROZEN_EXECUTABLE)
        self.assertEqual(verify.executable_lines(verify.SUBJECT.read_text()), verify.FROZEN_EXECUTABLE)
        spec = verify.spec_lines(verify.SUBJECT.read_text())
        translated = verify.translate(verify.FROZEN_EXECUTABLE, spec)
        self.assertIn("(res uint64)", translated)
        for forbidden in ("assume", "axiom", "admit", "requires"):
            self.assertIsNone(verify.FORBIDDEN_SPEC.search("\n".join(spec)), forbidden)

    def test_negative_controls_require_the_specific_postcondition(self) -> None:
        rejected = {
            "exit_code": 1,
            "combined": "Postcondition might not hold. Assertion res == 4 might not hold.",
        }
        verify.assert_rejected(rejected, "Assertion res == 4 might not hold.", "false")
        with self.assertRaises(SystemExit):
            verify.assert_rejected(rejected, "Assertion res == 5 might not hold.", "wrong needle")
        with self.assertRaises(SystemExit):
            verify.assert_rejected({"exit_code": 0, "combined": "found 0 errors"}, "x", "accepted")


if __name__ == "__main__":
    unittest.main()
