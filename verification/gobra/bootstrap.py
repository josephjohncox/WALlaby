#!/usr/bin/env python3
"""Fetch and build the checksum-pinned macOS/arm64 Gobra pilot toolchain."""
from __future__ import annotations

import os
import platform
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

import verify


def checked(*command: str, cwd: Path | None = None, env: dict | None = None) -> None:
    subprocess.run(command, cwd=cwd, env=env, check=True, timeout=1200)


def fetch(destination: Path, metadata: dict) -> None:
    if destination.exists():
        verify.require_file(destination, metadata["sha256"])
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".download")
    try:
        checked("curl", "--fail", "--location", "--proto", "=https", "--proto-redir", "=https",
                "--max-time", "180", "--output", str(temporary), metadata["url"])
        verify.require_file(temporary, metadata["sha256"])
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    if (platform.system(), platform.machine()) != ("Darwin", "arm64"):
        verify.fail("this pilot pins macOS/arm64 Z3 only; unsupported platform, not a skip")
    java = verify.resolve_java()
    downloads = verify.CACHE / "dl"
    for name, metadata in verify.PINS["downloads"].items():
        fetch(downloads / name, metadata)
    source = verify.GOBRA_SRC
    if not source.exists():
        checked("git", "clone", "--no-checkout", verify.PINS["gobra"]["repository"], str(source))
        checked("git", "checkout", "--detach", verify.PINS["gobra"]["commit"], cwd=source)
    if verify.git_rev(source) != verify.PINS["gobra"]["commit"]:
        verify.fail("cached Gobra checkout revision differs; refusing to overwrite it")
    checked("git", "diff", "--exit-code", "HEAD", cwd=source)
    checked("git", "submodule", "update", "--init", "--recursive", cwd=source)
    toolchain = verify.CACHE / "toolchain"
    toolchain.mkdir(parents=True, exist_ok=True)
    sbt_name, _ = verify.pinned_download("sbt-", ".tgz")
    if not (toolchain / "sbt" / "bin" / "sbt").exists():
        with tarfile.open(downloads / sbt_name) as archive:
            for entry in archive.getmembers():
                destination = (toolchain / entry.name).resolve()
                if not destination.is_relative_to(toolchain.resolve()):
                    verify.fail("pinned sbt archive contains an escaping path")
                if entry.isdir():
                    destination.mkdir(parents=True, exist_ok=True)
                elif entry.isfile():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    contents = archive.extractfile(entry)
                    if contents is None:
                        verify.fail("pinned sbt archive has no regular file data")
                    with contents, destination.open("wb") as output:
                        shutil.copyfileobj(contents, output)
                    destination.chmod(entry.mode & 0o777)
                else:
                    verify.fail("pinned sbt archive contains an unsupported link or special file")
    z3_name, _ = verify.pinned_download("z3-", ".zip")
    z3 = verify.pinned_z3()
    if not z3.exists():
        directory = toolchain / "z3"
        directory.mkdir(exist_ok=True)
        with zipfile.ZipFile(downloads / z3_name) as archive:
            for entry in archive.infolist():
                destination = (directory / entry.filename).resolve()
                if not destination.is_relative_to(directory.resolve()):
                    verify.fail("pinned Z3 archive contains an escaping path")
                if entry.is_dir():
                    destination.mkdir(parents=True, exist_ok=True)
                else:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(entry) as contents, destination.open("wb") as output:
                        shutil.copyfileobj(contents, output)
        z3.chmod(0o755)
    antlr_name, metadata = verify.pinned_download("antlr-", ".jar")
    parser = source / ".genparser" / antlr_name
    parser.parent.mkdir(parents=True, exist_ok=True)
    if not parser.exists():
        parser.write_bytes((downloads / antlr_name).read_bytes())
    verify.require_file(parser, metadata["sha256"])
    if not verify.JAR.exists():
        env = dict(os.environ, JAVA_HOME=str(java.parent.parent))
        checked(str(toolchain / "sbt" / "bin" / "sbt"), "-batch", "assembly", cwd=source, env=env)
    # This is the recorded local assembly, not an invented upstream binary.
    # A different assembly fails rather than silently changing the trust pin.
    verify.require_tools()
    print("Pinned Gobra prerequisites ready; run mise run proof-gobra")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile, subprocess.SubprocessError) as error:
        verify.fail(f"bootstrap failed: {error}")
