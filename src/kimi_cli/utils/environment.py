from __future__ import annotations

import os
import platform
from dataclasses import dataclass
from typing import Literal

from kaos.path import KaosPath


@dataclass(slots=True, frozen=True, kw_only=True)
class Environment:
    os_kind: Literal["Windows", "Linux", "macOS"] | str
    os_arch: str
    os_version: str
    shell_name: Literal["bash", "sh", "Windows PowerShell"]
    shell_path: KaosPath

    @staticmethod
    async def detect() -> Environment:
        from kaos import get_current_kaos
        from kaos.ssh import SSHKaos

        if isinstance(get_current_kaos(), SSHKaos):
            return await Environment._detect_remote()

        match platform.system():
            case "Darwin":
                os_kind = "macOS"
            case "Windows":
                os_kind = "Windows"
            case "Linux":
                os_kind = "Linux"
            case system:
                os_kind = system

        os_arch = platform.machine()
        os_version = platform.version()

        if os_kind == "Windows":
            shell_name = "Windows PowerShell"
            system_root = os.environ.get("SYSTEMROOT", r"C:\Windows")
            possible_paths = [
                KaosPath(
                    os.path.join(
                        system_root, "System32", "WindowsPowerShell", "v1.0", "powershell.exe"
                    )
                ),
            ]
            fallback_path = KaosPath("powershell.exe")
            for path in possible_paths:
                if await path.is_file():
                    shell_path = path
                    break
            else:
                shell_path = fallback_path
        else:
            possible_paths = [
                KaosPath("/bin/bash"),
                KaosPath("/usr/bin/bash"),
                KaosPath("/usr/local/bin/bash"),
            ]
            fallback_path = KaosPath("/bin/sh")
            for path in possible_paths:
                if await path.is_file():
                    shell_name = "bash"
                    shell_path = path
                    break
            else:
                shell_name = "sh"
                shell_path = fallback_path

        return Environment(
            os_kind=os_kind,
            os_arch=os_arch,
            os_version=os_version,
            shell_name=shell_name,
            shell_path=shell_path,
        )

    @staticmethod
    async def _detect_remote() -> Environment:
        """Detect environment on the remote host via SSH."""
        import kaos

        async def _run_remote(*args: str) -> str:
            proc = await kaos.exec(*args)
            data = await proc.stdout.read()
            await proc.wait()
            return data.decode("utf-8", errors="replace").strip()

        os_kind_raw = await _run_remote("uname", "-s")
        match os_kind_raw:
            case "Darwin":
                os_kind = "macOS"
            case "Linux":
                os_kind = "Linux"
            case _:
                os_kind = os_kind_raw

        os_arch = await _run_remote("uname", "-m")
        os_version = await _run_remote("uname", "-r")

        possible_paths = [
            KaosPath("/bin/bash"),
            KaosPath("/usr/bin/bash"),
            KaosPath("/usr/local/bin/bash"),
        ]
        fallback_path = KaosPath("/bin/sh")
        for path in possible_paths:
            if await path.is_file():
                shell_name = "bash"
                shell_path = path
                break
        else:
            shell_name = "sh"
            shell_path = fallback_path

        return Environment(
            os_kind=os_kind,
            os_arch=os_arch,
            os_version=os_version,
            shell_name=shell_name,
            shell_path=shell_path,
        )
