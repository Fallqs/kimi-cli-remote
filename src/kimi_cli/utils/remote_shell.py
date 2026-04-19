"""Persistent remote shell session for SSH execution.

A single ``bash -i`` process is kept alive on the remote host for the lifetime
of a CLI session.  Commands are sent via stdin and their outputs are read back
via stdout/stderr.  A sentinel protocol is used to detect when a command has
finished and to capture its exit code.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from asyncssh import SSHClientConnection, SSHClientProcess


class PersistentRemoteShell:
    """A persistent interactive bash session on a remote host."""

    def __init__(self, connection: SSHClientConnection) -> None:
        self._connection = connection
        self._process: SSHClientProcess[bytes] | None = None

    async def start(self) -> None:
        """Start the persistent bash process and drain initial banner output."""
        self._process = await self._connection.create_process(
            "bash -i",
            encoding=None,
        )
        # Allow bash to start up and then consume any initial prompt / banner.
        await asyncio.sleep(0.3)
        await self._drain_output(timeout=0.3)

    async def run(
        self,
        command: str,
        stdout_cb: Callable[[bytes], None] | None = None,
        stderr_cb: Callable[[bytes], None] | None = None,
        timeout: int | None = None,
    ) -> int:
        """Execute *command* in the persistent shell and return its exit code.

        Output is streamed to *stdout_cb* and *stderr_cb* as it arrives.
        A sentinel line ``__KIMI_DONE__<marker>__ <exitcode>`` is injected
        after the command so that we know exactly where its output ends.
        """
        if self._process is None:
            raise RuntimeError("PersistentRemoteShell has not been started.")

        marker = secrets.token_hex(8)
        sentinel = f"__KIMI_DONE__{marker}__"
        # Append the sentinel *after* the user command.  Using ``echo`` is
        # reliable because it is a shell builtin and always writes a single
        # line to stdout.
        full_cmd = f"{command}\necho '{sentinel}' $?\n"
        self._process.stdin.write(full_cmd.encode())
        await self._process.stdin.drain()

        exitcode = 0
        stdout_buffer = b""
        stderr_buffer = b""
        stdout_done = False

        async def _read_stdout() -> None:
            nonlocal stdout_buffer, stdout_done, exitcode
            while not stdout_done:
                data = await self._process.stdout.read(4096)
                if not data:
                    break
                stdout_buffer += data
                # Process complete lines; keep any partial line in the buffer.
                while b"\n" in stdout_buffer:
                    line, stdout_buffer = stdout_buffer.split(b"\n", 1)
                    line_str = line.decode("utf-8", errors="replace")
                    if sentinel in line_str:
                        # Extract exit code from the sentinel line.
                        parts = line_str.strip().split()
                        if len(parts) >= 2:
                            try:
                                exitcode = int(parts[-1])
                            except ValueError:
                                pass
                        stdout_done = True
                        break
                    if stdout_cb is not None:
                        stdout_cb(line + b"\n")

            # Flush any remaining buffered data that is not the sentinel.
            if stdout_buffer and not stdout_done:
                text = stdout_buffer.decode("utf-8", errors="replace")
                if sentinel in text:
                    parts = text.strip().split()
                    if len(parts) >= 2:
                        try:
                            exitcode = int(parts[-1])
                        except ValueError:
                            pass
                    stdout_done = True
                elif stdout_cb is not None:
                    stdout_cb(stdout_buffer)

        async def _read_stderr() -> None:
            nonlocal stderr_buffer
            while True:
                try:
                    data = await asyncio.wait_for(
                        self._process.stderr.read(4096), timeout=0.5
                    )
                    if not data:
                        break
                    stderr_buffer += data
                    if stderr_cb is not None:
                        stderr_cb(data)
                except asyncio.TimeoutError:
                    break

        stdout_task = asyncio.create_task(_read_stdout())
        stderr_task = asyncio.create_task(_read_stderr())

        if timeout is not None:
            try:
                await asyncio.wait_for(stdout_task, timeout=timeout)
            except asyncio.TimeoutError:
                stderr_task.cancel()
                try:
                    await stderr_task
                except asyncio.CancelledError:
                    pass
                try:
                    self._process.kill()
                except Exception:
                    pass
                raise
        else:
            await stdout_task

        stderr_task.cancel()
        try:
            await stderr_task
        except asyncio.CancelledError:
            pass

        return exitcode

    async def _drain_output(self, timeout: float = 0.5) -> None:
        """Read and discard any pending output (used during startup)."""
        if self._process is None:
            return
        while True:
            try:
                data = await asyncio.wait_for(
                    self._process.stdout.read(4096), timeout=timeout
                )
                if not data:
                    break
            except asyncio.TimeoutError:
                break

    async def close(self) -> None:
        """Terminate the persistent shell."""
        if self._process is None:
            return
        try:
            self._process.stdin.write_eof()
            await self._process.wait_closed()
        except Exception:
            pass
        finally:
            self._process = None
