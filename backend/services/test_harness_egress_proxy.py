"""Minimal HTTPS CONNECT proxy used by untrusted Harness setup containers.

The source container is attached only to a Docker ``--internal`` network.  A
separate copy of this manager-owned program is the sole member with outbound
network access.  Every CONNECT re-resolves the requested hostname and rejects
the whole answer set if any address is not globally routable.

This file intentionally uses only the Python standard library because it is
copied verbatim into the small Harness image and executed there.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
import ipaddress
import os
import re
import socket


_HOST_RE = re.compile(
    r"(?=.{1,253}\Z)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\Z"
)
_MAX_HEADER_BYTES = 64 * 1024


class EgressPolicyError(ValueError):
    pass


def normalize_allowed_hosts(raw: str) -> frozenset[str]:
    if not isinstance(raw, str) or len(raw) > 8192:
        raise EgressPolicyError("egress host allowlist is invalid")
    hosts: set[str] = set()
    for item in raw.split(","):
        host = item.strip().lower().rstrip(".")
        if not host:
            continue
        if _HOST_RE.fullmatch(host) is None:
            raise EgressPolicyError("egress host allowlist contains an invalid host")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise EgressPolicyError("egress host allowlist cannot contain IP literals")
        hosts.add(host)
    if not hosts or len(hosts) > 64:
        raise EgressPolicyError("egress host allowlist must contain 1 to 64 hosts")
    return frozenset(hosts)


def require_public_addresses(addresses: list[str]) -> tuple[str, ...]:
    if not addresses:
        raise EgressPolicyError("egress DNS returned no addresses")
    normalized: list[str] = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise EgressPolicyError("egress DNS returned an invalid address") from exc
        if (
            not address.is_global
            or address.is_multicast
            or address.is_reserved
            or address.is_unspecified
            or address.is_loopback
            or address.is_link_local
            or address.is_private
        ):
            raise EgressPolicyError("egress DNS returned a non-public address")
        normalized.append(address.compressed)
    return tuple(dict.fromkeys(normalized))


async def resolve_public_host(host: str, port: int) -> tuple[str, ...]:
    loop = asyncio.get_running_loop()
    try:
        records = await loop.getaddrinfo(
            host,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except socket.gaierror as exc:
        raise EgressPolicyError("egress DNS lookup failed") from exc
    return require_public_addresses([record[4][0] for record in records])


async def _copy_limited(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    max_bytes: int,
) -> None:
    total = 0
    while True:
        chunk = await reader.read(64 * 1024)
        if not chunk:
            return
        total += len(chunk)
        if total > max_bytes:
            raise EgressPolicyError("egress connection exceeded its byte limit")
        writer.write(chunk)
        await writer.drain()


async def _reject(writer: asyncio.StreamWriter, status: str) -> None:
    writer.write(
        f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".encode(
            "ascii"
        )
    )
    try:
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


class ConnectProxy:
    def __init__(
        self,
        *,
        allowed_hosts: frozenset[str],
        max_bytes: int,
        connection_timeout: float,
    ) -> None:
        self.allowed_hosts = allowed_hosts
        self.max_bytes = max_bytes
        self.connection_timeout = connection_timeout

    async def handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        upstream_writer: asyncio.StreamWriter | None = None
        pumps: set[asyncio.Task[None]] = set()
        try:
            header = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                timeout=10,
            )
            if len(header) > _MAX_HEADER_BYTES:
                raise EgressPolicyError("proxy request header is too large")
            lines = header.decode("ascii", errors="strict").split("\r\n")
            request = lines[0].split(" ")
            if len(request) != 3 or request[0] != "CONNECT":
                await _reject(writer, "405 Method Not Allowed")
                return
            authority = request[1]
            if authority.count(":") != 1:
                raise EgressPolicyError("CONNECT authority is invalid")
            host, raw_port = authority.rsplit(":", 1)
            host = host.lower().rstrip(".")
            if (
                _HOST_RE.fullmatch(host) is None
                or host not in self.allowed_hosts
                or raw_port != "443"
            ):
                await _reject(writer, "403 Forbidden")
                return
            addresses = await resolve_public_host(host, 443)
            upstream_reader: asyncio.StreamReader | None = None
            last_error: OSError | None = None
            for address in addresses:
                try:
                    upstream_reader, upstream_writer = await asyncio.wait_for(
                        asyncio.open_connection(address, 443),
                        timeout=10,
                    )
                    break
                except OSError as exc:
                    last_error = exc
            if upstream_reader is None or upstream_writer is None:
                raise EgressPolicyError(
                    "could not connect to the approved public host"
                ) from last_error
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            downstream = asyncio.create_task(
                _copy_limited(reader, upstream_writer, max_bytes=self.max_bytes)
            )
            upstream = asyncio.create_task(
                _copy_limited(upstream_reader, writer, max_bytes=self.max_bytes)
            )
            pumps = {downstream, upstream}
            done, pending = await asyncio.wait(
                pumps,
                timeout=self.connection_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()
        except (asyncio.IncompleteReadError, UnicodeError, EgressPolicyError):
            if not writer.is_closing():
                await _reject(writer, "403 Forbidden")
        except (asyncio.TimeoutError, OSError):
            if not writer.is_closing():
                await _reject(writer, "502 Bad Gateway")
        finally:
            for task in pumps:
                if not task.done():
                    task.cancel()
            if pumps:
                await asyncio.gather(*pumps, return_exceptions=True)
            if upstream_writer is not None and not upstream_writer.is_closing():
                upstream_writer.close()
                with suppress(Exception):
                    await upstream_writer.wait_closed()
            if not writer.is_closing():
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()


async def main() -> None:
    allowed = normalize_allowed_hosts(os.environ.get("CCM_ALLOWED_HOSTS", ""))
    try:
        max_bytes = int(os.environ.get("CCM_PROXY_MAX_BYTES", str(1024**3)))
        timeout = float(os.environ.get("CCM_PROXY_TIMEOUT_SECONDS", "1200"))
    except ValueError as exc:
        raise EgressPolicyError("egress proxy limits are invalid") from exc
    if not 1024 <= max_bytes <= 4 * 1024**3 or not 10 <= timeout <= 3600:
        raise EgressPolicyError("egress proxy limits are out of range")
    proxy = ConnectProxy(
        allowed_hosts=allowed,
        max_bytes=max_bytes,
        connection_timeout=timeout,
    )
    server = await asyncio.start_server(
        proxy.handle,
        host="0.0.0.0",
        port=3128,
        limit=_MAX_HEADER_BYTES,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
