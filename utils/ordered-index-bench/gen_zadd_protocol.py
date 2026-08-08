#!/usr/bin/env python3
"""Emit a RESP-encoded stream of ZADD commands for mass insertion via
`valkey-cli --pipe`. Writes to stdout.

Usage: gen_zadd_protocol.py <key> <count> [start]
Adds `count` members named mem_<i> with score <i>, for i in [start, start+count).
"""
import sys


def resp_cmd(*parts):
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        b = p.encode() if isinstance(p, str) else p
        out.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
    return b"".join(out)


def main():
    key = sys.argv[1]
    count = int(sys.argv[2])
    start = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    out = sys.stdout.buffer
    for i in range(start, start + count):
        member = f"mem_{i}"
        out.write(resp_cmd("ZADD", key, str(i), member))
    out.flush()


if __name__ == "__main__":
    main()
