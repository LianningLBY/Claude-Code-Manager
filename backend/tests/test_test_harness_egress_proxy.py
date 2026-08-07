from __future__ import annotations

import pytest

from backend.services.test_harness_egress_proxy import (
    EgressPolicyError,
    normalize_allowed_hosts,
    require_public_addresses,
)


def test_egress_allowlist_is_exact_and_normalized():
    assert normalize_allowed_hosts(
        "GitHub.com, registry.npmjs.org.,github.com"
    ) == frozenset({"github.com", "registry.npmjs.org"})


@pytest.mark.parametrize(
    "value",
    [
        "",
        "*.github.com",
        "github.com/path",
        "127.0.0.1",
        "good.example,bad host",
    ],
)
def test_egress_allowlist_rejects_ambiguous_hosts(value):
    with pytest.raises(EgressPolicyError):
        normalize_allowed_hosts(value)


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "::1",
        "fc00::1",
        "fe80::1",
        "0.0.0.0",
        "224.0.0.1",
    ],
)
def test_egress_rejects_every_nonpublic_dns_answer(address):
    with pytest.raises(EgressPolicyError, match="non-public"):
        require_public_addresses(["8.8.8.8", address])


def test_egress_accepts_only_fully_public_answer_sets():
    assert require_public_addresses(["8.8.8.8", "1.1.1.1", "8.8.8.8"]) == (
        "8.8.8.8",
        "1.1.1.1",
    )
