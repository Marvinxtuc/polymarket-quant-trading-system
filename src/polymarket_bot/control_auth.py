from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field
from typing import Iterable

IpNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network

SOURCE_POLICY_LOCAL_ONLY = "local_only"
SOURCE_POLICY_INTERNAL_ONLY = "internal_only"
SOURCE_POLICY_ANY = "any"
SOURCE_POLICIES = {
    SOURCE_POLICY_LOCAL_ONLY,
    SOURCE_POLICY_INTERNAL_ONLY,
    SOURCE_POLICY_ANY,
}

WRITE_API_ROUTE_WHITELIST: set[tuple[str, str]] = {
    ("POST", "/api/control"),
    ("POST", "/api/operator"),
    ("POST", "/api/candidate/action"),
    ("POST", "/api/mode"),
    ("POST", "/api/journal/note"),
    ("POST", "/api/wallet-profiles/update"),
}

READ_API_ROUTE_WHITELIST: set[tuple[str, str]] = {
    ("GET", "/api/state"),
    ("GET", "/metrics"),
    ("GET", "/api/control"),
    ("GET", "/api/monitor/30m"),
    ("GET", "/api/monitor/12h"),
    ("GET", "/api/reconciliation/eod"),
    ("GET", "/api/blockbeats"),
    ("GET", "/api/candidates"),
    ("GET", "/api/wallet-profiles"),
    ("GET", "/api/journal"),
    ("GET", "/api/stats"),
    ("GET", "/api/archive"),
    ("GET", "/api/export"),
    ("GET", "/api/mode"),
}

READ_API_DYNAMIC_PREFIXES: tuple[str, ...] = ("/api/candidates/",)

_WEAK_TOKENS = {
    "admin",
    "changeme",
    "default",
    "password",
    "poly",
    "poly_token",
    "token",
}


@dataclass(frozen=True)
class ControlPlaneSecurityStatus:
    live_mode: bool
    write_api_requested: bool
    write_api_available: bool
    readonly_mode: bool
    token_configured: bool
    source_policy: str
    trusted_proxy_configured: bool
    reason_codes: list[str] = field(default_factory=list)

    def as_state_payload(self) -> dict[str, object]:
        return {
            "write_api_requested": bool(self.write_api_requested),
            "write_api_available": bool(self.write_api_available),
            "write_api_enabled": bool(self.write_api_available),
            "readonly_mode": bool(self.readonly_mode),
            "live_mode": bool(self.live_mode),
            "token_configured": bool(self.token_configured),
            "source_policy": str(self.source_policy or SOURCE_POLICY_LOCAL_ONLY),
            "trusted_proxy_configured": bool(self.trusted_proxy_configured),
            "reason_codes": list(self.reason_codes or []),
        }


def normalize_source_policy(value: str) -> str:
    policy = str(value or "").strip().lower()
    if not policy:
        return SOURCE_POLICY_LOCAL_ONLY
    if policy in {"internal", "internal_network", "private"}:
        return SOURCE_POLICY_INTERNAL_ONLY
    if policy in {"all", "public", "any"}:
        return SOURCE_POLICY_ANY
    if policy in SOURCE_POLICIES:
        return policy
    raise ValueError(f"unsupported source policy: {value}")


def parse_trusted_proxy_networks(value: str) -> tuple[tuple[IpNetwork, ...], str]:
    raw = str(value or "").strip()
    if not raw:
        return ((), "")
    networks: list[IpNetwork] = []
    for chunk in raw.split(","):
        token = str(chunk or "").strip()
        if not token:
            continue
        try:
            if "/" in token:
                network = ipaddress.ip_network(token, strict=False)
            else:
                addr = ipaddress.ip_address(token)
                network = ipaddress.ip_network(f"{addr}/{32 if addr.version == 4 else 128}", strict=False)
        except ValueError:
            return ((), "trusted_proxy_config_invalid")
        networks.append(network)
    return (tuple(networks), "")


def is_api_read_route_allowed(method: str, path: str) -> bool:
    key = (str(method or "").upper(), str(path or ""))
    if key in READ_API_ROUTE_WHITELIST:
        return True
    if key[0] != "GET":
        return False
    for prefix in READ_API_DYNAMIC_PREFIXES:
        if key[1].startswith(prefix) and key[1] != prefix:
            return True
    return False


def is_api_write_route_allowed(method: str, path: str) -> bool:
    return (str(method or "").upper(), str(path or "")) in WRITE_API_ROUTE_WHITELIST


def validate_control_token(token: str, *, min_length: int) -> tuple[bool, str]:
    value = str(token or "").strip()
    if not value:
        return (False, "control_token_missing")
    if len(value) < max(8, int(min_length or 8)):
        return (False, "control_token_weak")
    if value.lower() in _WEAK_TOKENS:
        return (False, "control_token_weak")
    return (True, "")


def is_remote_addr_trusted_proxy(remote_addr: str, trusted_proxy_networks: Iterable[IpNetwork]) -> bool:
    ip_text = str(remote_addr or "").strip()
    if not ip_text:
        return False
    try:
        remote_ip = ipaddress.ip_address(ip_text)
    except ValueError:
        return False
    for network in trusted_proxy_networks:
        if remote_ip in network:
            return True
    return False


def resolve_effective_client_ip(
    *,
    remote_addr: str,
    forwarded_for: str,
    trusted_proxy_networks: Iterable[IpNetwork],
) -> tuple[str, bool]:
    remote_ip = str(remote_addr or "").strip()
    if not remote_ip:
        return ("", False)
    if not is_remote_addr_trusted_proxy(remote_ip, trusted_proxy_networks):
        return (remote_ip, False)
    xff = str(forwarded_for or "").strip()
    if not xff:
        return (remote_ip, False)
    first = str(xff.split(",")[0] or "").strip()
    if not first:
        return (remote_ip, False)
    try:
        ipaddress.ip_address(first)
    except ValueError:
        return (remote_ip, False)
    return (first, True)


def is_write_source_allowed(client_ip: str, *, source_policy: str) -> bool:
    ip_text = str(client_ip or "").strip()
    if not ip_text:
        return False
    try:
        ip = ipaddress.ip_address(ip_text)
    except ValueError:
        return False
    normalized_policy = normalize_source_policy(source_policy)
    if normalized_policy == SOURCE_POLICY_ANY:
        return True
    if normalized_policy == SOURCE_POLICY_INTERNAL_ONLY:
        return bool(ip.is_loopback or ip.is_private)
    return bool(ip.is_loopback)
