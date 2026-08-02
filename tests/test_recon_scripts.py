"""Structural tests for the Milestone 0 recon scripts.

Scope note: these check that the probes are well-formed and importable. They
deliberately do NOT assert anything about exchange message schemas, because no
live feed has been observed yet (see DECISIONS.md D-000). Schema tests arrive
in Milestone 1, driven by real captured fixtures.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys

import pytest

RECON_DIR = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "recon"
SCRIPTS = ["kraken", "coinbase", "binance_us"]


def _load(name: str):
    # The recon scripts import a sibling module (`_common`), so their directory
    # has to be importable.
    if str(RECON_DIR) not in sys.path:
        sys.path.insert(0, str(RECON_DIR))
    spec = importlib.util.spec_from_file_location(name, RECON_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("name", SCRIPTS)
def test_script_exposes_a_websocket_url(name: str) -> None:
    module = _load(name)
    assert module.URL.startswith("wss://"), f"{name} must use a secure websocket"


@pytest.mark.parametrize("name", SCRIPTS)
def test_subscriptions_are_json_serializable(name: str) -> None:
    """Subscribe frames are sent as JSON, so they must serialize cleanly."""
    module = _load(name)
    subscriptions = module.SUBSCRIPTIONS if hasattr(module, "SUBSCRIPTIONS") else []
    for sub in subscriptions:
        json.dumps(sub)


def test_binance_subscribes_via_url_not_frame() -> None:
    """Binance combined streams encode the subscription in the URL path.

    This asymmetry with Kraken/Coinbase is a real constraint on the Milestone 1
    connector interface, so it is pinned here.
    """
    module = _load("binance_us")
    assert "streams=" in module.URL
    assert not getattr(module, "SUBSCRIPTIONS", [])


def test_capture_helper_writes_into_docs_samples() -> None:
    common = _load("_common")
    assert common.SAMPLES_DIR.name == "samples"
    assert common.SAMPLES_DIR.parent.name == "docs"


# --- error-frame detection ---------------------------------------------------
# Guards the case where an unconfirmed subscribe payload is rejected: without
# this, a capture full of error frames would exit 0 and get committed as a
# fixture. The error shapes below are the documented conventions of each venue.


@pytest.mark.parametrize(
    "frame,expected_fragment",
    [
        ({"type": "error", "message": "Failed to subscribe"}, "Failed to subscribe"),
        ({"type": "error", "reason": "invalid channel"}, "invalid channel"),
        ({"method": "subscribe", "success": False, "error": "Bad request"}, "Bad request"),
        ({"error": {"code": -1121, "msg": "Invalid symbol."}}, "Invalid symbol."),
    ],
)
def test_error_frames_are_detected(frame: dict, expected_fragment: str) -> None:
    common = _load("_common")
    problem = common.looks_like_error(frame)
    assert problem is not None
    assert expected_fragment in problem


@pytest.mark.parametrize(
    "frame",
    [
        {"channel": "book", "type": "snapshot", "data": [{"bids": [], "asks": []}]},
        {"type": "l2update", "product_id": "BTC-USD", "changes": []},
        {"method": "subscribe", "success": True, "result": {"channel": "trade"}},
        {"stream": "btcusd@trade", "data": {"p": "60000.00", "q": "0.01"}},
        {"type": "heartbeat", "sequence": 12345},
        "not-a-dict",
        [1, 2, 3],
    ],
)
def test_normal_frames_are_not_flagged(frame) -> None:
    common = _load("_common")
    assert common.looks_like_error(frame) is None
