"""Standard-library threaded HTTP transport for the deterministic mock API."""

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import cast

from batch_ingestion.source_seed import SourceSeedError, parse_source_seed

from .app import DEFAULT_MAX_FAULT_KEYS, FaultStateStore, handle_request

CONTAINER_PORT = 8080


class MockApiHttpServer(ThreadingHTTPServer):
    """Threaded server carrying immutable generation config and synchronized fault state."""

    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], source_seed: int) -> None:
        self.source_seed = source_seed
        self.fault_state = FaultStateStore(DEFAULT_MAX_FAULT_KEYS)
        super().__init__(server_address, MockApiRequestHandler)


class MockApiRequestHandler(BaseHTTPRequestHandler):
    """Translate HTTP requests to the pure mock API contract."""

    protocol_version = "HTTP/1.1"
    server_version = "OmniRetailMockAPI/1"
    sys_version = ""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        server = cast(MockApiHttpServer, self.server)
        response = handle_request(
            method=method,
            target=self.path,
            source_seed=server.source_seed,
            fault_state=server.fault_state,
        )
        self.send_response(response.status)
        for name, value in response.headers:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(response.body)

    def log_message(self, format: str, *args: object) -> None:
        """Emit one compact structured transport log without request headers."""
        event = {
            "client": self.client_address[0],
            "event": "mock_api_request",
            "message": format % args,
        }
        print(json.dumps(event, separators=(",", ":"), sort_keys=True), file=sys.stderr)


def main() -> int:
    """Validate config, bind the local container endpoint, and serve until stopped."""
    try:
        source_seed = parse_source_seed(os.environ.get("SOURCE_SEED", ""))
    except SourceSeedError as error:
        print(f"Mock API configuration: FAIL: {error}", file=sys.stderr)
        return 2

    server = MockApiHttpServer(("0.0.0.0", CONTAINER_PORT), source_seed)
    try:
        print(
            json.dumps(
                {
                    "event": "mock_api_started",
                    "port": CONTAINER_PORT,
                    "source_seed": source_seed,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            flush=True,
        )
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print('{"event":"mock_api_stopping","reason":"keyboard_interrupt"}', flush=True)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
