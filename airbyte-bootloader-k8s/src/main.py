"""
Mock Kubernetes API Server for Airbyte Docker deployment.

Provides a minimal K8s-compatible API that satisfies the fabric8 Kubernetes client
used by Airbyte's bootloader, server, worker, and cron services.

Supports:
- Secrets CRUD (GET/POST/PUT/PATCH/DELETE + List + /raw)
- Pods GET (always returns Succeeded status)
- Auth reviews (always allowed)
"""

import http.server
import json
import logging
import re
import signal
import socketserver
import base64
import threading

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(name)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('mock-k8s')


class SecretStore:
    """Thread-safe in-memory store for K8s secrets."""

    def __init__(self):
        self._secrets: dict[str, dict] = {}
        self._resource_version = 0
        self._lock = threading.Lock()

    def get(self, name: str) -> dict | None:
        with self._lock:
            return self._secrets.get(name)

    def put(self, name: str, data: dict) -> dict:
        with self._lock:
            self._resource_version += 1
            if 'metadata' not in data:
                data['metadata'] = {}
            data['metadata']['name'] = name
            data['metadata'].setdefault('namespace', 'default')
            data['metadata']['resourceVersion'] = str(self._resource_version)
            data.setdefault('apiVersion', 'v1')
            data.setdefault('kind', 'Secret')
            self._secrets[name] = data
            return data

    def delete(self, name: str) -> bool:
        with self._lock:
            return self._secrets.pop(name, None) is not None

    def list_all(self) -> list[dict]:
        with self._lock:
            return list(self._secrets.values())

    def get_raw(self, name: str) -> dict:
        with self._lock:
            secret = self._secrets.get(name)
            if not secret:
                return {}
            raw = secret.get('data', {})
            return {k: base64.b64decode(v).decode('utf-8') for k, v in raw.items()}


# Global store instance
store = SecretStore()


class K8sHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler for mock Kubernetes API requests."""

    def do_GET(self):
        self._dispatch()

    def do_POST(self):
        self._dispatch()

    def do_PUT(self):
        self._dispatch()

    def do_PATCH(self):
        self._dispatch()

    def do_DELETE(self):
        self._dispatch()

    def log_message(self, fmt, *args):
        log.debug('%s %s', self.command, self.path)

    # --- Dispatch ---

    def _dispatch(self):
        path = self.path
        log.info('%s %s', self.command, path)

        if 'secrets' in path:
            self._handle_secrets(path)
        elif 'pods' in path:
            self._handle_pods(path)
        else:
            self._handle_fallback()

    # --- Secrets ---

    def _handle_secrets(self, path: str):
        method = self.command
        name = self._extract_name(path)
        body = self._read_body()

        # /raw — decoded secret values
        if '/raw' in path:
            raw = store.get_raw(name) if name else {}
            self._json_response(200, raw)
            return

        # Store secret from body (POST/PUT/PATCH)
        if method in ('POST', 'PUT', 'PATCH') and body:
            try:
                data = json.loads(body)
                secret_name = name or data.get('metadata', {}).get('name')
                if secret_name:
                    store.put(secret_name, data)
                    log.info('Stored secret: %s', secret_name)
            except json.JSONDecodeError as e:
                log.error('Error parsing secret body: %s', e)

        # GET existing secret
        if method == 'GET' and name:
            secret = store.get(name)
            if secret:
                self._json_response(200, secret)
            else:
                self._json_response(404, {
                    'kind': 'Status', 'apiVersion': 'v1', 'metadata': {},
                    'status': 'Failure',
                    'message': f'secrets "{name}" not found',
                    'reason': 'NotFound',
                    'details': {'name': name, 'kind': 'secrets'},
                    'code': 404,
                })
            return

        # POST/PUT/PATCH — return stored secret
        if method in ('POST', 'PUT', 'PATCH'):
            secret_name = name
            if not secret_name and body:
                try:
                    secret_name = json.loads(body).get('metadata', {}).get('name')
                except json.JSONDecodeError:
                    pass
            status_code = 201 if method == 'POST' else 200
            secret = store.get(secret_name) if secret_name else None
            self._json_response(status_code, secret or {
                'apiVersion': 'v1', 'kind': 'Secret',
                'metadata': {'name': secret_name or 'unknown', 'namespace': 'default', 'resourceVersion': '1'},
            })
            return

        # DELETE
        if method == 'DELETE' and name:
            store.delete(name)
            self._json_response(200, {'kind': 'Status', 'apiVersion': 'v1', 'status': 'Success', 'code': 200})
            return

        # List secrets (fallback for GET without name)
        self._json_response(200, {'apiVersion': 'v1', 'kind': 'SecretList', 'items': store.list_all()})

    # --- Pods ---

    def _handle_pods(self, path: str):
        name = self._extract_name(path) or 'mock-pod'
        is_watch = 'watch=true' in path
        is_list = '?' in path or path.endswith('/pods')

        pod = {
            'apiVersion': 'v1', 'kind': 'Pod',
            'metadata': {'name': name, 'namespace': 'default', 'resourceVersion': '2'},
            'status': {
                'phase': 'Succeeded',
                'containerStatuses': [{'name': 'main', 'ready': True, 'state': {'terminated': {'exitCode': 0}}}],
            },
        }

        if is_watch:
            self._json_response(200, {'type': 'ADDED', 'object': pod}, newline=True)
        elif is_list:
            self._json_response(200, {'apiVersion': 'v1', 'kind': 'PodList', 'items': [pod]})
        else:
            self._json_response(200, pod)

    # --- Fallback (auth reviews, etc.) ---

    def _handle_fallback(self):
        self._json_response(200, {'status': {'allowed': True}})

    # --- Helpers ---

    def _extract_name(self, path: str) -> str | None:
        match = re.search(r'/(?:pods|secrets)/([^/?]+)', path)
        return match.group(1) if match else None

    def _read_body(self) -> str:
        length = int(self.headers.get('Content-Length', 0))
        return self.rfile.read(length).decode('utf-8') if length > 0 else ''

    def _json_response(self, status: int, data: dict, newline: bool = False):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        payload = json.dumps(data)
        if newline:
            payload += '\n'
        self.wfile.write(payload.encode())


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Multi-threaded HTTP server for concurrent K8s API requests."""
    daemon_threads = True


def main():
    server = ThreadedHTTPServer(('', 8080), K8sHandler)
    log.info('Mock K8s API starting on port 8080')

    def shutdown_handler(signum, frame):
        log.info('Received signal %d, shutting down...', signum)
        threading.Thread(target=server.shutdown).start()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    try:
        server.serve_forever()
    finally:
        server.server_close()
        log.info('Mock K8s API stopped.')


if __name__ == '__main__':
    main()
