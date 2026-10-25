"""Loopback S3 GET/HEAD fixture with SigV4 and temporary-credential validation."""

from contextlib import contextmanager
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import re
import threading
from urllib.parse import unquote, urlsplit


class S3Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(('127.0.0.1', 0), S3Handler)
        self.objects = {}
        self.requests = []
        self.access_key = 'iceberg-test-access'
        self.secret_key = 'iceberg-test-secret-never-log'
        self.session_token = 'iceberg-test-session-never-log'
        self.bucket = 'iceberg-read'
        self.short_read_key = None
        self.block_key = None
        self.blocked = threading.Event()
        self.release = threading.Event()

    def uri(self, key):
        return f's3://{self.bucket}/{key}'

    def authenticated(self, request):
        authorization = request.headers.get('Authorization', '')
        match = re.fullmatch(
            r'AWS4-HMAC-SHA256 Credential=([^/]+)/([^,]+),\s*SignedHeaders=([^,]+),\s*Signature=([0-9a-f]+)',
            authorization,
        )
        if not match or request.headers.get('x-amz-security-token') != self.session_token:
            return False
        access_key, scope, signed, signature = match.groups()
        if access_key != self.access_key:
            return False
        parts = scope.split('/')
        if len(parts) != 4 or parts[1:] != ['us-east-1', 's3', 'aws4_request']:
            return False
        parsed = urlsplit(request.path)
        # These tests use exact object paths, with no listing or presigned-URL query parameters.
        if parsed.query:
            return False
        headers = ''.join(f'{name}:{" ".join(request.headers.get(name, "").split())}\n' for name in signed.split(';'))
        payload_hash = request.headers.get('x-amz-content-sha256', hashlib.sha256(b'').hexdigest())
        canonical = '\n'.join((request.command, parsed.path, '', headers, signed, payload_hash))
        string_to_sign = '\n'.join(
            (
                'AWS4-HMAC-SHA256',
                request.headers.get('x-amz-date', ''),
                scope,
                hashlib.sha256(canonical.encode()).hexdigest(),
            )
        )
        signing_key = ('AWS4' + self.secret_key).encode()
        for part in parts:
            signing_key = hmac.digest(signing_key, part.encode(), 'sha256')
        return hmac.compare_digest(hmac.new(signing_key, string_to_sign.encode(), 'sha256').hexdigest(), signature)


class S3Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_args):
        pass

    def do_HEAD(self):
        self.serve_object(False)

    def do_GET(self):
        self.serve_object(True)

    def serve_object(self, body):
        prefix = '/' + self.server.bucket + '/'
        path = unquote(urlsplit(self.path).path)
        key = path[len(prefix) :] if path.startswith(prefix) else ''
        request = {'method': self.command, 'key': key, 'range': self.headers.get('Range'), 'bytes': 0}
        self.server.requests.append(request)
        if not self.server.authenticated(self):
            status, data = 403, b'AccessDenied'
        elif key not in self.server.objects:
            status, data = 404, b'NoSuchKey'
        else:
            status, data = 200, self.server.objects[key]
        request['status'] = status
        total_size = len(data)
        start, end = 0, total_size - 1
        if status == 200 and request['range']:
            match = re.fullmatch(r'bytes=(\d+)-(\d*)', request['range'])
            if not match:
                status, data = 416, b'InvalidRange'
            else:
                start = int(match[1])
                end = min(int(match[2]) if match[2] else total_size - 1, total_size - 1)
                if start > end:
                    status, data = 416, b'InvalidRange'
                else:
                    status, data = 206, data[start : end + 1]
            request['status'] = status
        if body and key == self.server.block_key and status in (200, 206):
            self.server.blocked.set()
            self.server.release.wait(timeout=10)
        self.send_response(status)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        if status in (200, 206):
            self.send_header('ETag', '"' + hashlib.md5(self.server.objects[key]).hexdigest() + '"')
        if status == 206:
            self.send_header('Content-Range', f'bytes {start}-{end}/{total_size}')
        self.end_headers()
        if body:
            if key == self.server.short_read_key and status in (200, 206):
                data = data[: len(data) // 2]
                self.close_connection = True
            try:
                self.wfile.write(data)
                self.wfile.flush()
                request['bytes'] = len(data)
            except (BrokenPipeError, ConnectionResetError):
                pass


@contextmanager
def s3_server():
    server = S3Server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.release.set()
        server.shutdown()
        server.server_close()
        thread.join()
