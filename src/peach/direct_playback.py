"""可选的单资产直链映射，不修改账本里的媒体路径。"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from http.client import HTTPException
from pathlib import Path, PureWindowsPath
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

import httpx

from .http import public_https_url, resolves_publicly
from .strm import validate_url

FILENAME = 'direct-playback.json'
MAX_BYTES = 1024 * 1024
AUTO_TOKEN_ENV = 'PEACH_115_RESOLVER_TOKEN'
_CACHE_SECONDS = 60.0
_CACHE_LIMIT = 256
_cache: OrderedDict[tuple, tuple[float, str]] = OrderedDict()
_cache_lock = threading.Lock()


@dataclass(frozen=True)
class AutomaticSource:
    endpoint: str
    user_id: str
    relative_path: str
    token: str = field(repr=False)
    size: int
    identity: tuple


def _private_endpoint(endpoint: str) -> str | None:
    """限定自动查询服务为明确端口的可信内网地址。"""
    parsed = urlsplit(endpoint)
    host = ipaddress.ip_address(parsed.hostname or '')
    if (parsed.scheme not in {'http', 'https'} or not host.is_private
            or host.is_link_local or host.is_unspecified or host.is_multicast
            or parsed.username or parsed.password or parsed.fragment or parsed.query
            or parsed.port in (None, 0) or parsed.path not in {'', '/'}):
        return None
    return endpoint.rstrip('/')


def automatic_source(root: Path, asset) -> AutomaticSource | None:
    """只对明确配置的 115 账本根启用，路径计算不读取挂载。"""
    from .platform import is_windows_path, resolve_root
    try:
        with (root / FILENAME).open('rb') as handle:
            raw = handle.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES or asset.location != '115':
            return None
        cfg = json.loads(raw).get('automatic', {})
        if cfg.get('enabled') is not True:
            return None
        base = _private_endpoint(cfg['endpoint'])
        if base is None:
            return None
        token = os.environ.get(AUTO_TOKEN_ENV, '')
        user_id = str(cfg['user_id'])
        if len(token) < 32 or not user_id.isascii() or not user_id.isdecimal():
            return None
        ledger_root = cfg['ledger_root']
        if (not isinstance(ledger_root, str) or not is_windows_path(ledger_root)
                or '..' in PureWindowsPath(ledger_root).parts):
            return None
        location, _, parts = resolve_root(str(asset.path), {'115': (ledger_root,)})
        if (location != '115' or not parts or any(p in {'.', '..'} or '%' in p or '\\' in p for p in parts)
                or not isinstance(asset.size, int) or asset.size <= 0):
            return None
        relative = '/'.join(parts)
        identity = (str(asset.id), str(asset.path), asset.location, asset.size, ledger_root,
                    base, user_id, hashlib.sha256(token.encode()).digest())
        return AutomaticSource(base, user_id, relative, token, asset.size, identity)
    except (OSError, ValueError, TypeError, AttributeError, KeyError, httpx.InvalidURL):
        return None


def invalidate_automatic(asset) -> None:
    with _cache_lock:
        for key in list(_cache):
            if key[0] == str(asset.id):
                _cache.pop(key, None)


def automatic_url(source: AutomaticSource) -> str | None:
    """查询后只在内存短期缓存文件提取码；失败时返回原播放流程。"""
    now = time.monotonic()
    with _cache_lock:
        cached = _cache.get(source.identity)
        if cached and cached[0] > now:
            _cache.move_to_end(source.identity)
            pickcode = cached[1]
        else:
            _cache.pop(source.identity, None)
            pickcode = ''
    if not pickcode:
        try:
            with httpx.Client(trust_env=False, follow_redirects=False, timeout=3.0) as client:
                with client.stream('POST', source.endpoint + '/internal/peach/115/resolve',
                                   headers={'Authorization': 'Bearer ' + source.token},
                                   json={'path': source.relative_path, 'size': source.size}) as response:
                    if response.status_code != 200:
                        return None
                    data = bytearray()
                    for chunk in response.iter_bytes():
                        data.extend(chunk)
                        if len(data) > 8192:
                            return None
            payload = json.loads(data)
            detail = payload.get('data') or {}
            pickcode = detail.get('pick_code', '')
            if (payload.get('code') != 200 or not detail.get('file_id')
                    or detail.get('size') != source.size or not isinstance(pickcode, str)
                    or re.fullmatch(r'[A-Za-z0-9]{1,128}', pickcode) is None):
                return None
        except (httpx.HTTPError, httpx.InvalidURL, OSError, ValueError, TypeError, AttributeError):
            return None
        with _cache_lock:
            _cache[source.identity] = (time.monotonic() + _CACHE_SECONDS, pickcode)
            _cache.move_to_end(source.identity)
            while len(_cache) > _CACHE_LIMIT:
                _cache.popitem(last=False)
    return source.endpoint + '/115/newurl?' + urlencode({'pickcode': pickcode, 'userid': source.user_id, 'force': '1'})


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def resolve_automatic(url: str, user_agent: str) -> str | None:
    """不跟随重定向，也不让 HTTP 客户端把含提取码的请求地址写入日志。"""
    try:
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        request = Request(url, headers={'User-Agent': user_agent})
        try:
            with opener.open(request, timeout=3):
                return None
        except HTTPError as response:
            with response:
                if response.code != 302:
                    return None
                target = response.headers.get('location', '')
        validate_url(target)
        if not public_https_url(target) or not resolves_publicly(urlsplit(target).hostname or ''):
            return None
        return target
    except (URLError, OSError, ValueError, TypeError, HTTPException):
        return None


def mapped_url(root: Path, asset) -> str | None:
    """读取用户复核的资产 ID、原路径和 QMediaSync 地址。"""
    try:
        with (root / FILENAME).open('rb') as handle:
            raw = handle.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            return None
        entries = json.loads(raw).get('assets', {})
        row = entries.get(str(asset.id), {})
        if row.get('path') != str(asset.path) or row.get('location') != asset.location:
            return None
        url = row.get('url', '')
        parsed = urlsplit(url)
        decoded_path = unquote(parsed.path)
        actual_path = httpx.URL(url).path
        ip = ipaddress.ip_address(parsed.hostname or '')
        query = parse_qs(parsed.query)
        if (parsed.scheme not in {'http', 'https'} or not ip.is_private
                or parsed.username or parsed.password or parsed.fragment
                or parsed.port in (None, 0)
                or '%' in decoded_path or '\\' in decoded_path
                or any(part in {'.', '..'} for part in decoded_path.split('/'))
                or not (actual_path == '/115/newurl' or actual_path.startswith('/115/url/'))
                or not (parsed.path == '/115/newurl' or parsed.path.startswith('/115/url/'))
                or len(query.get('pickcode', [])) != 1 or len(query.get('userid', [])) != 1):
            return None
        return url
    except (OSError, ValueError, TypeError, AttributeError, httpx.InvalidURL):
        return None


def resolve(url: str, user_agent: str) -> str | None:
    """只接受 QMediaSync 的公网 HTTPS 跳转，不读取视频内容。"""
    try:
        with httpx.Client(trust_env=False, follow_redirects=False, timeout=8) as client:
            with client.stream('GET', url, headers={'User-Agent': user_agent}) as response:
                if response.status_code != 302:
                    return None
                target = response.headers.get('location', '')
        validate_url(target)
        if not public_https_url(target) or not resolves_publicly(urlsplit(target).hostname or ''):
            return None
        return target
    except (httpx.HTTPError, httpx.InvalidURL, ValueError):
        return None
