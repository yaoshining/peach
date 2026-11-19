"""仅缓存已请求的网盘视频数据块；不写源文件、不主动下载整片。"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from contextlib import asynccontextmanager
import hashlib
import logging
import os
from pathlib import Path
import shutil
import tempfile
import threading
import weakref

LOGGER = logging.getLogger(__name__)
BLOCK_SIZE = 1 << 20
MIN_FILL = 64 << 10


async def _stream_io(function, *args):
    from .streaming import _stream_thread
    return await _stream_thread(function, *args)


class BlockCache:
    def __init__(self, directory: Path, *, max_bytes: int, min_free: int = 1 << 30):
        self.directory = Path(directory)
        self.max_bytes = max(0, max_bytes)
        self.min_free = min_free
        self._guard = threading.RLock()
        self._entries: OrderedDict[Path, int] | None = None
        self._used = 0
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()
        self._warned = False

    def _index(self):
        if self._entries is not None:
            return
        self.directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        entries = OrderedDict()
        used = 0
        files = []
        for p in self.directory.glob('*.block'):
            try:
                st = p.stat()
                files.append((st.st_mtime_ns, p, st.st_size))
            except FileNotFoundError:
                pass
        for _, p, size in sorted(files):
            entries[p] = size
            used += size
        self._entries = entries
        self._used = used
        self._trim(0)

    def _trim(self, additional: int):
        while self._entries and self._used + additional > self.max_bytes:
            p, size = self._entries.popitem(last=False)
            try:
                p.unlink(missing_ok=True)
            except OSError:
                self._entries[p] = size
                break
            self._used -= size

    def _warning(self):
        if not self._warned:
            self._warned = True
            LOGGER.warning('Cloud read cache unavailable; continuing with the source file')

    def get(self, key: str, index: int, expected: int) -> bytes | None:
        p = self.directory / f'{key}-{index:x}.block'
        try:
            with self._guard:
                self._index()
                try:
                    data = p.read_bytes()
                except FileNotFoundError:
                    old = self._entries.pop(p, 0)
                    self._used -= old
                    return None
                if len(data) != expected:
                    self._used -= self._entries.pop(p, 0)
                    p.unlink(missing_ok=True)
                    return None
                if p not in self._entries:
                    self._entries[p] = len(data)
                    self._used += len(data)
                self._entries.move_to_end(p)
                # mtime 只用于缓存淘汰，不改变源文件。
                os.utime(p, None)
                return data
        except OSError:
            self._warning()
            return None

    def put(self, key: str, index: int, data: bytes):
        if not data or len(data) > self.max_bytes:
            return
        temporary = None
        try:
            with self._guard:
                self._index()
                if shutil.disk_usage(self.directory).free < self.min_free + len(data):
                    return
                p = self.directory / f'{key}-{index:x}.block'
                if p in self._entries:
                    return
                self._trim(len(data))
                if self._used + len(data) > self.max_bytes:
                    return
                fd, name = tempfile.mkstemp(prefix='.pending-', dir=self.directory)
                temporary = Path(name)
                with os.fdopen(fd, 'wb') as output:
                    output.write(data)
                os.replace(temporary, p)
                self._entries[p] = len(data)
                self._used += len(data)
        except OSError:
            self._warning()
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass

    def lock(self, name: str) -> asyncio.Lock:
        lock = self._locks.get(name)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[name] = lock
        return lock


class CachedReader:
    def __init__(self, source: Path, stat_result, cache: BlockCache):
        self.source = Path(source)
        self.size = stat_result.st_size
        identity = f'{self.source.absolute()}\0{self.size}\0{stat_result.st_mtime_ns}'
        self.key = hashlib.sha256(identity.encode()).hexdigest()
        self.cache = cache
        self.position = 0
        self.origin = None
        self.origin_position = 0

    async def seek(self, offset: int, whence: int = 0):
        position = offset if whence == 0 else self.position + offset if whence == 1 else self.size + offset
        if position < 0:
            raise ValueError('negative seek')
        self.position = position
        return position

    async def _read_origin(self, start: int, size: int):
        if self.origin is None:
            self.origin = await _stream_io(open, self.source, 'rb')
        if self.origin_position != start:
            await _stream_io(self.origin.seek, start)
        data = await _stream_io(self.origin.read, size)
        self.origin_position = start + len(data)
        return data

    async def read(self, size: int = -1) -> bytes:
        remaining = max(0, self.size - self.position)
        wanted = remaining if size < 0 else min(size, remaining)
        if not wanted:
            return b''
        pieces = []
        while wanted:
            index, offset = divmod(self.position, BLOCK_SIZE)
            base = index * BLOCK_SIZE
            expected = min(BLOCK_SIZE, self.size - base)
            take = min(wanted, expected - offset)
            block = await _stream_io(self.cache.get, self.key, index, expected)
            if block is None and take < MIN_FILL:
                # Safari 的 bytes=0-1 等探测不得触发 1 MiB 下载。
                data = await self._read_origin(self.position, take)
            else:
                if block is None:
                    async with self.cache.lock(f'{self.key}:{index}'):
                        block = await _stream_io(self.cache.get, self.key, index, expected)
                        if block is None:
                            block = await self._read_origin(base, expected)
                            if len(block) == expected:
                                await _stream_io(self.cache.put, self.key, index, block)
                data = block[offset:offset + take]
            pieces.append(data)
            self.position += len(data)
            wanted -= len(data)
            if len(data) < take:
                break
        return b''.join(pieces)

    async def aclose(self):
        if self.origin is not None:
            await asyncio.shield(_stream_io(self.origin.close))


_DEFAULT = None


def _cloud_roots(mounts, locations, windows):
    return [Path(root) for location in ('115', 'pikpak')
            for root in (*mounts.get(location, ()), *(locations.get(location, ()) if windows else ()))]


def cache_for(path: Path):
    global _DEFAULT
    from . import config, platform, settings_file
    if path.suffix.lower() not in {'.mp4', '.m4v', '.webm', '.mov', '.mkv', '.avi', '.ts'}:
        return None
    mounts = platform.location_mount_values()
    locations = settings_file.active().locations
    roots = _cloud_roots(mounts, locations, os.name == 'nt')
    if not any(platform.within_root(path, root) for root in roots):
        return None
    try:
        budget = int(os.environ.get('PEACH_CLOUD_READ_CACHE_BYTES', str(4 << 30)))
    except ValueError:
        return None
    if budget < BLOCK_SIZE:
        return None
    if _DEFAULT is None:
        _DEFAULT = BlockCache(config.GENERATED_DIR / 'cloud-read-cache', max_bytes=budget)
    return _DEFAULT


@asynccontextmanager
async def open_cached(path: Path, cache: BlockCache):
    st = await _stream_io(path.stat)
    reader = CachedReader(path, st, cache)
    try:
        yield reader
    finally:
        await reader.aclose()
