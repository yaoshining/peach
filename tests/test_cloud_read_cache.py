import asyncio
from pathlib import Path
import os
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import httpx
from peach import cloud_read_cache as c
from peach.streaming import BufferedFileResponse, CancellableFileResponse, StreamSessionRegistry


class CacheTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source = self.root / 'source.mp4'
        self.data = bytes(range(256)) * (3 * c.BLOCK_SIZE // 256) + b'last-bytes'
        self.source.write_bytes(self.data)
        self.cache = c.BlockCache(self.root / 'cache', max_bytes=4*c.BLOCK_SIZE, min_free=0)

    def tearDown(self):
        self.tmp.cleanup()

    def reader(self):
        return c.CachedReader(self.source, self.source.stat(), self.cache)

    async def request(self, method='GET', headers=None, response=None):
        async def app(scope, receive, send):
            await (response or BufferedFileResponse(self.source))(scope, receive, send)
        with patch.object(c, 'cache_for', return_value=self.cache):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                return await client.request(method, '/', headers=headers)

    async def test_unaligned_reads_preserve_bytes(self):
        async with c.open_cached(self.source, self.cache) as f:
            await f.seek(c.BLOCK_SIZE-111)
            self.assertEqual(await f.read(c.BLOCK_SIZE+333), self.data[c.BLOCK_SIZE-111:2*c.BLOCK_SIZE+222])

    async def test_repeated_reads_do_not_open_origin(self):
        async with c.open_cached(self.source, self.cache) as f:
            self.assertEqual(await f.read(2*c.BLOCK_SIZE), self.data[:2*c.BLOCK_SIZE])
        async with c.open_cached(self.source, self.cache) as f:
            with patch.object(f, '_read_origin', side_effect=AssertionError('unexpected remote read')):
                self.assertEqual(await f.read(2*c.BLOCK_SIZE), self.data[:2*c.BLOCK_SIZE])
            self.assertIsNone(f.origin)

    async def test_tiny_probe_does_not_download_block(self):
        async with c.open_cached(self.source, self.cache) as f:
            with patch.object(f, '_read_origin', wraps=f._read_origin) as origin:
                self.assertEqual(await f.read(2), self.data[:2])
                origin.assert_awaited_once_with(0,2)
        self.assertFalse(list(self.cache.directory.glob('*.block')))

    async def test_cache_survives_new_service_instance(self):
        async with c.open_cached(self.source,self.cache) as f:await f.read(c.BLOCK_SIZE)
        restored=c.BlockCache(self.cache.directory,max_bytes=4*c.BLOCK_SIZE,min_free=0)
        async with c.open_cached(self.source,restored) as f:
            with patch.object(f,'_read_origin',side_effect=AssertionError('restart reread')):
                self.assertEqual(await f.read(c.BLOCK_SIZE),self.data[:c.BLOCK_SIZE])

    async def test_partial_cache_block_is_refetched(self):
        async with c.open_cached(self.source,self.cache) as f:await f.read(c.BLOCK_SIZE)
        next(self.cache.directory.glob('*.block')).write_bytes(b'partial')
        async with c.open_cached(self.source,self.cache) as f:
            self.assertEqual(await f.read(c.BLOCK_SIZE),self.data[:c.BLOCK_SIZE])

    async def test_unusable_cache_directory_falls_back_to_source(self):
        cache=c.BlockCache(self.source/'not-a-directory',max_bytes=4*c.BLOCK_SIZE,min_free=0)
        async with c.open_cached(self.source,cache) as f:
            self.assertEqual(await f.read(c.BLOCK_SIZE),self.data[:c.BLOCK_SIZE])

    async def test_source_revision_invalidates_cache(self):
        async with c.open_cached(self.source, self.cache) as f:
            old_key=f.key
            await f.read(c.BLOCK_SIZE)
        stamp=self.source.stat().st_mtime_ns
        with self.source.open('r+b') as f:f.write(b'CHANGED')
        os.utime(self.source,ns=(stamp+1000000000,stamp+1000000000))
        async with c.open_cached(self.source, self.cache) as f:
            self.assertNotEqual(f.key,old_key)
            self.assertEqual((await f.read(c.BLOCK_SIZE))[:7],b'CHANGED')

    async def test_capacity_lru_and_private_files(self):
        self.cache.max_bytes=c.BLOCK_SIZE
        async with c.open_cached(self.source,self.cache) as f:
            await f.read(2*c.BLOCK_SIZE)
        blocks=list(self.cache.directory.glob('*.block'))
        self.assertEqual(sum(p.stat().st_size for p in blocks),c.BLOCK_SIZE)
        self.assertEqual(len(blocks),1)
        self.assertEqual(blocks[0].stat().st_mode & 0o777,0o600)

    async def test_low_disk_space_keeps_playing_without_cache(self):
        with patch.object(c.shutil,'disk_usage',return_value=SimpleNamespace(free=0)):
            async with c.open_cached(self.source,self.cache) as f:
                self.assertEqual(await f.read(c.BLOCK_SIZE),self.data[:c.BLOCK_SIZE])
        self.assertFalse(list(self.cache.directory.glob('*.block')))

    async def test_concurrent_same_block_has_one_upstream_read(self):
        original=c.CachedReader._read_origin
        calls=[]
        async def slow(reader,start,size):
            calls.append((start,size));await asyncio.sleep(.02)
            return await original(reader,start,size)
        async def read():
            async with c.open_cached(self.source,self.cache) as f:return await f.read(c.BLOCK_SIZE)
        with patch.object(c.CachedReader,'_read_origin',slow):
            result=await asyncio.gather(read(),read(),read())
        self.assertEqual(len(calls),1)
        self.assertEqual(result,[self.data[:c.BLOCK_SIZE]]*3)

    async def test_head_does_not_open_source_or_fill_cache(self):
        with patch.object(c.CachedReader,'_read_origin',side_effect=AssertionError('HEAD read')):
            r=await self.request('HEAD',{'Range':'bytes=100-199'})
        self.assertEqual((r.status_code,len(r.content)),(206,0))
        self.assertEqual(r.headers['content-length'],'100')

    async def test_single_open_suffix_and_invalid_ranges(self):
        for header,start,end in [('bytes=1023-2048',1023,2049),('bytes=2000000-',2000000,len(self.data)),('bytes=-99',len(self.data)-99,len(self.data))]:
            r=await self.request(headers={'Range':header})
            self.assertEqual(r.status_code,206)
            self.assertEqual(r.content,self.data[start:end])
            self.assertEqual(r.headers['content-range'],f'bytes {start}-{end-1}/{len(self.data)}')
        r=await self.request(headers={'Range':f'bytes={len(self.data)+10}-'})
        self.assertEqual(r.status_code,416)

    async def test_multipart_range_and_if_range(self):
        r=await self.request(headers={'Range':'bytes=0-5,1048576-1048581'})
        self.assertEqual(r.status_code,206)
        self.assertIn('multipart/byteranges',r.headers['content-type'])
        self.assertEqual(int(r.headers['content-length']),len(r.content))
        self.assertIn(b'Content-Range: bytes 0-5/',r.content)
        self.assertIn(b'Content-Range: bytes 1048576-1048581/',r.content)
        r=await self.request(headers={'Range':'bytes=0-2','If-Range':'"different"'})
        self.assertEqual(r.status_code,200);self.assertEqual(r.content,self.data)

    async def test_cancelled_session_does_not_read_media(self):
        registry=StreamSessionRegistry();registry.cancel('cancelled')
        response=CancellableFileResponse(self.source,session='cancelled',registry=registry)
        with patch.object(c.CachedReader,'_read_origin',side_effect=AssertionError('cancelled read')):
            r=await self.request(response=response)
        self.assertEqual(r.status_code,410)

    async def test_local_files_and_disable_setting_bypass_cache(self):
        from peach import config, platform, settings_file
        mounts={'115':('/cloud/CloudDrive/JAV',),'pikpak':('/pikpak',)}
        with patch.object(platform, 'location_mount_values', return_value=mounts), \
                patch.object(config, 'GENERATED_DIR', self.root), \
                patch.object(settings_file, 'active', return_value=SimpleNamespace(locations={})):
            self.assertIsNone(c.cache_for(Path('/media/a.mp4')))
            self.assertIsNone(c.cache_for(Path('/pikpak/a.jpg')))
            with patch.dict(os.environ,{'PEACH_CLOUD_READ_CACHE_BYTES':'0'}):
                self.assertIsNone(c.cache_for(Path('/pikpak/a.mp4')))

    async def test_override_and_windows_direct_roots(self):
        from peach import config, settings_file
        settings = SimpleNamespace(mounts={'115': ('/old',)}, locations={'115': ('/declared',)})
        with patch.object(settings_file, 'active', return_value=settings), \
                patch.object(config, 'GENERATED_DIR', self.root), \
                patch.dict(os.environ, {'PEACH_MEDIA_MOUNTS': '115=/new'}):
            self.assertIsNotNone(c.cache_for(Path('/new/a.mp4')))
            self.assertIsNone(c.cache_for(Path('/old/a.mp4')))
        self.assertEqual(c._cloud_roots({}, settings.locations, True), [Path('/declared')])
        self.assertEqual(c._cloud_roots({}, settings.locations, False), [])

    async def test_cached_reads_do_not_wait_for_anyio_default_limiter(self):
        import anyio.to_thread
        limiter = anyio.to_thread.current_default_thread_limiter()
        previous = limiter.total_tokens
        limiter.total_tokens = 1
        await limiter.acquire()
        try:
            response = await asyncio.wait_for(self.request(
                headers={'Range': 'bytes=0-1048575'},
                response=BufferedFileResponse(self.source, stat_result=self.source.stat())), 5)
        finally:
            limiter.release()
            limiter.total_tokens = previous
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, self.data[:c.BLOCK_SIZE])

    async def test_cancelled_read_stops_without_filling_blocks(self):
        entered=asyncio.Event()
        async def block(*args):
            entered.set();await asyncio.Event().wait()
        async with c.open_cached(self.source,self.cache) as f:
            with patch.object(f,'_read_origin',block):
                task=asyncio.create_task(f.read(c.BLOCK_SIZE));await entered.wait();task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
        self.assertFalse(list(self.cache.directory.glob('*.block')))


if __name__=='__main__':unittest.main(verbosity=2)
