import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  FIRST_FRAME_MS, decodeParam, rememberDecodeFailure, resetDecodeProbe, watchDecoding,
} from '../src/player/decode-support';
import { fallbackStreamSource, playableStreamSource, streamFallbackChain, watchesDecoding } from '../src/player/stream';

function browserDecodes(codecs: string[], supported = true) {
  vi.spyOn(HTMLMediaElement.prototype, 'canPlayType')
    .mockImplementation((type: string) => (codecs.some(codec => type.includes(codec)) ? 'probably' : ''));
  vi.stubGlobal('navigator', Object.assign(Object.create(navigator), {
    mediaCapabilities: { decodingInfo: vi.fn(async () => ({ supported })) },
  }));
}

function plans(): string[] {
  const urls: string[] = [];
  vi.stubGlobal('fetch', vi.fn(async (url: string) => {
    urls.push(url);
    return new Response(JSON.stringify({ protocol: 'range', src: '/stream?id=7', mime_type: 'video/mp4' }),
      { status: 200, headers: { 'Content-Type': 'application/json' } });
  }));
  return urls;
}

beforeEach(() => resetDecodeProbe());
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers(); localStorage.clear() });

describe('解码能力探测', () => {
  it('按 canPlayType 与 mediaCapabilities 报 HEVC、10 位与 hev1', async () => {
    browserDecodes(['hvc1.1.6', 'hvc1.2.4', 'hev1']);
    expect(await decodeParam()).toBe('hevc,hevc10,hev1');
  });

  it('Safari 不认 hev1、没有 10 位时只报能解的', async () => {
    browserDecodes(['hvc1.1.6']);
    expect(await decodeParam()).toBe('hevc');
  });

  it('canPlayType 说能、mediaCapabilities 说不支持时不报', async () => {
    browserDecodes(['hvc1.1.6', 'hev1'], false);
    expect(await decodeParam()).toBe('');
  });

  it('设置为总是转码或总是直放时不看探测', async () => {
    browserDecodes([]);
    expect(await decodeParam('direct')).toBe('hevc,hevc10,hev1');
    browserDecodes(['hvc1.1.6']);
    expect(await decodeParam('transcode')).toBe('');
  });

  it('记过失败后 30 天内不报，过期后重新报', async () => {
    browserDecodes(['hvc1.1.6']);
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-11-15T00:00:00Z'));
    rememberDecodeFailure();
    expect(await decodeParam()).toBe('');
    expect(await decodeParam('direct')).toBe('hevc,hevc10,hev1');
    vi.setSystemTime(new Date('2026-12-16T00:00:00Z'));
    expect(await decodeParam()).toBe('hevc');
  });
});

describe('起播计划带上能力', () => {
  it('直链未出画或网络失败时按需取得原挂载的分片计划', async () => {
    browserDecodes([]);
    const urls: string[] = [];
    vi.stubGlobal('fetch', vi.fn(async (url: string) => {
      urls.push(url);
      const plan = url.includes('direct=0')
        ? { protocol: 'hls', src: '/stream/hls/7/index.m3u8', mime_type: 'application/vnd.apple.mpegurl' }
        : { protocol: 'range', src: '/stream?id=7&original=1', mime_type: 'video/mp4', reason: 'direct-playback' };
      return new Response(JSON.stringify(plan), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }));
    const item = { id: 7 };
    for (const code of [2, 3]) {
      const direct = await playableStreamSource(item, 's');
      expect(watchesDecoding(direct)).toBe(true);
      const next = streamFallbackChain(direct, (failed, error) => fallbackStreamSource(
        item, 's', failed.direct === true, Number((error as { code?: number }).code) === 2));
      expect(await next({ code }, direct)).toEqual({ src: '/stream/hls/7/index.m3u8', type: 'application/vnd.apple.mpegurl' });
    }
    expect(urls.filter(url => url.includes('&direct=0'))).toHaveLength(2);
  });

  it('回退计划请求失败时仍绕过直链', async () => {
    browserDecodes([]);
    vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('offline') }));
    expect(await fallbackStreamSource({ id: 7 }, 's', true, true)).toEqual({
      src: '/stream?id=7&session=s&direct=0', type: 'video/mp4',
    });
  });
  it('初始播放计划请求失败时绕过直链', async () => {
    browserDecodes([]);
    vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('offline') }));
    expect(await playableStreamSource({ id: 7 }, 's')).toEqual({
      src: '/stream?id=7&session=s&direct=0', type: 'video/mp4',
    });
  });
  it('直链格式错误不记录全局 HEVC 解码失败', async () => {
    browserDecodes(['hvc1.1.6']);
    plans();
    await fallbackStreamSource({ id: 7 }, 's', true, false);
    expect(await decodeParam()).toBe('hevc');
  });
  it('报了能解时计划带 decode，片源记着它；回退时记下失败、换不带能力的计划', async () => {
    browserDecodes(['hvc1.1.6']);
    const urls = plans();
    expect(await playableStreamSource({ id: 7 }, 's')).toEqual({ src: '/stream?id=7', type: 'video/mp4', decode: 'hevc' });
    expect(urls[0]).toBe('/api/stream-plan?id=7&session=s&decode=hevc');
    expect(await fallbackStreamSource({ id: 7 }, 's')).toEqual({ src: '/stream?id=7', type: 'video/mp4' });
    expect(urls[1]).toBe('/api/stream-plan?id=7&session=s');
    expect(await decodeParam()).toBe('');
  });
});

describe('首帧看守', () => {
  function video(frames: number, width = 1920) {
    const element = document.createElement('video') as HTMLVideoElement;
    let time = 0, error: MediaError | null = null;
    Object.defineProperties(element, {
      currentTime: { get: () => time, set: (value: number) => { time = value } },
      paused: { get: () => false },
      videoWidth: { get: () => width },
      error: { get: () => error },
    });
    element.getVideoPlaybackQuality = () => ({ totalVideoFrames: frames } as VideoPlaybackQuality);
    return { element, advance: (seconds: number) => { time += seconds }, fail: (code: number) => { error = { code } as MediaError } };
  }

  it('时间在走却 0 帧算解不出来，只回调一次', () => {
    vi.useFakeTimers();
    const { element, advance } = video(0);
    const failed = vi.fn();
    watchDecoding(element, failed, new AbortController().signal);
    element.dispatchEvent(new Event('playing'));
    advance(2); vi.advanceTimersByTime(FIRST_FRAME_MS);
    element.dispatchEvent(new Event('error'));
    expect(failed).toHaveBeenCalledOnce();
  });

  it('解出了帧就不再管；网络错误不算解码失败', () => {
    vi.useFakeTimers();
    const ok = video(30);
    const failed = vi.fn();
    watchDecoding(ok.element, failed, new AbortController().signal);
    ok.element.dispatchEvent(new Event('playing'));
    ok.advance(2); vi.advanceTimersByTime(FIRST_FRAME_MS);
    const network = video(0);
    watchDecoding(network.element, failed, new AbortController().signal);
    network.fail(2);
    network.element.dispatchEvent(new Event('error'));
    expect(failed).not.toHaveBeenCalled();
  });

  it('解码与格式错误立刻回调；拆掉播放器后不再回调', () => {
    const failed = vi.fn();
    const { element, fail } = video(0);
    const cleanup = new AbortController();
    watchDecoding(element, failed, cleanup.signal);
    fail(4);
    element.dispatchEvent(new Event('error'));
    expect(failed).toHaveBeenCalledOnce();
    const late = video(0);
    const gone = new AbortController();
    watchDecoding(late.element, failed, gone.signal);
    gone.abort();
    late.fail(3);
    late.element.dispatchEvent(new Event('error'));
    expect(failed).toHaveBeenCalledOnce();
  });
});
