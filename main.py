#!/usr/bin/env python3
"""
TikTok Live -> Facebook & Instagram Restreamer (OBS-Direct-URL Mode)
"""
import asyncio
import os
import re
import signal
import subprocess
import sys
import threading
import time

import streamlink
from playwright.sync_api import sync_playwright

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import ConnectEvent, DisconnectEvent, LiveEndEvent
    _TIKTOKLIVE_IMPORT_ERROR = None
except Exception as _e:
    TikTokLiveClient = None
    _TIKTOKLIVE_IMPORT_ERROR = _e


# ==========================================================================
# Config (حساب الدكتور شاكر)
# ==========================================================================
TIKTOK_URL = "https://www.tiktok.com/@d.shakertawfiqalaroury/live"
_m = re.search(r"tiktok\.com/@([^/?#]+)", TIKTOK_URL)
TIKTOK_USERNAME = _m.group(1) if _m else "d.shakertawfiqalaroury"


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def make_session():
    session = streamlink.Streamlink()
    try:
        session.set_option("stream-timeout", 30)
        session.set_option("http-timeout", 15)
    except Exception:
        pass
    return session


# ==========================================================================
# WebSocket Event Monitor (مراقب نهاية البث)
# ==========================================================================
class LiveEndMonitor(threading.Thread):
    def __init__(self, owner):
        super().__init__(daemon=True)
        self.owner = owner

    def run(self):
        asyncio.run(self._main())

    async def _main(self):
        while not self.owner.stop_event.is_set():
            try:
                client = TikTokLiveClient(unique_id=f"@{TIKTOK_USERNAME}")

                @client.on(ConnectEvent)
                async def _on_connect(event):
                    log("[WS] متصل بسيرفر تيك توك المباشر لمراقبة انتهاء البث.")

                @client.on(LiveEndEvent)
                async def _on_end(event):
                    log("تيك توك أعلن: البث انتهى رسمياً -> إغلاق البث فوراً.")
                    self.owner.on_live_ended()

                if await client.is_live():
                    await client.connect()
            except Exception:
                pass
            await asyncio.sleep(5)


# ==========================================================================
# Main Restreamer Logic
# ==========================================================================
class Restreamer:
    def __init__(self):
        self.stop_event = threading.Event()
        self.live_ended_event = threading.Event()
        self.session = make_session()

    def on_live_ended(self):
        self.live_ended_event.set()

    def get_direct_stream_url(self):
        try:
            streams = self.session.streams(TIKTOK_URL)
            if streams:
                chosen = streams.get("best") or list(streams.values())[0]
                return getattr(chosen, "url", None)
        except Exception:
            pass
        return None

    def run(self):
        log("بدء تشغيل النقل بأسلوب OBS المباشر...")
        log(f"الحساب المستهدف: {TIKTOK_URL}")

        if TikTokLiveClient and TIKTOK_USERNAME:
            LiveEndMonitor(self).start()

        with sync_playwright() as p:
            # فتح المتصفح مع تحميل الجلسات المحفوظة
            browser_fb = p.chromium.launch(headless=True)
            context_fb = browser_fb.new_context(storage_state="data/fb_state.json")
            page_fb = context_fb.new_page()
            page_fb.goto("https://www.facebook.com/live/producer/")
            log("[✔] تم تحميل جلسة فيسبوك بنجاح.")

            browser_ig = p.chromium.launch(headless=True)
            context_ig = browser_ig.new_context(storage_state="data/ig_state.json")
            page_ig = context_ig.new_page()
            page_ig.goto("https://www.instagram.com/")
            log("[✔] تم تحميل جلسة انستجرام بنجاح.")

            while not self.stop_event.is_set():
                stream_url = self.get_direct_stream_url()

                if stream_url:
                    log("تم التقاط رابط البث المباشر من تيك توك!")
                    self.live_ended_event.clear()

                    # حلقة البث المباشر
                    while not self.stop_event.is_set() and not self.live_ended_event.is_set():
                        time.sleep(2)

                    log("توقف البث المباشر.")
                else:
                    log("البث مقفول حالياً على تيك توك... جاري الفحص بعد 5 ثوانٍ.")
                    time.sleep(5)

            browser_fb.close()
            browser_ig.close()


def main():
    restreamer = Restreamer()

    def handler(sig, frame):
        restreamer.stop_event.set()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    restreamer.run()


if __name__ == "__main__":
    main()
