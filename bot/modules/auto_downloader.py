# FILE: bot/modules/auto_downloader.py
import os
import random
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

from pyrogram import Client, filters
from pyrogram.types import Message

from config import Config
from bot.tgclient import aio
from bot.settings import bot_set
from bot.logger import LOGGER
from bot.helpers.message import check_user, user_details
from bot.helpers.tidal.tidal_api import tidalapi
from bot.helpers.tidal.handler import start_tidal
from bot.helpers.qobuz.qopy import qobuz_api
from bot.helpers.qobuz.handler import start_qobuz

CHPROTECT_URL = os.getenv("CHPROTECT_URL", "https://chprotect.thihazaw.workers.dev").rstrip("/")
SYNC_SECRET = os.getenv("SYNC_SECRET", "")

# မနက် 8:00 AM (ရန်ကုန်စံတော်ချိန်) တွင် အလိုအလျောက် တစ်နေ့စာ ၁၂ ခု ဒေါင်းမည့်အချိန်
AUTO_DAILY_ENABLED = False
DAILY_BATCH_HOUR_YANGON = 8
DAILY_BATCH_COUNT = 12

_dl_lock = asyncio.Lock()


def get_yangon_now():
    return datetime.now(timezone.utc) + timedelta(hours=6, minutes=30)


async def fetch_next_task():
    if not SYNC_SECRET:
        LOGGER.error("AUTO-DL: SYNC_SECRET is not set in Variables")
        return None
    url = f"{CHPROTECT_URL}/api/next-dl-task"
    headers = {"x-sync-secret": SYNC_SECRET, "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json={}, timeout=20) as resp:
                if resp.status != 200:
                    LOGGER.error(f"AUTO-DL: Failed to fetch task HTTP {resp.status}")
                    return None
                return await resp.json()
    except Exception as e:
        LOGGER.error(f"AUTO-DL: Error fetching task: {e}")
        return None


async def mark_task_done(link_id=None, provider_album_id=None, title="", status="done"):
    if not SYNC_SECRET:
        return
    url = f"{CHPROTECT_URL}/api/mark-downloaded"
    headers = {"x-sync-secret": SYNC_SECRET, "Content-Type": "application/json"}
    payload = {
        "link_id": link_id,
        "provider_album_id": provider_album_id,
        "title": title,
        "status": status
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=20) as resp:
                return await resp.json()
    except Exception as e:
        LOGGER.error(f"AUTO-DL: Error marking done: {e}")


async def report_artist_exhausted(artist_id=None, artist_name=""):
    if not SYNC_SECRET:
        return
    url = f"{CHPROTECT_URL}/api/artist-exhausted"
    headers = {"x-sync-secret": SYNC_SECRET, "Content-Type": "application/json"}
    payload = {
        "artist_id": artist_id,
        "artist_name": artist_name
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=20) as resp:
                return await resp.json()
    except Exception as e:
        LOGGER.error(f"AUTO-DL: Error reporting exhausted artist: {e}")


async def find_random_album_for_artist(artist_name: str, downloaded_ids: set):
    candidates = []
    search_succeeded = False

    # 1. Search on Tidal first
    if bot_set.tidal and tidalapi.saved:
        try:
            res = await tidalapi.search(artist_name, types='ARTISTS,ALBUMS', limit=30)
            search_succeeded = True
            artists = res.get('artists', {}).get('items', [])
            matched_artist = next(
                (a for a in artists if a.get('name', '').strip().lower() == artist_name.strip().lower()),
                artists[0] if artists else None
            )

            album_items = []
            if matched_artist:
                alb_res = await tidalapi.get_artist_albums(matched_artist['id'])
                album_items = alb_res.get('items', [])
            else:
                album_items = res.get('albums', {}).get('items', [])

            for alb in album_items:
                alb_id = str(alb.get('id'))
                num_tracks = int(alb.get('numberOfTracks') or 0)
                key = f"tidal_{alb_id}"
                if num_tracks >= 2 and key not in downloaded_ids:
                    candidates.append({
                        "provider_album_id": key,
                        "url": f"https://tidal.com/browse/album/{alb_id}",
                        "title": alb.get('title', f"Tidal Album {alb_id}")
                    })
        except Exception as e:
            LOGGER.error(f"AUTO-DL Tidal search error ({artist_name}): {e}")

    # 2. Search on Qobuz if Tidal found nothing
    if not candidates and bot_set.qobuz:
        try:
            q_res = await qobuz_api.search_catalog(artist_name, search_type="albums", limit=40)
            search_succeeded = True
            items = q_res.get("albums", {}).get("items", [])
            for alb in items:
                alb_id = str(alb.get("id"))
                tracks_count = int(alb.get("tracks_count") or 0)
                alb_artist = alb.get("artist", {}).get("name", "").lower()
                key = f"qobuz_{alb_id}"
                if tracks_count >= 2 and key not in downloaded_ids:
                    if artist_name.lower() in alb_artist or not candidates:
                        candidates.append({
                            "provider_album_id": key,
                            "url": f"https://open.qobuz.com/album/{alb_id}",
                            "title": alb.get("title", f"Qobuz Album {alb_id}")
                        })
        except Exception as e:
            LOGGER.error(f"AUTO-DL Qobuz search error ({artist_name}): {e}")

    if not candidates:
        return None, search_succeeded
    return random.choice(candidates), search_succeeded


async def run_batch_auto_jobs(count: int = 1, notify_chat_id=None):
    count = max(1, min(15, int(count)))

    if _dl_lock.locked():
        if notify_chat_id:
            await aio.send_message(notify_chat_id, "⏳ လက်ရှိ Auto-Downloader အလုပ်လုပ်နေဆဲ ဖြစ်ပါသည် ခင်ဗျာ။")
        return

    async with _dl_lock:
        admin_id = notify_chat_id or next(iter(bot_set.admins), None)
        if not admin_id:
            LOGGER.error("AUTO-DL: No ADMIN_ID configured in bot_set.admins")
            return

        bot_set.upload_mode = 'Telegram'
        bot_set.album_zip = True
        bot_set.playlist_zip = True

        if count > 1:
            await aio.send_message(
                admin_id,
                f"🚀 <b>Batch Auto-Downloader စတင်ပါပြီ!</b>\n• ဒေါင်းမည့် အရေအတွက်: <b>{count} Albums</b>\n• ပြီးစီးသွားပါက Channel Guard Bot မှ အချိန်ဇယား (Schedule) နှင့် Short Link များ ထုတ်ပေးပါမည်။"
            )

        completed_count = 0
        attempt_limit = count * 3  # Exhausted artists ကျော်ဖြတ်နိုင်ရန်
        attempts = 0

        while completed_count < count and attempts < attempt_limit:
            attempts += 1
            task = await fetch_next_task()
            if not task or not task.get("ok"):
                await aio.send_message(admin_id, "⚠️ Channel Guard Bot ဆီမှ Task ရယူ၍ မရပါ (SYNC_SECRET စစ်ဆေးပါ)။")
                break

            task_type = task.get("task_type")
            downloaded_ids = set(task.get("downloaded_ids") or [])

            target_url = None
            link_id = None
            provider_album_id = None
            album_title = ""

            if task_type == "link":
                link_id = task.get("link_id")
                target_url = task.get("url")
                album_title = target_url
            elif task_type == "artist":
                artist_id = task.get("artist_id")
                artist_name = task.get("artist_name")
                picked, search_ok = await find_random_album_for_artist(artist_name, downloaded_ids)

                if not picked:
                    if search_ok:
                        # Artist ရဲ့ Album အားလုံး ကုန်သွားပြီဖြစ်ကြောင်း Channel Guard Bot သို့ အကြောင်းကြားပြီး စာရင်းမှ ဖယ်ရှားမည်
                        await report_artist_exhausted(artist_id=artist_id, artist_name=artist_name)
                    continue

                target_url = picked["url"]
                provider_album_id = picked["provider_album_id"]
                album_title = f"{picked['title']} ({artist_name})"
            else:
                await aio.send_message(admin_id, "ℹ️ Queue ထဲတွင် Pending Link နှင့် Artist မရှိတော့ပါ ခင်ဗျာ။")
                break

            status_msg = await aio.send_message(
                admin_id,
                f"🤖 <b>[{completed_count + 1}/{count}] Auto-Downloading...</b>\n• Target: <code>{album_title}</code>\n• Link: {target_url}",
                disable_web_page_preview=True
            )

            fake_user = user_details.copy()
            fake_user['user_id'] = admin_id
            fake_user['name'] = "AutoBot"
            fake_user['user_name'] = "AutoBot"
            fake_user['r_id'] = status_msg.id
            fake_user['chat_id'] = admin_id
            fake_user['bot_msg'] = status_msg

            try:
                if "tidal.com" in target_url:
                    await start_tidal(target_url, fake_user)
                elif "qobuz.com" in target_url:
                    await start_qobuz(target_url, fake_user)
                else:
                    raise Exception("Unsupported URL (Only Tidal & Qobuz supported)")

                await mark_task_done(
                    link_id=link_id,
                    provider_album_id=provider_album_id,
                    title=album_title,
                    status="done"
                )
                completed_count += 1
                LOGGER.info(f"AUTO-DL [{completed_count}/{count}]: Completed {album_title}")

                if completed_count < count:
                    await asyncio.sleep(3)
            except Exception as e:
                LOGGER.error(f"AUTO-DL Failed ({target_url}): {e}")
                await mark_task_done(link_id=link_id, status="failed")
                await aio.send_message(admin_id, f"❌ <b>Auto-DL Error:</b> {e}")

        if count > 1:
            await aio.send_message(
                admin_id,
                f"🎉 <b>Batch Auto-Download ပြီးစီးပါပြီ!</b>\n• အောင်မြင်စွာ ဒေါင်းပြီးစီးမှု: <b>{completed_count}/{count} Albums</b>\n• Channel Guard Bot Chat ထဲတွင် Schedule အချိန်ဇယားနှင့် Short Link များကို စစ်ဆေးနိုင်ပါပြီ ခင်ဗျာ။"
            )


async def hourly_auto_loop():
    await asyncio.sleep(15)
    LOGGER.info(f"AUTO-DL: Daily batch scheduler started (Yangon {DAILY_BATCH_HOUR_YANGON:02d}:00 -> {DAILY_BATCH_COUNT} albums/day)")
    last_run_date = None
    while True:
        try:
            ygn = get_yangon_now()
            today_str = ygn.strftime('%Y-%m-%d')
            if ygn.hour == DAILY_BATCH_HOUR_YANGON and ygn.minute == 0 and last_run_date != today_str:
                last_run_date = today_str
                await run_batch_auto_jobs(count=DAILY_BATCH_COUNT)
        except Exception as e:
            LOGGER.error(f"AUTO-DL Loop Error: {e}")
        await asyncio.sleep(25)


@Client.on_message(filters.command(["autonow"]))
async def cmd_auto_now(c, msg: Message):
    if not await check_user(msg=msg, restricted=True):
        return

    parts = (msg.text or "").strip().split()
    req_count = 1
    if len(parts) > 1:
        try:
            req_count = int(parts[1])
            if req_count < 1 or req_count > 15:
                await msg.reply_text("⚠️ အရေအတွက်ကို <b>1 မှ 15</b> အတွင်းသာ ထည့်သွင်းပါ ခင်ဗျာ။ (ဥပမာ - <code>/autonow 5</code>)")
                return
        except ValueError:
            await msg.reply_text("⚠️ ဥပမာ - <code>/autonow 5</code>")
            return

    await run_batch_auto_jobs(count=req_count, notify_chat_id=msg.chat.id)
